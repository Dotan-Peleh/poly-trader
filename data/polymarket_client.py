"""
Polymarket client — Gamma API for discovery, CLOB for books + orders.

Target product: 5-min BTC up/down events.
  Title pattern: "Bitcoin Up or Down - <Month> <Day>, <H:MM>AM/PM-<H:MM>AM/PM ET"
  Each event resolves on whether BTC at the END timestamp is higher than
  at the START timestamp (using a published Coinbase index).

Read endpoints (free, no auth):
  GET https://gamma-api.polymarket.com/events?closed=false&order=startDate&ascending=false
  GET https://clob.polymarket.com/book?token_id=...

Write endpoints (live only — Phase 5, requires HMAC creds):
  POST https://clob.polymarket.com/orders
  Order signing handled by py-clob-client when settings.trading_mode == 'live'.
"""
import logging
import re
from datetime import datetime, timedelta, timezone
from typing import Optional

import httpx

from config.settings import settings
from data.storage import upsert_market, save_book_snapshot

logger = logging.getLogger(__name__)


GAMMA_BASE = "https://gamma-api.polymarket.com"
CLOB_BASE = "https://clob.polymarket.com"

# Match: "Bitcoin Up or Down - May 7, 3:55PM-4:00PM ET"
# Capture: month_day, start_time, end_time
TITLE_RE = re.compile(
    r"Bitcoin Up or Down - "
    r"(?P<month>\w+) (?P<day>\d+), "
    r"(?P<start_h>\d+)(?::(?P<start_m>\d+))?(?P<start_ap>AM|PM)"
    r"-"
    r"(?P<end_h>\d+)(?::(?P<end_m>\d+))?(?P<end_ap>AM|PM)\s*ET",
    re.IGNORECASE,
)


def _gamma_client() -> httpx.Client:
    return httpx.Client(base_url=GAMMA_BASE,
                          timeout=httpx.Timeout(15.0, read=30.0),
                          headers={"User-Agent": "poly-trader/0.2"})


def _clob_client() -> httpx.Client:
    return httpx.Client(base_url=CLOB_BASE,
                          timeout=httpx.Timeout(10.0, read=20.0),
                          headers={"User-Agent": "poly-trader/0.2"})


def list_btc_events(window_minutes: int = 5, limit: int = 500) -> list[dict]:
    """Pull active BTC up/down events of a given window length.

    window_minutes: 5, 15, 60, 240, etc. We filter by computing the
    duration from the event title's start/end times.

    Bug history: previous query used `order=startDate&ascending=false` which
    returns events with the LATEST startDate first — i.e. tomorrow's
    pre-created markets — so the bot would see 42 active markets but every
    one of them resolved 20+ hours in the future, far outside its 4-min
    decision window. Switched to endDate ascending + end_date_min=NOW so
    we get markets ending soonest, which is what the late-window strategy
    actually trades.
    """
    out: list[dict] = []
    try:
        from datetime import datetime as _dt
        end_min = _dt.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
        with _gamma_client() as c:
            r = c.get(
                f"/events?closed=false&order=endDate&ascending=true"
                f"&end_date_min={end_min}&limit={limit}"
            )
            r.raise_for_status()
            events = r.json()
            if not isinstance(events, list):
                return []
            for e in events:
                title = e.get("title") or ""
                m = TITLE_RE.search(title)
                if not m:
                    continue
                try:
                    duration = _parse_duration_min(m)
                except Exception:
                    continue
                if duration != window_minutes:
                    continue
                out.append(e)
    except Exception as ex:
        logger.warning(f"list_btc_events failed: {ex}")
    return out


def _parse_duration_min(m) -> int:
    """Compute window duration in minutes from regex match groups."""
    def _to_min(h, mn, ap):
        h = int(h)
        mn = int(mn or 0)
        if ap.upper() == "PM" and h != 12:
            h += 12
        if ap.upper() == "AM" and h == 12:
            h = 0
        return h * 60 + mn
    start = _to_min(m.group("start_h"), m.group("start_m"), m.group("start_ap"))
    end = _to_min(m.group("end_h"), m.group("end_m"), m.group("end_ap"))
    if end < start:
        end += 24 * 60   # crosses midnight
    return end - start


def event_resolution_ts(event: dict) -> Optional[datetime]:
    """Pull the resolution timestamp from the event's endDate."""
    try:
        s = (event.get("endDate") or "").replace("Z", "+00:00")
        dt = datetime.fromisoformat(s)
        return dt.astimezone(timezone.utc).replace(tzinfo=None)
    except Exception:
        return None


def event_yes_no_token_ids(event: dict) -> tuple[Optional[str], Optional[str]]:
    """Extract YES + NO CLOB token IDs from an event's markets."""
    markets = event.get("markets") or []
    if not markets:
        return None, None
    m0 = markets[0]
    # Polymarket events sometimes embed clobTokenIds as JSON string
    raw = m0.get("clobTokenIds") or m0.get("clob_token_ids")
    if isinstance(raw, str):
        try:
            import json as _json
            raw = _json.loads(raw)
        except Exception:
            raw = None
    if isinstance(raw, list) and len(raw) >= 2:
        return str(raw[0]), str(raw[1])
    # Fallback to outcome list
    outcomes = m0.get("outcomes") or []
    if len(outcomes) >= 2:
        return str(outcomes[0]), str(outcomes[1])
    return None, None


def fetch_book(token_id: str) -> Optional[dict]:
    """Fetch CLOB order book for a single token. Returns:
      { 'bids': [{'price': '0.55', 'size': '100'}, ...], 'asks': [...] }
    """
    if not token_id:
        return None
    try:
        with _clob_client() as c:
            r = c.get(f"/book?token_id={token_id}")
            r.raise_for_status()
            return r.json()
    except Exception as e:
        logger.debug(f"fetch_book({token_id[:20]}...) failed: {e}")
        return None


def book_top(book: Optional[dict]) -> tuple[Optional[float], Optional[float], float]:
    """(best_bid, best_ask, depth_usd_near_market) from a CLOB book.

    BUGFIX 2026-05-09: Polymarket's /book endpoint returns bids/asks UNSORTED.
    The old implementation took bids[0]/asks[0] directly — which was the
    WORST order, not the best. On a real 0.50/0.51 market that stored
    0.01/0.99 (placeholder edges) and the tail filter rejected every
    candidate. Caused weeks of false rejections. Now we sort first.

    Depth metric also tightened: only orders within ±5% of best price.
    The old metric summed all top-5 orders incl. placeholders, making
    every market look $50k deep when real tradable depth was $200-500.
    """
    if not book:
        return None, None, 0.0
    bids = book.get("bids") or []
    asks = book.get("asks") or []

    def _row(r):
        if isinstance(r, dict):
            return float(r.get("price", 0)), float(r.get("size", 0))
        return float(r[0]), float(r[1]) if len(r) > 1 else 0.0

    # Sort: bids descending (best = highest), asks ascending (best = lowest)
    bids_sorted = sorted(bids, key=lambda r: -_row(r)[0])
    asks_sorted = sorted(asks, key=lambda r: _row(r)[0])

    best_bid = _row(bids_sorted[0])[0] if bids_sorted else None
    best_ask = _row(asks_sorted[0])[0] if asks_sorted else None

    # Near-market depth: orders within 5% of best price on each side
    depth = 0.0
    if best_bid is not None:
        for r in bids_sorted[:10]:
            p, s = _row(r)
            if p < best_bid * 0.95:
                break
            depth += p * s
    if best_ask is not None:
        for r in asks_sorted[:10]:
            p, s = _row(r)
            if p > best_ask * 1.05:
                break
            depth += p * s
    return best_bid, best_ask, round(depth, 2)


def snapshot_market(condition_id: str, yes_token: str, no_token: str,
                     btc_price_now: Optional[float] = None) -> Optional[dict]:
    """Pull both YES + NO order books, persist a snapshot row."""
    yes_book = fetch_book(yes_token)
    no_book = fetch_book(no_token)
    yes_bid, yes_ask, yes_depth = book_top(yes_book)
    no_bid, no_ask, no_depth = book_top(no_book)

    save_book_snapshot(
        condition_id=condition_id,
        yes_bid=yes_bid, yes_ask=yes_ask,
        no_bid=no_bid, no_ask=no_ask,
        book_depth_usd=yes_depth + no_depth,
        btc_price_at_snap=btc_price_now,
    )
    yes_mid = (yes_bid + yes_ask) / 2 if (yes_bid and yes_ask) else None
    return {
        "condition_id": condition_id,
        "yes_bid": yes_bid, "yes_ask": yes_ask, "yes_mid": yes_mid,
        "yes_depth_usd": yes_depth, "no_depth_usd": no_depth,
    }


def refresh_markets(window_minutes: int = 5) -> int:
    """Pull active BTC up/down events of given window, upsert into DB."""
    events = list_btc_events(window_minutes=window_minutes)
    saved = 0
    now = datetime.utcnow()
    for e in events:
        rt = event_resolution_ts(e)
        if rt is None or rt < now:
            continue
        yes_tok, no_tok = event_yes_no_token_ids(e)
        if not (yes_tok and no_tok):
            continue
        markets = e.get("markets") or []
        cid = (markets[0].get("conditionId") if markets else None) or e.get("id")
        if not cid:
            continue
        upsert_market(
            condition_id=str(cid),
            question=e.get("title", ""),
            resolution_ts=rt,
            yes_token_id=yes_tok,
            no_token_id=no_tok,
        )
        saved += 1
    return saved
