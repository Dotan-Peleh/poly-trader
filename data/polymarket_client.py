"""
Polymarket CLOB client — read-only for Phase 1.

Polymarket's CLOB API surfaces:
  - GET /markets                — list of markets (paginated)
  - GET /markets/{condition_id} — market detail
  - GET /book?token_id=...      — order book for a YES or NO token
  - GET /price?token_id=...     — last trade price

Hourly BTC binaries follow the pattern:
  question = "Bitcoin Up or Down on YYYY-MM-DD HH:MM ET?"
  resolution_ts ≈ next hour boundary in NY time

We filter to BTC up/down hourly markets only and store the active set
in the polymarket_markets table.

Phase 5 will add order placement (requires Polygon EOA signing). Phase 1-4
is read-only.
"""
import logging
import re
from datetime import datetime, timezone
from typing import Optional

import httpx

from config.settings import settings
from data.storage import upsert_market, save_book_snapshot

logger = logging.getLogger(__name__)


# Match: "Bitcoin Up or Down on Tuesday May 6, 9 PM ET?"  or  "...3 AM ET?"
BTC_HOURLY_PATTERN = re.compile(
    r"\bbitcoin\b.*\b(up|down|above|below)\b.*\bET\b",
    re.IGNORECASE,
)


def _client() -> httpx.Client:
    return httpx.Client(
        base_url=settings.polymarket_clob_base,
        timeout=httpx.Timeout(10.0, read=20.0),
        headers={"User-Agent": "poly-trader/0.1"},
    )


def list_active_markets() -> list[dict]:
    """Pull active markets, filter to BTC hourly binaries.

    Returns a list of dicts with: condition_id, question, end_date_iso,
    yes_token_id, no_token_id."""
    out: list[dict] = []
    try:
        with _client() as c:
            # Polymarket pages results; pull first 500 (more than enough for
            # active markets in any single moment).
            resp = c.get("/markets?active=true&closed=false&limit=500")
            resp.raise_for_status()
            data = resp.json()
            markets = data.get("data") if isinstance(data, dict) else data
            if not isinstance(markets, list):
                logger.warning(f"unexpected markets payload type: {type(markets)}")
                return []
            for m in markets:
                question = m.get("question") or m.get("title") or ""
                if not BTC_HOURLY_PATTERN.search(question):
                    continue
                # Pull token ids — varies by API version
                tokens = m.get("tokens") or []
                yes_token = no_token = None
                for t in tokens:
                    outcome = (t.get("outcome") or "").upper()
                    tok = t.get("token_id") or t.get("id")
                    if outcome in ("YES", "UP", "ABOVE"):
                        yes_token = tok
                    elif outcome in ("NO", "DOWN", "BELOW"):
                        no_token = tok
                end_iso = (
                    m.get("end_date_iso")
                    or m.get("endDate")
                    or m.get("end_date")
                )
                cid = m.get("condition_id") or m.get("conditionId") or m.get("id")
                if not (cid and end_iso):
                    continue
                out.append({
                    "condition_id": cid,
                    "question": question,
                    "end_date_iso": end_iso,
                    "yes_token_id": yes_token,
                    "no_token_id": no_token,
                })
    except Exception as e:
        logger.warning(f"list_active_markets failed: {e}")
    return out


def fetch_book(token_id: str) -> Optional[dict]:
    """Order book for a single YES or NO token.

    Returns: { 'bids': [[price, size], ...], 'asks': [[price, size], ...] }
    or None if unavailable."""
    if not token_id:
        return None
    try:
        with _client() as c:
            resp = c.get(f"/book?token_id={token_id}")
            resp.raise_for_status()
            return resp.json()
    except Exception as e:
        logger.debug(f"fetch_book({token_id}) failed: {e}")
        return None


def book_top(book: Optional[dict]) -> tuple[Optional[float], Optional[float], float]:
    """Return (best_bid, best_ask, top5_depth_usd) from a CLOB book."""
    if not book:
        return None, None, 0.0
    bids = book.get("bids") or []
    asks = book.get("asks") or []

    def _row(r):
        if isinstance(r, dict):
            return float(r.get("price")), float(r.get("size", 0))
        return float(r[0]), float(r[1]) if len(r) > 1 else 0.0

    bb = _row(bids[0])[0] if bids else None
    ba = _row(asks[0])[0] if asks else None

    depth = 0.0
    for r in bids[:5] + asks[:5]:
        p, s = _row(r)
        depth += p * s
    return bb, ba, round(depth, 2)


def snapshot_market(condition_id: str, yes_token: str, no_token: str,
                     btc_price_now: Optional[float] = None) -> Optional[dict]:
    """Pull both YES and NO order books, save a snapshot row, return summary."""
    yes_book = fetch_book(yes_token)
    no_book = fetch_book(no_token)
    yes_bid, yes_ask, yes_depth = book_top(yes_book)
    no_bid, no_ask, no_depth = book_top(no_book)
    total_depth = yes_depth + no_depth

    save_book_snapshot(
        condition_id=condition_id,
        yes_bid=yes_bid, yes_ask=yes_ask,
        no_bid=no_bid, no_ask=no_ask,
        book_depth_usd=total_depth,
        btc_price_at_snap=btc_price_now,
    )

    yes_mid = (yes_bid + yes_ask) / 2 if (yes_bid and yes_ask) else None
    return {
        "condition_id": condition_id,
        "yes_bid": yes_bid, "yes_ask": yes_ask, "yes_mid": yes_mid,
        "no_bid": no_bid, "no_ask": no_ask,
        "book_depth_usd": total_depth,
    }


def parse_resolution_ts(end_date_iso: str) -> Optional[datetime]:
    """Convert Polymarket's end_date_iso into a UTC datetime."""
    try:
        # Common formats: '2026-05-06T22:00:00Z', '2026-05-06T22:00:00.000Z'
        s = end_date_iso.rstrip("Z")
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is not None:
            dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
        return dt
    except Exception:
        return None


def refresh_markets() -> int:
    """Pull active BTC hourly markets, upsert to DB. Returns count saved."""
    markets = list_active_markets()
    saved = 0
    for m in markets:
        rt = parse_resolution_ts(m["end_date_iso"])
        if rt is None:
            continue
        upsert_market(
            condition_id=m["condition_id"],
            question=m["question"],
            resolution_ts=rt,
            yes_token_id=m["yes_token_id"],
            no_token_id=m["no_token_id"],
        )
        saved += 1
    return saved
