"""
Kalshi CLOB client — read-only for Phase 1.

Target product: series KXBTC15M  ("BTC Up or Down - 15 minutes")
Each event is one 15-minute window; each event has ONE market with
ticker pattern KXBTC15M-<YYMMM>-<HHMM>-00 that asks "BTC price up
in next 15 mins?" YES means BTC closes higher than its price at the
hour mark; NO means lower or equal.

Public endpoints (no auth needed for market data):
  GET /trade-api/v2/events?series_ticker=KXBTC15M
  GET /trade-api/v2/markets?event_ticker=KXBTC15M-...
  GET /trade-api/v2/markets/{ticker}/orderbook

Phase 5 (live trading) will add HMAC-signed POST /portfolio/orders using
an API key + private key from Kalshi account settings, stored in GCP
Secret Manager.
"""
import logging
from datetime import datetime, timezone
from typing import Optional

import httpx

from config.settings import settings
from data.storage import upsert_market, save_book_snapshot

logger = logging.getLogger(__name__)


KALSHI_BASE = "https://api.elections.kalshi.com/trade-api/v2"
SERIES_TICKER = "KXBTC15M"


def _client() -> httpx.Client:
    return httpx.Client(
        base_url=KALSHI_BASE,
        timeout=httpx.Timeout(10.0, read=20.0),
        headers={"User-Agent": "poly-trader/0.1"},
    )


def list_active_btc15m_events(limit: int = 20) -> list[dict]:
    """Pull recent KXBTC15M events. Returns most-recent first.

    Each event resolves at the named time (HH:MM UTC encoded in the
    event_ticker, e.g. KXBTC15M-26MAY062345 = May 6 23:45 UTC)."""
    try:
        with _client() as c:
            r = c.get(f"/events?series_ticker={SERIES_TICKER}&limit={limit}")
            r.raise_for_status()
            return r.json().get("events", []) or []
    except Exception as e:
        logger.warning(f"list_active_btc15m_events failed: {e}")
        return []


def parse_event_resolution_ts(event_ticker: str) -> Optional[datetime]:
    """Parse 'KXBTC15M-26MAY062345' → datetime(2026,5,6,23,45) UTC.

    Format: KXBTC15M-{YY}{MMM}{DD}{HH}{MM}
    """
    try:
        suffix = event_ticker.split("-", 1)[1]   # '26MAY062345'
        if len(suffix) < 11:
            return None
        yy = int(suffix[0:2])
        mon_str = suffix[2:5].upper()
        dd = int(suffix[5:7])
        hh = int(suffix[7:9])
        mm = int(suffix[9:11])
        months = {
            "JAN": 1, "FEB": 2, "MAR": 3, "APR": 4, "MAY": 5, "JUN": 6,
            "JUL": 7, "AUG": 8, "SEP": 9, "OCT": 10, "NOV": 11, "DEC": 12,
        }
        mon = months.get(mon_str)
        if mon is None:
            return None
        # 2-digit year: 20YY
        return datetime(2000 + yy, mon, dd, hh, mm, 0)
    except Exception:
        return None


def get_event_market(event_ticker: str) -> Optional[dict]:
    """Each KXBTC15M event has one market. Return its full record."""
    try:
        with _client() as c:
            r = c.get(f"/markets?event_ticker={event_ticker}&limit=10")
            r.raise_for_status()
            ms = r.json().get("markets", []) or []
            return ms[0] if ms else None
    except Exception as e:
        logger.debug(f"get_event_market({event_ticker}) failed: {e}")
        return None


def get_orderbook(market_ticker: str) -> Optional[dict]:
    """Pull the L2 order book for a market. Returns:
    {
      'yes': [[price_cents, size], ...],   # bids on YES
      'no':  [[price_cents, size], ...],   # bids on NO
    }
    Kalshi prices are in CENTS (1-99), so divide by 100 for probability."""
    try:
        with _client() as c:
            r = c.get(f"/markets/{market_ticker}/orderbook")
            r.raise_for_status()
            return r.json().get("orderbook") or {}
    except Exception as e:
        logger.debug(f"get_orderbook({market_ticker}) failed: {e}")
        return None


def book_summary(book: Optional[dict]) -> tuple[Optional[float], Optional[float], float]:
    """Convert Kalshi book into (yes_bid_prob, yes_ask_prob, depth_usd).

    Kalshi: 'yes' bids are people willing to BUY YES at that cents-price.
            'no'  bids are people willing to BUY NO  at that cents-price.
            YES ask = 100 - highest NO bid (since selling YES = buying NO).
    """
    if not book:
        return None, None, 0.0
    yes_bids = book.get("yes") or []      # list of [price_cents, size]
    no_bids = book.get("no") or []

    def _best(rows: list, want_max: bool = True) -> Optional[tuple[float, float]]:
        if not rows:
            return None
        # rows look like [[price, size], ...]; price is in cents 1..99
        prices = [(float(r[0]), float(r[1])) for r in rows if r and len(r) >= 2]
        if not prices:
            return None
        prices.sort(key=lambda x: x[0], reverse=want_max)
        return prices[0]

    yes_best_bid = _best(yes_bids, want_max=True)
    no_best_bid = _best(no_bids, want_max=True)

    yes_bid_prob = yes_best_bid[0] / 100.0 if yes_best_bid else None
    yes_ask_prob = (100 - no_best_bid[0]) / 100.0 if no_best_bid else None

    # Total liquidity = sum of contract notionals across top 5 levels both sides
    depth = 0.0
    for r in (yes_bids[:5] + no_bids[:5]):
        if r and len(r) >= 2:
            try:
                depth += float(r[0]) / 100.0 * float(r[1])
            except Exception:
                pass
    return yes_bid_prob, yes_ask_prob, round(depth, 2)


def refresh_markets() -> int:
    """Pull recent KXBTC15M events, upsert into polymarket_markets table.
    (Re-uses the existing DB schema — same shape applies to Kalshi.)
    Returns count saved."""
    events = list_active_btc15m_events(limit=20)
    saved = 0
    now = datetime.utcnow()
    for e in events:
        et = e.get("event_ticker")
        if not et:
            continue
        rt = parse_event_resolution_ts(et)
        if rt is None or rt < now:
            continue   # past event
        # Pull the market for token IDs
        mkt = get_event_market(et)
        if not mkt:
            continue
        ticker = mkt.get("ticker")
        upsert_market(
            condition_id=ticker,                      # use market ticker as our key
            question=e.get("title", ""),
            resolution_ts=rt,
            yes_token_id=ticker,                      # Kalshi has one ticker per market;
            no_token_id=ticker,                       # NO is just 1 - YES on the same ticker
            reference_price=None,                     # Kalshi sets it at hour-tick start
        )
        saved += 1
    return saved


def snapshot_market(market_ticker: str, btc_price_now: Optional[float] = None
                     ) -> Optional[dict]:
    """Pull the order book for a market and persist a snapshot row."""
    book = get_orderbook(market_ticker)
    yes_bid, yes_ask, depth = book_summary(book)
    save_book_snapshot(
        condition_id=market_ticker,
        yes_bid=yes_bid, yes_ask=yes_ask,
        no_bid=(1 - yes_ask) if yes_ask is not None else None,
        no_ask=(1 - yes_bid) if yes_bid is not None else None,
        book_depth_usd=depth,
        btc_price_at_snap=btc_price_now,
    )
    yes_mid = (yes_bid + yes_ask) / 2 if (yes_bid and yes_ask) else None
    return {
        "ticker": market_ticker,
        "yes_bid": yes_bid, "yes_ask": yes_ask, "yes_mid": yes_mid,
        "book_depth_usd": depth,
    }
