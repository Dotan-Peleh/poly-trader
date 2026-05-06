"""
Reference-price tracker.

A Polymarket "Bitcoin Up at HH:00 ET?" hourly binary resolves on whether
the price at HH:00 is **above** the price at the prior HH:00. Polymarket
publishes the reference once it's locked in; before then we infer it from
the BTC tick at the previous hour boundary in our own DB.

We persist it to polymarket_markets.reference_price so that the pricer
doesn't need to re-derive it on every tick.
"""
import logging
from datetime import datetime, timedelta
from typing import Optional

from sqlalchemy import asc
from sqlalchemy.orm import Session

from data.storage import (
    engine, BtcTick, PolymarketMarket,
)

logger = logging.getLogger(__name__)


def previous_hour_boundary(now: datetime) -> datetime:
    """Round DOWN to the previous HH:00:00 UTC."""
    return now.replace(minute=0, second=0, microsecond=0)


def btc_price_at(ts: datetime, window_minutes: int = 5) -> Optional[float]:
    """Find the BTC tick closest to a target timestamp within ±window_minutes.
    Returns the price, or None if no nearby tick exists."""
    lo = ts - timedelta(minutes=window_minutes)
    hi = ts + timedelta(minutes=window_minutes)
    with Session(engine) as session:
        # Tick ≥ ts, take earliest
        after = (session.query(BtcTick)
                 .filter(BtcTick.ts >= ts, BtcTick.ts <= hi)
                 .order_by(asc(BtcTick.ts))
                 .first())
        if after:
            return float(after.price)
        # Tick ≤ ts, take latest
        before = (session.query(BtcTick)
                  .filter(BtcTick.ts >= lo, BtcTick.ts <= ts)
                  .order_by(BtcTick.ts.desc())
                  .first())
        if before:
            return float(before.price)
    return None


def reference_for(market: PolymarketMarket) -> Optional[float]:
    """Determine the resolution reference price for a market.

    Convention: hourly market resolving at HH:00 UTC uses the price at
    (HH-1):00 UTC as its reference. We look for the tick closest to that
    boundary in our btc_ticks table.
    """
    if market.reference_price is not None:
        return float(market.reference_price)
    boundary = market.resolution_ts - timedelta(hours=1)
    return btc_price_at(boundary)


def backfill_reference_prices() -> int:
    """For every active market without a reference_price, try to compute one
    from btc_ticks. Returns count updated."""
    updated = 0
    with Session(engine) as session:
        markets = (session.query(PolymarketMarket)
                   .filter(PolymarketMarket.state == "active",
                           PolymarketMarket.reference_price.is_(None))
                   .all())
        for m in markets:
            ref = reference_for(m)
            if ref is None:
                continue
            m.reference_price = ref
            updated += 1
        session.commit()
    return updated
