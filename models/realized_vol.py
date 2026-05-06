"""
Per-minute realized volatility estimator.

We compute log-returns from 1s BTC ticks, resample to 1-minute closes,
and estimate σ via EWMA with configurable half-life.

Output is sigma_per_minute — used by digital_option.price_binary().
"""
import logging
import math
from datetime import datetime, timedelta
from typing import Optional

import numpy as np
import pandas as pd
from sqlalchemy.orm import Session

from data.storage import engine, BtcTick
from config.settings import settings

logger = logging.getLogger(__name__)


def _load_recent_ticks(minutes: int) -> pd.DataFrame:
    cutoff = datetime.utcnow() - timedelta(minutes=minutes)
    with Session(engine) as session:
        rows = (session.query(BtcTick.ts, BtcTick.price)
                .filter(BtcTick.ts >= cutoff)
                .order_by(BtcTick.ts.asc())
                .all())
    if not rows:
        return pd.DataFrame()
    return pd.DataFrame(rows, columns=["ts", "price"])


def estimate_sigma_per_minute(window_minutes: Optional[int] = None) -> float:
    """EWMA realized volatility per minute, in fractional units.

    Default window matches settings.vol_window_minutes (30). Returns
    settings.min_sigma_per_minute (a floor) when data is insufficient,
    so callers don't divide by zero downstream.
    """
    w = window_minutes or settings.vol_window_minutes
    df = _load_recent_ticks(w)
    if df.empty or len(df) < 30:
        return settings.min_sigma_per_minute

    # Resample to 1-minute closes
    df = df.set_index("ts").sort_index()
    df_1m = df["price"].resample("1min").last().dropna()
    if len(df_1m) < 5:
        return settings.min_sigma_per_minute

    # Log-returns
    log_returns = np.log(df_1m / df_1m.shift(1)).dropna()
    if len(log_returns) < 5:
        return settings.min_sigma_per_minute

    # EWMA half-life ~10 min → α = 1 - 0.5^(1/10) ≈ 0.067
    half_life = max(5, w // 3)
    alpha = 1.0 - 0.5 ** (1.0 / half_life)
    var_ewma = log_returns.ewm(alpha=alpha, adjust=False).var().iloc[-1]
    sigma = math.sqrt(float(var_ewma)) if var_ewma > 0 else settings.min_sigma_per_minute
    return max(sigma, settings.min_sigma_per_minute)


def latest_btc_price() -> Optional[float]:
    """Most recent tick price; None if no data yet."""
    with Session(engine) as session:
        last = (session.query(BtcTick.price)
                .order_by(BtcTick.ts.desc())
                .first())
        return float(last[0]) if last else None
