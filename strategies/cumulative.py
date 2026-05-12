"""
Helper: today's cumulative P&L line for Telegram notifications.

Resets at midnight Israel time (Asia/Jerusalem). Mirrors the
dashboard's "Today's P&L" widget so the two never disagree.
"""
import logging
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

from sqlalchemy import func
from sqlalchemy.orm import Session

from data.storage import engine, Decision

logger = logging.getLogger(__name__)

_IST = ZoneInfo("Asia/Jerusalem")


def _ist_midnight_utc() -> datetime:
    """UTC timestamp of the most recent 00:00 in Asia/Jerusalem."""
    now_ist = datetime.now(_IST)
    midnight_ist = now_ist.replace(hour=0, minute=0, second=0, microsecond=0)
    return midnight_ist.astimezone(timezone.utc).replace(tzinfo=None)


def today_cumulative_line() -> str:
    """One-line cumulative P&L for the current IST day.

    Returns an HTML-formatted string ready to append to a Telegram
    notification, or an empty string on failure (so callers can append
    unconditionally without breaking the message).
    """
    try:
        cutoff = _ist_midnight_utc()
        with Session(engine) as session:
            rows = (
                session.query(Decision.pnl_usd)
                .filter(Decision.resolved_at >= cutoff,
                         Decision.pnl_usd.isnot(None))
                .all()
            )
        if not rows:
            return "📈 Today: <b>$+0.00</b> (0W/0L · no trades yet)"
        pnls = [float(r[0]) for r in rows]
        total = sum(pnls)
        wins = sum(1 for p in pnls if p > 0)
        losses = sum(1 for p in pnls if p <= 0)
        n = wins + losses
        wr = (wins / n * 100.0) if n else 0.0
        return (
            f"📈 Today: <b>${total:+,.2f}</b> "
            f"({wins}W/{losses}L · {wr:.0f}% WR)"
        )
    except Exception as e:
        logger.warning(f"today_cumulative_line failed: {e}")
        return ""
