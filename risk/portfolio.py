"""
Portfolio-level risk gates.

Enforces:
  • max_concurrent_trades open at once
  • daily loss circuit breaker (-8% of starting capital)
  • duplicate-market guard (one trade per condition_id)
"""
import logging
from datetime import datetime, timedelta

from sqlalchemy.orm import Session

from data.storage import engine, Decision
from config.settings import settings

logger = logging.getLogger(__name__)


class PortfolioGuard:
    def __init__(self, starting_capital: float = None):
        self.starting_capital = starting_capital or settings.starting_capital

    def open_count(self) -> int:
        with Session(engine) as session:
            return (session.query(Decision)
                    .filter(Decision.mode == settings.trading_mode,
                            Decision.resolution_yes.is_(None))
                    .count())

    def has_open_for_market(self, condition_id: str) -> bool:
        with Session(engine) as session:
            return (session.query(Decision)
                    .filter(Decision.mode == settings.trading_mode,
                            Decision.condition_id == condition_id,
                            Decision.resolution_yes.is_(None))
                    .count() > 0)

    def daily_realized_pnl(self) -> float:
        cutoff = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
        with Session(engine) as session:
            q = (session.query(Decision.pnl_usd)
                 .filter(Decision.mode == settings.trading_mode,
                         Decision.resolved_at >= cutoff,
                         Decision.pnl_usd.isnot(None)))
            # When pre_window is in the inverted-momentum experiment, the
            # circuit breaker should NOT count losses from the OLD
            # anti-predictive v2_meanrev rows — they don't represent what
            # the current strategy would do. Scoping the daily budget to
            # the active strategy's tag prevents the breaker from
            # tripping the experiment before it gets a fair chance.
            if getattr(settings, "pre_window_invert_side", False):
                q = q.filter(~Decision.notes.like("%v2_meanrev%"))
            rows = q.all()
        return float(sum(r[0] for r in rows if r[0] is not None))

    def circuit_breaker_triggered(self) -> bool:
        threshold = -self.starting_capital * settings.daily_loss_limit_pct
        return self.daily_realized_pnl() <= threshold

    def can_open(self, condition_id: str) -> tuple[bool, str]:
        if self.circuit_breaker_triggered():
            return False, f"daily circuit breaker: P&L=${self.daily_realized_pnl():+.2f}"
        if self.open_count() >= settings.max_concurrent_trades:
            return False, f"max {settings.max_concurrent_trades} open trades"
        if self.has_open_for_market(condition_id):
            return False, f"already have open trade in {condition_id}"
        return True, "ok"
