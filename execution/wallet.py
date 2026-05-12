"""
Bankroll + fee tracker — paper mode for Phase 2-4, live in Phase 5.

Kalshi fees:
  • Trading fee: $0.07 per contract on entry (rounded to nearest 0.01)
    Actually it's max(0.0035 × contracts × price_cents, 0.01)
  • No fee on exit/resolution
  • Standard tier; volume tier reduces this

For our small-trade paper sim we approximate as 1% of notional per leg.
That's slightly conservative; refine in Phase 5 against real fills.
"""
import logging
from datetime import datetime
from typing import Optional

from sqlalchemy.orm import Session

from data.storage import engine, Decision
from config.settings import settings

logger = logging.getLogger(__name__)


PAPER_FEE_RATE = 0.01    # 1% per leg approximation


class Wallet:
    """Simple bankroll ledger; persisted by virtue of being computed
    fresh from `decisions` table each call (no in-memory state to lose
    on restart)."""

    def __init__(self, starting_capital: Optional[float] = None,
                  mode: Optional[str] = None):
        self.starting_capital = starting_capital if starting_capital is not None else settings.starting_capital
        self.mode = mode or settings.trading_mode

    def realized_pnl(self) -> float:
        """Sum pnl_usd across all RESOLVED decisions in this mode."""
        with Session(engine) as session:
            rows = (session.query(Decision.pnl_usd)
                    .filter(Decision.mode == self.mode,
                            Decision.pnl_usd.isnot(None))
                    .all())
        return float(sum(r[0] for r in rows if r[0] is not None))

    def open_capital_at_risk(self) -> float:
        """Sum of size_usd across decisions still pending resolution."""
        with Session(engine) as session:
            rows = (session.query(Decision.size_usd)
                    .filter(Decision.mode == self.mode,
                            Decision.resolution_yes.is_(None),
                            Decision.size_usd.isnot(None))
                    .all())
        return float(sum(r[0] for r in rows if r[0] is not None))

    def open_count(self) -> int:
        with Session(engine) as session:
            return (session.query(Decision)
                    .filter(Decision.mode == self.mode,
                            Decision.resolution_yes.is_(None))
                    .count())

    def available_balance(self) -> float:
        """How much we can deploy on a NEW trade right now."""
        return self.starting_capital + self.realized_pnl() - self.open_capital_at_risk()

    def nav(self) -> dict:
        """Net asset value snapshot — like crypto-trader's compute_account_nav.

        nav = starting + realized + (sum of expected value of open trades)

        For binary contracts the "MTM" of an open trade is shares × current
        market mid. We approximate as size_usd × current_implied_prob, which
        in practice only affects the dashboard display — closed trades are
        the only thing that moves realized_pnl.
        """
        starting = self.starting_capital
        realized = self.realized_pnl()
        # For now we don't mark-to-market open binary trades — they resolve
        # quickly anyway. Just return cost basis.
        open_at_cost = self.open_capital_at_risk()
        return {
            "starting": starting,
            "realized_pnl": realized,
            "open_at_cost": open_at_cost,
            "available": starting + realized - open_at_cost,
            "balance_nav": starting + realized,
            "open_count": self.open_count(),
        }


def record_paper_trade(
    condition_id: str, side: str, btc_price: float, reference_price: float,
    minutes_to_close: float, sigma_per_minute: float,
    model_yes_prob: float, implied_yes_prob: float, edge: float,
    size_usd: float, paid_per_unit: float,
    mode_override: Optional[str] = None,
) -> int:
    """Persist a trade row. Returns the decision id.

    `mode_override` lets callers force the row's mode independent of the
    global setting — used by strategies that haven't yet been wired to
    real on-chain execution (e.g. smart_money) so their decisions stay
    tagged `paper` even when the bot's effective_mode is `live`. This
    prevents phantom "live" rows for code paths that never actually
    placed an on-chain order.
    """
    units = size_usd / paid_per_unit if paid_per_unit > 0 else 0.0
    fill_price = paid_per_unit  # paper assumption: take the offered price flat
    with Session(engine) as session:
        d = Decision(
            ts=datetime.utcnow(),
            condition_id=condition_id,
            side=side,
            btc_price_at_entry=btc_price,
            reference_price=reference_price,
            minutes_to_close=minutes_to_close,
            sigma_per_minute=sigma_per_minute,
            model_yes_prob=model_yes_prob,
            implied_yes_prob=implied_yes_prob,
            edge=edge,
            size_usd=size_usd,
            paid_per_unit=paid_per_unit,
            units_bought=units,
            fill_price=fill_price,
            mode=mode_override or settings.trading_mode,
        )
        session.add(d)
        session.commit()
        return d.id


def settle_paper_trade(decision_id: int, btc_close_price: float) -> Optional[dict]:
    """Mark a paper trade as resolved. Compute pnl based on outcome.

    Each Kalshi contract pays $1 on YES win, $0 on YES lose. (Or $1/$0 on NO.)
    P&L = units_bought × (1.0 if won else 0.0) − size_usd − fees
    """
    with Session(engine) as session:
        d = session.get(Decision, decision_id)
        if d is None or d.resolution_yes is not None:
            return None

        # Resolve YES if BTC close >= reference
        resolution_yes = btc_close_price >= float(d.reference_price)
        won = (d.side == "YES" and resolution_yes) or (d.side == "NO" and not resolution_yes)

        units = float(d.units_bought or 0)
        size = float(d.size_usd or 0)
        gross_payout = units * (1.0 if won else 0.0)
        fee = size * PAPER_FEE_RATE   # entry-leg fee approximation
        pnl_usd = gross_payout - size - fee

        d.resolution_yes = resolution_yes
        d.pnl_usd = round(pnl_usd, 4)
        d.resolved_at = datetime.utcnow()
        session.commit()

        return {
            "decision_id": d.id,
            "won": won,
            "pnl_usd": d.pnl_usd,
            "resolution_yes": resolution_yes,
            "gross_payout": gross_payout,
            "fee": fee,
        }
