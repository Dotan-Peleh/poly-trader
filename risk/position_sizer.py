"""
Quarter-Kelly sizing for binary contracts.

For a YES contract bought at price p (paid p per $1 payout):
  • If model says win prob = w, then expected payout per $1 staked
    in net terms = w × (1−p)/p − (1−w)
  • Kelly fraction = (w − p) / (1 − p)         (when w > p)

For a NO contract: same with (1 − w) and (1 − p_yes_ask).

Rules:
  • Always use quarter-Kelly (divisor 4) for variance control
  • Cap at max_position_pct of bankroll
  • Floor at $1 (Kalshi min trade) — if the math says less, SKIP
"""
from dataclasses import dataclass
import logging

from config.settings import settings

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class SizingResult:
    size_usd: float          # capital to deploy
    units: float             # number of contracts (≈ size / paid_per_unit)
    paid_per_unit: float     # the price (in [0, 1]) we'd pay
    kelly_fraction: float    # raw Kelly before scaling (for diagnostics)


def kelly_fraction_yes(model_prob: float, ask_price: float) -> float:
    """Kelly fraction for a YES bet at ask_price.
    f* = (w − p) / (1 − p) when w > p, else 0."""
    if ask_price <= 0 or ask_price >= 1:
        return 0.0
    if model_prob <= ask_price:
        return 0.0
    return max(0.0, (model_prob - ask_price) / (1.0 - ask_price))


def kelly_fraction_no(model_prob: float, no_ask_price: float) -> float:
    """Kelly fraction for a NO bet at no_ask_price.

    Bought at no_ask, win prob = (1 - model_prob).
    f* = ((1 − w) − no_ask) / (1 − no_ask).
    """
    no_win_prob = 1.0 - model_prob
    if no_ask_price <= 0 or no_ask_price >= 1:
        return 0.0
    if no_win_prob <= no_ask_price:
        return 0.0
    return max(0.0, (no_win_prob - no_ask_price) / (1.0 - no_ask_price))


def size_trade(
    side: str, model_yes_prob: float,
    yes_ask: float, no_ask: float,
    bankroll: float,
) -> SizingResult:
    """Decide how much to stake.

    Args:
      side: 'YES' or 'NO'
      model_yes_prob: our model's probability that YES wins
      yes_ask: best ask for YES (price you'd pay to buy YES)
      no_ask: best ask for NO  (price you'd pay to buy NO)
      bankroll: available USD

    Returns SizingResult with size=0 if no edge or below min trade size.
    """
    if side == "YES":
        kelly = kelly_fraction_yes(model_yes_prob, yes_ask)
        paid_per_unit = yes_ask
    elif side == "NO":
        kelly = kelly_fraction_no(model_yes_prob, no_ask)
        paid_per_unit = no_ask
    else:
        return SizingResult(0.0, 0.0, 0.0, 0.0)

    if kelly <= 0 or paid_per_unit <= 0 or paid_per_unit >= 1:
        return SizingResult(0.0, 0.0, paid_per_unit or 0.0, kelly)

    # Quarter-Kelly + position cap
    fraction = min(
        kelly / settings.kelly_fraction_divisor,
        settings.max_position_pct,
    )
    size_usd = round(bankroll * fraction, 2)

    # Kalshi minimum is $1 / 100 contracts; below that, skip
    if size_usd < 1.0:
        return SizingResult(0.0, 0.0, paid_per_unit, kelly)

    units = size_usd / paid_per_unit
    return SizingResult(
        size_usd=size_usd,
        units=round(units, 4),
        paid_per_unit=paid_per_unit,
        kelly_fraction=kelly,
    )
