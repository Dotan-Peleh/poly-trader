"""
Digital option pricer — the core trading edge.

A Polymarket "Will BTC be > $X at HH:00 ET?" binary is mathematically a
digital call option. Standard Black-Scholes-style pricing applies, but
we work in a log-return + recent realized vol framework instead of
implied vol (Polymarket doesn't publish IV).

Core formula:
  move_needed   = (R − P) / P                   R = reference price, P = current price
                                                 Positive if YES needs price to fall.
  σ_per_min     = EWMA realized vol (per-minute log returns)
  σ_remaining   = σ_per_min × √(T − t)          time in minutes
  z             = move_needed / σ_remaining
  P(YES wins)   = 1 − Φ(z)                      normal CDF

  edge          = P(YES, model) − P(YES, market mid)

Why this works in the LATE WINDOW specifically:
  - Per-minute log returns are well-approximated by a normal at short horizons
  - Vol is tractable from recent ticks (no IV surface to estimate)
  - Market makers often use stale vol assumptions; the gap is our edge
  - At T−5min, P(YES) is sharp (>90% or <10%); MM lag means real money to make

Limits / failure modes:
  - Fat tails on long horizons (T−60min) make normal assumption shaky
  - Macro events (CPI, FOMC) violate normal assumption — strategy disabled
  - Pin-risk near the reference price: tiny moves flip outcome, we may misprice
"""
from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Optional


# ── Pure stats helpers ──────────────────────────────────────────────────────

def _norm_cdf(z: float) -> float:
    """Standard normal CDF using erf. No scipy dependency for portability."""
    return 0.5 * (1.0 + math.erf(z / math.sqrt(2.0)))


# ── Public API ──────────────────────────────────────────────────────────────

@dataclass(frozen=True)
class BinaryQuote:
    """A snapshot of a Polymarket binary at a moment in time."""
    symbol: str               # e.g., 'BTC-2026-05-06-15:00-UP'
    reference_price: float    # the price the market resolves against (set at HH:00 prior hour)
    current_price: float      # latest BTC spot price
    minutes_to_close: float   # T − t, in minutes
    sigma_per_minute: float   # EWMA realized vol per minute (e.g., 0.0008 = 0.08%/min)
    implied_yes_prob: float   # market mid for YES, in [0, 1]


@dataclass(frozen=True)
class PricerOutput:
    model_yes_prob: float
    implied_yes_prob: float
    edge: float                # model − implied; positive = YES is cheap
    z_score: float             # standardised move-needed
    move_needed_pct: float     # signed % move from current to reference (positive = need to fall)


def price_binary(q: BinaryQuote) -> PricerOutput:
    """Price a binary "Will BTC > reference at close?" market.

    Returns model probability, implied probability, edge, and intermediate
    z-score / move-needed for diagnostics.

    Convention: YES wins if current_price stays at or above reference at close.
                NO wins if current_price ends below reference at close.
    """
    if q.minutes_to_close <= 0:
        # Market is closing/closed; the answer is whatever current vs ref says
        yes_wins = 1.0 if q.current_price >= q.reference_price else 0.0
        return PricerOutput(
            model_yes_prob=yes_wins,
            implied_yes_prob=q.implied_yes_prob,
            edge=yes_wins - q.implied_yes_prob,
            z_score=float("inf") if yes_wins == 0 else float("-inf"),
            move_needed_pct=0.0,
        )

    if q.sigma_per_minute <= 0 or q.current_price <= 0:
        raise ValueError("sigma_per_minute and current_price must be positive")

    # Move needed for YES to LOSE (current must fall to reference)
    # Positive value means current is currently above reference (YES leading).
    move_needed = (q.reference_price - q.current_price) / q.current_price

    # Total expected vol from now to close
    sigma_total = q.sigma_per_minute * math.sqrt(q.minutes_to_close)

    # Standardise — z is the number of standard deviations needed to reverse
    z = move_needed / sigma_total

    # P(NO wins) = P(price ends below reference) = Φ(z)
    # P(YES wins) = 1 − Φ(z)
    p_yes = 1.0 - _norm_cdf(z)
    edge = p_yes - q.implied_yes_prob

    return PricerOutput(
        model_yes_prob=p_yes,
        implied_yes_prob=q.implied_yes_prob,
        edge=edge,
        z_score=z,
        move_needed_pct=move_needed * 100,
    )


# ── Decision wrapper ────────────────────────────────────────────────────────

EDGE_THRESHOLD = 0.04        # require ≥4% edge to fire
MIN_TIME_TO_CLOSE_SEC = 60   # don't fire in last minute (filling risk + 1s tail vol)
MAX_TIME_TO_CLOSE_MIN = 15   # don't fire if too far out (model less reliable on long horizons)


@dataclass(frozen=True)
class TradeIntent:
    side: str                  # 'YES' or 'NO' or 'SKIP'
    edge: float
    model_yes_prob: float
    implied_yes_prob: float
    reason: str


def decide_trade(q: BinaryQuote) -> TradeIntent:
    """Should we fire on this market right now? Pure function."""
    out = price_binary(q)

    # Time gates
    if q.minutes_to_close * 60 < MIN_TIME_TO_CLOSE_SEC:
        return TradeIntent("SKIP", out.edge, out.model_yes_prob, q.implied_yes_prob,
                            f"too close ({q.minutes_to_close*60:.0f}s left)")
    if q.minutes_to_close > MAX_TIME_TO_CLOSE_MIN:
        return TradeIntent("SKIP", out.edge, out.model_yes_prob, q.implied_yes_prob,
                            f"too far out ({q.minutes_to_close:.1f}m left, max {MAX_TIME_TO_CLOSE_MIN}m)")

    # Edge gate
    if out.edge >= EDGE_THRESHOLD:
        return TradeIntent("YES", out.edge, out.model_yes_prob, q.implied_yes_prob,
                            f"YES cheap: model={out.model_yes_prob:.3f} > implied={q.implied_yes_prob:.3f}, edge={out.edge*100:+.1f}%")
    if out.edge <= -EDGE_THRESHOLD:
        return TradeIntent("NO", out.edge, out.model_yes_prob, q.implied_yes_prob,
                            f"NO cheap: model={out.model_yes_prob:.3f} < implied={q.implied_yes_prob:.3f}, edge={out.edge*100:+.1f}%")

    return TradeIntent("SKIP", out.edge, out.model_yes_prob, q.implied_yes_prob,
                        f"|edge| {abs(out.edge)*100:.1f}% below {EDGE_THRESHOLD*100:.0f}% threshold")
