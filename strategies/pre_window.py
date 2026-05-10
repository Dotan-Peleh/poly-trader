"""
Pre-window decision engine — Phase 2 v2.

The previous strategy (late_window.py) tried to fire in the LAST few minutes
before resolution. By that point HFT MMs have already priced BTC's
trajectory and books snap to 0.99/0.01 — nothing for us to do.

Pre-window strategy fires BEFORE the measurement window opens:
  • Firing window: [pre_window_min_minutes, pre_window_max_minutes] to resolution
  • Defaults: 6–30 min to resolution
  • At T-30 min: 25 min until window opens, market at ~50/50 with thin books
  • At T-6 min: 1 min until window opens, momentum signal is freshest
  • After window opens (T-5 min and in): late_window territory — skip

Key model difference: pre-window markets do NOT have a known reference
price. The reference is set when the measurement window OPENS. So fair
probability isn't "P(BTC drifts above ref)" — it's "P(BTC moves up over
the future 5-min window)". Without other info that's exactly 50%, modulated by:

  1. Momentum   — BTC's 15-min log return ÷ 15-min realized vol.
                  Crypto exhibits weak short-term continuation (~52% hit on
                  1-bar momentum signals; documented in microstructure lit).
  2. Imbalance  — Polymarket book depth tilt. Treated as small fade signal
                  (-3% to +3% bias).

Combined fair_yes_prob is bounded in [0.30, 0.70] so a single dominant
signal can't push us into lottery-ticket territory. Edge gate fires when
|fair − implied| ≥ settings.edge_threshold AND book quality is real
(yes_ask + no_ask < 1.05 — placeholder books rejected).

Position management is intentionally simple in v1: hold to resolution.
settle_resolved_markets (in late_window.py) handles win/loss accounting.
DB columns model_yes_prob + implied_yes_prob enable post-hoc calibration
analysis (predicted vs actual hit rate buckets) for v2 tuning.
"""
import logging
import math
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Optional

from sqlalchemy.orm import Session

from config.settings import settings
from data.storage import engine, PolymarketMarket, PolymarketBookSnapshot, Decision, BtcTick
from execution.wallet import Wallet, record_paper_trade
from risk.portfolio import PortfolioGuard
from risk.position_sizer import size_trade
from models.realized_vol import estimate_sigma_per_minute, latest_btc_price

logger = logging.getLogger(__name__)


# ── Constants ────────────────────────────────────────────────────────────
SIGNAL_FAIR_FLOOR = 0.30
SIGNAL_FAIR_CEIL = 0.70
# v2 (2026-05-10): inverted momentum → mean-reversion. v1's calibration
# data (n=19) showed Strong YES bucket was 80% wrong and NO side 0/8 —
# consistent with BTC mean-reverting at 5-min horizons rather than
# continuing. We bet AGAINST recent move rather than with it.
MAX_REVERSION_BIAS = 0.07    # ±7% prob from inverted momentum
MAX_IMBALANCE_BIAS = 0.03    # ±3% prob from book imbalance
BASE_YES_PRIOR = 0.505       # BTC has slight long-run drift; 0.5% YES bias
MAX_PLACEHOLDER_SUM = 1.05   # yes_ask + no_ask above this = no real liquidity
TAIL_FILTER_LO = 0.10        # don't pay < $0.10 on a side (deep tail)
TAIL_FILTER_HI = 0.90        # don't pay > $0.90 on a side (deep tail)
# Consecutive-loss cooldown (risk management, no martingale)
COOLDOWN_3_LOSSES_HOURS = 1.0
COOLDOWN_5_LOSSES_HOURS = 6.0
STRATEGY_TAG = "v2_meanrev"  # written to Decision.notes for A/B split


# ── Helpers ──────────────────────────────────────────────────────────────

def _latest_book(condition_id: str) -> Optional[PolymarketBookSnapshot]:
    with Session(engine) as session:
        return (session.query(PolymarketBookSnapshot)
                .filter(PolymarketBookSnapshot.condition_id == condition_id)
                .order_by(PolymarketBookSnapshot.id.desc())
                .first())


def _btc_return_over(minutes: int) -> float:
    """BTC log return over the last `minutes`. 0.0 if insufficient data."""
    cutoff = datetime.utcnow() - timedelta(minutes=minutes)
    with Session(engine) as session:
        old = (session.query(BtcTick.price)
               .filter(BtcTick.ts >= cutoff)
               .order_by(BtcTick.ts.asc())
               .first())
        new = (session.query(BtcTick.price)
               .order_by(BtcTick.ts.desc())
               .first())
    if not old or not new or not old[0] or not new[0]:
        return 0.0
    try:
        return math.log(float(new[0]) / float(old[0]))
    except (ValueError, ZeroDivisionError):
        return 0.0


# ── Microstructure signal ────────────────────────────────────────────────

@dataclass(frozen=True)
class Signal:
    fair_yes_prob: float
    reversion_bias: float
    imbalance_bias: float
    btc_15min_return: float
    sigma_15min: float


def compute_signal(
    btc_15min_return: float,
    sigma_per_minute: float,
    yes_depth_usd: float,
    no_depth_usd: float,
) -> Signal:
    """v2 (2026-05-10): mean-reversion + book imbalance + slight YES prior.

    v1 was momentum-following and lost 13/19 trades. Calibration data
    showed Strong YES bucket was 80% wrong and NO side 0/8 wins —
    classic signature of BTC mean-reverting at 5-min horizons in this
    vol regime. v2 inverts the momentum tail: after a STRONG up-move
    we bet NO (expecting reversion), after a STRONG down-move we bet
    YES (expecting bounce). Soft moves stay near 50/50.
    """
    sigma_15min = sigma_per_minute * math.sqrt(15.0) if sigma_per_minute > 0 else 0.001

    # z-score of recent return; clip extremes to keep tanh well-behaved
    z = (btc_15min_return / sigma_15min) if sigma_15min > 0 else 0.0
    z_clipped = max(-2.0, min(2.0, z))
    # NEGATIVE coefficient = inverted momentum. After +1σ up, bias DOWN.
    reversion_bias = -MAX_REVERSION_BIAS * math.tanh(z_clipped)

    # Imbalance: more $ on NO ask = MMs willing to sell NO cheap; we lean YES
    total = yes_depth_usd + no_depth_usd
    imbalance = ((no_depth_usd - yes_depth_usd) / total) if total > 1.0 else 0.0
    imbalance_bias = MAX_IMBALANCE_BIAS * imbalance

    # Base prior slightly above 0.50: BTC has long-run positive drift.
    # Over a 5-min window the drift contribution is tiny but cumulatively
    # it pushes 0/8 NO outcomes seen in v1 toward more balanced sides.
    fair = BASE_YES_PRIOR + reversion_bias + imbalance_bias
    fair = max(SIGNAL_FAIR_FLOOR, min(SIGNAL_FAIR_CEIL, fair))

    return Signal(
        fair_yes_prob=fair,
        reversion_bias=reversion_bias,
        imbalance_bias=imbalance_bias,
        btc_15min_return=btc_15min_return,
        sigma_15min=sigma_15min,
    )


# ── Cooldown manager ─────────────────────────────────────────────────────

def _cooldown_active() -> Optional[str]:
    """Return reason string if a consecutive-loss cooldown is active, else None.
    Counts consecutive losses (most recent backward) across all decisions
    with size_usd > 0 and resolution_yes set."""
    with Session(engine) as session:
        # Most recent N resolved decisions (any tag)
        recent = (session.query(Decision)
                  .filter(Decision.size_usd > 0,
                          Decision.resolution_yes.isnot(None))
                  .order_by(Decision.id.desc())
                  .limit(5).all())
    if not recent:
        return None

    def _is_loss(d: Decision) -> bool:
        if d.resolution_yes is None: return False
        won = (d.resolution_yes == 1 and d.side == "YES") or \
              (d.resolution_yes == 0 and d.side == "NO")
        return not won

    consecutive = 0
    last_loss_ts: Optional[datetime] = None
    for d in recent:
        if _is_loss(d):
            consecutive += 1
            if last_loss_ts is None:
                last_loss_ts = d.resolved_at or d.ts
        else:
            break

    now = datetime.utcnow()
    if consecutive >= 5 and last_loss_ts is not None:
        if (now - last_loss_ts) < timedelta(hours=COOLDOWN_5_LOSSES_HOURS):
            return f"5-loss cooldown ({consecutive} in a row, {COOLDOWN_5_LOSSES_HOURS}h)"
    if consecutive >= 3 and last_loss_ts is not None:
        if (now - last_loss_ts) < timedelta(hours=COOLDOWN_3_LOSSES_HOURS):
            return f"3-loss cooldown ({consecutive} in a row, {COOLDOWN_3_LOSSES_HOURS}h)"
    return None


# ── Per-market evaluator ─────────────────────────────────────────────────

def evaluate_market(
    market: PolymarketMarket,
    wallet: Wallet,
    portfolio: PortfolioGuard,
    notifier=None,
    claude_gate=None,
) -> Optional[dict]:
    """One market's full pipeline. Returns recorded trade dict if fired, else None."""
    now = datetime.utcnow()
    minutes_to_close = (market.resolution_ts - now).total_seconds() / 60.0

    if minutes_to_close < settings.pre_window_min_minutes:
        return None
    if minutes_to_close > settings.pre_window_max_minutes:
        return None

    book = _latest_book(market.condition_id)
    if book is None or book.yes_bid is None or book.yes_ask is None:
        return None

    yes_bid = float(book.yes_bid)
    yes_ask = float(book.yes_ask)
    no_bid = float(book.no_bid) if book.no_bid is not None else (1.0 - yes_ask)
    no_ask = float(book.no_ask) if book.no_ask is not None else (1.0 - yes_bid)

    # Reject placeholder books — both asks at 0.99 means no real ask-side liquidity
    if yes_ask + no_ask > MAX_PLACEHOLDER_SUM:
        logger.debug(f"[{market.condition_id}] placeholder book "
                      f"(yes_ask+no_ask={yes_ask+no_ask:.3f})")
        return None

    # Implied YES probability via mid (averaging removes the spread)
    yes_mid = (yes_bid + yes_ask) / 2.0
    no_mid = (no_bid + no_ask) / 2.0
    s = yes_mid + no_mid
    implied_yes = (yes_mid / s) if s > 0 else 0.5

    # Approximate per-side depth: book_depth_usd is total; split by mid weight
    total_depth = float(book.book_depth_usd or 0.0)
    weight = yes_mid / max(yes_mid + no_mid, 1e-6)
    yes_depth_usd = total_depth * weight
    no_depth_usd = total_depth * (1.0 - weight)

    # Signal
    sigma = estimate_sigma_per_minute()
    if sigma <= 0:
        return None
    btc_ret_15m = _btc_return_over(15)
    sig = compute_signal(
        btc_15min_return=btc_ret_15m,
        sigma_per_minute=sigma,
        yes_depth_usd=yes_depth_usd,
        no_depth_usd=no_depth_usd,
    )

    edge = sig.fair_yes_prob - implied_yes
    if abs(edge) < settings.edge_threshold:
        return None

    side = "YES" if edge > 0 else "NO"
    side_ask = yes_ask if side == "YES" else no_ask
    if side_ask < TAIL_FILTER_LO or side_ask > TAIL_FILTER_HI:
        logger.info(f"[{market.condition_id}] tail-filter: skipping {side} "
                     f"at ask {side_ask:.3f} (outside [{TAIL_FILTER_LO},{TAIL_FILTER_HI}])")
        return None

    # Portfolio gate
    can, gate_reason = portfolio.can_open(market.condition_id)
    if not can:
        logger.info(f"[{market.condition_id}] portfolio block: {gate_reason}")
        return None

    # Sizing
    bankroll = wallet.available_balance()
    sizing = size_trade(
        side=side,
        model_yes_prob=sig.fair_yes_prob,
        yes_ask=yes_ask,
        no_ask=no_ask,
        bankroll=bankroll,
    )
    if sizing.size_usd <= 0:
        return None
    final_size = round(sizing.size_usd, 2)
    if final_size < 1.0:
        return None

    # Live mode would add safety checks here; pre_window v1 is paper-only by
    # default (live can be enabled via existing effective_mode() gate later).
    btc_now = latest_btc_price() or 0.0

    decision_id = record_paper_trade(
        condition_id=market.condition_id,
        side=side,
        btc_price=float(btc_now),
        reference_price=float(btc_now),  # not yet set — placeholder for pre-window rows
        minutes_to_close=minutes_to_close,
        sigma_per_minute=sigma,
        model_yes_prob=sig.fair_yes_prob,
        implied_yes_prob=implied_yes,
        edge=edge,
        size_usd=final_size,
        paid_per_unit=sizing.paid_per_unit,
    )

    # Tag the row so the summary can A/B v1 vs v2 cleanly
    try:
        from sqlalchemy import update
        with Session(engine) as session:
            session.execute(
                update(Decision).where(Decision.id == decision_id)
                .values(notes=STRATEGY_TAG)
            )
            session.commit()
    except Exception as e:
        logger.debug(f"could not tag decision with strategy version: {e}")

    reason = (f"pre-window {side}: fair={sig.fair_yes_prob:.3f} vs implied={implied_yes:.3f} "
              f"(rev={sig.reversion_bias:+.3f}, imb={sig.imbalance_bias:+.3f}, "
              f"btc_15m_ret={btc_ret_15m*100:+.2f}%, T-{minutes_to_close:.1f}min)")
    logger.info(f"FIRED {side} {market.condition_id[:24]}... size=${final_size:.2f} "
                 f"@ {sizing.paid_per_unit:.3f} | edge={edge*100:+.1f}% | {reason}")
    if notifier:
        icon = "🟢" if side == "YES" else "🔴"
        notifier.send(
            f"{icon} <b>{side} fired ({STRATEGY_TAG})</b> "
            f"<code>{market.condition_id[:24]}...</code>\n"
            f"📊 fair={sig.fair_yes_prob*100:.1f}% vs implied={implied_yes*100:.1f}%\n"
            f"💸 edge={edge*100:+.1f}% | size=${final_size:.2f} "
            f"({sizing.units:.0f} contracts @ {sizing.paid_per_unit*100:.0f}¢)\n"
            f"⏱ T-{minutes_to_close:.1f}min | "
            f"rev={sig.reversion_bias:+.3f}, imb={sig.imbalance_bias:+.3f}\n"
            f"💡 BTC 15m ret={btc_ret_15m*100:+.2f}%, σ_15m={sig.sigma_15min*100:.2f}%"
        )

    return {
        "decision_id": decision_id,
        "side": side,
        "size_usd": final_size,
        "edge": edge,
        "minutes_to_close": minutes_to_close,
    }


def decision_tick_impl(notifier=None, claude_gate=None) -> int:
    """Sweep every active market in pre-window. Returns count of fires."""
    # Risk management: pause new entries after consecutive losses
    cd_reason = _cooldown_active()
    if cd_reason is not None:
        # Log once per minute at most so we don't spam at 5s tick rate
        global _LAST_COOLDOWN_LOG_TS
        try:
            last = _LAST_COOLDOWN_LOG_TS
        except NameError:
            last = None
        nowts = datetime.utcnow()
        if last is None or (nowts - last).total_seconds() > 60:
            logger.info(f"cooldown active: {cd_reason}")
            globals()["_LAST_COOLDOWN_LOG_TS"] = nowts
        return 0

    wallet = Wallet()
    portfolio = PortfolioGuard()
    fires = 0

    now = datetime.utcnow()
    upper = now + timedelta(minutes=settings.pre_window_max_minutes)
    lower = now + timedelta(minutes=settings.pre_window_min_minutes)

    with Session(engine) as session:
        markets = (session.query(PolymarketMarket)
                   .filter(PolymarketMarket.state == "active",
                           PolymarketMarket.resolution_ts >= lower,
                           PolymarketMarket.resolution_ts <= upper)
                   .all())

    for m in markets:
        try:
            result = evaluate_market(m, wallet, portfolio,
                                       notifier=notifier, claude_gate=claude_gate)
            if result:
                fires += 1
        except Exception as e:
            logger.warning(f"[{m.condition_id}] pre_window eval failed: {e}")

    return fires
