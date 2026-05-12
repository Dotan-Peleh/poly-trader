"""
The late-window decision engine — Phase 2 core.

Every decision_tick (every 5s):
  • For each market resolving in [min_seconds_to_close..max_minutes_to_close]:
      − Pull latest book → implied YES probability
      − Estimate σ_per_min from recent BTC ticks
      − Get reference price (price at start of this 15-min window)
      − Get current BTC price
      − Build BinaryQuote and call decide_trade()
      − If YES/NO: portfolio gate → Claude gate → size → record paper trade

Idempotency: PortfolioGuard.has_open_for_market() prevents duplicate
trades on the same market.
"""
import logging
from datetime import datetime, timedelta
from typing import Optional

from sqlalchemy.orm import Session

from config.settings import settings
from data.storage import engine, PolymarketMarket, PolymarketBookSnapshot
from data import kalshi_client
from data.reference_tracker import reference_for, btc_price_at
from models.digital_option import BinaryQuote, decide_trade
from models.realized_vol import estimate_sigma_per_minute, latest_btc_price
from risk.portfolio import PortfolioGuard
from risk.position_sizer import size_trade
from execution.wallet import Wallet, record_paper_trade

logger = logging.getLogger(__name__)


def _latest_book(condition_id: str) -> Optional[PolymarketBookSnapshot]:
    with Session(engine) as session:
        return (session.query(PolymarketBookSnapshot)
                .filter(PolymarketBookSnapshot.condition_id == condition_id)
                .order_by(PolymarketBookSnapshot.id.desc())
                .first())


def evaluate_market(market: PolymarketMarket, wallet: Wallet, portfolio: PortfolioGuard,
                     notifier=None, claude_gate=None) -> Optional[dict]:
    """Run one decision pass on one market.

    Returns the recorded trade dict if we fired, else None.
    """
    now = datetime.utcnow()
    minutes_to_close = (market.resolution_ts - now).total_seconds() / 60.0
    if minutes_to_close <= 0 or minutes_to_close > settings.max_minutes_to_close:
        return None
    if minutes_to_close * 60 < settings.min_seconds_to_close:
        return None

    book = _latest_book(market.condition_id)
    if book is None or book.yes_bid is None or book.yes_ask is None:
        return None

    yes_mid = (float(book.yes_bid) + float(book.yes_ask)) / 2.0
    no_ask = 1.0 - float(book.yes_bid)   # buy NO = (1 - best_yes_bid)
    yes_ask = float(book.yes_ask)

    btc_now = latest_btc_price()
    if btc_now is None:
        return None

    # Reference price: from market record, else infer from BTC tick at hour boundary
    ref_price = market.reference_price
    if ref_price is None:
        ref_price = reference_for(market)
    if ref_price is None:
        return None

    sigma = estimate_sigma_per_minute()
    if sigma <= 0:
        return None

    quote = BinaryQuote(
        symbol=market.condition_id,
        reference_price=float(ref_price),
        current_price=float(btc_now),
        minutes_to_close=minutes_to_close,
        sigma_per_minute=sigma,
        implied_yes_prob=yes_mid,
    )
    intent = decide_trade(quote)

    if intent.side == "SKIP":
        return None

    # ── Extreme-tail filter — no lottery tickets ─────────────────────
    # The digital pricer is most reliable when implied is between 5%
    # and 95%. At the tails (<5% or >95%) tiny vol-estimate noise
    # creates apparent 'edge' that's really just market makers being
    # right. The 30-min live run lost \$75 buying YES tokens at 2¢ here.
    # 60-min markets have real uncertainty 30+ min from close, so the
    # tight bound is appropriate (no need to loosen for activity).
    side_ask = yes_ask if intent.side == "YES" else no_ask
    if side_ask < 0.05 or side_ask > 0.95:
        logger.info(
            f"[{market.condition_id}] tail-filter: skipping {intent.side} "
            f"at ask {side_ask:.3f} (extreme tail; model edge is likely noise)"
        )
        return None

    # Portfolio gate
    can, gate_reason = portfolio.can_open(market.condition_id)
    if not can:
        logger.info(f"[{market.condition_id}] portfolio block: {gate_reason}")
        return None

    # Claude gate (optional). Notification policy:
    #   • REJECT → log only (no Telegram noise — vetoes were spammy)
    #   • REQUEST_SIZE_CUT → log only (still a trade, fires below)
    #   • APPROVE / UNAVAILABLE → silent, normal flow
    size_mult = 1.0
    if claude_gate is not None:
        verdict = claude_gate(market, quote, intent, wallet)
        if verdict["decision"] == "REJECT":
            logger.info(
                f"[{market.condition_id}] CLAUDE REJECT {intent.side}: "
                f"{verdict.get('reason', '')[:160]}"
            )
            return None
        elif verdict["decision"] == "REQUEST_SIZE_CUT":
            size_mult = 0.5
            logger.info(
                f"[{market.condition_id}] CLAUDE HALF-SIZE {intent.side}: "
                f"{verdict.get('reason', '')[:160]}"
            )

    # Size the trade
    bankroll = wallet.available_balance()
    sizing = size_trade(
        side=intent.side,
        model_yes_prob=intent.model_yes_prob,
        yes_ask=yes_ask,
        no_ask=no_ask,
        bankroll=bankroll,
    )
    if sizing.size_usd <= 0:
        return None
    final_size = round(sizing.size_usd * size_mult, 2)
    if final_size < 1.0:
        return None

    # ── LIVE branch: place a real Polymarket order ──────────────────
    # effective_mode() reads gs://.../poly_live/mode.txt at runtime so the
    # dashboard can flip live↔paper without a restart.
    from monitor.halt_flag import effective_mode
    fill_units = sizing.units
    fill_price = sizing.paid_per_unit
    if effective_mode() == "live":
        from execution.polymarket_orders import (
            place_market_order, cap_for_smoke_period, daily_loss_halts,
            LIVE_MAX_TRADE_USD,
        )
        # Daily loss halt
        from execution.wallet import Wallet as _W
        today_pnl = _W().realized_pnl()
        if daily_loss_halts(today_pnl):
            if notifier:
                notifier.send(
                    f"🛑 <b>Daily loss halt</b> — refusing new entries "
                    f"(today P&L ${today_pnl:+.2f})"
                )
            return None

        # ── Real on-chain balance gate ───────────────────────────────
        # The paper `wallet.available_balance()` is starting_capital + paper
        # P&L − open paper risk. In LIVE mode we must size against the
        # REAL pUSD sitting in the Safe, not the simulated bankroll, or
        # the CLOB will reject orders bigger than what's actually there.
        try:
            from data.polymarket_wallet import _fetch_balance
            real_bal = _fetch_balance() or {}
            real_avail = float(real_bal.get("effective") or 0.0)
        except Exception as e:
            logger.warning(
                f"[{market.condition_id}] live balance fetch failed: {e}; "
                "refusing live order to avoid blind sizing"
            )
            return None
        if real_avail < 1.0:
            logger.info(
                f"[{market.condition_id}] live skipped: real pUSD ${real_avail:.2f}"
            )
            return None
        # Leave a 5% buffer for the 1% CLOB fee + slippage.
        max_real_size = round(real_avail * 0.95, 2)
        if final_size > max_real_size:
            logger.info(
                f"[{market.condition_id}] live size capped: paper sized "
                f"${final_size:.2f}, real pUSD allows ${max_real_size:.2f}"
            )
            final_size = max_real_size
        if final_size < 1.0:
            return None

        # Smoke-period cap (first 24h)
        bot_started = getattr(evaluate_market, "_bot_started_at", datetime.utcnow())
        evaluate_market._bot_started_at = bot_started
        capped_size = cap_for_smoke_period(final_size, bot_started)
        if capped_size < final_size:
            logger.info(
                f"[{market.condition_id}] smoke-period cap: "
                f"${final_size:.2f} → ${capped_size:.2f}"
            )
            final_size = capped_size

        # Choose token to BUY: YES token for a YES bet, NO token for a NO bet
        token_id = (market.yes_token_id if intent.side == "YES"
                     else market.no_token_id)
        if not token_id:
            if notifier:
                notifier.send(
                    f"⚠️ <code>{market.condition_id}</code>: missing "
                    f"{intent.side} token id — cannot place live order"
                )
            return None
        ask = yes_ask if intent.side == "YES" else no_ask
        fill = place_market_order(token_id, intent.side, final_size, ask)
        if not fill.success:
            err = fill.raw_response.get("error", "unknown")
            if notifier:
                notifier.send(
                    f"❌ <b>LIVE order failed</b> on {intent.side} "
                    f"<code>{market.condition_id}</code>\n"
                    f"💡 {err[:200]}"
                )
            return None
        fill_units = fill.units
        fill_price = fill.paid_per_unit

    # Persist trade row (paper or live; outcome backfilled at settlement)
    decision_id = record_paper_trade(
        condition_id=market.condition_id,
        side=intent.side,
        btc_price=float(btc_now),
        reference_price=float(ref_price),
        minutes_to_close=minutes_to_close,
        sigma_per_minute=sigma,
        model_yes_prob=intent.model_yes_prob,
        implied_yes_prob=intent.implied_yes_prob,
        edge=intent.edge,
        size_usd=final_size,
        paid_per_unit=fill_price,
    )

    icon = "🟢" if intent.side == "YES" else "🔴"
    if notifier:
        from strategies.cumulative import mode_tag
        _mt = mode_tag()
        notifier.send(
            f"{icon} <b>[{_mt}] {intent.side} fired</b> <code>{market.condition_id}</code>\n"
            f"📊 model={intent.model_yes_prob*100:.1f}% vs implied={intent.implied_yes_prob*100:.1f}%\n"
            f"💸 edge={intent.edge*100:+.1f}% | "
            f"size=${final_size:.2f} ({sizing.units:.0f} contracts @ {sizing.paid_per_unit*100:.0f}¢)\n"
            f"⏱ T-{minutes_to_close:.1f}min | BTC=${btc_now:,.2f} | ref=${ref_price:,.2f}\n"
            f"💡 {intent.reason}"
        )

    return {
        "decision_id": decision_id,
        "side": intent.side,
        "size_usd": final_size,
        "edge": intent.edge,
        "minutes_to_close": minutes_to_close,
    }


def decision_tick_impl(notifier=None, claude_gate=None) -> int:
    """Sweep every active market within firing window. Returns count of fires."""
    wallet = Wallet()
    portfolio = PortfolioGuard()
    fires = 0

    now = datetime.utcnow()
    upper_cutoff = now + timedelta(minutes=settings.max_minutes_to_close)
    lower_cutoff = now + timedelta(seconds=settings.min_seconds_to_close)

    with Session(engine) as session:
        markets = (session.query(PolymarketMarket)
                   .filter(PolymarketMarket.state == "active",
                           PolymarketMarket.resolution_ts >= lower_cutoff,
                           PolymarketMarket.resolution_ts <= upper_cutoff)
                   .all())

    for m in markets:
        try:
            result = evaluate_market(m, wallet, portfolio, notifier=notifier,
                                       claude_gate=claude_gate)
            if result:
                fires += 1
        except Exception as e:
            logger.warning(f"[{m.condition_id}] decision eval failed: {e}")

    return fires


def settle_resolved_markets() -> int:
    """For every market whose resolution time has passed, settle any paper
    trades against the BTC price at the close timestamp.

    Returns count settled."""
    from data.storage import Decision
    from execution.wallet import settle_paper_trade

    settled = 0
    now = datetime.utcnow()
    with Session(engine) as session:
        # Find unresolved decisions whose market has closed
        decisions = (session.query(Decision)
                     .filter(Decision.mode == settings.trading_mode,
                             Decision.resolution_yes.is_(None))
                     .all())
        # For each, look up the market
        for d in decisions:
            m = (session.query(PolymarketMarket)
                 .filter(PolymarketMarket.condition_id == d.condition_id)
                 .one_or_none())
            if not m or m.resolution_ts > now:
                continue
            # BTC price at the close timestamp
            close_price = btc_price_at(m.resolution_ts, window_minutes=2)
            if close_price is None:
                continue
            settle_paper_trade(d.id, close_price)
            settled += 1
    return settled
