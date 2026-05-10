"""
Early-exit manager — cash out Polymarket positions before resolution.

Polymarket binaries are sellable any time before close. Once we hold
YES (or NO) tokens at avg cost C, we can sell them at the current
market mid M. Profit per share = M − C.

Default exit rules (all ratios are of M / C):
  • TAKE_PROFIT_RATIO = 1.5  → if mid is 50%+ above our avg cost, sell
  • STOP_LOSS_RATIO   = 0.5  → if mid is 50%+ below our avg cost, sell
  • EDGE_FLIP_THRESHOLD = -0.04 → if our model edge has flipped to
                                  -4%+ against us, sell regardless of P&L
  • TIME_LEFT_BAILOUT_SEC = 120 → if a position is in the red and
                                   <2 min remain, sell rather than gamble
                                   on the tail

For paper mode: simulates the sell at the current mid, marks the
decision row as resolved with pnl = sell_proceeds − cost − fees.

For live mode: places a real SELL order (with the same py-clob-client
flow as opening) and updates the decision row when the fill clears.
"""
import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Optional

from sqlalchemy.orm import Session

from config.settings import settings
from data.storage import engine, Decision, PolymarketMarket, PolymarketBookSnapshot
from data import polymarket_client
from monitor.halt_flag import effective_mode

logger = logging.getLogger(__name__)


# Tunables
TAKE_PROFIT_RATIO = 1.5      # mid / cost ≥ 1.5 → lock in
STOP_LOSS_RATIO = 0.5        # mid / cost ≤ 0.5 → cap loss
EDGE_FLIP_THRESHOLD = -0.04  # model says edge has flipped -4% against us
TIME_LEFT_BAILOUT_SEC = 120  # in red + <2 min left → bail
MIN_HOLD_SECONDS = 30        # don't churn — must hold ≥30s before exit


@dataclass
class ExitDecision:
    decision_id: int
    condition_id: str
    side: str
    cost_per_share: float
    current_mid: float
    units: float
    pnl_usd: float
    reason: str


def _latest_book(condition_id: str) -> Optional[PolymarketBookSnapshot]:
    with Session(engine) as session:
        return (session.query(PolymarketBookSnapshot)
                .filter(PolymarketBookSnapshot.condition_id == condition_id)
                .order_by(PolymarketBookSnapshot.id.desc())
                .first())


def _resolve_paper_at_mid(decision_id: int, current_mid: float, reason: str
                          ) -> Optional[dict]:
    """Mark a paper decision as resolved at the current mid (early exit).
    The pnl is units × current_mid − size_usd − fees."""
    PAPER_FEE_RATE = 0.01
    with Session(engine) as session:
        d = session.get(Decision, decision_id)
        if d is None or d.resolution_yes is not None:
            return None
        units = float(d.units_bought or 0)
        size = float(d.size_usd or 0)
        side = d.side
        if side == "YES":
            proceeds = units * current_mid
        else:
            # NO position: we own NO tokens worth (1-yes_mid) per share
            proceeds = units * (1 - current_mid)
        fee = size * PAPER_FEE_RATE
        pnl_usd = proceeds - size - fee

        # Resolution: derive boolean win/loss from realized P&L so summary
        # queries that filter on resolution_yes IS NOT NULL pick this up.
        # (Previously left as None, hiding early exits from win-rate stats.)
        won = pnl_usd > 0
        if side == "YES":
            d.resolution_yes = 1 if won else 0
        else:  # NO bet — won if BTC ended NO; resolution_yes 0 = NO won
            d.resolution_yes = 0 if won else 1
        d.pnl_usd = round(pnl_usd, 4)
        d.resolved_at = datetime.utcnow()
        # Preserve original strategy tag so v1/v2 A/B can still be split.
        # Format: early_exit:reason|original_tag (e.g. early_exit:tp_1.81x|v2_meanrev)
        prev = (d.notes or "").strip()
        d.notes = f"early_exit:{reason}|{prev}" if prev else f"early_exit:{reason}"
        session.commit()

        return {
            "decision_id": d.id,
            "side": side,
            "current_mid": current_mid,
            "proceeds": proceeds,
            "fee": fee,
            "pnl_usd": d.pnl_usd,
            "reason": reason,
        }


def _live_sell(condition_id: str, side: str, units: float,
                token_id_yes: str, token_id_no: str,
                book) -> Optional[dict]:
    """Place a live SELL on Polymarket via py-clob-client.
    For YES position: sell YES tokens at the current best bid.
    For NO position: sell NO tokens at the current best NO bid.
    """
    try:
        from execution.polymarket_orders import _get_client
        from py_clob_client.clob_types import OrderArgs, OrderType

        # Choose token + price
        if side == "YES":
            token_id = token_id_yes
            limit_price = float(book.yes_bid or 0)
        else:
            token_id = token_id_no
            no_bid = 1 - float(book.yes_ask or 1)
            limit_price = no_bid
        if limit_price <= 0 or limit_price >= 1:
            return None
        # Cross slightly into the bid for fill certainty
        cross_price = round(max(limit_price - 0.005, 0.005), 3)
        client = _get_client()
        args = OrderArgs(token_id=token_id, price=cross_price,
                          size=round(units, 4), side="SELL")
        signed = client.create_order(args)
        resp = client.post_order(signed, OrderType.GTC)
        return {
            "success": bool(resp and resp.get("success")),
            "limit_price": cross_price,
            "raw": resp or {},
        }
    except Exception as e:
        logger.error(f"_live_sell failed: {e}")
        return {"success": False, "error": str(e)[:200]}


def evaluate_open_positions(notifier=None) -> int:
    """Scan all open paper/live decisions, close any that hit exit rules.
    Returns count closed."""
    closed = 0
    now = datetime.utcnow()
    with Session(engine) as session:
        opens = (session.query(Decision)
                 .filter(Decision.mode == settings.trading_mode,
                         Decision.resolution_yes.is_(None),
                         Decision.pnl_usd.is_(None))
                 .all())
        # Pull market info for each
        for d in opens:
            try:
                m = (session.query(PolymarketMarket)
                     .filter(PolymarketMarket.condition_id == d.condition_id)
                     .one_or_none())
                if not m:
                    continue
                # Time gates
                age_sec = (now - d.ts).total_seconds() if d.ts else 0
                if age_sec < MIN_HOLD_SECONDS:
                    continue
                seconds_to_close = (m.resolution_ts - now).total_seconds()
                if seconds_to_close <= 0:
                    continue  # let regular settlement handle it

                book = _latest_book(d.condition_id)
                if not book or book.yes_bid is None or book.yes_ask is None:
                    continue
                yes_mid = (float(book.yes_bid) + float(book.yes_ask)) / 2.0

                # Compute current value vs cost
                cost = float(d.paid_per_unit or 0)
                if cost <= 0:
                    continue
                if d.side == "YES":
                    cur_per_share = yes_mid
                else:
                    cur_per_share = 1.0 - yes_mid
                ratio = cur_per_share / cost if cost else 0
                pnl_pct = (cur_per_share - cost) / cost if cost else 0

                # Decide: take-profit / stop-loss / time-bailout
                exit_reason = None
                if ratio >= TAKE_PROFIT_RATIO:
                    exit_reason = f"tp_{ratio:.2f}x"
                elif ratio <= STOP_LOSS_RATIO:
                    exit_reason = f"sl_{ratio:.2f}x"
                elif (seconds_to_close < TIME_LEFT_BAILOUT_SEC
                       and pnl_pct < 0):
                    exit_reason = f"time_bailout_{seconds_to_close:.0f}s"

                if not exit_reason:
                    continue

                logger.info(
                    f"[{d.condition_id}] EARLY EXIT {d.side} "
                    f"cost={cost:.2f} mid={cur_per_share:.2f} "
                    f"ratio={ratio:.2f}x reason={exit_reason}"
                )

                if effective_mode() == "live":
                    fill = _live_sell(d.condition_id, d.side,
                                       float(d.units_bought or 0),
                                       m.yes_token_id, m.no_token_id, book)
                    if not fill or not fill.get("success"):
                        if notifier:
                            notifier.send(
                                f"⚠️ Early-exit SELL FAILED <code>{d.condition_id}</code>\n"
                                f"💡 {(fill or {}).get('error', 'unknown')[:120]}"
                            )
                        continue

                # Always also book the paper P&L (for both modes the
                # decision row's pnl_usd reflects the realised exit P&L)
                result = _resolve_paper_at_mid(d.id, yes_mid, exit_reason)
                if result and notifier:
                    icon = "🎯" if result["pnl_usd"] > 0 else "🔴"
                    tag = "WIN" if result["pnl_usd"] > 0 else "LOSS"
                    mode_label = "LIVE" if effective_mode() == "live" else "PAPER"
                    notifier.send(
                        f"{icon} <b>EARLY {tag} ({mode_label})</b> <code>{d.condition_id[:20]}…</code>\n"
                        f"📊 {d.side} @ {cost*100:.0f}¢ → exit @ {cur_per_share*100:.0f}¢\n"
                        f"💸 P&amp;L: <b>${result['pnl_usd']:+.4f}</b>\n"
                        f"💡 reason: {exit_reason}"
                    )
                closed += 1
            except Exception as e:
                logger.warning(f"[{d.condition_id}] exit eval error: {e}")
    return closed
