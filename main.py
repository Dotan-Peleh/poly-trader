"""
poly-trader main orchestrator.

Phase 1 ticks:
  • binance_ws.run_forever (background thread) — 1s BTC ticks → DB
  • polymarket_refresh_tick (every 2 min) — refresh active hourly markets
  • polymarket_book_tick (every 30s) — snapshot order books for active markets
  • reference_backfill_tick (every 5 min) — fill in reference prices for new markets
  • heartbeat_tick (every 5 min) — GCS heartbeat for watchdog

Phase 2+ ticks (placeholders, wired but no logic yet):
  • decision_tick (every 5s during last 15min of each hourly market)
"""
import argparse
import logging
import threading
import time
from datetime import datetime, timedelta

from apscheduler.schedulers.background import BackgroundScheduler

from config.settings import settings
from data.storage import init_db
from data import binance_ws, kalshi_client, reference_tracker
from monitor.notifier import Notifier
from monitor.heartbeat import write_heartbeat
from monitor.halt_flag import is_halted

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger(__name__)


def kalshi_refresh_tick(notifier: Notifier):
    """Pull active KXBTC15M events into the markets table."""
    if is_halted():
        logger.info("HALT active — skipping market refresh")
        return
    try:
        n = kalshi_client.refresh_markets()
        logger.info(f"kalshi_refresh: {n} active KXBTC15M markets")
    except Exception as e:
        logger.error(f"kalshi_refresh failed: {e}")


def kalshi_book_tick():
    """Snapshot the order book for every active market resolving in next 20 min."""
    if is_halted():
        return
    try:
        from sqlalchemy.orm import Session
        from data.storage import engine, PolymarketMarket
        from models.realized_vol import latest_btc_price
        cutoff = datetime.utcnow() + timedelta(minutes=20)
        with Session(engine) as session:
            ms = (session.query(PolymarketMarket)
                  .filter(PolymarketMarket.state == "active",
                          PolymarketMarket.resolution_ts <= cutoff,
                          PolymarketMarket.resolution_ts >= datetime.utcnow())
                  .all())
        btc_now = latest_btc_price()
        for m in ms:
            kalshi_client.snapshot_market(
                market_ticker=m.condition_id,
                btc_price_now=btc_now,
            )
        if ms:
            logger.info(f"kalshi_book: snapshotted {len(ms)} markets")
    except Exception as e:
        logger.error(f"kalshi_book_tick failed: {e}")


def reference_backfill_tick():
    if is_halted():
        return
    try:
        n = reference_tracker.backfill_reference_prices()
        if n:
            logger.info(f"reference_backfill: filled {n} markets")
    except Exception as e:
        logger.error(f"reference_backfill failed: {e}")


def decision_tick(notifier: Notifier):
    """Phase 2: sweep markets in firing window, run pricer + Claude gate,
    record paper trades."""
    if is_halted():
        return
    try:
        from strategies.late_window import decision_tick_impl
        from claude_decision.decision import claude_gate
        n = decision_tick_impl(notifier=notifier, claude_gate=claude_gate)
        if n:
            logger.info(f"decision_tick: fired {n} paper trade(s)")
    except Exception as e:
        logger.error(f"decision_tick failed: {e}")


def settle_tick(notifier: Notifier):
    """Settle resolved paper trades — runs every minute. Pings Telegram on
    each settlement so user sees outcomes in real time."""
    if is_halted():
        return
    try:
        from strategies.late_window import settle_resolved_markets
        from execution.wallet import Wallet
        from sqlalchemy.orm import Session
        from data.storage import engine, Decision

        # Find unresolved decisions whose markets have closed before settling
        # (so we can capture and ping the actual P&L)
        before_ids = set()
        with Session(engine) as session:
            for d in session.query(Decision).filter(
                Decision.mode == settings.trading_mode,
                Decision.resolution_yes.is_(None),
            ).all():
                before_ids.add(d.id)

        n = settle_resolved_markets()
        if n == 0:
            return

        # Re-query to find which decisions just resolved
        with Session(engine) as session:
            newly_resolved = session.query(Decision).filter(
                Decision.id.in_(before_ids),
                Decision.resolution_yes.isnot(None),
            ).all()
            for d in newly_resolved:
                won = bool(d.pnl_usd is not None and d.pnl_usd > 0)
                icon = "🎯" if won else "🔴"
                tag = "WIN" if won else "LOSS"
                notifier.send(
                    f"{icon} <b>{tag}</b> <code>{d.condition_id}</code>\n"
                    f"📊 {d.side} @ {d.paid_per_unit*100:.0f}¢ | "
                    f"size=${d.size_usd:.2f}\n"
                    f"💸 P&amp;L: <b>${d.pnl_usd:+,.2f}</b>\n"
                    f"💡 model={d.model_yes_prob*100:.1f}% vs implied={d.implied_yes_prob*100:.1f}%, "
                    f"resolution_yes={d.resolution_yes}"
                )
        logger.info(f"settle_tick: {n} decisions settled")
    except Exception as e:
        logger.error(f"settle_tick failed: {e}")


def main():
    parser = argparse.ArgumentParser(description="poly-trader bot")
    parser.add_argument("--mode", choices=["paper", "live"], default="paper")
    parser.add_argument("--capital", type=float, default=settings.starting_capital)
    args = parser.parse_args()

    if args.mode == "live":
        print("\n" + "=" * 60)
        print("WARNING: LIVE MODE — real USDC will be traded on Polymarket")
        print("Polygon wallet must have USDC and gas. Trade-only signing key.")
        print("=" * 60)
        confirm = input("Type exactly 'yes i understand the risk' to proceed: ")
        if confirm.strip() != "yes i understand the risk":
            print("Aborted."); return

    init_db()
    notifier = Notifier()

    # Run Binance WS in a daemon thread (keeps main thread free for scheduler)
    ws_thread = threading.Thread(target=binance_ws.run_forever, daemon=True, name="binance_ws")
    ws_thread.start()
    logger.info("Binance WS thread started")

    scheduler = BackgroundScheduler(timezone="UTC")
    scheduler.add_job(kalshi_refresh_tick, "interval", minutes=2,
                       args=[notifier], id="kalshi_refresh")
    scheduler.add_job(kalshi_book_tick, "interval", seconds=30,
                       id="kalshi_book")
    scheduler.add_job(reference_backfill_tick, "interval", minutes=5,
                       id="reference_backfill")
    scheduler.add_job(decision_tick, "interval", seconds=5,
                       args=[notifier], id="decision_tick")
    scheduler.add_job(settle_tick, "interval", seconds=60,
                       args=[notifier], id="settle_tick")
    scheduler.add_job(write_heartbeat, "interval", minutes=5,
                       id="heartbeat")
    scheduler.start()
    logger.info("Scheduler started")

    notifier.send(
        f"🎲 <b>poly-trader started</b>\n"
        f"Mode: {args.mode}\n"
        f"Capital: ${args.capital:,.2f}\n"
        f"Phase 1: data feeds (BTC ticks + Polymarket books)"
    )

    try:
        # Block forever — scheduler is on background thread, WS is on its own
        while True:
            time.sleep(60)
    except KeyboardInterrupt:
        logger.info("shutting down")
        scheduler.shutdown()
        notifier.send("🎲 <b>poly-trader stopped</b>")


if __name__ == "__main__":
    main()
