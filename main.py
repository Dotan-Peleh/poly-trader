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
from data import binance_ws, polymarket_client, reference_tracker
from monitor.notifier import Notifier
from monitor.heartbeat import write_heartbeat
from monitor.halt_flag import is_halted

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger(__name__)


def polymarket_refresh_tick(notifier: Notifier):
    if is_halted():
        logger.info("HALT active — skipping market refresh")
        return
    try:
        n = polymarket_client.refresh_markets()
        logger.info(f"polymarket_refresh: {n} active BTC hourly markets")
    except Exception as e:
        logger.error(f"polymarket_refresh failed: {e}")


def polymarket_book_tick():
    """Snapshot order books for every active market with end_date in next 30 min."""
    if is_halted():
        return
    try:
        from sqlalchemy.orm import Session
        from data.storage import engine, PolymarketMarket
        from models.realized_vol import latest_btc_price
        cutoff = datetime.utcnow() + timedelta(minutes=30)
        with Session(engine) as session:
            ms = (session.query(PolymarketMarket)
                  .filter(PolymarketMarket.state == "active",
                          PolymarketMarket.resolution_ts <= cutoff,
                          PolymarketMarket.resolution_ts >= datetime.utcnow())
                  .all())
        btc_now = latest_btc_price()
        for m in ms:
            polymarket_client.snapshot_market(
                condition_id=m.condition_id,
                yes_token=m.yes_token_id,
                no_token=m.no_token_id,
                btc_price_now=btc_now,
            )
        if ms:
            logger.info(f"polymarket_book: snapshotted {len(ms)} markets")
    except Exception as e:
        logger.error(f"polymarket_book_tick failed: {e}")


def reference_backfill_tick():
    if is_halted():
        return
    try:
        n = reference_tracker.backfill_reference_prices()
        if n:
            logger.info(f"reference_backfill: filled {n} markets")
    except Exception as e:
        logger.error(f"reference_backfill failed: {e}")


def decision_tick():
    """Phase 2 will populate this. For now just logs that it fired."""
    if is_halted():
        return
    logger.debug("decision_tick: (not implemented yet — Phase 2)")


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
    scheduler.add_job(polymarket_refresh_tick, "interval", minutes=2,
                       args=[notifier], id="polymarket_refresh")
    scheduler.add_job(polymarket_book_tick, "interval", seconds=30,
                       id="polymarket_book")
    scheduler.add_job(reference_backfill_tick, "interval", minutes=5,
                       id="reference_backfill")
    scheduler.add_job(decision_tick, "interval", seconds=5,
                       id="decision_tick")
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
