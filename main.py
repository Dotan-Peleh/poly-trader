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
from apscheduler.events import EVENT_JOB_EXECUTED

from config.settings import settings
from monitor.health import start_health_server, mark_alive
from data.storage import init_db
from data import binance_ws, polymarket_client, reference_tracker
# Kalshi client kept available for venue switching but not wired by default
from data import kalshi_client  # noqa: F401
from monitor.notifier import Notifier
from monitor.heartbeat import write_heartbeat
from monitor.halt_flag import is_halted

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger(__name__)


def polymarket_refresh_tick(notifier: Notifier):
    """Pull active 5-min BTC up/down events from Polymarket Gamma API."""
    if is_halted():
        logger.info("HALT active — skipping market refresh")
        return
    try:
        n = polymarket_client.refresh_markets(
            window_minutes=settings.polymarket_window_minutes
        )
        logger.info(f"polymarket_refresh: {n} active {settings.polymarket_window_minutes}-min BTC markets")
    except Exception as e:
        logger.error(f"polymarket_refresh failed: {e}")


def polymarket_book_tick():
    """Snapshot YES + NO books for every market resolving in next 35 min.

    Pre_window strategy fires at 6-30 min to resolution. Smart-money copy
    strategy fetches books ON-DEMAND when it needs them (a tracked wallet
    just entered a sports/politics market), so it doesn't need this tick
    to cover long-horizon markets — that would explode the HTTP load."""
    if is_halted():
        return
    try:
        from sqlalchemy.orm import Session
        from data.storage import engine, PolymarketMarket
        from models.realized_vol import latest_btc_price
        cutoff = datetime.utcnow() + timedelta(minutes=35)
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


def exit_manager_tick(notifier: Notifier):
    """Every 10s: scan open positions for take-profit/stop-loss/time-bailout
    early exits. Polymarket binaries are sellable any time before close.
    Works in BOTH paper and live mode."""
    if is_halted():
        return
    try:
        from strategies.exit_manager import evaluate_open_positions
        closed = evaluate_open_positions(notifier=notifier)
        if closed:
            logger.info(f"exit_manager: closed {closed} positions early")
    except Exception as e:
        logger.error(f"exit_manager_tick failed: {e}")


def wallet_snapshot_tick():
    """Pull Polymarket wallet state (balance + positions + recent trades)
    and upload a JSON snapshot to GCS for the dashboard. Runs in BOTH
    paper and live mode so the dashboard always reflects the real wallet."""
    if is_halted():
        return
    try:
        from data.polymarket_wallet import write_wallet_snapshot
        write_wallet_snapshot()
    except Exception as e:
        logger.error(f"wallet_snapshot_tick failed: {e}")


def decision_tick(notifier: Notifier):
    """Phase 2 v2: pre-window strategy. Fires BEFORE the 5-min measurement
    window opens, when books are at ~50/50 with real depth. See
    strategies/pre_window.py for the full thesis."""
    if is_halted():
        return
    try:
        from strategies.pre_window import decision_tick_impl
        # claude_gate is wired for late_window's signature; pre_window v1
        # ignores it (passes None). v2 will adapt the gate to use signal
        # decomposition instead of vol-based BinaryQuote.
        n = decision_tick_impl(notifier=notifier, claude_gate=None)
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
        # (so we can capture and ping the actual P&L).
        # No mode filter — paper AND live rows both need settle/notify.
        before_ids = set()
        with Session(engine) as session:
            for d in session.query(Decision).filter(
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
            from strategies.cumulative import today_cumulative_line
            for d in newly_resolved:
                won = bool(d.pnl_usd is not None and d.pnl_usd > 0)
                icon = "🎯" if won else "🔴"
                tag = "WIN" if won else "LOSS"
                cum = today_cumulative_line()
                # Tag per row's actual mode — a paper-tagged decision
                # always reports as PAPER even if the bot is currently
                # in live mode, and vice versa.
                _mt = "LIVE" if d.mode == "live" else "PAPER"
                notifier.send(
                    f"{icon} <b>[{_mt}] {tag}</b> <code>{d.condition_id}</code>\n"
                    f"📊 {d.side} @ {d.paid_per_unit*100:.0f}¢ | "
                    f"size=${d.size_usd:.2f}\n"
                    f"💸 P&amp;L: <b>${d.pnl_usd:+,.2f}</b>\n"
                    f"💡 model={d.model_yes_prob*100:.1f}% vs implied={d.implied_yes_prob*100:.1f}%, "
                    f"resolution_yes={d.resolution_yes}\n"
                    f"{cum}"
                )
        logger.info(f"settle_tick: {n} decisions settled")
    except Exception as e:
        logger.error(f"settle_tick failed: {e}")


def main():
    parser = argparse.ArgumentParser(description="poly-trader bot")
    parser.add_argument("--mode", choices=["paper", "live"], default="paper")
    parser.add_argument("--capital", type=float, default=settings.starting_capital)
    parser.add_argument("--yes", action="store_true",
                         help="Auto-accept LIVE mode confirmation (for systemd)")
    args = parser.parse_args()

    if args.mode == "live":
        # Safety check — refuse to start live unless all 5 secrets resolve
        from execution.polymarket_orders import live_readiness_check
        ready, missing = live_readiness_check()
        if not ready:
            print(f"\n❌ Cannot start LIVE: missing secrets {missing}")
            return
        print("\n" + "=" * 60)
        print("WARNING: LIVE MODE — real USDC will be traded on Polymarket")
        print("Polygon wallet must have USDC and gas. Trade-only signing key.")
        print(f"Funder: {settings.polymarket_funder_address}")
        print(f"Per-trade cap: $5 (first 24h)  |  Daily loss halt: -$10")
        print("=" * 60)
        if not args.yes:
            confirm = input("Type exactly 'yes i understand the risk' to proceed: ")
            if confirm.strip() != "yes i understand the risk":
                print("Aborted.")
                return
        else:
            logger.warning("LIVE mode auto-confirmed via --yes flag")

    init_db()
    notifier = Notifier()

    # Run Binance WS in a daemon thread (keeps main thread free for scheduler)
    ws_thread = threading.Thread(target=binance_ws.run_forever, daemon=True, name="binance_ws")
    ws_thread.start()
    logger.info("Binance WS thread started")

    # ── Phase 2: Polygon RPC real-time stream ──────────────────────────
    # Subscribe to OrderFilled events from Polymarket CTF Exchange on chain.
    # Sub-second latency from on-chain confirmation to signal. Only starts
    # if Alchemy key is available (graceful degrade for paper/dev runs).
    if getattr(settings, "polygon_alchemy_api_key", ""):
        try:
            from data.polygon_stream import start_stream_in_thread, RealTimeSignal

            # Use a closure to track stream health: count events, log a
            # heartbeat every 100. Confirms the WebSocket is alive even
            # when no tracked-wallet trades come through.
            _stream_stats = {"events": 0, "tracked_hits": 0, "last_log": 0}

            def _on_realtime_signal(sig):
                import time as _tm
                _stream_stats["events"] += 1
                # Heartbeat
                if (_stream_stats["events"] % 500 == 0
                    or _tm.time() - _stream_stats["last_log"] > 300):
                    logger.info(
                        f"polygon_stream: heartbeat — "
                        f"{_stream_stats['events']} events, "
                        f"{_stream_stats['tracked_hits']} hits"
                    )
                    _stream_stats["last_log"] = _tm.time()
                try:
                    from sqlalchemy.orm import Session
                    from sqlalchemy import text as _t
                    from data.storage import engine as _eng
                    with Session(_eng) as s:
                        row = s.execute(_t(
                            "SELECT pseudonym, realized_pnl_lifetime FROM "
                            "smart_wallet_rankings WHERE wallet = :w"
                        ), {"w": sig.wallet}).first()
                    if not row:
                        return  # not on our radar
                    _stream_stats["tracked_hits"] += 1
                    pseudonym = row[0] or "anon"
                    lifetime = float(row[1] or 0)
                    logger.info(
                        f"polygon_stream: ⚡ {sig.wallet[:12]} ({pseudonym}, "
                        f"${lifetime:,.0f}) × {sig.amount_filled:.0f} block {sig.block_number}"
                    )
                    # Phase 3: trigger immediate copy via the same path the
                    # 30s poll uses. We construct a SmartSignal and dispatch
                    # to execute_copy_trades synchronously here. This drops
                    # entry latency from ~30s to <2s.
                    fired = False
                    try:
                        from strategies.smart_money import (
                            SmartSignal, execute_copy_trades,
                        )
                        # We don't have condition_id from the stream event
                        # directly — but we DO have the asset_id (token_id).
                        # Look up condition_id from polymarket_markets table.
                        with Session(_eng) as s:
                            mrow = s.execute(_t(
                                "SELECT condition_id, question FROM polymarket_markets "
                                "WHERE yes_token_id = :a OR no_token_id = :a"
                            ), {"a": sig.asset_id}).first()
                        if mrow is None:
                            # Market not in our DB yet — let the 30s poll
                            # path handle it (it auto-adds via gamma API)
                            return
                        cid, title = mrow[0], mrow[1] or ""
                        # Determine outcome side: yes or no
                        with Session(_eng) as s:
                            yrow = s.execute(_t(
                                "SELECT yes_token_id FROM polymarket_markets "
                                "WHERE condition_id = :c"
                            ), {"c": cid}).first()
                        outcome = "Yes" if yrow and yrow[0] == sig.asset_id else "No"
                        # Build SmartSignal mirroring the structure
                        from datetime import datetime as _dt, timezone as _tz
                        ss = SmartSignal(
                            wallet=sig.wallet,
                            wallet_name=pseudonym,
                            wallet_lifetime_pnl=lifetime,
                            condition_id=cid,
                            asset_id=sig.asset_id,
                            market_title=title,
                            outcome=outcome,
                            new_size=sig.amount_filled,
                            delta_size=sig.amount_filled,  # treat stream hit
                                                            # as a delta event
                            current_price=0.0,             # not in stream
                            last_fill_price=None,          # let drift check
                                                            # pull from book
                            last_fill_ts=sig.received_at,
                            detected_at=_dt.now(_tz.utc).isoformat(),
                        )
                        n = execute_copy_trades([ss], notifier=notifier)
                        if n:
                            fired = True
                            logger.info(
                                f"polygon_stream: ⚡⚡ INSTANT COPY fired "
                                f"({pseudonym} @ block {sig.block_number})"
                            )
                    except Exception as e:
                        logger.warning(f"polygon_stream copy attempt: {e}")
                    # Record stream hit for dashboard visibility
                    try:
                        from data.firehose import record_stream_hit
                        record_stream_hit(
                            _eng, wallet=sig.wallet, pseudonym=pseudonym,
                            lifetime_pnl=lifetime, asset_id=sig.asset_id,
                            amount=sig.amount_filled, tx_hash=sig.tx_hash,
                            block_number=sig.block_number,
                            fired_copy=fired,
                        )
                    except Exception as e:
                        logger.debug(f"polygon_stream record_hit: {e}")
                except Exception as e:
                    logger.warning(f"polygon_stream signal handler: {e}")

            from data.storage import engine as _eng_for_stream
            start_stream_in_thread(settings.polygon_alchemy_api_key,
                                     _on_realtime_signal,
                                     engine=_eng_for_stream)
        except Exception as e:
            logger.error(f"polygon_stream failed to start: {e}")
    else:
        logger.info("polygon_stream: no Alchemy key set — Phase 2 disabled")

    # Sidecar watchdog reads /health on port 8766; mark_alive on every job
    # so a deadlocked scheduler triggers HTTP 503 within 10 min.
    start_health_server()
    scheduler = BackgroundScheduler(timezone="UTC")
    scheduler.add_listener(
        lambda evt: mark_alive(evt.job_id),
        EVENT_JOB_EXECUTED,
    )
    scheduler.add_job(polymarket_refresh_tick, "interval", minutes=2,
                       args=[notifier], id="polymarket_refresh")
    scheduler.add_job(polymarket_book_tick, "interval", seconds=20,
                       id="polymarket_book")
    scheduler.add_job(reference_backfill_tick, "interval", minutes=5,
                       id="reference_backfill")
    # ── v2_meanrev HALTED 2026-05-19 (post-execution-fix validation) ─────
    # The 2026-05-17 execution-cost fix delivered its predicted change in
    # fire rate (8.0→5.0/hr, −37.5%) but DID NOT unlock the edge:
    #   POST paper (n=200, 40h): WR 38.0%, Wilson 95% CI [31.6%, 44.9%].
    #   Fee-adjusted breakeven = 51.0%. CI ceiling 6pp below breakeven →
    #   strategy is statistically losing at 95% confidence even AFTER fix.
    # Calibration gap on the 0.50–0.60 bucket: 19.7pp → 17.8pp (negligible).
    # Conclusion: the problem is MODEL CALIBRATION, not execution friction.
    # exit_manager_tick + settle_tick stay enabled so open v2 positions
    # resolve cleanly. Smart-money copy continues with per-wallet caps.
    # Re-enable: uncomment after recalibrating model_yes_prob (isotonic /
    # Platt fit on 0.50–0.60 bucket where 95% of v2 trades live).
    # scheduler.add_job(decision_tick, "interval", seconds=5,
    #                    args=[notifier], id="decision_tick")
    scheduler.add_job(exit_manager_tick, "interval", seconds=10,
                       args=[notifier], id="exit_manager_tick")
    scheduler.add_job(settle_tick, "interval", seconds=60,
                       args=[notifier], id="settle_tick")
    scheduler.add_job(write_heartbeat, "interval", minutes=5,
                       id="heartbeat")
    scheduler.add_job(wallet_snapshot_tick, "interval", seconds=30,
                       id="wallet_snapshot")
    # Twice-daily Telegram summary so user can see win/loss + calibration
    # without opening the dashboard. Times chosen to land in Israel
    # daytime hours (11:00 + 23:00 IL = 08:00 + 20:00 UTC).
    from strategies.summary import poly_summary
    scheduler.add_job(poly_summary, "cron", hour=8, minute=0,
                       args=["morning", "🌅", notifier], id="poly_summary_morning",
                       coalesce=True, max_instances=1, misfire_grace_time=600)
    scheduler.add_job(poly_summary, "cron", hour=20, minute=0,
                       args=["evening", "🌙", notifier], id="poly_summary_evening",
                       coalesce=True, max_instances=1, misfire_grace_time=600)
    # Smart-money copy-trading — Phase 2. Re-rank wallet list every 12h;
    # poll positions every 90s. Phase 3 (copy execution) hooks decision_tick.
    from strategies.smart_money import refresh_tick as smart_refresh_tick
    from strategies.smart_money import poll_tick as smart_poll_tick
    scheduler.add_job(smart_refresh_tick, "cron", hour="*/12", minute=5,
                       args=[notifier], id="smart_money_refresh",
                       coalesce=True, max_instances=1, misfire_grace_time=600)
    # 30s poll for amazing real-time scanning — 3x faster than 90s baseline,
    # gets us out of positions BEFORE other copy bots even detect the exit
    scheduler.add_job(smart_poll_tick, "interval", seconds=30,
                       args=[notifier], id="smart_money_poll",
                       coalesce=True, max_instances=1, misfire_grace_time=60)
    # ─── Phase 1: Trade firehose ─────────────────────────────────────
    # Polls Polymarket's /trades endpoint every 2s and captures EVERY
    # trade across the platform — not just our 18 hardcoded whales.
    # Then ranks the wallet universe every 5 min to dynamically promote
    # newly-emerged smart money. Phase 2 will replace polling with
    # Polygon RPC WebSocket via Alchemy for <1s latency.
    from data.firehose import (
        init_firehose_schema, ingest_tick as firehose_ingest_tick,
        rank_wallets_from_firehose,
    )
    from data.storage import engine as _engine
    init_firehose_schema(_engine)

    def _firehose_ingest():
        try:
            n = firehose_ingest_tick(_engine)
            if n:
                logger.debug(f"firehose: +{n} trades")
        except Exception as e:
            logger.warning(f"firehose ingest failed: {e}")

    def _firehose_rank():
        try:
            n = rank_wallets_from_firehose(_engine, lookback_days=7, top_n=500)
            if n:
                logger.info(f"firehose: ranked {n} smart wallets (top 500)")
        except Exception as e:
            logger.error(f"firehose rank failed: {e}")

    # /trades has a ~5-min processing lag (cache or indexer delay) — polling
    # at 2s is wasteful. 60s captures every fresh window without spamming.
    # For real-time copy signals we use Polygon RPC WebSocket (Phase 2).
    scheduler.add_job(_firehose_ingest, "interval", seconds=60,
                       id="firehose_ingest",
                       coalesce=True, max_instances=1, misfire_grace_time=30)
    scheduler.add_job(_firehose_rank, "cron", minute="*/15",
                       id="firehose_rank",
                       coalesce=True, max_instances=1, misfire_grace_time=600)
    # Prune firehose trades older than 24h every hour + VACUUM the DB.
    # Critical for keeping poly.db under ~200MB so the GCS sync (1/min)
    # completes within its budget — without this the DB grew to 600MB
    # and dashboard data went 50min stale.
    def _firehose_prune():
        try:
            from data.firehose import prune_old_rows
            prune_old_rows(_engine, keep_hours=24, vacuum=True)
        except Exception as e:
            logger.error(f"firehose prune failed: {e}")
    scheduler.add_job(_firehose_prune, "cron", minute=7,
                       id="firehose_prune",
                       coalesce=True, max_instances=1, misfire_grace_time=300)
    # Daily BigQuery export at 00:30 UTC — dumps decisions / stream_hits /
    # wallet_rankings to gs://crypto-trader-backups-494710 first, then
    # WRITE_TRUNCATE-loads into poly_trader_analytics.* tables. User can
    # query via BigQuery console for ad-hoc analytics.
    def _bq_export_tick():
        try:
            from data.bq_export import export_all
            export_all(_engine)
        except Exception as e:
            logger.error(f"bq_export failed: {e}")
    scheduler.add_job(_bq_export_tick, "cron", hour=0, minute=30,
                       id="bq_export",
                       coalesce=True, max_instances=1, misfire_grace_time=3600)
    scheduler.start()
    logger.info("Scheduler started")

    # Startup message must reflect REAL state, not the hard-coded launch
    # args. Mode is the runtime flag in GCS (the dashboard toggles it).
    # Capital is the on-chain pUSD balance — your actual trading USD —
    # not the static $100 starting_capital config.
    try:
        from monitor.halt_flag import effective_mode as _eff_mode
        runtime_mode = _eff_mode() or args.mode
    except Exception:
        runtime_mode = args.mode
    try:
        from data.polymarket_wallet import _fetch_pusd_balance
        real_pusd = _fetch_pusd_balance(settings.polymarket_funder_address)
    except Exception:
        real_pusd = None
    cap_line = (
        f"Capital (pUSD on-chain): ${real_pusd:,.2f}"
        if real_pusd is not None
        else f"Capital (config): ${args.capital:,.2f}"
    )
    notifier.send(
        f"🎲 <b>poly-trader started</b>\n"
        f"Mode: {runtime_mode}\n"
        f"{cap_line}\n"
        f"Phase 1: data feeds (BTC ticks + Polymarket books)"
    )
    # Observability + caps active as of 2026-05-19. v2_meanrev is firing
    # to validate the 2026-05-17 execution-cost fix (predicted WR 31% → 45-55%).
    # If post-resume rigor check shows WR still <45% at n≥50, halt v2.
    notifier.send(
        "⛔ <b>v2_meanrev HALTED + smart_money caps active</b>\n"
        "Post-fix data (paper, n=200, 40h): WR 38%, Wilson CI [31.6, 44.9], "
        "breakeven 51% — CI ceiling 6pp below breakeven. "
        "Execution-cost fix cut fire rate as predicted but did NOT unlock "
        "edge. Problem is model calibration (17.8pp overconfident).\n"
        "✅ smart_money still firing with per-wallet caps (3/$50 per day)\n"
        "🔄 exit_manager + settle keep running for open v2 positions"
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
