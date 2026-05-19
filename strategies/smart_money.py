"""
Smart-money copy-trading strategy for poly-trader.

Phase 1 (smart_wallets.py — offline): discover top wallets by lifetime P&L
who are currently active. Outputs gs://crypto-trader-backups-494710/
poly_live/smart_wallets.json.

Phase 2 (this module): poll each tracked wallet every N seconds via
data-api.polymarket.com/positions, snapshot their position state. When
a NEW position appears (or existing one grows), emit a SmartSignal.

Phase 3 (decision_tick wiring): on each SmartSignal, decide whether to
copy. Gates: market must be tradable (yes_ask+no_ask < 1.05, real depth),
we must not be already in the position, our bankroll must support
Kelly-fraction sized stake.

Phase 4 (exit_manager hook): when a tracked wallet's position SHRINKS,
exit our copy at market.

Persistence: position snapshots stored in a small JSON file so we can
detect deltas across restarts. (SQLite would be heavier and we don't
need transactional guarantees here.)
"""
import json
import logging
import os
import time
import urllib.error
import urllib.request
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)

DATA_API = "https://data-api.polymarket.com"
LB_API = "https://lb-api.polymarket.com"

# Where Phase 1 wrote the wallet list and where Phase 2 caches snapshots
_REPO_ROOT = Path(__file__).resolve().parent.parent
WALLET_LIST_PATH = str(_REPO_ROOT / "var" / "smart_wallets.json")
SNAPSHOT_PATH = str(_REPO_ROOT / "var" / "smart_wallet_positions.json")

# Smart-wallet selection thresholds (used by refresh_smart_wallets)
MIN_LIFETIME_PNL = 1_000_000
MAX_DAYS_SINCE_LAST_TRADE = 14
TARGET_WALLET_COUNT = 50               # raised from 30 — more scanning surface

# Phase 4 exit thresholds
EXIT_REDUCTION_THRESHOLD = 0.30        # if smart wallet drops position size by
                                        # ≥30%, treat as exit signal and close
                                        # OUR copy 100%. Faster than waiting for
                                        # full close — the goal is to get out
                                        # BEFORE they finish dumping.
STALE_ENTRY_MAX_AGE_MIN = 15           # don't copy a smart-money entry that's
                                        # already > 15 min old. By then the
                                        # easy edge is gone and other copy bots
                                        # are likely already in.

# Daily cap — fire at most this many copy trades per UTC day. Higher-quality
# signals get priority via the score-based adaptive threshold.
# CAN BE OVERRIDDEN from the dashboard via GCS runtime_config.json (see
# _runtime_overrides() — refreshed every 60s).
DAILY_COPY_CAP = 200

# ── Per-wallet concentration caps (added 2026-05-19) ─────────────────────
# Live data over 87 trades (2026-05-09 → 2026-05-12) showed top 2 wallets
# contributed +$193 of the $171 total PnL — the rest of the book was
# net-negative. The strategy's apparent edge depended on a single lucky
# copy ($23.55 trade → +$100). These caps prevent any single wallet from
# making OR breaking the strategy:
#   • At most N copies per UTC day from any one wallet
#   • At most $X notional exposure per UTC day to any one wallet
# Both overridable via runtime_config.json so the dashboard can tune.
MAX_COPIES_PER_WALLET_PER_DAY = 3
MAX_NOTIONAL_PER_WALLET_PER_DAY_USD = 50.0

# Runtime overrides loaded from GCS so dashboard can tune without restart.
# Cached for 60s in-process to bound GCS read load.
_RUNTIME_CONFIG = {"data": {}, "fetched_at": 0.0}


def _runtime_overrides() -> dict:
    """Fetch the dashboard's runtime_config.json from GCS, 60s cache.
    Returns {} on any failure so callers can fall back to in-source defaults."""
    import time as _tm
    if _tm.time() - _RUNTIME_CONFIG["fetched_at"] < 60:
        return _RUNTIME_CONFIG["data"]
    try:
        from google.cloud import storage as _gcs
        import json as _json
        client = _gcs.Client()
        blob = (client.bucket("crypto-trader-backups-494710")
                .blob("poly_live/runtime_config.json"))
        if blob.exists():
            _RUNTIME_CONFIG["data"] = _json.loads(blob.download_as_text())
    except Exception as e:
        logger.debug(f"runtime_overrides fetch failed: {e}")
    _RUNTIME_CONFIG["fetched_at"] = _tm.time()
    return _RUNTIME_CONFIG["data"]


def _effective_daily_cap() -> int:
    return int(_runtime_overrides().get("daily_cap", DAILY_COPY_CAP))


def _effective_edge_min() -> float:
    return float(_runtime_overrides().get("edge_min_pct", 4.0)) / 100.0


def _effective_edge_max() -> float:
    return float(_runtime_overrides().get("edge_max_pct", 10.0)) / 100.0


def _effective_max_copies_per_wallet() -> int:
    return int(_runtime_overrides().get("max_copies_per_wallet_per_day",
                                          MAX_COPIES_PER_WALLET_PER_DAY))


def _effective_max_notional_per_wallet() -> float:
    return float(_runtime_overrides().get("max_notional_per_wallet_per_day_usd",
                                            MAX_NOTIONAL_PER_WALLET_PER_DAY_USD))


def _todays_wallet_exposure(wallet: str) -> tuple:
    """(count, total_size_usd) of TODAY's smart_copy fires from this wallet.

    Looks at BOTH the new source_wallet column (post-2026-05-19 migration)
    AND the legacy notes pattern (`0xWALLET[:12]`), so pre-migration rows
    are still counted toward the cap during the transition window.
    """
    try:
        from sqlalchemy.orm import Session
        from sqlalchemy import text as _t
        from data.storage import engine as _eng
        # Use the 12-char prefix for both: source_wallet stores the full
        # 42-char address; notes embed only `wallet[:12]`. LIKE-prefix on
        # the column gives an indexed match without needing exact equality.
        prefix = wallet[:12]
        with Session(_eng) as s:
            r = s.execute(_t("""
                SELECT COUNT(*), COALESCE(SUM(size_usd), 0.0)
                FROM decisions
                WHERE ts >= datetime('now', 'start of day')
                  AND size_usd > 0
                  AND (source_wallet LIKE :w || '%'
                       OR notes LIKE '%' || :w || '%')
            """), {"w": prefix}).first()
        return int(r[0] or 0), float(r[1] or 0.0)
    except Exception:
        return 0, 0.0


def _todays_copy_count() -> int:
    """Count of smart_copy fires fired since UTC midnight."""
    try:
        from sqlalchemy.orm import Session
        from sqlalchemy import text as _t
        from data.storage import engine as _eng
        with Session(_eng) as s:
            r = s.execute(_t("""
                SELECT COUNT(*) FROM decisions
                WHERE notes LIKE '%smart_copy%'
                  AND ts >= datetime('now', 'start of day')
                  AND size_usd > 0
            """)).first()
        return int(r[0] or 0)
    except Exception:
        return 0


def _quality_score(*, lifetime_pnl: float, edge: float, size_usd: float,
                    implied: float) -> float:
    """Composite signal quality. Higher = better.
      lifetime_pnl_log × edge_pct × (1+size_log) × (1 - tail_penalty/2)
    Bounds typical 0-2000."""
    import math
    lpnl_log = math.log(1 + max(lifetime_pnl, 0))    # 14-16 for $1M-$8M
    edge_pct = max(edge, 0) * 100                     # 0-10 typical
    size_log = math.log(1 + max(size_usd, 0))         # 0-5
    tail_pen = abs(implied - 0.50) * 2                # 0 @ 0.50, 1 @ 0.10/0.90
    return lpnl_log * edge_pct * (1 + size_log) * (1 - 0.5 * tail_pen)


def _adaptive_min_score(today_count: int, cap: int = DAILY_COPY_CAP) -> float:
    """Adaptive threshold that rises as we approach the daily cap.
    Early in day: low threshold so we fire on moderate-quality signals.
    Late in day: high threshold so only the absolute best slip through."""
    if today_count >= cap:
        return float("inf")  # hard cap
    headroom = (cap - today_count) / cap
    # Smooth scaling: ~50 at start of day, ~200 when 80% full
    return 50 + (1 - headroom) ** 2 * 200


# ── HTTP helper ──────────────────────────────────────────────────────────

def _get_json(url: str, timeout: float = 15.0):
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read())


# ── Wallet discovery (Phase 1, called periodically by scheduler) ─────────

def refresh_smart_wallets() -> List[dict]:
    """Re-rank: pull top profit leaderboard, filter to recently-active.

    Persists to WALLET_LIST_PATH for Phase 2 to consume.
    Returns the list of wallets selected.
    """
    try:
        # Top 500 by all-time profit. After filtering to wallets that
        # traded in last 14 days we typically keep 30-50 active wallets.
        top = _get_json(f"{LB_API}/profit?window=All&limit=500")
    except Exception as e:
        logger.error(f"smart_money: leaderboard fetch failed: {e}")
        return []

    if not isinstance(top, list):
        logger.error(f"smart_money: leaderboard returned {type(top).__name__}")
        return []

    now = datetime.now(timezone.utc).timestamp()
    cutoff = now - MAX_DAYS_SINCE_LAST_TRADE * 86400
    active = []
    for w_info in top:
        wallet = (w_info.get("proxyWallet") or "").lower()
        lifetime = float(w_info.get("amount") or 0)
        if not wallet.startswith("0x") or lifetime < MIN_LIFETIME_PNL:
            continue
        # Check last trade timestamp
        try:
            trades = _get_json(f"{DATA_API}/trades?user={wallet}&limit=1", timeout=10)
        except Exception:
            continue
        if not isinstance(trades, list) or not trades:
            continue
        last_ts = trades[0].get("timestamp", 0)
        if last_ts <= cutoff:
            continue
        active.append({
            "wallet": wallet,
            "name": w_info.get("pseudonym") or w_info.get("name") or "anon",
            "lifetime_pnl": lifetime,
            "last_trade_ts": last_ts,
            "days_since_last_trade": round((now - last_ts) / 86400, 1),
        })
        if len(active) >= TARGET_WALLET_COUNT:
            break
        time.sleep(0.05)

    active.sort(key=lambda x: -x["lifetime_pnl"])
    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "wallets": active,
    }
    Path(WALLET_LIST_PATH).parent.mkdir(parents=True, exist_ok=True)
    with open(WALLET_LIST_PATH, "w") as f:
        json.dump(payload, f, indent=2)
    logger.info(f"smart_money: refreshed {len(active)} active smart wallets, "
                 f"top: {active[0]['name'] if active else '-'}")
    return active


def _load_wallet_list() -> List[dict]:
    if not os.path.exists(WALLET_LIST_PATH):
        return []
    try:
        with open(WALLET_LIST_PATH) as f:
            return json.load(f).get("wallets", [])
    except Exception as e:
        logger.warning(f"smart_money: wallet list load failed: {e}")
        return []


# ── Position snapshotting (Phase 2) ──────────────────────────────────────

@dataclass
class SmartSignal:
    """Emitted when a tracked wallet enters / grows a position."""
    wallet: str
    wallet_name: str
    wallet_lifetime_pnl: float
    condition_id: str
    asset_id: str        # the YES or NO token id
    market_title: str
    outcome: str         # "Yes" / "No" — which side they bet
    new_size: float      # current size of their position
    delta_size: float    # how much it grew vs previous snapshot
    current_price: float  # smart wallet's avg price (across all fills)
    last_fill_price: Optional[float]  # most-recent fill (best proxy for entry now)
    last_fill_ts: Optional[float]     # unix seconds — for stale-entry filter
    detected_at: str


@dataclass
class SmartExitSignal:
    """Emitted when a tracked wallet reduces position by ≥ EXIT_REDUCTION_THRESHOLD.
    Triggers close of our matching copy position."""
    wallet: str
    wallet_name: str
    condition_id: str
    market_title: str
    outcome: str            # side they HAD (we should be on the same side if we copied)
    prev_size: float
    new_size: float
    reduction_pct: float    # 0.30 = 30% reduction
    detected_at: str


def _fetch_last_trade(wallet: str, asset_id: str) -> tuple:
    """Most-recent fill (price, timestamp) for this wallet on this specific
    asset (token_id). Returns (None, None) if not found.

    This is more accurate than the avgPrice from /positions because it
    reflects what they paid moments ago, not their lifetime average.
    """
    try:
        # Hit /trades?user=X&limit=20 and filter to this asset
        trades = _get_json(f"{DATA_API}/trades?user={wallet}&limit=20", timeout=8)
        if not isinstance(trades, list):
            return (None, None)
        for t in trades:
            if t.get("asset") != asset_id:
                continue
            if (t.get("side") or "").upper() != "BUY":
                continue
            return (float(t.get("price") or 0), float(t.get("timestamp") or 0))
    except Exception as e:
        logger.debug(f"_fetch_last_trade({wallet[:12]}, {asset_id[:8]}): {e}")
    return (None, None)


def _load_snapshots() -> Dict[str, dict]:
    if not os.path.exists(SNAPSHOT_PATH):
        return {}
    try:
        with open(SNAPSHOT_PATH) as f:
            return json.load(f)
    except Exception:
        return {}


def _save_snapshots(snaps: Dict[str, dict]) -> None:
    Path(SNAPSHOT_PATH).parent.mkdir(parents=True, exist_ok=True)
    tmp = SNAPSHOT_PATH + ".tmp"
    with open(tmp, "w") as f:
        json.dump(snaps, f)
    os.replace(tmp, SNAPSHOT_PATH)


def poll_smart_wallets(notifier=None):
    """Pull /positions for each tracked wallet, diff against previous snapshot.
    Emits two kinds of signals:
      • SmartSignal     — new open / grew ≥ 10% — for Phase 3 (copy entry)
      • SmartExitSignal — reduced ≥ EXIT_REDUCTION_THRESHOLD — for Phase 4

    Returns (entry_signals, exit_signals)."""
    wallets = _load_wallet_list()
    if not wallets:
        logger.info("smart_money: no wallet list (run refresh_smart_wallets first)")
        return [], []

    snaps = _load_snapshots()
    entry_signals: List[SmartSignal] = []
    exit_signals: List[SmartExitSignal] = []

    for w in wallets:
        wallet = w["wallet"]
        try:
            pos = _get_json(f"{DATA_API}/positions?user={wallet}&sizeThreshold=1&limit=100",
                            timeout=10)
        except Exception as e:
            logger.debug(f"smart_money: positions fetch failed for {wallet[:12]}: {e}")
            continue
        if not isinstance(pos, list):
            continue

        # Build current state
        cur = {}
        for p in pos:
            asset = p.get("asset") or ""
            sz = float(p.get("size") or 0)
            if sz <= 0 or not asset:
                continue
            cur[asset] = {
                "size": sz,
                "price": float(p.get("avgPrice") or p.get("price") or 0),
                "market_title": p.get("title") or "",
                "outcome": p.get("outcome") or "",
                "condition_id": p.get("conditionId") or "",
            }

        prev = snaps.get(wallet, {}).get("positions", {})

        # ── Detect NEW OPENS / GROWTH (Phase 3 entry signals) ──
        for asset, info in cur.items():
            prev_size = float(prev.get(asset, {}).get("size") or 0)
            delta = info["size"] - prev_size
            if delta > 0 and (prev_size == 0 or delta / max(prev_size, 1) > 0.10):
                # Fetch latest fill price + timestamp for accurate copy
                last_price, last_ts = _fetch_last_trade(wallet, asset)
                entry_signals.append(SmartSignal(
                    wallet=wallet,
                    wallet_name=w["name"],
                    wallet_lifetime_pnl=w["lifetime_pnl"],
                    condition_id=info["condition_id"],
                    asset_id=asset,
                    market_title=info["market_title"],
                    outcome=info["outcome"],
                    new_size=info["size"],
                    delta_size=delta,
                    current_price=info["price"],
                    last_fill_price=last_price,
                    last_fill_ts=last_ts,
                    detected_at=datetime.now(timezone.utc).isoformat(),
                ))

        # ── Detect EXITS / REDUCTIONS (Phase 4 exit signals) ──
        # We compute fraction reduced from PREV. If position size dropped by
        # at least EXIT_REDUCTION_THRESHOLD, signal copy-exit. Catches the
        # FIRST sign of unwind, not waiting for full close.
        for asset, prev_info in prev.items():
            prev_size = float(prev_info.get("size") or 0)
            new_size = float(cur.get(asset, {}).get("size") or 0)
            if prev_size <= 0:
                continue
            reduction = (prev_size - new_size) / prev_size
            if reduction >= EXIT_REDUCTION_THRESHOLD:
                exit_signals.append(SmartExitSignal(
                    wallet=wallet,
                    wallet_name=w["name"],
                    condition_id=prev_info.get("condition_id", ""),
                    market_title=prev_info.get("market_title", ""),
                    outcome=prev_info.get("outcome", ""),
                    prev_size=prev_size,
                    new_size=new_size,
                    reduction_pct=reduction,
                    detected_at=datetime.now(timezone.utc).isoformat(),
                ))
                logger.info(f"smart_money: SMART EXIT {w['name']} "
                             f"{prev_info.get('market_title','?')[:45]} "
                             f"{prev_size:.0f}→{new_size:.0f} "
                             f"(-{reduction*100:.0f}%)")

        snaps[wallet] = {
            "name": w["name"],
            "positions": cur,
            "last_polled": datetime.now(timezone.utc).isoformat(),
        }

        time.sleep(0.05)  # be polite to API but faster than before

    _save_snapshots(snaps)

    # Note: we deliberately do NOT Telegram-ping every detected entry signal.
    # Most signals get filtered out (price drift, stale entry, edge gate, etc)
    # and only ~10% become actual copies. User only wants to see real
    # BUY (COPIED) and SELL (CLOSED COPY) events — those are sent by
    # execute_copy_trades and execute_copy_exits respectively.

    if entry_signals or exit_signals:
        logger.info(f"smart_money: {len(entry_signals)} new entries + "
                     f"{len(exit_signals)} exits from {len(wallets)} wallets")
    return entry_signals, exit_signals


# ── Scheduler entrypoints ────────────────────────────────────────────────

def refresh_tick(notifier=None) -> None:
    """Re-rank smart wallets daily. Scheduler: cron hour=*/12."""
    try:
        n = len(refresh_smart_wallets())
        if notifier is not None and n:
            notifier.send(f"🐳 smart-money wallet list refreshed: {n} active wallets")
    except Exception as e:
        logger.error(f"smart_money refresh_tick failed: {e}")


def poll_tick(notifier=None) -> None:
    """Poll tracked wallets and act on signals. Scheduler: interval seconds=30."""
    try:
        entry_signals, exit_signals = poll_smart_wallets(notifier=notifier)
        if exit_signals:
            # Process exits FIRST — we want to free capital before entering new positions
            n = execute_copy_exits(exit_signals, notifier=notifier)
            if n:
                logger.info(f"smart_money: exited {n}/{len(exit_signals)} smart-exit signals")
        if entry_signals:
            n = execute_copy_trades(entry_signals, notifier=notifier)
            if n:
                logger.info(f"smart_money: copied {n}/{len(entry_signals)} entry signals")
    except Exception as e:
        logger.error(f"smart_money poll_tick failed: {e}")


# ── Phase 3: copy execution ──────────────────────────────────────────────

# Assumed-win-probability tiers based on smart wallet's track record.
# Smart wallets with $5M+ lifetime aren't just lucky — assume real edge.
# This is the prior; bounded so a single signal can't blow up Kelly sizing.
SMART_WIN_PROB_TIERS = [
    (10_000_000, 0.62),
    (5_000_000,  0.58),
    (2_000_000,  0.55),
    (1_000_000,  0.53),
]
DEFAULT_SMART_WIN_PROB = 0.52

# How much worse than smart wallet's avg price are we willing to pay?
# The /positions endpoint returns avgPrice across ALL their fills, which
# may span days. By the time we see them, the book has often moved 10-30¢.
# Set to 0.25 — generous enough to actually fire on real signals while
# still rejecting markets where the easy money is clearly gone.
# Future v2: compare to most-recent /trades fill instead of avgPrice.
MAX_PRICE_DRIFT_FROM_SMART = 0.25  # absolute, in probability units


def _assumed_win_prob(lifetime_pnl: float) -> float:
    for threshold, prob in SMART_WIN_PROB_TIERS:
        if lifetime_pnl >= threshold:
            return prob
    return DEFAULT_SMART_WIN_PROB


def execute_copy_trades(signals: list, notifier=None) -> int:
    """For each SmartSignal, fire a paper copy trade if all gates pass.
    Returns count of fires. Skipped signals are NOT errors — most signals
    will be in markets we don't track (sports / politics) which we can't
    trade since we don't have books for them. v2 will expand discovery."""
    # Late imports — keep smart_money module loadable even if these are
    # being modified by a parallel session
    from sqlalchemy.orm import Session
    from sqlalchemy import update
    from datetime import datetime as _dt, timedelta as _td
    from data.storage import (
        engine, PolymarketMarket, PolymarketBookSnapshot, Decision,
        record_rejection, RejectReason,
    )
    from execution.wallet import Wallet, record_paper_trade
    from risk.portfolio import PortfolioGuard
    from risk.position_sizer import size_trade
    from models.realized_vol import estimate_sigma_per_minute, latest_btc_price
    from monitor.halt_flag import effective_mode as _eff_mode

    # Snapshot current mode once per tick — used to tag every rejection
    # so dashboard mode-divergence checks have correct attribution.
    _mode_now = _eff_mode() or "paper"

    if not signals:
        return 0

    wallet = Wallet()
    portfolio = PortfolioGuard()
    fires = 0
    untrackable = 0  # signals on markets we don't have in our DB

    # Lazy import — heavy on first call (httpx client + gamma session)
    from data.polymarket_client import track_market_by_condition_id

    for sig in signals:
        # 0. STALE-ENTRY FILTER — don't copy yesterday's news.
        # Only fire if smart wallet's most-recent buy on this asset is
        # within STALE_ENTRY_MAX_AGE_MIN. Other copy bots watch too; if
        # their fill is > 15 min old we're racing into a worse price.
        if sig.last_fill_ts:
            age_min = (_dt.utcnow().timestamp() - sig.last_fill_ts) / 60.0
            if age_min > STALE_ENTRY_MAX_AGE_MIN:
                logger.info(f"smart_money: SKIP (entry {age_min:.0f}m old > "
                             f"{STALE_ENTRY_MAX_AGE_MIN}m) {sig.market_title[:40]}")
                record_rejection(mode=_mode_now, strategy="smart_copy",
                                  condition_id=sig.condition_id,
                                  source_wallet=sig.wallet,
                                  reject_reason=RejectReason.STALE_SIGNAL,
                                  reject_detail=f"{age_min:.0f}m > {STALE_ENTRY_MAX_AGE_MIN}m")
                continue

        # 1. Do we know this market? If not, auto-add it so book_tick can
        # start snapshotting. This is what unlocks copying SPORTS/POLITICS
        # markets where the actual smart money lives. First copy on a new
        # market will skip with "no book" — but the next 30s poll has one.
        with Session(engine) as session:
            m = (session.query(PolymarketMarket)
                 .filter(PolymarketMarket.condition_id == sig.condition_id)
                 .one_or_none())
        if m is None:
            if track_market_by_condition_id(sig.condition_id):
                # Re-query: it's now in our DB
                with Session(engine) as session:
                    m = (session.query(PolymarketMarket)
                         .filter(PolymarketMarket.condition_id == sig.condition_id)
                         .one_or_none())
            if m is None:
                untrackable += 1
                logger.info(f"smart_money: SKIP (could not track market) "
                             f"{sig.wallet_name}: {sig.market_title[:50]}")
                record_rejection(mode=_mode_now, strategy="smart_copy",
                                  condition_id=sig.condition_id,
                                  source_wallet=sig.wallet,
                                  reject_reason=RejectReason.UNTRACKED_MARKET,
                                  reject_detail=sig.market_title[:120])
                continue

        # 2. Resolves too soon?
        now = _dt.utcnow()
        mins_to_close = (m.resolution_ts - now).total_seconds() / 60.0
        if mins_to_close < 2.0:
            logger.info(f"smart_money: SKIP (resolves in {mins_to_close:.1f}min) "
                         f"{sig.market_title[:50]}")
            record_rejection(mode=_mode_now, strategy="smart_copy",
                              condition_id=sig.condition_id,
                              source_wallet=sig.wallet,
                              reject_reason=RejectReason.RESOLVES_TOO_SOON,
                              reject_detail=f"{mins_to_close:.1f}min")
            continue

        # 3. Latest book — fetch on-demand if we don't have a recent snapshot.
        # The regular polymarket_book_tick only covers markets resolving in
        # next 35 min (BTC binaries). Sports/politics markets have longer
        # horizons, so we have to fetch their books inline here.
        with Session(engine) as session:
            book = (session.query(PolymarketBookSnapshot)
                    .filter(PolymarketBookSnapshot.condition_id == sig.condition_id)
                    .order_by(PolymarketBookSnapshot.id.desc())
                    .first())
        snap_age = None
        if book is not None and book.ts is not None:
            snap_age = (_dt.utcnow() - book.ts).total_seconds()
        if book is None or snap_age is None or snap_age > 120:
            # Fetch + persist a fresh snapshot
            try:
                from data.polymarket_client import snapshot_market
                snapshot_market(
                    condition_id=sig.condition_id,
                    yes_token=m.yes_token_id,
                    no_token=m.no_token_id,
                )
                with Session(engine) as session:
                    book = (session.query(PolymarketBookSnapshot)
                            .filter(PolymarketBookSnapshot.condition_id == sig.condition_id)
                            .order_by(PolymarketBookSnapshot.id.desc())
                            .first())
            except Exception as e:
                logger.warning(f"smart_money: book fetch failed for "
                                f"{sig.market_title[:40]}: {e}")
                continue
        if book is None or book.yes_bid is None or book.yes_ask is None:
            logger.info(f"smart_money: SKIP (no book after fetch) {sig.market_title[:50]}")
            record_rejection(mode=_mode_now, strategy="smart_copy",
                              condition_id=sig.condition_id,
                              source_wallet=sig.wallet,
                              reject_reason=RejectReason.NO_BOOK)
            continue

        yes_bid = float(book.yes_bid)
        yes_ask = float(book.yes_ask)
        no_bid = float(book.no_bid) if book.no_bid is not None else (1.0 - yes_ask)
        no_ask = float(book.no_ask) if book.no_ask is not None else (1.0 - yes_bid)
        if yes_ask + no_ask > 1.05:
            logger.info(f"smart_money: SKIP (placeholder book) {sig.market_title[:50]}")
            record_rejection(mode=_mode_now, strategy="smart_copy",
                              condition_id=sig.condition_id,
                              source_wallet=sig.wallet,
                              reject_reason=RejectReason.BOOK_PLACEHOLDER,
                              reject_detail=f"sum={yes_ask+no_ask:.3f}")
            continue

        side = "YES" if sig.outcome.lower().startswith("y") else "NO"
        our_ask = yes_ask if side == "YES" else no_ask

        # 4. Has market run away from smart's entry?
        # Prefer last_fill_price (their actual recent buy) over current_price
        # (avgPrice across all fills — can be misleading on scaled-in positions).
        ref_price = sig.last_fill_price if sig.last_fill_price else sig.current_price
        # Polymarket's data-api sometimes returns avgPrice=0 for very-fresh
        # positions (the field hasn't been computed yet). If we have no
        # usable reference price, skip the drift gate rather than blocking
        # every signal — the tail filter below + Claude gate + edge gate
        # still protect against blind copies.
        if ref_price is not None and ref_price >= 0.02:
            if abs(our_ask - ref_price) > MAX_PRICE_DRIFT_FROM_SMART:
                logger.info(f"smart_money: SKIP (price drift {ref_price:.3f}→"
                             f"{our_ask:.3f}) {sig.market_title[:40]}")
                record_rejection(mode=_mode_now, strategy="smart_copy",
                                  condition_id=sig.condition_id,
                                  source_wallet=sig.wallet, side=side,
                                  implied_yes_prob=yes_ask if side == "YES" else 1.0 - yes_bid,
                                  reject_reason=RejectReason.PRICE_DRIFT,
                                  reject_detail=f"{ref_price:.3f}->{our_ask:.3f}")
                continue
        else:
            logger.info(f"smart_money: drift gate skipped (no ref price) "
                         f"{sig.market_title[:40]}")
        # 5. Tail filter — refuse lottery tickets even when smart bets them
        if our_ask < 0.05 or our_ask > 0.95:
            logger.info(f"smart_money: SKIP (tail {our_ask:.2f}) {sig.market_title[:40]}")
            record_rejection(mode=_mode_now, strategy="smart_copy",
                              condition_id=sig.condition_id,
                              source_wallet=sig.wallet, side=side,
                              reject_reason=RejectReason.TAIL,
                              reject_detail=f"paid={our_ask:.3f}")
            continue

        # 6. Idempotency — already have a position on this market?
        with Session(engine) as session:
            existing = (session.query(Decision)
                        .filter(Decision.condition_id == sig.condition_id,
                                Decision.size_usd > 0,
                                Decision.resolution_yes.is_(None))
                        .first())
        if existing:
            record_rejection(mode=_mode_now, strategy="smart_copy",
                              condition_id=sig.condition_id,
                              source_wallet=sig.wallet, side=side,
                              reject_reason=RejectReason.DUPLICATE_POSITION)
            continue

        # ── Per-wallet concentration cap (count) ─────────────────────
        # Block if this wallet has already used its daily count budget.
        # Notional check happens AFTER sizing so we know the actual $.
        wallet_count_today, wallet_notional_today = _todays_wallet_exposure(sig.wallet)
        max_count = _effective_max_copies_per_wallet()
        max_notional = _effective_max_notional_per_wallet()
        if wallet_count_today >= max_count:
            logger.info(f"smart_money: SKIP (wallet cap count "
                         f"{wallet_count_today}/{max_count}) {sig.wallet[:12]} "
                         f"{sig.market_title[:40]}")
            record_rejection(mode=_mode_now, strategy="smart_copy",
                              condition_id=sig.condition_id,
                              source_wallet=sig.wallet, side=side,
                              reject_reason=RejectReason.WALLET_CAP_COUNT,
                              reject_detail=f"{wallet_count_today}/{max_count}")
            continue

        # 7. Portfolio gate
        can, reason = portfolio.can_open(sig.condition_id)
        if not can:
            logger.info(f"smart_money: portfolio block — {reason}")
            record_rejection(mode=_mode_now, strategy="smart_copy",
                              condition_id=sig.condition_id,
                              source_wallet=sig.wallet, side=side,
                              reject_reason=RejectReason.CONCURRENT,
                              reject_detail=str(reason)[:120])
            continue

        # 8. Size via Kelly, using assumed win prob from smart's track record
        win_prob = _assumed_win_prob(sig.wallet_lifetime_pnl)
        # model_yes_prob for size_trade = our prob of YES winning
        model_yes_prob = win_prob if side == "YES" else (1.0 - win_prob)
        edge = (win_prob - our_ask)  # signed edge on the side we're buying
        if edge < _effective_edge_min():
            logger.info(f"smart_money: SKIP (edge {edge*100:+.1f}% < 4%) "
                         f"{sig.market_title[:40]}")
            record_rejection(mode=_mode_now, strategy="smart_copy",
                              condition_id=sig.condition_id,
                              source_wallet=sig.wallet, side=side,
                              model_yes_prob=model_yes_prob,
                              implied_yes_prob=our_ask if side == "YES" else 1.0 - our_ask,
                              reject_reason=RejectReason.EDGE_TOO_SMALL,
                              reject_detail=f"edge={edge*100:+.2f}%")
            continue

        sizing = size_trade(
            side=side,
            model_yes_prob=model_yes_prob,
            yes_ask=yes_ask,
            no_ask=no_ask,
            bankroll=wallet.available_balance(),
        )
        if sizing.size_usd < 1.0:
            record_rejection(mode=_mode_now, strategy="smart_copy",
                              condition_id=sig.condition_id,
                              source_wallet=sig.wallet, side=side,
                              intended_size_usd=float(sizing.size_usd),
                              reject_reason=RejectReason.BANKROLL,
                              reject_detail=f"size ${sizing.size_usd:.2f} < $1")
            continue
        final_size = round(sizing.size_usd, 2)

        # ── Per-wallet concentration cap (notional) ─────────────────
        # Clip-or-skip semantics: if the new trade would push us past the
        # daily $ budget for this wallet, skip rather than partial-fill.
        # Partial fills distort Kelly sizing and confuse calibration.
        if wallet_notional_today + final_size > max_notional:
            logger.info(f"smart_money: SKIP (wallet notional "
                         f"${wallet_notional_today:.0f}+${final_size:.0f} > "
                         f"${max_notional:.0f}) {sig.wallet[:12]}")
            record_rejection(mode=_mode_now, strategy="smart_copy",
                              condition_id=sig.condition_id,
                              source_wallet=sig.wallet, side=side,
                              intended_size_usd=final_size,
                              reject_reason=RejectReason.WALLET_CAP_NOTIONAL,
                              reject_detail=f"${wallet_notional_today:.0f}+${final_size:.0f}>${max_notional:.0f}")
            continue

        # ── Quality gate: score + adaptive threshold + daily cap ──
        implied = yes_ask if side == "YES" else (1 - yes_bid)
        score = _quality_score(
            lifetime_pnl=sig.wallet_lifetime_pnl,
            edge=edge,
            size_usd=final_size,
            implied=implied,
        )
        today_count = _todays_copy_count()
        eff_cap = _effective_daily_cap()
        min_score = _adaptive_min_score(today_count, eff_cap)
        if today_count >= eff_cap:
            global _CAP_ALERTED_DATE
            try:
                _alerted = _CAP_ALERTED_DATE
            except NameError:
                _alerted = None
            today_str = _dt.utcnow().strftime("%Y-%m-%d")
            if notifier and _alerted != today_str:
                notifier.send(
                    f"🛑 <b>Daily smart-copy cap reached</b>\n"
                    f"Hit {eff_cap} fires for today. New signals will\n"
                    f"be rejected until UTC midnight (~{24 - _dt.utcnow().hour}h from now)."
                )
                globals()["_CAP_ALERTED_DATE"] = today_str
            logger.info(f"smart_money: SKIP (daily cap {eff_cap} reached) "
                         f"{sig.market_title[:40]}")
            record_rejection(mode=_mode_now, strategy="smart_copy",
                              condition_id=sig.condition_id,
                              source_wallet=sig.wallet, side=side,
                              reject_reason=RejectReason.DAILY_CAP,
                              reject_detail=f"{today_count}/{eff_cap}")
            continue
        if score < min_score:
            logger.info(f"smart_money: SKIP (score {score:.0f} < threshold "
                         f"{min_score:.0f}, today={today_count}/{eff_cap}) "
                         f"{sig.market_title[:40]}")
            record_rejection(mode=_mode_now, strategy="smart_copy",
                              condition_id=sig.condition_id,
                              source_wallet=sig.wallet, side=side,
                              reject_reason=RejectReason.QUALITY_SCORE,
                              reject_detail=f"score={score:.0f}<{min_score:.0f}")
            continue
        # Edge upper cap from runtime overrides
        if abs(edge) > _effective_edge_max():
            logger.info(f"smart_money: SKIP (edge {edge*100:+.1f}% > max "
                         f"{_effective_edge_max()*100:.1f}%) {sig.market_title[:40]}")
            record_rejection(mode=_mode_now, strategy="smart_copy",
                              condition_id=sig.condition_id,
                              source_wallet=sig.wallet, side=side,
                              reject_reason=RejectReason.EDGE_TOO_LARGE,
                              reject_detail=f"edge={edge*100:+.2f}%")
            continue

        # ── Claude final gate (last call before fire) ──
        verdict = {"decision": "APPROVE", "confidence": 0.5,
                   "reason": "claude gate skipped"}
        try:
            from claude_decision.smart_money_gate import claude_gate
            verdict = claude_gate(
                wallet_name=sig.wallet_name,
                wallet_lifetime_pnl=sig.wallet_lifetime_pnl,
                market_title=sig.market_title,
                side=side,
                paid_per_unit=sizing.paid_per_unit,
                edge=edge,
                size_usd=final_size,
                minutes_to_close=mins_to_close,
                today_fire_count=today_count,
                daily_cap=eff_cap,
                quality_score=score,
            )
        except Exception as e:
            logger.warning(f"claude_gate import/call failed: {e}")
        if verdict.get("decision") == "REJECT":
            logger.info(f"smart_money: SKIP (claude REJECT — "
                         f"{verdict.get('reason','')[:80]}) {sig.market_title[:40]}")
            record_rejection(mode=_mode_now, strategy="smart_copy",
                              condition_id=sig.condition_id,
                              source_wallet=sig.wallet, side=side,
                              intended_size_usd=final_size,
                              reject_reason=RejectReason.CLAUDE_REJECT,
                              reject_detail=str(verdict.get("reason", ""))[:200])
            continue

        # 9. Fire trade — paper math vs real on-chain via place_market_order
        from monitor.halt_flag import effective_mode as _eff_mode
        is_live = (_eff_mode() == "live")
        fill_units = sizing.units
        fill_price = sizing.paid_per_unit
        fill_size = final_size
        mode_row = "paper"

        if is_live:
            # Cap to real on-chain pUSD with a 5% buffer for fee+slip
            try:
                from data.polymarket_wallet import _fetch_balance
                real_bal = _fetch_balance() or {}
                real_avail = float(real_bal.get("effective") or 0.0)
            except Exception as e:
                logger.warning(f"smart_money: live balance fetch failed: {e}; "
                                "refusing live order")
                continue
            if real_avail < 1.0:
                logger.info(f"smart_money: SKIP (live pUSD ${real_avail:.2f} < $1) "
                             f"{sig.market_title[:40]}")
                continue
            max_real = round(real_avail * 0.95, 2)
            if final_size > max_real:
                logger.info(f"smart_money: live size capped {final_size:.2f}→{max_real:.2f}")
                final_size = max_real
            # Smoke-period cap (first 24h: $5/trade)
            from execution.polymarket_orders import (
                place_market_order, cap_for_smoke_period, daily_loss_halts,
            )
            from execution.wallet import Wallet as _W
            today_real_pnl = _W().realized_pnl()
            if daily_loss_halts(today_real_pnl):
                logger.info(f"smart_money: SKIP (daily-loss halt at ${today_real_pnl:+.2f})")
                continue
            bot_started = globals().get("_BOT_STARTED_AT") or _dt.utcnow()
            globals()["_BOT_STARTED_AT"] = bot_started
            capped = cap_for_smoke_period(final_size, bot_started)
            if capped < final_size:
                logger.info(f"smart_money: smoke-cap {final_size:.2f}→{capped:.2f}")
                final_size = capped
            if final_size < 1.0:
                continue
            # Token id for this side — pulled from the market row loaded earlier
            token_id = m.yes_token_id if side == "YES" else m.no_token_id
            if not token_id:
                logger.warning(f"smart_money: missing {side} token id "
                                f"for {sig.market_title[:40]} — cannot fire live")
                continue
            fill = place_market_order(token_id, side, final_size, our_ask)
            if not fill.success:
                err = fill.raw_response.get("error", "no fill")
                status = fill.raw_response.get("status")
                if notifier:
                    notifier.send(
                        f"❌ <b>SMART COPY LIVE failed</b> on {side} "
                        f"<code>{sig.condition_id[:24]}…</code>\n"
                        f"💡 {err[:160]} status={status}"
                    )
                continue
            # Persist what actually filled (not the request)
            fill_units = fill.units
            fill_price = fill.paid_per_unit
            fill_size = round(fill.size_usd, 4)
            if fill_size < final_size * 0.99:
                logger.info(
                    f"smart_money: PARTIAL FILL requested ${final_size:.2f} "
                    f"filled ${fill_size:.2f} ({fill_units:.2f}@{fill_price:.3f})"
                )
            mode_row = "live"

        btc_now = latest_btc_price() or 0.0
        sigma = estimate_sigma_per_minute() or 0.0001
        decision_id = record_paper_trade(
            condition_id=sig.condition_id,
            side=side,
            btc_price=float(btc_now),
            reference_price=float(btc_now),
            minutes_to_close=mins_to_close,
            sigma_per_minute=sigma,
            model_yes_prob=model_yes_prob,
            implied_yes_prob=yes_ask if side == "YES" else 1.0 - yes_bid,
            edge=edge if side == "YES" else -edge,
            size_usd=fill_size,
            paid_per_unit=fill_price,
            mode_override=mode_row,
        )

        # Tag: smart_copy:wallet|score=N|claude=APPROVE|conf=0.X — learning loop reads these
        claude_dec = verdict.get("decision", "?")
        claude_conf = verdict.get("confidence", 0.5)
        tag = (f"smart_copy:{sig.wallet_name[:20]}|{sig.wallet[:12]}|"
               f"score={score:.0f}|claude={claude_dec}|conf={claude_conf:.2f}")
        try:
            with Session(engine) as session:
                session.execute(
                    update(Decision).where(Decision.id == decision_id).values(
                        notes=tag,
                        strategy="smart_copy",
                        source_wallet=sig.wallet,
                        decision_outcome="held_to_expiry",  # exit_manager updates if it intervenes
                        edge_definition="smart_copy_follow",
                    )
                )
                session.commit()
        except Exception as e:
            logger.debug(f"could not tag decision: {e}")

        fires += 1
        logger.info(f"smart_money: COPIED {sig.wallet_name} → {side} "
                     f"{sig.market_title[:35]} @ {fill_price:.3f} | size=${fill_size:.2f} "
                     f"| edge={edge*100:+.1f}% | assumed_win_prob={win_prob:.0%} "
                     f"| mode={mode_row}")
        if notifier:
            # Tag from the decision row's actual mode (real fill = live;
            # paper = paper). Avoids the prior phantom-WIN class of bug.
            _mt = "LIVE" if mode_row == "live" else "PAPER"
            notifier.send(
                f"🐳➡️🤖 <b>[{_mt}] COPIED {sig.wallet_name}</b>\n"
                f"📊 {sig.market_title[:55]}\n"
                f"➡️ {side} @ ${fill_price:.3f} | size=${fill_size:.2f}\n"
                f"💰 their position: {sig.new_size:.0f} contracts\n"
                f"📈 assumed win prob={win_prob:.0%} | edge={edge*100:+.1f}%\n"
                f"💡 wallet lifetime: ${sig.wallet_lifetime_pnl:,.0f}"
            )

    if untrackable:
        logger.info(f"smart_money: {untrackable}/{len(signals)} signals on "
                     f"markets we don't track (sports/politics — v2 task)")
    return fires


# ── Phase 4: copy-exit execution ─────────────────────────────────────────

def execute_copy_exits(exit_signals: list, notifier=None) -> int:
    """For each SmartExitSignal, find our matching open smart_copy position
    on the same market AND same side, close it at current book.

    The principle: when a tracked smart wallet starts unwinding (≥30%
    reduction), close OUR copy fully. They typically scale out over a few
    minutes — closing 100% now puts us out before the price moves further
    against the position they're dumping.
    """
    from sqlalchemy.orm import Session
    from sqlalchemy import update
    from datetime import datetime as _dt
    from data.storage import engine, PolymarketBookSnapshot, Decision

    if not exit_signals:
        return 0

    closed = 0
    for sig in exit_signals:
        # Find our matching open copy. Match on condition_id AND that the
        # current notes contain smart_copy: (we don't constrain on side
        # since smart wallet could change sides; just close whatever we have).
        with Session(engine) as session:
            our_pos = (session.query(Decision)
                       .filter(Decision.condition_id == sig.condition_id,
                               Decision.size_usd > 0,
                               Decision.resolution_yes.is_(None),
                               Decision.notes.like("%smart_copy%"))
                       .order_by(Decision.id.desc())
                       .first())
        if our_pos is None:
            continue  # we don't have a copy on this market — nothing to close

        # Get current book mid for the exit price
        with Session(engine) as session:
            book = (session.query(PolymarketBookSnapshot)
                    .filter(PolymarketBookSnapshot.condition_id == sig.condition_id)
                    .order_by(PolymarketBookSnapshot.id.desc())
                    .first())
        if book is None or book.yes_bid is None or book.yes_ask is None:
            # No book — try fetching one
            try:
                from data.polymarket_client import snapshot_market
                from data.storage import PolymarketMarket
                with Session(engine) as session:
                    m = (session.query(PolymarketMarket)
                         .filter(PolymarketMarket.condition_id == sig.condition_id)
                         .one_or_none())
                if m is None:
                    logger.warning(f"smart_money: can't close — no market record "
                                    f"for {sig.market_title[:40]}")
                    continue
                snapshot_market(condition_id=sig.condition_id,
                                yes_token=m.yes_token_id, no_token=m.no_token_id)
                with Session(engine) as session:
                    book = (session.query(PolymarketBookSnapshot)
                            .filter(PolymarketBookSnapshot.condition_id == sig.condition_id)
                            .order_by(PolymarketBookSnapshot.id.desc())
                            .first())
            except Exception as e:
                logger.warning(f"smart_money: book fetch for close failed: {e}")
                continue
        if book is None or book.yes_bid is None or book.yes_ask is None:
            continue

        # Use the BID for our side (we're selling — we get hit at the bid)
        yes_mid = (float(book.yes_bid) + float(book.yes_ask)) / 2.0
        reason = f"smart_exit:{sig.wallet_name[:18]}_{int(sig.reduction_pct*100)}pct"
        row_is_live = (our_pos.mode == "live")
        result = None
        if row_is_live:
            # Real on-chain SELL via _live_sell. Same FAK + fill-check
            # pattern as the BUY: only mark resolved if shares + USD both
            # truly moved.
            from strategies.exit_manager import _live_sell
            sell = _live_sell(our_pos.condition_id, our_pos.side,
                              float(our_pos.units_bought or 0),
                              m.yes_token_id, m.no_token_id, book)
            if not sell or not sell.get("success"):
                err = (sell or {}).get('error', 'no fill')[:120]
                logger.warning(f"smart_money: live SELL FAILED for "
                                f"{sig.market_title[:40]}: {err}")
                if notifier:
                    notifier.send(
                        f"⚠️ <b>SMART EXIT SELL FAILED</b> "
                        f"<code>{our_pos.condition_id[:24]}…</code>\n💡 {err}"
                    )
                continue
            proceeds = float(sell.get("proceeds_usd") or 0.0)
            cost_basis = float(our_pos.size_usd or 0)
            fee = proceeds * 0.01
            pnl_usd = proceeds - cost_basis - fee
            won = pnl_usd > 0
            with Session(engine) as session:
                d = session.get(Decision, our_pos.id)
                if d is not None and d.resolution_yes is None:
                    d.resolution_yes = (1 if won else 0) if d.side == "YES" \
                        else (0 if won else 1)
                    d.pnl_usd = round(pnl_usd, 4)
                    d.resolved_at = _dt.utcnow()
                    prev = (d.notes or "").strip()
                    d.notes = f"{reason}|live_sell|{prev}" if prev else f"{reason}|live_sell"
                    session.commit()
                    result = {"pnl_usd": pnl_usd, "proceeds": proceeds}
        else:
            # Paper path unchanged
            from strategies.exit_manager import _resolve_paper_at_mid
            result = _resolve_paper_at_mid(our_pos.id, yes_mid, reason)
        if result is None:
            continue
        closed += 1
        logger.info(f"smart_money: CLOSED COPY {sig.wallet_name} dumped → "
                     f"{sig.market_title[:35]} | our pnl=${result.get('pnl_usd', 0):+.2f} "
                     f"({'LIVE' if row_is_live else 'PAPER'})")
        if notifier:
            pnl = result.get("pnl_usd", 0)
            icon = "✅" if pnl > 0 else "🔴"
            from strategies.cumulative import today_cumulative_line
            cum = today_cumulative_line()
            _mt = "LIVE" if row_is_live else "PAPER"
            notifier.send(
                f"🐳⬅️🤖 <b>[{_mt}] SMART EXIT — CLOSED COPY</b>\n"
                f"👤 {sig.wallet_name} reduced {sig.prev_size:.0f}→{sig.new_size:.0f} "
                f"(-{sig.reduction_pct*100:.0f}%)\n"
                f"📊 {sig.market_title[:55]}\n"
                f"{icon} our P&amp;L: <b>${pnl:+.2f}</b>\n"
                f"{cum}"
            )

    return closed
