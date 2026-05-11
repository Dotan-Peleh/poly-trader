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
WALLET_LIST_PATH = "/home/dotanwork/poly-trader/var/smart_wallets.json"
SNAPSHOT_PATH = "/home/dotanwork/poly-trader/var/smart_wallet_positions.json"

# Smart-wallet selection thresholds (used by refresh_smart_wallets)
MIN_LIFETIME_PNL = 1_000_000
MAX_DAYS_SINCE_LAST_TRADE = 14
TARGET_WALLET_COUNT = 30


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
        top = _get_json(f"{LB_API}/profit?window=All&limit=200")
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
    current_price: float
    detected_at: str


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


def poll_smart_wallets(notifier=None) -> List[SmartSignal]:
    """Pull /positions for each tracked wallet, diff against previous
    snapshot, emit SmartSignal for new opens / size growth.

    Run from scheduler every 60–120s. Returns list of signals (also
    persisted in DB by the copy-execution path if wired)."""
    wallets = _load_wallet_list()
    if not wallets:
        logger.info("smart_money: no wallet list (run refresh_smart_wallets first)")
        return []

    snaps = _load_snapshots()
    signals: List[SmartSignal] = []

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

        # Build current state: {asset_id: {size, price, market_title, outcome, condition_id}}
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

        # Emit signals for NEW assets or grown sizes
        for asset, info in cur.items():
            prev_size = float(prev.get(asset, {}).get("size") or 0)
            delta = info["size"] - prev_size
            if delta > 0 and (prev_size == 0 or delta / max(prev_size, 1) > 0.10):
                # New position OR grew by >10%
                signals.append(SmartSignal(
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
                    detected_at=datetime.now(timezone.utc).isoformat(),
                ))

        # Detect EXITS (asset present in prev, gone or shrunk in cur) — for Phase 4
        for asset, prev_info in prev.items():
            prev_size = float(prev_info.get("size") or 0)
            new_size = float(cur.get(asset, {}).get("size") or 0)
            if prev_size > 0 and new_size < prev_size * 0.5:
                logger.info(f"smart_money: SMART EXIT {w['name']} "
                             f"{prev_info.get('market_title','?')[:50]} "
                             f"{prev_size:.0f}→{new_size:.0f}")
                # Phase 4 will hook here to close our matching copy

        snaps[wallet] = {
            "name": w["name"],
            "positions": cur,
            "last_polled": datetime.now(timezone.utc).isoformat(),
        }

        time.sleep(0.1)  # be polite to the API

    _save_snapshots(snaps)

    # Notify on first emit per signal (the calling copy-executor decides actual fires)
    if signals and notifier is not None:
        for s in signals[:5]:  # cap noise to 5 per tick
            notifier.send(
                f"🐳 <b>SMART MONEY</b> entered\n"
                f"👤 {s.wallet_name} (lifetime ${s.wallet_lifetime_pnl:,.0f})\n"
                f"📊 {s.market_title[:60]}\n"
                f"➡️  {s.outcome} @ ${s.current_price:.3f} | size={s.new_size:.0f} "
                f"(+{s.delta_size:.0f})"
            )

    if signals:
        logger.info(f"smart_money: {len(signals)} new signals from {len(wallets)} wallets")
    return signals


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
    """Poll tracked wallets and emit signals. Scheduler: interval seconds=90."""
    try:
        poll_smart_wallets(notifier=notifier)
    except Exception as e:
        logger.error(f"smart_money poll_tick failed: {e}")
