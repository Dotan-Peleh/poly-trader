"""
Trade firehose — Phase 1: REST polling of Polymarket's /trades endpoint.

Captures EVERY trade on Polymarket (not just our tracked whales' positions),
stores them in trades_firehose table, lets us:
  • dynamically rank wallets by 7-day realized P&L (auto-discovery)
  • detect smart-wallet entries in ~2s instead of 30s
  • watch trade flow across all 10k+ active wallets

REST polling at 2s = ~30 req/min, well under Polymarket's rate limit.
Each call returns up to 500 most-recent trades. Pagination via id (we
remember the highest id we've ingested and only insert new ones).

Phase 2 will replace this with Polygon RPC WebSocket on OrderFilled events
(latency 2s → <1s, no polling load).
"""
import json
import logging
import time
import urllib.request
from datetime import datetime, timezone
from typing import Optional

from sqlalchemy import (
    BigInteger, Column, Float, Integer, String, Text, create_engine,
    text as sql_text,
)
from sqlalchemy.orm import Session, declarative_base

logger = logging.getLogger(__name__)

DATA_API = "https://data-api.polymarket.com"
PAGE_SIZE = 500
USER_AGENT = "poly-trader-firehose/0.1"

Base = declarative_base()


# ── ORM models — stored in poly_trader.db alongside existing tables ──────

class FirehoseTrade(Base):
    __tablename__ = "trades_firehose"
    id = Column(Integer, primary_key=True, autoincrement=True)
    api_id = Column(String(80), unique=True, index=True)  # polymarket trade ID
    ts = Column(BigInteger, nullable=False, index=True)
    wallet = Column(String(80), nullable=False, index=True)
    condition_id = Column(String(80), nullable=False, index=True)
    asset_id = Column(String(80), nullable=False)
    side = Column(String(8), nullable=False)   # BUY / SELL
    size = Column(Float, nullable=False)
    price = Column(Float, nullable=False)
    usdc_size = Column(Float, nullable=False)
    market_title = Column(Text)
    outcome = Column(String(32))


class SmartWalletRanking(Base):
    """Computed every 5 min from rolling 7d firehose data + /positions enrich."""
    __tablename__ = "smart_wallet_rankings"
    wallet = Column(String(80), primary_key=True)
    last_ranked_ts = Column(BigInteger, nullable=False)
    realized_pnl_7d = Column(Float)
    realized_pnl_lifetime = Column(Float)
    trade_count_7d = Column(Integer)
    avg_trade_usd_7d = Column(Float)
    win_rate_7d = Column(Float)
    rank_score = Column(Float, index=True)
    category = Column(String(32))         # "sports" | "politics" | "crypto" | "mixed"
    pseudonym = Column(String(80))


class PolygonStreamHit(Base):
    """Every on-chain trade by a tracked smart wallet (via TransferSingle).
    Written from polygon_stream's signal handler. Capped at last 500 rows by
    the prune job — keeps the dashboard's view manageable + DB compact."""
    __tablename__ = "polygon_stream_hits"
    id = Column(Integer, primary_key=True, autoincrement=True)
    detected_at = Column(BigInteger, nullable=False, index=True)
    wallet = Column(String(80), nullable=False, index=True)
    pseudonym = Column(String(80))
    lifetime_pnl = Column(Float)
    asset_id = Column(String(80))
    amount = Column(Float)
    tx_hash = Column(String(80))
    block_number = Column(BigInteger)
    market_title = Column(Text)            # filled by lookup if available
    fired_copy = Column(Integer, default=0)  # 1 if this hit triggered a copy


# ── HTTP helper ──────────────────────────────────────────────────────────

def _get_json(url: str, timeout: float = 10.0):
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read())


# ── Init / migrate ───────────────────────────────────────────────────────

def init_firehose_schema(engine) -> None:
    """Create firehose tables if they don't exist. Safe to call repeatedly."""
    Base.metadata.create_all(engine)


def prune_old_rows(engine, keep_hours: int = 24, vacuum: bool = True) -> dict:
    """Delete trades_firehose rows older than keep_hours + polygon_stream_hits
    older than keep_hours, then VACUUM to reclaim space. Without this the
    DB grows unboundedly (1M+ trades/day × 250 B = ~250 MB/day) which
    breaks the GCS sync the dashboard depends on.

    Returns counts of pruned rows + final DB size.
    """
    cutoff_ts = int(time.time()) - keep_hours * 3600
    # btc_ticks are stored with `ts` as a SQLite DATETIME — convert
    cutoff_dt = datetime.utcfromtimestamp(cutoff_ts).strftime("%Y-%m-%d %H:%M:%S")
    btc_cutoff_ts = int(time.time()) - 6 * 3600  # btc_ticks: only keep 6h
    btc_cutoff_dt = datetime.utcfromtimestamp(btc_cutoff_ts).strftime("%Y-%m-%d %H:%M:%S")

    with Session(engine) as session:
        fh_pruned = session.execute(sql_text(
            "DELETE FROM trades_firehose WHERE ts < :cutoff"
        ), {"cutoff": cutoff_ts}).rowcount
        # btc_ticks → keep just 6h (vol estimator uses 30-min window)
        btc_pruned = session.execute(sql_text(
            "DELETE FROM btc_ticks WHERE ts < :cutoff"
        ), {"cutoff": btc_cutoff_dt}).rowcount
        # polymarket_book_snapshots → keep 24h
        book_pruned = session.execute(sql_text(
            "DELETE FROM polymarket_book_snapshots WHERE ts < :cutoff"
        ), {"cutoff": cutoff_dt}).rowcount
        # Stream hits: keep last 500 by id
        session.execute(sql_text("""
            DELETE FROM polygon_stream_hits
            WHERE id NOT IN (
              SELECT id FROM polygon_stream_hits ORDER BY id DESC LIMIT 500
            )
        """))
        session.commit()
    if vacuum:
        try:
            with engine.connect() as conn:
                conn.execute(sql_text("VACUUM"))
        except Exception as e:
            logger.warning(f"firehose: vacuum failed: {e}")
    # Report final size
    db_path = str(engine.url).replace("sqlite:///", "")
    try:
        import os as _os
        size_mb = _os.path.getsize(db_path) / 1_000_000.0
    except Exception:
        size_mb = -1
    logger.info(f"firehose: pruned {fh_pruned} trades + {btc_pruned} btc_ticks "
                 f"+ {book_pruned} books, db now {size_mb:.1f} MB")
    return {"trades_pruned": fh_pruned, "btc_pruned": btc_pruned,
            "books_pruned": book_pruned, "db_size_mb": size_mb}


def record_stream_hit(engine, wallet: str, pseudonym: str,
                       lifetime_pnl: float, asset_id: str, amount: float,
                       tx_hash: str, block_number: int,
                       market_title: str = "", fired_copy: bool = False) -> None:
    """Insert a row into polygon_stream_hits for dashboard visibility."""
    try:
        with Session(engine) as session:
            session.execute(sql_text("""
                INSERT INTO polygon_stream_hits
                  (detected_at, wallet, pseudonym, lifetime_pnl, asset_id,
                   amount, tx_hash, block_number, market_title, fired_copy)
                VALUES (:t, :w, :p, :l, :a, :amt, :tx, :b, :m, :f)
            """), {
                "t": int(time.time()), "w": wallet, "p": pseudonym,
                "l": lifetime_pnl, "a": asset_id, "amt": amount,
                "tx": tx_hash, "b": block_number, "m": market_title,
                "f": 1 if fired_copy else 0,
            })
            session.commit()
    except Exception as e:
        logger.debug(f"firehose: record_stream_hit failed: {e}")


def _last_ingested_api_id(engine) -> Optional[str]:
    with Session(engine) as session:
        row = session.execute(sql_text(
            "SELECT api_id FROM trades_firehose ORDER BY id DESC LIMIT 1"
        )).first()
        return row[0] if row else None


# ── Ingest tick ──────────────────────────────────────────────────────────

def ingest_tick(engine) -> int:
    """Pull the latest /trades page, insert any not-yet-seen rows.
    Returns count inserted. Called every ~2s by the scheduler."""
    try:
        # ascending=false → newest trades first. Critical for catching live
        # activity — default (oldest-first) returns a stale historical window.
        batch = _get_json(f"{DATA_API}/trades?limit={PAGE_SIZE}&ascending=false")
    except Exception as e:
        logger.debug(f"firehose: fetch failed: {e}")
        return 0
    if not isinstance(batch, list) or not batch:
        return 0

    # Use raw INSERT OR IGNORE so dup api_ids silently skip instead of
    # aborting the whole batch. This is the right semantics — /trades
    # returns overlapping pages and we always re-process them.
    inserted = 0
    with Session(engine) as session:
        for t in batch:
            api_id = t.get("transactionHash") or t.get("id") or ""
            if not api_id:
                api_id = (f"{t.get('proxyWallet','?')[-8:]}_"
                          f"{t.get('timestamp')}_{(t.get('asset') or '')[-8:]}")
            wallet = (t.get("proxyWallet") or "").lower()
            if not wallet:
                continue
            try:
                size = float(t.get("size") or 0)
                price = float(t.get("price") or 0)
                result = session.execute(sql_text("""
                    INSERT OR IGNORE INTO trades_firehose
                      (api_id, ts, wallet, condition_id, asset_id, side,
                       size, price, usdc_size, market_title, outcome)
                    VALUES (:api_id, :ts, :wallet, :cond, :asset, :side,
                            :size, :price, :usdc, :title, :outcome)
                """), {
                    "api_id": api_id,
                    "ts": int(t.get("timestamp") or 0),
                    "wallet": wallet,
                    "cond": t.get("conditionId") or "",
                    "asset": t.get("asset") or "",
                    "side": (t.get("side") or "").upper(),
                    "size": size,
                    "price": price,
                    "usdc": size * price,
                    "title": (t.get("title") or "")[:200],
                    "outcome": t.get("outcome") or "",
                })
                if result.rowcount > 0:
                    inserted += 1
            except Exception as e:
                logger.debug(f"firehose: row skip: {e}")
        try:
            session.commit()
        except Exception as e:
            session.rollback()
            logger.warning(f"firehose: commit failed: {e}")
            return 0
    return inserted


# ── Wallet ranking ───────────────────────────────────────────────────────

def rank_wallets_from_firehose(engine, lookback_days: int = 7,
                                 top_n: int = 200) -> int:
    """Aggregate trades_firehose into per-wallet stats over lookback window,
    enrich with /positions realized P&L, write to smart_wallet_rankings.
    Returns count of wallets ranked."""
    cutoff_ts = int(time.time()) - lookback_days * 86400
    with Session(engine) as session:
        # Aggregate: every wallet with >= 5 trades in last 7d
        rows = session.execute(sql_text("""
            SELECT wallet,
                   COUNT(*)            AS n,
                   SUM(usdc_size)      AS volume_usd,
                   AVG(usdc_size)      AS avg_trade_usd,
                   SUM(CASE WHEN side='BUY' THEN 1 ELSE 0 END) AS buys
            FROM trades_firehose
            WHERE ts >= :cutoff
            GROUP BY wallet
            HAVING n >= 5
            ORDER BY volume_usd DESC
            LIMIT 1000
        """), {"cutoff": cutoff_ts}).all()

    if not rows:
        logger.info("firehose: no qualifying wallets to rank")
        return 0

    logger.info(f"firehose: deep-analyzing top {len(rows)} wallets by 7d volume...")
    candidates = []
    for r in rows:
        wallet = r.wallet
        # Enrich with realized P&L from /positions endpoint
        try:
            pos = _get_json(f"{DATA_API}/positions?user={wallet}&sizeThreshold=0&limit=100")
            if not isinstance(pos, list):
                continue
            realized = sum(float(p.get("realizedPnl") or 0) for p in pos)
            if realized < 1000:  # ignore wallets with < $1k lifetime profit
                continue
            # Pseudonym is on the trade row; pull from any recent trade
            with Session(engine) as session:
                n = session.execute(sql_text(
                    "SELECT outcome, market_title FROM trades_firehose "
                    "WHERE wallet=:w ORDER BY id DESC LIMIT 1"
                ), {"w": wallet}).first()
            # Category guess from most-recent market title
            cat = "mixed"
            if n and n[1]:
                t = (n[1] or "").lower()
                if any(x in t for x in ["bitcoin", "btc", "ethereum", "eth", "sol"]):
                    cat = "crypto"
                elif any(x in t for x in ["nba", "nfl", "vs ", "fc ", "match", "tennis", " win on"]):
                    cat = "sports"
                elif any(x in t for x in ["trump", "election", "president"]):
                    cat = "politics"
            # Composite rank: realized × log(volume) — protects from tiny-sample wallets
            import math
            score = realized * math.log(1 + (r.volume_usd or 0))
            candidates.append({
                "wallet": wallet,
                "realized": realized,
                "n_trades": r.n,
                "volume": r.volume_usd or 0,
                "avg_trade": r.avg_trade_usd or 0,
                "score": score,
                "category": cat,
            })
        except Exception as e:
            logger.debug(f"firehose: enrich skip {wallet[:12]}: {e}")
        time.sleep(0.05)  # rate-limit ourselves

    candidates.sort(key=lambda c: -c["score"])
    top = candidates[:top_n]

    # Upsert into smart_wallet_rankings
    now_ts = int(time.time())
    with Session(engine) as session:
        for c in top:
            session.execute(sql_text("""
                INSERT INTO smart_wallet_rankings
                  (wallet, last_ranked_ts, realized_pnl_lifetime, trade_count_7d,
                   avg_trade_usd_7d, rank_score, category)
                VALUES (:w, :ts, :r, :n, :a, :s, :c)
                ON CONFLICT(wallet) DO UPDATE SET
                  last_ranked_ts=:ts, realized_pnl_lifetime=:r,
                  trade_count_7d=:n, avg_trade_usd_7d=:a,
                  rank_score=:s, category=:c
            """), {
                "w": c["wallet"], "ts": now_ts, "r": c["realized"],
                "n": c["n_trades"], "a": c["avg_trade"],
                "s": c["score"], "c": c["category"],
            })
        session.commit()
    logger.info(f"firehose: ranked top {len(top)} smart wallets")
    return len(top)
