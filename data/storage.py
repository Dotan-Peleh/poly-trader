"""
SQLAlchemy ORM schema for poly-trader.

Five tables:
  - btc_ticks               — 1s Binance trade stream
  - polymarket_markets      — active hourly BTC binaries we're tracking
  - polymarket_book_snapshots — order-book mids over time per market
  - decisions               — every fire (paper or live) with model + implied probs
  - claude_decisions        — Claude gate verdicts (mirrors crypto-trader)

Designed for high-frequency inserts on btc_ticks (~1/s) so we can
backtest the model later. Other tables are low-frequency.
"""
from datetime import datetime
from sqlalchemy import (
    Column, Integer, Float, String, DateTime, Boolean, Text,
    create_engine, Index,
)
from sqlalchemy.orm import declarative_base, Session

from config.settings import settings

Base = declarative_base()
# SQLite-specific tuning: WAL mode + 30s busy timeout. Without this, the
# decision_tick (every 5s) and polymarket_refresh_tick (every 2 min) compete
# for the single writer lock and the refresh task fails with
# "database is locked" — which is why the markets table goes stale and the
# bot has no markets in window to evaluate.
_is_sqlite = settings.database_url.startswith("sqlite")
engine = create_engine(
    settings.database_url,
    future=True,
    connect_args={"timeout": 30, "check_same_thread": False} if _is_sqlite else {},
)
if _is_sqlite:
    from sqlalchemy import event as _sa_event
    @_sa_event.listens_for(engine, "connect")
    def _enable_wal(dbapi_conn, _conn_record):
        cur = dbapi_conn.cursor()
        cur.execute("PRAGMA journal_mode=WAL")
        cur.execute("PRAGMA synchronous=NORMAL")
        cur.execute("PRAGMA busy_timeout=30000")
        cur.close()


class BtcTick(Base):
    __tablename__ = "btc_ticks"
    id = Column(Integer, primary_key=True, autoincrement=True)
    ts = Column(DateTime, index=True, nullable=False)        # exchange-side timestamp
    price = Column(Float, nullable=False)
    qty = Column(Float)                                      # base-asset volume
    is_buyer_maker = Column(Boolean)                         # True = sell aggression
    source = Column(String(16), default="binance")           # 'binance', 'coinbase', etc.


Index("ix_btc_ticks_ts_source", BtcTick.ts, BtcTick.source)


class PolymarketMarket(Base):
    __tablename__ = "polymarket_markets"
    id = Column(Integer, primary_key=True, autoincrement=True)
    condition_id = Column(String(80), unique=True, nullable=False)   # Polymarket's market ID
    question = Column(Text)                                          # human-readable
    resolution_ts = Column(DateTime, index=True, nullable=False)     # HH:00 UTC
    reference_price = Column(Float)                                  # set when known
    yes_token_id = Column(String(80))
    no_token_id = Column(String(80))
    state = Column(String(16), default="active")                     # active|resolved|cancelled
    last_seen = Column(DateTime, default=datetime.utcnow)
    created_at = Column(DateTime, default=datetime.utcnow)


class PolymarketBookSnapshot(Base):
    __tablename__ = "polymarket_book_snapshots"
    id = Column(Integer, primary_key=True, autoincrement=True)
    ts = Column(DateTime, index=True, nullable=False, default=datetime.utcnow)
    condition_id = Column(String(80), index=True, nullable=False)
    yes_bid = Column(Float)
    yes_ask = Column(Float)
    yes_mid = Column(Float)                                          # implied prob
    no_bid = Column(Float)
    no_ask = Column(Float)
    book_depth_usd = Column(Float)                                   # sum of bids + asks at top 5
    btc_price_at_snap = Column(Float)


class Decision(Base):
    """Every fire (paper or live) goes here — the audit trail."""
    __tablename__ = "decisions"
    id = Column(Integer, primary_key=True, autoincrement=True)
    ts = Column(DateTime, index=True, default=datetime.utcnow)
    condition_id = Column(String(80), index=True, nullable=False)
    side = Column(String(8), nullable=False)                         # 'YES' or 'NO'
    btc_price_at_entry = Column(Float)
    reference_price = Column(Float)
    minutes_to_close = Column(Float)
    sigma_per_minute = Column(Float)
    model_yes_prob = Column(Float)
    implied_yes_prob = Column(Float)
    edge = Column(Float)
    size_usd = Column(Float)
    paid_per_unit = Column(Float)                                    # cost per share
    units_bought = Column(Float)                                     # shares
    fill_price = Column(Float)                                       # actual fill (paper or live)
    mode = Column(String(8), default="paper")                        # 'paper' | 'live'
    # Outcome filled in after market resolves
    resolution_yes = Column(Boolean)                                 # did YES win?
    pnl_usd = Column(Float)
    resolved_at = Column(DateTime)
    notes = Column(Text)
    # Observability columns added 2026-05-19. Pulled out of `notes` so
    # queries don't need regex. Backfilled by
    # scripts/backfill_decision_columns.py for historical rows.
    strategy = Column(String(16), index=True)                        # 'v2_meanrev' | 'smart_copy'
    source_wallet = Column(String(64), index=True)                   # '0x...' for smart_copy; NULL for v2
    decision_outcome = Column(String(32), index=True)                # see DecisionOutcome strings below
    edge_definition = Column(String(32))                             # 'model_minus_implied' | 'smart_copy_follow'


class ClaudeDecision(Base):
    """Mirrors crypto-trader/claude_decisions for the same learning loop."""
    __tablename__ = "claude_decisions"
    id = Column(Integer, primary_key=True, autoincrement=True)
    ts = Column(DateTime, index=True, default=datetime.utcnow)
    condition_id = Column(String(80), index=True, nullable=False)
    side = Column(String(8))
    decision = Column(String(20))                                    # APPROVE/REJECT/HALF_SIZE
    confidence = Column(Float)
    reason = Column(Text)
    edge_at_call = Column(Float)
    btc_price_at_call = Column(Float)
    minutes_to_close = Column(Float)
    # Outcome backfilled after resolution
    pnl_usd = Column(Float)
    outcome = Column(String(8))                                      # 'win' | 'loss' | 'pending'
    closed_at = Column(DateTime)


class RejectedDecision(Base):
    """Every fire attempt that was BLOCKED by a gate.

    Added 2026-05-19 after the v2_meanrev autopsy revealed we had no
    way to attribute volume gaps to specific gates — `decisions` only
    logs fills. With this table, "why didn't mode X fire" becomes one
    query against reject_reason."""
    __tablename__ = "rejected_decisions"
    id = Column(Integer, primary_key=True, autoincrement=True)
    ts = Column(DateTime, index=True, default=datetime.utcnow)
    mode = Column(String(8), index=True)                             # 'paper' | 'live'
    strategy = Column(String(16), index=True)                        # 'v2_meanrev' | 'smart_copy'
    condition_id = Column(String(80), index=True)
    source_wallet = Column(String(64), index=True)                   # NULL for v2_meanrev
    side = Column(String(8))                                          # 'YES' | 'NO' | NULL
    intended_size_usd = Column(Float)
    model_yes_prob = Column(Float)
    implied_yes_prob = Column(Float)
    reject_reason = Column(String(48), index=True)                   # see RejectReason below
    reject_detail = Column(Text)                                      # free-form ("$23.5+$30 > $50")


class RejectReason:
    """String constants for RejectedDecision.reject_reason."""
    BAND = "blocked_band"                          # implied outside [0.40, 0.60]
    EDGE_TOO_SMALL = "blocked_edge_too_small"
    EDGE_TOO_LARGE = "blocked_edge_too_large"
    TAIL = "blocked_tail"                          # paid outside [0.10, 0.90]
    BOOK_PLACEHOLDER = "blocked_book_placeholder"  # yes_ask+no_ask > 1.05
    COOLDOWN = "blocked_cooldown"
    DAILY_LOSS = "blocked_daily_loss"
    CONCURRENT = "blocked_concurrent"
    BANKROLL = "blocked_bankroll"
    HALT_FLAG = "blocked_halt_flag"
    WALLET_CAP_COUNT = "blocked_wallet_cap_count"
    WALLET_CAP_NOTIONAL = "blocked_wallet_cap_notional"
    STALE_SIGNAL = "blocked_stale_signal"
    PRICE_DRIFT = "blocked_price_drift"
    UNTRACKED_MARKET = "blocked_untracked_market"
    NO_BOOK = "blocked_no_book"
    RESOLVES_TOO_SOON = "blocked_resolves_too_soon"
    QUALITY_SCORE = "blocked_quality_score"
    DAILY_CAP = "blocked_daily_cap"
    CLAUDE_REJECT = "blocked_claude_reject"
    DUPLICATE_POSITION = "blocked_duplicate_position"


def init_db():
    Base.metadata.create_all(engine)
    # SQLite ALTER TABLE for in-place upgrade from pre-2026-05-19 schemas.
    # create_all() only creates missing TABLES, not missing COLUMNS, so
    # an existing decisions table on the running bot needs explicit ALTERs.
    if _is_sqlite:
        with engine.connect() as conn:
            existing = {r[1] for r in conn.exec_driver_sql("PRAGMA table_info(decisions)")}
            for col, ddl in [
                ("strategy",         "ALTER TABLE decisions ADD COLUMN strategy VARCHAR(16)"),
                ("source_wallet",    "ALTER TABLE decisions ADD COLUMN source_wallet VARCHAR(64)"),
                ("decision_outcome", "ALTER TABLE decisions ADD COLUMN decision_outcome VARCHAR(32)"),
                ("edge_definition",  "ALTER TABLE decisions ADD COLUMN edge_definition VARCHAR(32)"),
            ]:
                if col not in existing:
                    conn.exec_driver_sql(ddl)
            conn.commit()


def record_rejection(*, mode: str, strategy: str, condition_id: str,
                     reject_reason: str, source_wallet: str = None,
                     side: str = None, intended_size_usd: float = None,
                     model_yes_prob: float = None, implied_yes_prob: float = None,
                     reject_detail: str = None) -> None:
    """Log a blocked fire attempt. Non-throwing — observability must NOT
    break the hot path. If the DB write fails (lock, schema mismatch),
    we swallow silently rather than block trading."""
    try:
        with Session(engine) as session:
            session.add(RejectedDecision(
                mode=mode, strategy=strategy, condition_id=condition_id,
                source_wallet=source_wallet, side=side,
                intended_size_usd=intended_size_usd,
                model_yes_prob=model_yes_prob,
                implied_yes_prob=implied_yes_prob,
                reject_reason=reject_reason, reject_detail=reject_detail,
            ))
            session.commit()
    except Exception:
        pass


# ── Common write helpers ────────────────────────────────────────────────────

def save_btc_tick(ts: datetime, price: float, qty: float = None,
                   is_buyer_maker: bool = None, source: str = "binance"):
    with Session(engine) as session:
        session.add(BtcTick(
            ts=ts, price=price, qty=qty,
            is_buyer_maker=is_buyer_maker, source=source,
        ))
        session.commit()


def upsert_market(condition_id: str, question: str, resolution_ts: datetime,
                   yes_token_id: str = None, no_token_id: str = None,
                   reference_price: float = None):
    with Session(engine) as session:
        existing = session.query(PolymarketMarket).filter_by(condition_id=condition_id).one_or_none()
        if existing is None:
            session.add(PolymarketMarket(
                condition_id=condition_id, question=question,
                resolution_ts=resolution_ts, reference_price=reference_price,
                yes_token_id=yes_token_id, no_token_id=no_token_id,
                last_seen=datetime.utcnow(),
            ))
        else:
            existing.last_seen = datetime.utcnow()
            if reference_price is not None and existing.reference_price is None:
                existing.reference_price = reference_price
        session.commit()


def save_book_snapshot(condition_id: str, yes_bid: float, yes_ask: float,
                        no_bid: float, no_ask: float,
                        book_depth_usd: float = None,
                        btc_price_at_snap: float = None):
    with Session(engine) as session:
        yes_mid = (yes_bid + yes_ask) / 2 if (yes_bid and yes_ask) else None
        session.add(PolymarketBookSnapshot(
            condition_id=condition_id,
            yes_bid=yes_bid, yes_ask=yes_ask, yes_mid=yes_mid,
            no_bid=no_bid, no_ask=no_ask,
            book_depth_usd=book_depth_usd,
            btc_price_at_snap=btc_price_at_snap,
        ))
        session.commit()
