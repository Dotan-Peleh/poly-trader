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
engine = create_engine(settings.database_url, future=True)


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


def init_db():
    Base.metadata.create_all(engine)


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
