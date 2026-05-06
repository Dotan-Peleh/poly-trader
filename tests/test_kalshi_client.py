"""Unit tests for the Kalshi client (parsing + book conversion)."""
from datetime import datetime

from data.kalshi_client import (
    parse_event_resolution_ts,
    book_summary,
)


# ── Ticker parsing ──────────────────────────────────────────────────────────

def test_parse_event_resolution_ts_basic():
    """KXBTC15M-26MAY062345 → 2026-05-06 23:45 UTC."""
    assert parse_event_resolution_ts("KXBTC15M-26MAY062345") == datetime(2026, 5, 6, 23, 45)


def test_parse_event_resolution_ts_midnight():
    """KXBTC15M-26MAY070000 → 2026-05-07 00:00 UTC."""
    assert parse_event_resolution_ts("KXBTC15M-26MAY070000") == datetime(2026, 5, 7, 0, 0)


def test_parse_event_resolution_ts_handles_lowercase_month():
    assert parse_event_resolution_ts("KXBTC15M-26may062345") == datetime(2026, 5, 6, 23, 45)


def test_parse_event_resolution_ts_returns_none_on_garbage():
    assert parse_event_resolution_ts("garbage") is None
    assert parse_event_resolution_ts("KXBTC15M-toosmall") is None
    assert parse_event_resolution_ts("KXBTC15M-26ZZZ062345") is None


# ── Book summary ────────────────────────────────────────────────────────────

def test_book_summary_basic():
    """yes_bid=55, no_bid=42 → yes_bid_prob=0.55, yes_ask_prob=0.58."""
    book = {
        "yes": [[55, 100], [54, 200]],   # someone wants to BUY YES at 55¢ for 100 contracts
        "no":  [[42, 80], [40, 150]],    # someone wants to BUY NO at 42¢
    }
    yes_bid, yes_ask, depth = book_summary(book)
    assert yes_bid == 0.55
    assert abs(yes_ask - 0.58) < 1e-9   # 100 - 42 = 58
    assert depth > 0


def test_book_summary_empty():
    """Empty book → all None / 0."""
    yes_bid, yes_ask, depth = book_summary({"yes": [], "no": []})
    assert yes_bid is None
    assert yes_ask is None
    assert depth == 0.0


def test_book_summary_one_sided():
    """Only YES bids, no NO bids → yes_bid known, yes_ask unknown."""
    yes_bid, yes_ask, depth = book_summary({"yes": [[60, 50]], "no": []})
    assert yes_bid == 0.60
    assert yes_ask is None
    assert depth > 0


def test_book_summary_picks_best_yes_bid():
    """Order matters — best (highest) YES bid is the bid we'd hit selling."""
    book = {
        "yes": [[40, 100], [55, 50], [50, 200]],   # top is 55
        "no": [],
    }
    yes_bid, _, _ = book_summary(book)
    assert yes_bid == 0.55


def test_book_summary_picks_best_no_bid_for_yes_ask():
    """Best NO bid → worst YES ask. Higher NO bid means cheaper to buy YES."""
    book = {
        "yes": [],
        "no":  [[30, 100], [45, 50], [40, 200]],   # top NO bid is 45
    }
    _, yes_ask, _ = book_summary(book)
    # YES ask = 100 - 45 = 55¢
    assert abs(yes_ask - 0.55) < 1e-9
