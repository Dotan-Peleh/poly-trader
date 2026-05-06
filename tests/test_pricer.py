"""
Core unit tests for models/digital_option.py.

The pricer is the entire trading edge — a bug here is invisible until
real money is on the line. These tests cover:
  - Mathematical edge cases (zero time, zero vol, exact reference)
  - Late-window sharpening (P approaches 0/1 as t → 0)
  - Edge sign convention (YES cheap → fire YES)
  - Decision wrapper time gates
"""
import math

import pytest

from models.digital_option import (
    BinaryQuote,
    PricerOutput,
    TradeIntent,
    decide_trade,
    price_binary,
    EDGE_THRESHOLD,
)


def _quote(**kwargs) -> BinaryQuote:
    """Convenience builder with sensible defaults."""
    defaults = dict(
        symbol="BTC-TEST",
        reference_price=100_000.0,
        current_price=100_000.0,
        minutes_to_close=10.0,
        sigma_per_minute=0.001,   # 0.1%/min ≈ realistic late-window BTC
        implied_yes_prob=0.50,
    )
    defaults.update(kwargs)
    return BinaryQuote(**defaults)


# ── Mathematical sanity ─────────────────────────────────────────────────────

def test_at_reference_with_neutral_implied_yields_zero_edge():
    """If current = reference and implied = 0.5, model should also = 0.5."""
    q = _quote(current_price=100_000.0, reference_price=100_000.0, implied_yes_prob=0.5)
    out = price_binary(q)
    assert abs(out.model_yes_prob - 0.5) < 1e-9
    assert abs(out.edge) < 1e-9


def test_well_above_reference_late_window_is_high_yes():
    """BTC at +0.4% above ref with 5min left and 0.13%/min vol → YES highly likely."""
    q = _quote(
        current_price=100_400.0,
        reference_price=100_000.0,
        minutes_to_close=5.0,
        sigma_per_minute=0.0013,
    )
    out = price_binary(q)
    assert out.model_yes_prob > 0.90, f"expected >0.90, got {out.model_yes_prob:.3f}"


def test_well_below_reference_late_window_is_low_yes():
    """Symmetric: BTC at -0.4% below ref → YES very unlikely."""
    q = _quote(
        current_price=99_600.0,
        reference_price=100_000.0,
        minutes_to_close=5.0,
        sigma_per_minute=0.0013,
    )
    out = price_binary(q)
    assert out.model_yes_prob < 0.10


def test_late_window_sharpens_probability():
    """Same edge over reference, less time → probability sharpens toward 1."""
    base = dict(current_price=100_400.0, reference_price=100_000.0, sigma_per_minute=0.0013)
    far = price_binary(_quote(**base, minutes_to_close=60.0))
    near = price_binary(_quote(**base, minutes_to_close=5.0))
    assert near.model_yes_prob > far.model_yes_prob, (
        f"near={near.model_yes_prob:.3f} should exceed far={far.model_yes_prob:.3f}"
    )


def test_zero_time_resolves_deterministically():
    """At T = 0, the answer is just current vs reference."""
    yes_q = _quote(current_price=100_000.01, minutes_to_close=0.0)
    no_q = _quote(current_price=99_999.99, minutes_to_close=0.0)
    assert price_binary(yes_q).model_yes_prob == 1.0
    assert price_binary(no_q).model_yes_prob == 0.0


def test_zero_vol_raises():
    with pytest.raises(ValueError):
        price_binary(_quote(sigma_per_minute=0.0))


def test_negative_price_raises():
    with pytest.raises(ValueError):
        price_binary(_quote(current_price=-1.0))


# ── Edge calculation ────────────────────────────────────────────────────────

def test_yes_cheap_means_positive_edge():
    """If model says 0.95 and market says 0.80, fire YES (edge = +0.15)."""
    q = _quote(
        current_price=100_400.0,
        reference_price=100_000.0,
        minutes_to_close=5.0,
        sigma_per_minute=0.0013,
        implied_yes_prob=0.80,
    )
    out = price_binary(q)
    assert out.edge > 0.10, f"expected meaningful positive edge, got {out.edge:+.3f}"


def test_no_cheap_means_negative_edge():
    """If model says 0.05 and market says 0.20, fire NO (edge = -0.15)."""
    q = _quote(
        current_price=99_600.0,
        reference_price=100_000.0,
        minutes_to_close=5.0,
        sigma_per_minute=0.0013,
        implied_yes_prob=0.20,
    )
    out = price_binary(q)
    assert out.edge < -0.10


# ── Decision wrapper ────────────────────────────────────────────────────────

def test_decide_skips_when_too_close_to_close():
    q = _quote(minutes_to_close=0.5)   # 30s left
    intent = decide_trade(q)
    assert intent.side == "SKIP"
    assert "too close" in intent.reason


def test_decide_skips_when_too_far_from_close():
    q = _quote(minutes_to_close=30.0)  # 30 min — beyond max
    intent = decide_trade(q)
    assert intent.side == "SKIP"
    assert "too far" in intent.reason


def test_decide_fires_yes_on_strong_positive_edge():
    q = _quote(
        current_price=100_400.0,
        reference_price=100_000.0,
        minutes_to_close=5.0,
        sigma_per_minute=0.0013,
        implied_yes_prob=0.80,
    )
    intent = decide_trade(q)
    assert intent.side == "YES"
    assert intent.edge >= EDGE_THRESHOLD


def test_decide_fires_no_on_strong_negative_edge():
    q = _quote(
        current_price=99_600.0,
        reference_price=100_000.0,
        minutes_to_close=5.0,
        sigma_per_minute=0.0013,
        implied_yes_prob=0.20,
    )
    intent = decide_trade(q)
    assert intent.side == "NO"
    assert intent.edge <= -EDGE_THRESHOLD


def test_decide_skips_when_edge_under_threshold():
    """Model and market agree closely → SKIP."""
    q = _quote(
        current_price=100_050.0,       # only +0.05% — small edge
        reference_price=100_000.0,
        minutes_to_close=5.0,
        sigma_per_minute=0.001,
        implied_yes_prob=0.60,
    )
    intent = decide_trade(q)
    assert intent.side == "SKIP"
