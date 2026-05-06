"""Tests for risk/position_sizer.py — Kelly math + sizing rules."""
import pytest

from risk.position_sizer import (
    kelly_fraction_yes, kelly_fraction_no, size_trade, SizingResult,
)
from config import settings as settings_mod


def test_kelly_yes_no_edge_returns_zero():
    """If model = ask, no edge → no bet."""
    assert kelly_fraction_yes(0.50, 0.50) == 0.0


def test_kelly_yes_full_edge():
    """Model 0.70, ask 0.50 → Kelly = (0.7 − 0.5) / (1 − 0.5) = 0.4."""
    assert abs(kelly_fraction_yes(0.70, 0.50) - 0.4) < 1e-9


def test_kelly_yes_no_negative():
    """When model < ask, fraction should be 0 not negative."""
    assert kelly_fraction_yes(0.40, 0.50) == 0.0


def test_kelly_no_basic():
    """Model YES = 0.30 → win prob NO = 0.70. NO ask = 0.50 → Kelly = (0.7−0.5)/(1−0.5) = 0.4."""
    assert abs(kelly_fraction_no(0.30, 0.50) - 0.4) < 1e-9


def test_size_trade_yes_with_edge():
    """Strong edge produces a sized trade."""
    result = size_trade(
        side="YES", model_yes_prob=0.70,
        yes_ask=0.50, no_ask=0.55,
        bankroll=100.0,
    )
    assert result.size_usd > 0
    assert result.kelly_fraction > 0


def test_size_trade_caps_at_max_pct(monkeypatch):
    """Even with huge Kelly, must not exceed max_position_pct."""
    monkeypatch.setattr(settings_mod.settings, "max_position_pct", 0.05)
    monkeypatch.setattr(settings_mod.settings, "kelly_fraction_divisor", 1.0)  # full Kelly
    result = size_trade(
        side="YES", model_yes_prob=0.95,
        yes_ask=0.50, no_ask=0.55,
        bankroll=100.0,
    )
    # 5% of 100 = $5 cap
    assert result.size_usd <= 5.0


def test_size_trade_below_min_returns_zero():
    """Tiny bankroll → trade size below $1 should skip."""
    result = size_trade(
        side="YES", model_yes_prob=0.55,
        yes_ask=0.52, no_ask=0.50,
        bankroll=1.0,
    )
    assert result.size_usd == 0.0


def test_size_trade_no_edge_skips():
    """Edge in wrong direction → SKIP."""
    result = size_trade(
        side="YES", model_yes_prob=0.40,
        yes_ask=0.50, no_ask=0.55,
        bankroll=100.0,
    )
    assert result.size_usd == 0.0
    assert result.units == 0.0
