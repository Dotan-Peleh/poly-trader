"""Tests for reference-tracker — uses an in-memory SQLite for isolation."""
from datetime import datetime, timedelta

import pytest

from data import storage
from data import reference_tracker


@pytest.fixture(autouse=True)
def _isolated_db(monkeypatch, tmp_path):
    """Each test gets its own SQLite file — keeps tests deterministic."""
    db = tmp_path / "test.db"
    from sqlalchemy import create_engine
    new_engine = create_engine(f"sqlite:///{db}", future=True)
    storage.engine = new_engine
    storage.Base.metadata.create_all(new_engine)
    monkeypatch.setattr(reference_tracker, "engine", new_engine, raising=False)
    yield


def test_previous_hour_boundary_strips_seconds():
    t = datetime(2026, 5, 6, 18, 35, 12, 999_999)
    assert reference_tracker.previous_hour_boundary(t) == datetime(2026, 5, 6, 18, 0, 0)


def test_btc_price_at_returns_nearest_within_window():
    base = datetime(2026, 5, 6, 18, 0, 0)
    storage.save_btc_tick(base + timedelta(seconds=10), 80_100.0)
    storage.save_btc_tick(base + timedelta(minutes=2), 80_200.0)
    storage.save_btc_tick(base - timedelta(minutes=1), 80_050.0)

    # Looking up exactly at base — should return the one 10s after (next-or-equal)
    assert reference_tracker.btc_price_at(base) == 80_100.0


def test_btc_price_at_returns_none_when_no_ticks_in_window():
    base = datetime(2026, 5, 6, 18, 0, 0)
    storage.save_btc_tick(base + timedelta(minutes=20), 80_500.0)
    assert reference_tracker.btc_price_at(base, window_minutes=5) is None
