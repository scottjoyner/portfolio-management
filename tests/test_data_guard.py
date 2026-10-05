"""Tests for ``tests/data_guard`` — the guard against writing live operator state.

This guard is the last line of defence after ``TRADING_DATA_DIR`` redirection
proved insufficient: background ``socketserver`` threads started by the trader
suites outlive their fixture and write once the override is gone. On 2026-10-04
that silently destroyed a paper-trading ledger and the trader's live-performance
records before the guard existed.

The guard is installed for the whole session by ``tests/conftest.py``, so these
tests deliberately do **not** install/uninstall it per test: doing so would fight
the session state and, worse, a test that uninstalled it and then failed could
leave the rest of the session unguarded. Instead the decision function
(``_is_blocked`` / ``_check``) is exercised directly, and a single test covers the
install/uninstall lifecycle with its state saved and restored.
"""

from __future__ import annotations

import builtins
import os
import pathlib

import pytest

from tests import data_guard


@pytest.fixture
def _guard_state():
    """Snapshot the module's patching state and restore it unconditionally."""
    was_installed = data_guard._INSTALLED
    saved_originals = dict(data_guard._ORIGINALS)
    try:
        yield
    finally:
        if was_installed and not data_guard._INSTALLED:
            data_guard.install()
        elif not was_installed and data_guard._INSTALLED:
            data_guard.uninstall()
        data_guard._ORIGINALS.clear()
        data_guard._ORIGINALS.update(saved_originals)


# ---------------------------------------------------------------------------
# Decision logic
# ---------------------------------------------------------------------------

def test_live_data_dir_points_at_the_repo_data_directory():
    assert data_guard.live_data_dir().name == "data"
    assert (data_guard.live_data_dir() / "paper_trader_v4_state.json").exists()


@pytest.mark.parametrize(
    "target",
    [
        "data/live_performance.json",
        "data/paper_trader_v4_state.json",
        "data/capital_buckets.json",
        "data/state_backups/paper_trader_v4_20261004_074757.json",
        "data/feed_cache/candles.parquet",
        "data",
    ],
)
def test_writes_into_data_are_blocked(target):
    assert data_guard._is_blocked(target) is True


@pytest.mark.parametrize(
    "target",
    [
        "data/.gitkeep",
        "/etc/passwd",
        "logs/trader.log",
        "/tmp/scratch.json",
        "tests/data_guard.py",
        "database/x.json",
    ],
)
def test_everything_else_is_allowed(target):
    assert data_guard._is_blocked(target) is False


def test_absolute_paths_under_the_live_data_dir_are_blocked():
    nested = data_guard.live_data_dir() / "state_backups" / "x.json"
    assert data_guard._is_blocked(str(nested)) is True
    assert data_guard._is_blocked(str(data_guard.live_data_dir() / "x.json")) is True


def test_check_raises_for_blocked_paths():
    with pytest.raises(RuntimeError, match="live data directory"):
        data_guard._check("data/live_performance.json", "unit-test")


def test_check_is_silent_for_allowed_paths():
    assert data_guard._check("/tmp/scratch.json", "unit-test") is None


def test_check_warns_when_the_allow_env_is_set(monkeypatch):
    monkeypatch.setenv(data_guard.ALLOW_ENV, "1")
    with pytest.warns(RuntimeWarning, match="live data directory"):
        data_guard._check("data/live_performance.json", "unit-test")


def test_allow_env_needs_exactly_one(monkeypatch):
    """A typo'd opt-out must not silently disable the guard."""
    monkeypatch.setenv(data_guard.ALLOW_ENV, "true")
    with pytest.raises(RuntimeError, match="live data directory"):
        data_guard._check("data/live_performance.json", "unit-test")


# ---------------------------------------------------------------------------
# Lifecycle — one test, because the guard is session-global
# ---------------------------------------------------------------------------

def test_install_blocks_writes_and_uninstall_restores_them(tmp_path, _guard_state):
    data_guard.uninstall()
    assert not data_guard._INSTALLED
    real_write_text = pathlib.Path.write_text
    real_open = builtins.open
    real_unlink = pathlib.Path.unlink
    real_touch = pathlib.Path.touch

    data_guard.install()
    try:
        assert data_guard._INSTALLED
        blocked = data_guard.live_data_dir() / "_guard_lifecycle_probe.json"
        for action in (
            lambda: blocked.write_text("nope"),
            lambda: blocked.touch(),
            lambda: open(blocked, "w"),
            lambda: blocked.unlink(missing_ok=True),
            lambda: os.unlink(blocked),
        ):
            with pytest.raises(RuntimeError, match="live data directory"):
                action()
        assert not blocked.exists()
    finally:
        data_guard.uninstall()

    assert not data_guard._INSTALLED
    assert pathlib.Path.write_text is real_write_text
    assert pathlib.Path.touch is real_touch
    assert pathlib.Path.unlink is real_unlink
    assert builtins.open is real_open

    # And the filesystem genuinely works again afterwards.
    probe = tmp_path / "after.json"
    probe.write_text("ok")
    probe.unlink()
    assert not probe.exists()


def test_install_is_idempotent(_guard_state):
    data_guard.install()
    first = pathlib.Path.write_text
    data_guard.install()
    assert pathlib.Path.write_text is first


def test_uninstall_is_safe_when_not_installed(_guard_state):
    data_guard.uninstall()
    data_guard.uninstall()  # must not raise


# ---------------------------------------------------------------------------
# Session-wide behaviour
# ---------------------------------------------------------------------------

def test_reads_are_never_blocked(tmp_path):
    """A guard that blocked reads would fail every test that inspects live state."""
    probe = tmp_path / "r.json"
    probe.write_text("readable")
    assert probe.read_text() == "readable"
    with probe.open("r") as fh:
        assert fh.read() == "readable"
    assert list(probe.parent.iterdir())


def test_reading_live_state_is_allowed():
    state = data_guard.live_data_dir() / "paper_trader_v4_state.json"
    if state.exists():
        assert state.read_text()


def test_the_session_override_is_installed_for_this_run():
    """tests/conftest.py points the trader at scratch storage for the session."""
    override = os.environ.get("TRADING_DATA_DIR")
    assert override, "TRADING_DATA_DIR must be set for the whole test session"
    assert pathlib.Path(override).resolve() != data_guard.live_data_dir()


def test_guard_is_active_for_this_session():
    """If this ever regresses, tests would silently start writing live state.

    Asserted by observable behaviour rather than ``_INSTALLED`` so it does not
    depend on whether an earlier test in this file exercised the lifecycle.
    """
    probe = data_guard.live_data_dir() / "_guard_session_probe.json"
    with pytest.raises(RuntimeError, match="live data directory"):
        probe.write_text("must not land")
    assert not probe.exists()