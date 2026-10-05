"""Tests for ``trading_paths`` — the single resolver for operator data paths.

The value of this module is that ``TRADING_DATA_DIR`` redirects everything, so
these tests pin both halves of the contract: the override is honoured, and with
no override the paths are byte-identical to the literals they replaced. A change
that quietly turned the relative ``data`` into an absolute path would relocate a
deployment's state, so that case is asserted explicitly.
"""

from __future__ import annotations

import os
from pathlib import Path

import pytest

import trading_paths


@pytest.fixture(autouse=True)
def _no_ambient_override():
    """These tests must control TRADING_DATA_DIR themselves."""
    previous = os.environ.pop("TRADING_DATA_DIR", None)
    try:
        yield
    finally:
        if previous is not None:
            os.environ["TRADING_DATA_DIR"] = previous


def test_data_dir_is_relative_data_when_unset():
    # Relative, not absolute: deployed services and existing assertions compare
    # against "data", and absolutising it would relocate a live deployment.
    assert str(trading_paths.data_dir()) == "data"


def test_data_dir_honours_the_override(tmp_path):
    os.environ["TRADING_DATA_DIR"] = str(tmp_path)
    assert trading_paths.data_dir() == tmp_path


def test_state_path_joins_under_the_data_dir():
    assert trading_paths.state_path("paper_trader_v4_state.json") == Path(
        "data/paper_trader_v4_state.json"
    )


def test_state_path_follows_the_override(tmp_path):
    os.environ["TRADING_DATA_DIR"] = str(tmp_path)
    assert trading_paths.state_path("a.json") == tmp_path / "a.json"
    assert trading_paths.state_path("nested", "b.json") == tmp_path / "nested" / "b.json"


@pytest.mark.parametrize(
    "literal,expected",
    [
        ("data/trading_kill_switch", Path("data/trading_kill_switch")),
        ("data/nested/deep.json", Path("data/nested/deep.json")),
    ],
)
def test_resolve_is_identity_for_data_paths_when_unset(literal, expected):
    assert trading_paths.resolve(literal) == expected


@pytest.mark.parametrize(
    "literal",
    [
        "/etc/passwd",
        "logs/trader.log",
        "config/app.yaml",
        "data",  # the directory itself has no trailing part
    ],
)
def test_resolve_leaves_non_data_paths_alone(literal):
    assert trading_paths.resolve(literal) == Path(literal)


def test_resolve_rewrites_data_paths_under_the_override(tmp_path):
    os.environ["TRADING_DATA_DIR"] = str(tmp_path)
    assert trading_paths.resolve("data/trading_kill_switch") == tmp_path / "trading_kill_switch"
    assert trading_paths.resolve("data/nested/deep.json") == tmp_path / "nested" / "deep.json"


def test_resolve_does_not_touch_other_paths_under_override(tmp_path):
    os.environ["TRADING_DATA_DIR"] = str(tmp_path)
    assert trading_paths.resolve("logs/x.log") == Path("logs/x.log")
    assert trading_paths.resolve("/var/lib/thing") == Path("/var/lib/thing")


def test_is_data_path():
    assert trading_paths.is_data_path("data/x.json")
    assert trading_paths.is_data_path("data")
    assert not trading_paths.is_data_path("database/x.json")
    assert not trading_paths.is_data_path("/abs/data/x.json")
    assert not trading_paths.is_data_path("logs/x.log")


def test_kill_switch_sentinel_is_redirectable():
    """The reason this module exists: this path must not be a bare literal."""
    from coinbase.src.config import KillSwitch

    os.environ["TRADING_DATA_DIR"] = "/tmp/kill-probe"
    try:
        assert KillSwitch.kill_path() == Path("/tmp/kill-probe/trading_kill_switch")
    finally:
        KillSwitch.KILL_PATH = None


def test_kill_switch_honours_an_explicit_override_seam():
    """Tests pin KILL_PATH directly; that must still win over the env redirect."""
    from coinbase.src.config import KillSwitch

    previous = KillSwitch.KILL_PATH
    try:
        KillSwitch.KILL_PATH = Path("/tmp/explicit-kill")
        assert KillSwitch.kill_path() == Path("/tmp/explicit-kill")
    finally:
        KillSwitch.KILL_PATH = previous


def test_production_defaults_actually_land_under_the_override(tmp_path):
    """Behavioural guard for the leak this module was introduced to stop.

    A literal ``data/...`` default is unreachable by TRADING_DATA_DIR, so a test
    run writes operator state again. Asserting the resolved *value* rather than
    the source text is deliberate: a module may legitimately declare a readable
    default and resolve it at use, which a grep for literals cannot distinguish
    from a genuine leak.
    """
    os.environ["TRADING_DATA_DIR"] = str(tmp_path)

    from coinbase.src.capital_buckets import CapitalBucketLedger
    from coinbase.src.live_performance import LivePerformanceTracker
    from coinbase.src.strategy_registry import StrategyRegistry

    tracker = LivePerformanceTracker()
    assert Path(tracker._path).parent == tmp_path

    ledger = CapitalBucketLedger()
    assert Path(ledger.state_path).parent == tmp_path

    registry = StrategyRegistry()
    assert Path(registry.db_path).parent == tmp_path


def test_explicit_non_data_paths_are_still_honoured(tmp_path):
    """An explicit path must win over the redirect, or callers cannot isolate."""
    os.environ["TRADING_DATA_DIR"] = str(tmp_path)
    elsewhere = tmp_path / "elsewhere" / "perf.json"

    from coinbase.src.live_performance import LivePerformanceTracker

    tracker = LivePerformanceTracker(path=str(elsewhere))
    assert Path(tracker._path) == elsewhere