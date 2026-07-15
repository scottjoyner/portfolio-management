"""Tests for trading_system.core.state_manager (Bracket persistence)."""

import json
import os
import threading

import pytest

from trading_system.core import state_manager as sm_mod
from trading_system.core.state_manager import StateManager
from trading_system.core.models.domain import Bracket


def _make_bracket(cid="c1", product="BTC-USD"):
    return Bracket(
        client_order_id=cid,
        product_id=product,
        side="BUY",
        base_size=1.0,
        quote_size=100.0,
        entry_price=100.0,
        stop_loss=90.0,
        take_profit=110.0,
        status="OPEN",
        strategy_id="s1",
        timestamp=123.0,
    )


def _bracket_dict(cid="c1"):
    return {
        "client_order_id": cid,
        "product_id": "BTC-USD",
        "side": "BUY",
        "base_size": 1.0,
        "quote_size": 100.0,
        "entry_price": 100.0,
        "stop_loss": 90.0,
        "take_profit": 110.0,
        "status": "OPEN",
        "strategy_id": "s1",
        "timestamp": 123.0,
        "metadata": {},
    }


@pytest.fixture
def mgr(tmp_path):
    return StateManager(state_dir=str(tmp_path / "state"))


def test_ensure_dir_created(mgr, tmp_path):
    assert os.path.isdir(tmp_path / "state")


def test_load_no_file_returns_empty(mgr):
    assert mgr.load_brackets() == []


def test_save_writes_list_format(mgr):
    # save_brackets writes {"brackets": [...]} (matches load_brackets format)
    mgr.save_brackets([_make_bracket()])
    with open(mgr.brackets_path) as f:
        data = json.load(f)
    assert isinstance(data, dict)
    assert "brackets" in data
    assert data["brackets"][0]["client_order_id"] == "c1"


def test_save_uses_dict_fallback(mgr):
    # Cover the `else b.dict()` branch when object lacks model_dump.
    class FakeB:
        def dict(self):
            return _bracket_dict("fake")

    mgr.save_brackets([FakeB()])  # type: ignore[list-item]
    with open(mgr.brackets_path) as f:
        data = json.load(f)
    assert data["brackets"][0]["client_order_id"] == "fake"


def test_load_success_with_brackets_key(mgr):
    # load_brackets expects {"brackets": [...]} format
    with open(mgr.brackets_path, "w") as f:
        json.dump({"brackets": [_bracket_dict("c1")]}, f)
    loaded = mgr.load_brackets()
    assert len(loaded) == 1
    assert loaded[0].client_order_id == "c1"
    assert loaded[0].stop_loss == 90.0


def test_load_corrupt_json_returns_empty(mgr):
    with open(mgr.brackets_path, "w") as f:
        f.write("{ this is not json ")
    assert mgr.load_brackets() == []


def test_load_missing_brackets_key(mgr):
    with open(mgr.brackets_path, "w") as f:
        json.dump({"other": []}, f)
    assert mgr.load_brackets() == []


def test_add_bracket(mgr):
    # Fixed: source now uses a reentrant RLock so add_bracket() can safely call
    # load_brackets() while holding the lock (no deadlock).
    mgr.add_bracket(_make_bracket("a"))
    assert os.path.exists(mgr.brackets_path)
    loaded = mgr.load_brackets()
    assert len(loaded) == 1
    assert loaded[0].client_order_id == "a"


def test_remove_bracket_by_id(mgr):
    # Pre-seed with the load-expected format so remove can find it.
    with open(mgr.brackets_path, "w") as f:
        json.dump({"brackets": [_bracket_dict("a"), _bracket_dict("b")]}, f)
    mgr.remove_bracket_by_id("a")
    # save_brackets writes {"brackets": [...]}; verify removal via the raw file
    with open(mgr.brackets_path) as f:
        raw = json.load(f)
    assert len(raw["brackets"]) == 1
    assert raw["brackets"][0]["client_order_id"] == "b"


def test_remove_nonexistent_no_error(mgr):
    mgr.lock = threading.RLock()
    mgr.remove_bracket_by_id("nope")
    assert mgr.load_brackets() == []


def test_singleton_exists():
    assert isinstance(sm_mod.state_manager, StateManager)
