"""Tests for trading_system.core.state_manager (atomic writes, locking, recovery)."""
import os
import json
import threading

import pytest

from trading_system.core.state_manager import StateManager, FileLock
from trading_system.core.models.domain import Bracket


def _make_bracket(client_order_id="b1"):
    return Bracket(
        client_order_id=client_order_id,
        product_id="BTC-USD",
        side="BUY",
        base_size=0.01,
        quote_size=100.0,
        entry_price=10000.0,
        stop_loss=9000.0,
        take_profit=11000.0,
        status="OPEN",
        strategy_id="s1",
        timestamp=1.0,
    )


def test_atomic_write_never_leaves_partial_target(tmp_path):
    sm = StateManager(state_dir=str(tmp_path))
    brackets = [_make_bracket()]
    sm.save_brackets(brackets)

    target = sm.brackets_path
    assert os.path.exists(target)
    content = open(target, "r").read()
    parsed = json.loads(content)
    assert len(parsed["brackets"]) == 1

    stray = [p for p in os.listdir(str(tmp_path)) if p.startswith(".tmp-")]
    assert stray == [], "temp file should have been renamed/removed"


def test_atomic_write_rollback_on_failure(tmp_path, monkeypatch):
    sm = StateManager(state_dir=str(tmp_path))
    sm.save_brackets([_make_bracket()])

    def boom(*a, **k):
        raise RuntimeError("simulated fs failure")

    monkeypatch.setattr("os.rename", boom)
    with pytest.raises(RuntimeError):
        sm.save_brackets([_make_bracket("b2")])

    stray = [p for p in os.listdir(str(tmp_path)) if p.endswith(".tmp")]
    assert stray == [], "partial temp must be cleaned up after failure"

    restored = sm.load_brackets()
    assert len(restored) == 1
    assert restored[0].client_order_id == "b1"


def test_load_recovers_from_backup_on_corruption(tmp_path):
    sm = StateManager(state_dir=str(tmp_path))
    sm.save_brackets([_make_bracket("orig")])

    with open(sm.brackets_path, "w") as f:
        f.write("{ this is not valid json")

    recovered = sm.load_brackets()
    assert len(recovered) == 1
    assert recovered[0].client_order_id == "orig"


def test_load_raises_when_no_backup(tmp_path):
    sm = StateManager(state_dir=str(tmp_path))
    with open(sm.brackets_path, "w") as f:
        f.write("not json at all")

    with pytest.raises(Exception):
        sm.load_brackets()


def test_filelock_serializes_concurrent_writers(tmp_path):
    lock_path = str(tmp_path / "x.lock")
    results = []
    errors = []

    def worker():
        try:
            with FileLock(lock_path, timeout=5):
                results.append(threading.get_ident())
                import time
                time.sleep(0.05)
        except Exception as e:
            errors.append(e)

    threads = [threading.Thread(target=worker) for _ in range(5)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert errors == [], f"unexpected lock errors: {errors}"
    assert len(results) == 5


def test_load_skips_invalid_record(tmp_path):
    sm = StateManager(state_dir=str(tmp_path))
    good = _make_bracket("ok")
    sm.save_brackets([good])

    data = {
        "brackets": [
            good.model_dump(),
            {"client_order_id": "bad"},
        ]
    }
    with open(sm.brackets_path, "w") as f:
        json.dump(data, f)

    loaded = sm.load_brackets()
    assert len(loaded) == 1
    assert loaded[0].client_order_id == "ok"


def test_concurrent_save_no_corruption(tmp_path):
    sm = StateManager(state_dir=str(tmp_path))

    def writer(i):
        sm.save_brackets([_make_bracket(f"b{i}") for i in range(3)])

    threads = [threading.Thread(target=writer, args=(i,)) for i in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    loaded = sm.load_brackets()
    assert len(loaded) == 3
