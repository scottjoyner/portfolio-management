"""Coverage tests for the strategy registry and lifecycle manager."""
from __future__ import annotations

import importlib
import sys
import types

import pytest


def test_registry_registry_load():
    from trading_system.strategies.registry import registry as registry

    strategies = registry.load_strategies()
    assert strategies
    strategy_ids = [strategy.strategy_id for strategy in strategies]
    assert len(strategy_ids) == len(set(strategy_ids))
    index = registry.strategy_metadata_index()
    assert len(index) == len(strategies)
    assert all(isinstance(metadata, dict) for metadata in index.values())


class _FakeQuery:
    def __init__(self, result_first=None, result_all=None):
        self._first = result_first
        self._all = result_all or []

    def filter(self, *args, **kwargs):
        return self

    def first(self):
        return self._first

    def all(self):
        return self._all


class _FakeDB:
    def __init__(self):
        self.added = []
        self.commits = 0
        self._query_result = _FakeQuery()

    def query(self, *args, **kwargs):
        return self._query_result

    def add(self, obj):
        self.added.append(obj)

    def commit(self):
        self.commits += 1


class _FakeRepo:
    def __init__(self, db):
        self.db = db
        self.created = []
        self.updates = []
        self._runs = {}

    def create_strategy_run(self, run):
        self.created.append(run)
        self._runs[run.task_id] = run
        return run

    def get_strategy_run(self, task_id):
        return self._runs.get(task_id)

    def update_strategy_run(self, task_id, **kwargs):
        self.updates.append((task_id, kwargs))
        return {"task_id": task_id, **kwargs}


@pytest.fixture
def lifecycle(monkeypatch):
    # The root generated-coverage conftest installs a minimal lifecycle stub for
    # unrelated tests. This file explicitly exercises the real implementation.
    sys.modules.pop("strategies.lifecycle", None)
    sys.modules.pop("trading_system.strategies.lifecycle", None)
    module = importlib.import_module("trading_system.strategies.lifecycle")
    from unittest.mock import MagicMock

    monkeypatch.setattr(module, "StrategyConfig", MagicMock(name="StrategyConfig"))
    monkeypatch.setattr(module, "StrategyRun", MagicMock(name="StrategyRun"))
    return module


def test_lifecycle_start_creates_config(lifecycle):
    db = _FakeDB()
    db._query_result = _FakeQuery(result_first=None)
    repo = _FakeRepo(db)
    run = lifecycle.StrategyLifecycleManager(repo=repo).start("strat-1", mode="paper")
    assert run is not None
    assert db.added and db.commits >= 1
    assert repo.created


def test_lifecycle_start_existing_enabled(lifecycle):
    db = _FakeDB()
    db._query_result = _FakeQuery(result_first=types.SimpleNamespace(enabled=True))
    repo = _FakeRepo(db)
    lifecycle.StrategyLifecycleManager(repo=repo).start("strat-2")
    assert repo.created


def test_lifecycle_start_disabled_raises(lifecycle):
    db = _FakeDB()
    db._query_result = _FakeQuery(result_first=types.SimpleNamespace(enabled=False))
    with pytest.raises(lifecycle.StrategyLifecycleError):
        lifecycle.StrategyLifecycleManager(repo=_FakeRepo(db)).start("strat-3")


def test_lifecycle_stop_pause_resume(lifecycle):
    repo = _FakeRepo(_FakeDB())
    manager = lifecycle.StrategyLifecycleManager(repo=repo)
    repo._runs["t1"] = types.SimpleNamespace(task_id="t1")
    assert manager.stop("t1") is not None
    assert manager.stop("nope") is None
    assert manager.pause("t1") is not None
    assert manager.resume("t1") is not None


def test_lifecycle_running_and_enable_disable(lifecycle):
    db = _FakeDB()
    db._query_result = _FakeQuery(result_all=["r1", "r2"])
    manager = lifecycle.StrategyLifecycleManager(repo=_FakeRepo(db))
    assert manager.running_strategies() == ["r1", "r2"]

    config = types.SimpleNamespace(enabled=False)
    db._query_result = _FakeQuery(result_first=config)
    assert manager.enable("s").enabled is True
    assert manager.disable("s").enabled is False

    db._query_result = _FakeQuery(result_first=None)
    assert manager.enable("missing") is None
    assert manager.disable("missing") is None


def test_lifecycle_disabled_ids(lifecycle):
    db = _FakeDB()
    db._query_result = _FakeQuery(result_all=[("a",), ("b",)])
    manager = lifecycle.StrategyLifecycleManager(repo=_FakeRepo(db))
    assert manager.disabled_ids() == {"a", "b"}


def test_lifecycle_sync_catalog(monkeypatch, lifecycle):
    fake_repo_module = types.ModuleType("storage.postgres.repository")

    class _OpsRepo:
        def __init__(self, db):
            self.db = db

    fake_repo_module.OpsRepository = _OpsRepo
    monkeypatch.setitem(sys.modules, "storage.postgres.repository", fake_repo_module)

    class _Strategy:
        def __init__(self, strategy_id):
            self.strategy_id = strategy_id

        def metadata(self):
            return {"strategy_type": "trend", "config": {"k": 1}}

    monkeypatch.setattr(lifecycle, "load_strategies", lambda: [_Strategy("new-1"), _Strategy("old-1")])
    existing_config = types.SimpleNamespace(strategy_type="x", config_json="{}")

    class _DB:
        def __init__(self):
            self.added = []
            self.commits = 0
            self.calls = 0

        def query(self, model):
            database = self

            class _Query:
                def filter(self, *args, **kwargs):
                    return self

                def first(self):
                    database.calls += 1
                    return None if database.calls == 1 else existing_config

            return _Query()

        def add(self, obj):
            self.added.append(obj)

        def commit(self):
            self.commits += 1

    database = _DB()
    synced = lifecycle.sync_catalog_to_db(database)
    assert len(synced) == 2
    assert database.added
    assert database.commits >= 1
