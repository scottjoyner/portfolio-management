"""Coverage tests for strategies.registry.registry and strategies.lifecycle.

The lifecycle module is heavily DB-coupled; the ORM classes and repository are
replaced with lightweight fakes/mocks so no database is touched.
"""
from __future__ import annotations

import sys
import types

import pytest


# ---------------------------------------------------------------------------
# registry/registry.py
# ---------------------------------------------------------------------------

def test_registry_registry_load():
    from trading_system.strategies.registry import registry as reg

    strategies = reg.load_strategies()
    assert strategies
    ids = [s.strategy_id for s in strategies]
    assert len(ids) == len(set(ids))  # no duplicate ids

    index = reg.strategy_metadata_index()
    assert len(index) == len(strategies)
    # every entry maps id -> metadata dict
    for sid, meta in index.items():
        assert isinstance(meta, dict)


# ---------------------------------------------------------------------------
# lifecycle.py
# ---------------------------------------------------------------------------

class _FakeQuery:
    def __init__(self, result_first=None, result_all=None):
        self._first = result_first
        self._all = result_all or []

    def filter(self, *a, **k):
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

    def query(self, *a, **k):
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
    import trading_system.strategies.lifecycle as lc
    from unittest.mock import MagicMock
    # Replace ORM model classes with MagicMock so class-attribute access used in
    # SQLAlchemy-style query filters does not blow up on the plain data classes.
    monkeypatch.setattr(lc, "StrategyConfig", MagicMock(name="StrategyConfig"))
    monkeypatch.setattr(lc, "StrategyRun", MagicMock(name="StrategyRun"))
    return lc


def test_lifecycle_start_creates_config(lifecycle):
    db = _FakeDB()
    db._query_result = _FakeQuery(result_first=None)  # config missing
    repo = _FakeRepo(db)
    mgr = lifecycle.StrategyLifecycleManager(repo=repo)

    run = mgr.start("strat-1", mode="paper")
    assert run is not None
    assert db.added and db.commits >= 1
    assert repo.created


def test_lifecycle_start_existing_enabled(lifecycle):
    cfg = types.SimpleNamespace(enabled=True)
    db = _FakeDB()
    db._query_result = _FakeQuery(result_first=cfg)
    repo = _FakeRepo(db)
    mgr = lifecycle.StrategyLifecycleManager(repo=repo)
    run = mgr.start("strat-2")
    assert repo.created


def test_lifecycle_start_disabled_raises(lifecycle):
    cfg = types.SimpleNamespace(enabled=False)
    db = _FakeDB()
    db._query_result = _FakeQuery(result_first=cfg)
    repo = _FakeRepo(db)
    mgr = lifecycle.StrategyLifecycleManager(repo=repo)
    with pytest.raises(lifecycle.StrategyLifecycleError):
        mgr.start("strat-3")


def test_lifecycle_stop_pause_resume(lifecycle):
    db = _FakeDB()
    repo = _FakeRepo(db)
    mgr = lifecycle.StrategyLifecycleManager(repo=repo)

    # stop: run exists
    repo._runs["t1"] = types.SimpleNamespace(task_id="t1")
    assert mgr.stop("t1") is not None
    # stop: run missing
    assert mgr.stop("nope") is None

    assert mgr.pause("t1") is not None
    assert mgr.resume("t1") is not None


def test_lifecycle_running_and_enable_disable(lifecycle):
    db = _FakeDB()
    db._query_result = _FakeQuery(result_all=["r1", "r2"])
    repo = _FakeRepo(db)
    mgr = lifecycle.StrategyLifecycleManager(repo=repo)
    assert mgr.running_strategies() == ["r1", "r2"]

    # enable / disable when config present
    cfg = types.SimpleNamespace(enabled=False)
    db._query_result = _FakeQuery(result_first=cfg)
    assert mgr.enable("s").enabled is True
    assert mgr.disable("s").enabled is False

    # enable / disable when config missing
    db._query_result = _FakeQuery(result_first=None)
    assert mgr.enable("missing") is None
    assert mgr.disable("missing") is None


def test_lifecycle_disabled_ids(lifecycle):
    db = _FakeDB()
    db._query_result = _FakeQuery(result_all=[("a",), ("b",)])
    repo = _FakeRepo(db)
    mgr = lifecycle.StrategyLifecycleManager(repo=repo)
    assert mgr.disabled_ids() == {"a", "b"}


def test_lifecycle_sync_catalog(monkeypatch, lifecycle):
    # Fake the OpsRepository module resolved via __import__.
    fake_repo_mod = types.ModuleType("storage.postgres.repository")

    class _OpsRepo:
        def __init__(self, db):
            self.db = db

    fake_repo_mod.OpsRepository = _OpsRepo
    monkeypatch.setitem(sys.modules, "storage.postgres.repository", fake_repo_mod)

    # Fake strategies returned by load_strategies.
    class _Strat:
        def __init__(self, sid, exists):
            self.strategy_id = sid
            self._exists = exists

        def metadata(self):
            return {"strategy_type": "trend", "config": {"k": 1}}

    strat_new = _Strat("new-1", False)
    strat_existing = _Strat("old-1", True)
    monkeypatch.setattr(lifecycle, "load_strategies",
                        lambda: [strat_new, strat_existing])

    existing_cfg = types.SimpleNamespace(strategy_type="x", config_json="{}")

    class _DB:
        def __init__(self):
            self.added = []
            self.commits = 0
            self.calls = 0

        def query(self, model):
            db = self

            class _Q:
                def filter(self, *a, **k):
                    return self

                def first(self):
                    db.calls += 1
                    # first strategy -> missing, second -> existing
                    return None if db.calls == 1 else existing_cfg
            return _Q()

        def add(self, obj):
            self.added.append(obj)

        def commit(self):
            self.commits += 1

    db = _DB()
    synced = lifecycle.sync_catalog_to_db(db)
    assert len(synced) == 2
    assert db.added  # new config was added
    assert db.commits >= 1
