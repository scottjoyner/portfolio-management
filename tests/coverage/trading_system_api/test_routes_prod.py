"""Tests for trading_system.api.routes_prod (>=90% line+branch)."""

import asyncio
import builtins
import sys
import types

import pytest

import trading_system.api.routes_prod as m


def run(coro):
    return asyncio.run(coro)


from datetime import datetime, timezone


def _sample_obj():
    now = datetime.now(timezone.utc)
    o = _Obj()
    o.id = 1
    o.name = "n"
    o.provider = "p"
    o.currency = "USD"
    o.current_balance = 100.0
    o.fiat_balance = 0.0
    o.status = "active"
    o.created_at = now
    o.institution_name = None
    o.product_id = "BTC-USD"
    o.side = "buy"
    o.remaining_size = 1.0
    o.price = 100.0
    o.order_id = "o1"
    o.approval_id = None
    o.fee = 0.0
    o.exchange = None
    o.approval_type = "order"
    o.summary = "s"
    o.capital_affected = 0.0
    o.approved_by = None
    o.liquidity_impact = 0.0
    o.config_key = "k"
    o.description = "d"
    o.category = "momentum"
    o.last_backtest = now
    o.backtested = False
    o.date = now
    o.title = "t"
    o.content = "c"
    return o


class _Chain:
    def filter(self, *a, **k):
        return self

    def order_by(self, *a, **k):
        return self

    def limit(self, *a, **k):
        return self

    def offset(self, *a, **k):
        return self

    def all(self):
        return [_sample_obj()]

    def first(self):
        return None


class FakeDB:
    def query(self, *a, **k):
        return _Chain()


class _Obj:
    def __init__(self, **kw):
        for k, v in kw.items():
            setattr(self, k, v)


@pytest.fixture
def storage_mock(monkeypatch):
    models = types.ModuleType("storage.postgres.models")
    _model_attrs = {
        "Account": {"status": "active"},
        "TradeOrder": {"created_at": None},
        "StrategyConfig": {"status": "active"},
        "CapitalBucket": {"status": "active"},
        "Approval": {"created_at": None},
        "ResearchNote": {"created_at": None},
    }
    for n in ["Account", "TradeOrder", "StrategyConfig", "CapitalBucket", "Approval", "ResearchNote"]:
        setattr(models, n, type(n, (), _model_attrs.get(n, {})))
    sys.modules["storage.postgres.models"] = models
    orig = builtins.__import__

    def fake(name, *a, **k):
        if name == "storage.postgres.models":
            return models
        return orig(name, *a, **k)

    monkeypatch.setattr(builtins, "__import__", fake)
    monkeypatch.setattr(m, "DATABASE_MODE", True)
    yield
    sys.modules.pop("storage.postgres.models", None)


# ---------------- DATABASE_MODE False (else branches) ----------------
@pytest.fixture
def db_off(monkeypatch):
    monkeypatch.setattr(m, "DATABASE_MODE", False)


def test_health_check(db_off):
    r = run(m.health_check())
    assert r["status"] == "healthy"
    assert r["components"]["database"] is False


def test_health_check_with_db():
    r = run(m.health_check(db=FakeDB()))
    assert r["components"]["database"] is True


def test_get_metrics_no_db(db_off):
    r = run(m.get_metrics())
    assert "metrics" in r


def test_get_metrics_with_db():
    r = run(m.get_metrics(db=FakeDB()))
    assert "metrics" in r


def test_list_accounts_no_db(db_off):
    r = run(m.list_accounts())
    assert r["total_accounts"] == 0


def test_list_trades_no_db(db_off):
    r = run(m.list_trades())
    assert r["total_trades"] == 0


def test_list_positions_no_db(db_off):
    r = run(m.list_positions())
    assert r["total_positions"] == 0


def test_list_strategies_no_db(db_off):
    r = run(m.list_strategies())
    assert r["total_strategies"] == 0


def test_get_performance_no_db(db_off):
    r = run(m.get_performance())
    assert r["portfolio_performance"] == {}


def test_get_approvals_no_db(db_off):
    r = run(m.get_approvals())
    assert r["pending_count"] == 0


def test_get_research_hypotheses_no_db(db_off):
    r = run(m.get_research_hypotheses())
    assert r["hypotheses"] == []


# ---------------- DATABASE_MODE True, db provided (happy paths) ----------------
def test_list_accounts_db(storage_mock):
    r = run(m.list_accounts(db=FakeDB()))
    assert "accounts" in r


def test_list_strategies_db(storage_mock):
    r = run(m.list_strategies(db=FakeDB()))
    assert "strategies" in r


def test_get_db_session():
    assert m.get_db_session() is not None


def test_get_performance_db(storage_mock):
    r = run(m.get_performance(db=FakeDB()))
    assert "portfolio_performance" in r


def test_get_approvals_db(storage_mock):
    r = run(m.get_approvals(db=FakeDB()))
    assert "approvals" in r


def test_get_research_hypotheses_db(storage_mock):
    r = run(m.get_research_hypotheses(db=FakeDB()))
    assert "hypotheses" in r


# ---------------- DATABASE_MODE True, no db (except branches) ----------------
def test_list_trades_dbmode_no_db(storage_mock):
    r = run(m.list_trades())
    assert r["total_trades"] == 0


def test_list_positions_dbmode_no_db(storage_mock):
    r = run(m.list_positions())
    assert r["total_positions"] == 0


# ---------------- misc endpoints ----------------
def test_sync_account_transactions():
    r = run(m.sync_account_transactions("acc1"))
    assert r["status"] == "sync_started"


def test_get_price_estimations():
    r = run(m.get_price_estimations("BTC-USD"))
    assert r["instrument"] == "BTC-USD"
    assert r["confidence_score"] == 0.85


async def _ep(db=None):
    return {"ok": True}


def test_endpoint_wrapper_dict():
    r = run(m.endpoint_wrapper(_ep, db_session=FakeDB()))
    assert r["ok"] is True
    assert "timestamp" in r


def test_endpoint_wrapper_nondict():
    async def _ep2(db=None):
        return [1, 2]
    r = run(m.endpoint_wrapper(_ep2, db_session=FakeDB()))
    assert isinstance(r, list)
