"""Tests for trading_system.api.routes (>=90% line+branch)."""

import asyncio

import pytest

import trading_system.api.routes as m


class MappingRow:
    def __init__(self, data):
        self._mapping = data

    def __getattr__(self, k):
        return self._mapping.get(k)


class PosRow:
    def __init__(self, data):
        for k, v in data.items():
            setattr(self, k, v)


class FakeResult:
    def __init__(self, rows, mapping=True):
        self._rows = rows
        self._mapping = mapping

    def fetchall(self):
        return [self._mk(r) for r in self._rows]

    def fetchone(self):
        return self._mk(self._rows[0]) if self._rows else None

    def _mk(self, data):
        return MappingRow(data) if self._mapping else PosRow(data)


def run(coro):
    return asyncio.run(coro)


def patch_exec(monkeypatch, rows, mapping=True):
    res = FakeResult(rows, mapping=mapping)
    monkeypatch.setattr(m, "_exec", lambda q, p=None: res)
    return res


def patch_exec_raise(monkeypatch):
    def _boom(q, p=None):
        raise RuntimeError("db down")
    monkeypatch.setattr(m, "_exec", _boom)


# ----------------------------- health_check -----------------------------
def test_health_check_ok(monkeypatch):
    patch_exec(monkeypatch, [{"status": "healthy"}])
    r = run(m.health_check())
    assert r["status"] == "healthy"
    assert r["database"] == "connected"


def test_health_check_none_status(monkeypatch):
    patch_exec(monkeypatch, [{"status": None}])
    r = run(m.health_check())
    assert r["status"] == "healthy"


def test_health_check_error(monkeypatch):
    patch_exec_raise(monkeypatch)
    r = run(m.health_check())
    assert r["status"] == "unhealthy"


# ----------------------------- get_accounts -----------------------------
def test_get_accounts_named(monkeypatch):
    patch_exec(monkeypatch, [{
        "id": "1", "name": "A", "type": "ACTIVE", "provider": "P",
        "currency": "USD", "balance_usd": 100.0,
    }])
    accts = run(m.get_accounts())
    assert accts[0]["id"] == "1"
    assert accts[0]["balance_usd"] == 100.0


def test_get_accounts_col_keys(monkeypatch):
    patch_exec(monkeypatch, [{
        "col_0": "1", "col_1": "A", "col_2": "ACTIVE", "col_3": "P",
        "col_4": "USD", "col_5": 100.0,
    }])
    accts = run(m.get_accounts())
    assert accts[0]["name"] == "A"
    assert accts[0]["balance_usd"] == 100.0


def test_get_accounts_positional(monkeypatch):
    patch_exec(monkeypatch, [{
        "id": "1", "name": "A", "type": "ACTIVE", "provider": "P",
        "currency": "USD", "balance_usd": 100.0,
    }], mapping=False)
    accts = run(m.get_accounts())
    assert accts[0]["id"] == "1"


def test_get_accounts_error(monkeypatch):
    patch_exec_raise(monkeypatch)
    assert run(m.get_accounts()) == []


# ----------------------------- get_metrics -----------------------------
def test_get_metrics_named(monkeypatch):
    patch_exec(monkeypatch, [{"col_0": 5, "col_1": 1000.0, "col_2": 3}])
    r = run(m.get_metrics())
    assert r["active_portfolios"] == 5
    assert r["total_assets_usd"] == 1000.0


def test_get_metrics_positional(monkeypatch):
    patch_exec(monkeypatch, [{"col_0": 5, "col_1": 1000.0, "col_2": 3}], mapping=False)
    r = run(m.get_metrics())
    assert r is None


def test_get_metrics_error(monkeypatch):
    patch_exec_raise(monkeypatch)
    r = run(m.get_metrics())
    assert r["active_portfolios"] == 0


# ----------------------------- list_trades -----------------------------
def test_list_trades_named(monkeypatch):
    patch_exec(monkeypatch, [{
        "order_id": "o1", "product_id": "BTC-USD", "side": "buy",
        "original_size": 1.0, "filled_size": 1.0, "remaining_size": 0.0,
        "price_per_unit": 100.0, "created_at": "t", "status": "CLOSED",
    }])
    trades = run(m.list_trades(10, 0))
    assert trades[0]["product_id"] == "BTC-USD"
    assert trades[0]["price_per_unit"] == 100.0


def test_list_trades_positional(monkeypatch):
    patch_exec(monkeypatch, [{
        "order_id": "o1", "product_id": "BTC-USD", "side": "buy",
        "original_size": 1.0, "filled_size": 1.0, "remaining_size": 0.0,
        "price_per_unit": 100.0,
    }], mapping=False)
    trades = run(m.list_trades())
    assert trades[0]["id"] == "o1"


def test_list_trades_error(monkeypatch):
    patch_exec_raise(monkeypatch)
    assert run(m.list_trades()) == []


# ----------------------------- list_positions -----------------------------
def test_list_positions_named(monkeypatch):
    patch_exec(monkeypatch, [{
        "product_id": "BTC-USD", "side": "buy", "initial_quantity": 1.0,
        "filled_quantity": 1.0, "avg_fill_price": 100.0, "created_at": "t",
    }])
    pos = run(m.list_positions())
    assert pos[0]["product_id"] == "BTC-USD"


def test_list_positions_positional(monkeypatch):
    patch_exec(monkeypatch, [{
        "product_id": "BTC-USD", "side": "buy", "initial_quantity": 1.0,
        "filled_quantity": 1.0, "avg_fill_price": 100.0,
    }], mapping=False)
    pos = run(m.list_positions())
    assert pos[0]["product_id"] == "BTC-USD"


def test_list_positions_error(monkeypatch):
    patch_exec_raise(monkeypatch)
    assert run(m.list_positions()) == []


# ----------------------------- list_strategies -----------------------------
def test_list_strategies_named(monkeypatch):
    patch_exec(monkeypatch, [{
        "strategy_id": "s1", "name": "S", "description": "d",
        "category": "momentum", "backtested": True,
    }])
    s = run(m.list_strategies())
    assert s[0]["strategy_id"] == "s1"
    assert s[0]["status"] == "ACTIVE"


def test_list_strategies_positional(monkeypatch):
    patch_exec(monkeypatch, [{
        "strategy_id": "s1", "name": "S", "description": "d",
        "category": "momentum", "backtested": True,
    }], mapping=False)
    s = run(m.list_strategies())
    assert s[0]["strategy_id"] == "s1"


def test_list_strategies_error(monkeypatch):
    patch_exec_raise(monkeypatch)
    assert run(m.list_strategies()) == []


# ----------------------------- get_performance -----------------------------
def test_get_performance_named(monkeypatch):
    patch_exec(monkeypatch, [{"total_realized_pnl": 50.0, "unique_portfolios_with_trades": 2}])
    # second call (buckets) returns rows
    calls = {"n": 0}
    orig = FakeResult
    res_pnl = FakeResult([{"total_realized_pnl": 50.0, "unique_portfolios_with_trades": 2}])
    res_buckets = FakeResult([{"name": "Bucket1", "current_percentage": 80.0}])

    def _fake(q, p=None):
        calls["n"] += 1
        return res_buckets if calls["n"] > 1 else res_pnl
    monkeypatch.setattr(m, "_exec", _fake)
    r = run(m.get_performance())
    assert r["total_realized_pnl_usd"] == 50.0
    assert r["bucket_allocations"][0]["name"] == "Bucket1"


def test_get_performance_buckets_error(monkeypatch):
    res_pnl = FakeResult([{"total_realized_pnl": 50.0, "unique_portfolios_with_trades": 2}])
    calls = {"n": 0}

    def _fake(q, p=None):
        calls["n"] += 1
        if calls["n"] == 1:
            return res_pnl
        raise RuntimeError("buckets down")
    monkeypatch.setattr(m, "_exec", _fake)
    r = run(m.get_performance())
    assert r["bucket_allocations"] == []


def test_get_performance_positional(monkeypatch):
    patch_exec(monkeypatch, [{"total_realized_pnl": 50.0, "unique_portfolios_with_trades": 2}], mapping=False)
    r = run(m.get_performance())
    assert r["total_realized_pnl_usd"] == 50.0


def test_get_performance_error(monkeypatch):
    patch_exec_raise(monkeypatch)
    r = run(m.get_performance())
    assert r["total_realized_pnl_usd"] == 0.0


# ----------------------------- get_price_estimations -----------------------------
def test_get_price_estimations_named(monkeypatch):
    patch_exec(monkeypatch, [{
        "current_market_price": 100.0, "dcf_intrinsic_value": 120.0,
        "technical_score": 0.5, "consensus_vs_current_pct": 10.0,
        "confidence_score": 0.9,
    }])
    r = run(m.get_price_estimations("BTC-USD"))
    assert r["current_price"] == 100.0
    assert r["price_estimates"]["dcf_intrinsic_value"] == 120.0


def test_get_price_estimations_positional(monkeypatch):
    patch_exec(monkeypatch, [{
        "current_market_price": 100.0, "dcf_intrinsic_value": 120.0,
    }], mapping=False)
    r = run(m.get_price_estimations("BTC-USD"))
    assert r["current_price"] is None


def test_get_price_estimations_error(monkeypatch):
    patch_exec_raise(monkeypatch)
    r = run(m.get_price_estimations("BTC-USD"))
    assert r["current_price"] is None


# ----------------------------- get_approvals -----------------------------
def test_get_approvals_named(monkeypatch):
    patch_exec(monkeypatch, [
        {"status": "pending", "product_id": "BTC-USD", "side_text": "buy",
         "quantity": 1.0, "estimated_cost": 100.0, "created_at": "t"},
        {"status": "in_review", "product_id": "ETH-USD", "side_text": "sell",
         "quantity": 2.0, "estimated_cost": 200.0, "created_at": "t"},
    ])
    r = run(m.get_approvals())
    assert r["pending_count"] == 2
    assert r["approvals"][0]["status"] == "PENDING"


def test_get_approvals_positional(monkeypatch):
    patch_exec(monkeypatch, [{
        "status": "approved", "product_id": "BTC-USD", "side": "buy",
        "quantity": 1.0, "estimated_cost": 100.0, "created_at": "t",
    }], mapping=False)
    r = run(m.get_approvals())
    assert r["approvals"][0]["product_id"] == "buy"
    assert r["pending_count"] == 0


def test_get_approvals_error(monkeypatch):
    patch_exec_raise(monkeypatch)
    r = run(m.get_approvals())
    assert r["pending_count"] == 0


# ----------------------------- get_research_hypotheses -----------------------------
def test_get_research_hypotheses_named(monkeypatch):
    patch_exec(monkeypatch, [{
        "id": "1", "product_id": "BTC-USD", "hypothesis_text": "h",
        "confidence_score": 0.7, "expiration_datetime": "e", "timestamp": "t",
    }])
    r = run(m.get_research_hypotheses())
    assert r["hypotheses"][0]["product_id"] == "BTC-USD"


def test_get_research_hypotheses_positional(monkeypatch):
    patch_exec(monkeypatch, [{
        "id": "1", "product_id": "BTC-USD", "hypothesis_text": "h",
        "confidence_score": 0.7,
    }], mapping=False)
    r = run(m.get_research_hypotheses())
    assert r["hypotheses"][0]["product_id"] == "BTC-USD"


def test_get_research_hypotheses_error(monkeypatch):
    patch_exec_raise(monkeypatch)
    r = run(m.get_research_hypotheses())
    assert r["hypotheses"] == []


# ----------------------------- get_market_regime_snapshot -----------------------------
def test_market_regime_found_named(monkeypatch):
    patch_exec(monkeypatch, [{
        "regime": "BULL", "bullish_pct": 70.0, "bearish_pct": 20.0,
        "sentiment_score": 0.5, "timestamp": "t",
    }])
    r = run(m.get_market_regime_snapshot())
    assert r["regime"] == "BULL"


def test_market_regime_found_positional(monkeypatch):
    patch_exec(monkeypatch, [{
        "regime": "BULL", "bullish_pct": 70.0, "bearish_pct": 20.0,
        "sentiment_score": 0.5, "timestamp": "t",
    }], mapping=False)
    r = run(m.get_market_regime_snapshot())
    assert r["regime"] == "NEUTRAL"


def test_market_regime_none(monkeypatch):
    patch_exec(monkeypatch, [])
    r = run(m.get_market_regime_snapshot())
    assert r["regime"] == "NEUTRAL"


def test_market_regime_error(monkeypatch):
    patch_exec_raise(monkeypatch)
    r = run(m.get_market_regime_snapshot())
    assert r["regime"] == "NEUTRAL"


# ----------------------------- list_backtests -----------------------------
def test_list_backtests():
    r = run(m.list_backtests())
    assert r["backtests"] == []


def test_list_backtests_with_id():
    r = run(m.list_backtests("s1"))
    assert r["backtests"] == []


# ----------------------------- get_capital_allocation -----------------------------
def test_capital_allocation_named(monkeypatch):
    patch_exec(monkeypatch, [{"name": "B1", "amount": 100.0, "status": "idle"}])
    r = run(m.get_capital_allocation())
    assert r["buckets"][0]["name"] == "B1"
    assert r["total_capital"] == 100.0


def test_capital_allocation_positional(monkeypatch):
    patch_exec(monkeypatch, [{"name": "B1", "amount": 100.0, "status": "idle"}], mapping=False)
    r = run(m.get_capital_allocation())
    assert r["buckets"][0]["name"] == "Unknown"


def test_capital_allocation_error(monkeypatch):
    patch_exec_raise(monkeypatch)
    r = run(m.get_capital_allocation())
    assert r["buckets"] == []


# ----------------------------- _parse_sqlalchemy_row (dead-code coverage) -----------------------------
def test_parse_row_mapping():
    r = m._parse_sqlalchemy_row(MappingRow({"a": 1, "b": 2}))
    assert r["a"] == 1


def test_parse_row_keys():
    r = m._parse_sqlalchemy_row({"a": 1, "b": 2})
    assert r["a"] == 1


class AttrRow:
    def __init__(self):
        self.id = 5
        self.name = "x"
        self._private = "secret"


def test_parse_row_attr():
    r = m._parse_sqlalchemy_row(AttrRow())
    assert r["id"] == 5
    assert r["name"] == "x"
    assert "_private" not in r


def test_extract_from_row_keys():
    assert m._extract_from_row({"x": 1}) == {"x": 1}


class AttrRow2:
    def __init__(self):
        self.id = 5
        self.none_val = None

def test_parse_row_attr_none():
    r = m._parse_sqlalchemy_row(AttrRow2())
    assert r["id"] == 5
    assert "none_val" not in r


def test_extract_from_row_attr_none():
    r = m._extract_from_row(AttrRow2())
    assert r["id"] == 5
    assert "none_val" not in r


def test_exec_helper(monkeypatch):
    from sqlalchemy import text

    class FakeConn:
        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

        def execute(self, q, p=None):
            class R:
                def fetchall(self):
                    return []

            return R()

    class FakeEngine:
        def connect(self):
            return FakeConn()

    monkeypatch.setattr(m, "engine", FakeEngine())
    res = m._exec(text("SELECT 1"))
    assert res.fetchall() == []

