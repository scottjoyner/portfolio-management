from datetime import datetime
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
import sqlalchemy.orm as sa_orm

from trading_system.api.databases import queries as dbqueries


class _Type:
    value = "ACTIVE"


class _Side:
    value = "buy"


class Row:
    """Generic result row supporting both attribute and index access."""

    def __init__(self, **kw):
        d = dict(
            id=1, name="P", type=_Type(), provider="X", currency="USD",
            balance_usd=100.0, product_id="BTC-USD", side=_Side(),
            original_size=1.0, filled_size=1.0, remaining_size=1.0, price=100.0,
            config_key="ck", description="d", category="momentum", status="PENDING",
            dcf_intrinsic_value=1.0, technical_score=2.0, consensus_vs_current_pct=3.0,
            confidence_score=0.5, approval_type="trade", summary="s", capital_affected=10.0,
            liquidity_impact="l", risk_impact="r", expires_at=None,
            hypothesis_text="h", created_at="2026", timestamp="2026",
            current_market_price=100.0,
        )
        d.update(kw)
        self.__dict__.update(d)

    def __getitem__(self, idx):
        return list(self.__dict__.values())[idx]

    def __repr__(self):
        return "Row"


class StrRow(Row):
    def __getitem__(self, idx):
        return "skip"


class TupleRow(tuple):
    pass


def make_session(monkeypatch, *, rows=None, fetchone=None, fetchall=None,
                 raise_on_query=False, raise_on_execute=False):
    rows = rows if rows is not None else []
    sm = MagicMock()
    q = sm.query.return_value
    q.filter.return_value = q
    q.order_by.return_value = q
    q.offset.return_value = q
    q.limit.return_value = q
    q.all.return_value = rows
    ex = sm.execute.return_value
    ex.fetchone.return_value = fetchone
    ex.fetchall.return_value = fetchall if fetchall is not None else []
    if raise_on_query:
        sm.query.side_effect = RuntimeError("boom")
    if raise_on_execute:
        sm.execute.side_effect = RuntimeError("boom")
    monkeypatch.setattr(sa_orm, "Session", lambda *a, **k: sm)
    return sm


# ----- happy paths -----

def test_get_accounts(monkeypatch):
    make_session(monkeypatch, rows=[Row(), StrRow()])
    accts = dbqueries.get_accounts()
    # StrRow is skipped, Row is kept.
    assert len(accts) == 1
    assert accts[0]["balance_usd"] == 100.0


def test_get_trades(monkeypatch):
    make_session(monkeypatch, rows=[Row()])
    trades = dbqueries.get_trades()
    assert trades[0]["product_id"] == "BTC-USD"


def test_get_positions(monkeypatch):
    make_session(monkeypatch, rows=[Row()])
    positions = dbqueries.get_positions()
    assert positions[0]["product_id"] == "BTC-USD"


def test_get_strategies(monkeypatch):
    make_session(monkeypatch, rows=[Row()])
    strategies = dbqueries.get_strategies()
    assert strategies[0]["strategy_id"] == "ck"


def test_get_performance(monkeypatch):
    fetch = SimpleNamespace(total_pnl=42.0)
    make_session(monkeypatch, rows=[Row()], fetchone=fetch)
    perf = dbqueries.get_performance()
    assert perf["total_realized_pnl_usd"] == 42.0


def test_get_performance_tuple_ids(monkeypatch):
    fetch = SimpleNamespace(total_pnl=7.0)
    make_session(monkeypatch, rows=[TupleRow(("id1",))], fetchone=fetch)
    perf = dbqueries.get_performance()
    assert perf["total_realized_pnl_usd"] == 7.0


def test_get_performance_inner_except(monkeypatch):
    make_session(monkeypatch, rows=[Row()], raise_on_execute=True)
    perf = dbqueries.get_performance()
    assert perf["total_realized_pnl_usd"] == 0.0


def test_get_price_estimates_present(monkeypatch):
    fetch = SimpleNamespace(
        current_market_price=100.0, dcf_intrinsic_value=1.0, technical_score=2.0,
        consensus_vs_current_pct=3.0, confidence_score=0.5)
    make_session(monkeypatch, fetchone=fetch)
    out = dbqueries.get_price_estimates("BTC-USD")
    assert out["current_price"] == 100.0
    assert out["price_estimates"]["dcf_intrinsic_value"] == 1.0


def test_get_price_estimates_none(monkeypatch):
    make_session(monkeypatch, fetchone=None)
    out = dbqueries.get_price_estimates("BTC-USD")
    assert out["current_price"] is None


def test_get_approvals(monkeypatch):
    make_session(monkeypatch, fetchall=[Row(), TupleRow(("a",))])
    out = dbqueries.get_approvals()
    assert out["pending_count"] == 1
    assert out["completed_count"] == 1


def test_get_research_hypotheses(monkeypatch):
    make_session(monkeypatch, fetchall=[Row(), TupleRow(("x",))])
    out = dbqueries.get_research_hypotheses()
    assert len(out["hypotheses"]) == 2


# ----- exception paths (DB unavailable) -----

def test_get_accounts_except(monkeypatch):
    make_session(monkeypatch, raise_on_query=True)
    assert dbqueries.get_accounts() == []


def test_get_trades_except(monkeypatch):
    make_session(monkeypatch, raise_on_query=True)
    assert dbqueries.get_trades() == []


def test_get_positions_except(monkeypatch):
    make_session(monkeypatch, raise_on_query=True)
    assert dbqueries.get_positions() == []


def test_get_strategies_except(monkeypatch):
    make_session(monkeypatch, raise_on_query=True)
    assert dbqueries.get_strategies() == []


def test_get_performance_except(monkeypatch):
    make_session(monkeypatch, raise_on_query=True)
    assert dbqueries.get_performance() == {"total_realized_pnl_usd": 0.0}


def test_get_price_estimates_except(monkeypatch):
    make_session(monkeypatch, raise_on_execute=True)
    assert dbqueries.get_price_estimates("X") == {"current_price": None, "price_estimates": {}}


def test_get_approvals_except(monkeypatch):
    make_session(monkeypatch, raise_on_execute=True)
    assert dbqueries.get_approvals() == {"pending_count": 0, "completed_count": 0}


def test_get_research_hypotheses_except(monkeypatch):
    make_session(monkeypatch, raise_on_execute=True)
    assert dbqueries.get_research_hypotheses() == {"hypotheses": [], "market_regimes": {}}
