"""Coverage tests for PortfolioOptimizer state fetch, cost-basis computation,
and live bracket-execution paths."""
from __future__ import annotations

import time
from unittest import mock

import pytest

import portfolio_optimizer as P
from tests.coverage.optimizer.conftest import holding, make_state


def mkopp(opp_type, side, currency, **kw):
    return P.Opportunity(
        opp_type=opp_type, currency=currency, side=side,
        size_usd=kw.get("size_usd", 1000.0), reason=kw.get("reason", "t"),
        priority=kw.get("priority", 0.5),
        product_id=kw.get("product_id", f"{currency}-USD"),
        entry_price_est=kw.get("entry_price_est", 0.0),
        stop_loss_pct=kw.get("stop_loss_pct", 0.0),
        take_profit_pct=kw.get("take_profit_pct", 0.0),
        meta=kw.get("meta", {}),
    )


def test_fetch_state_success(opt):
    opt.cli.get_products.return_value = {}
    opt.cli.get_balances.return_value = [
        {"currency": "USDC", "available_balance": {"value": "40000"},
         "hold": {"value": "0"}},
        {"currency": "XRP", "available_balance": {"value": "5000"},
         "hold": {"value": "0"}},
    ]
    opt.cli.get_fees.return_value = {"advanced_trade_only_volume": 1_000_000.0}
    opt.cli.get_price.return_value = {"price": "0.5",
                                      "price_percentage_change_24h": "2.0",
                                      "volume_24h": "1000"}
    opt._fetch_state()
    assert "XRP" in opt.state.holdings
    assert opt.state.fee_volume_30d == 1_000_000.0


def test_fetch_state_balances_failed(opt):
    opt.cli.get_products.return_value = {}
    opt.cli.get_balances.side_effect = RuntimeError("boom")
    opt.cli.get_fees.return_value = {}
    opt._fetch_state()
    assert opt.state.total_value == 0.0


def test_compute_cost_bases(opt):
    opt.cli.get_fills.return_value = [
        {"product_id": "XRP-USD", "side": "BUY", "size": "100", "price": "0.5"},
        {"product_id": "XRP-USD", "side": "SELL", "size": "40", "price": "0.6"},
    ]
    cb = opt._compute_cost_bases()
    assert "XRP" in cb and cb["XRP"] > 0


def test_execute_with_bracket_live(opt):
    opt.dry_run = False
    opt._exec_engine = None
    opt.require_approval = False
    mgr = mock.MagicMock()
    mgr.place_bracket.return_value = {
        "status": "OPEN", "bracket_id": "b1", "entry_result": {"client_order_id": "c1"},
    }
    opt._bracket_mgr = mgr
    op = mkopp(P.OpportunityType.STRATEGY_SIGNAL, "BUY", "XRP", size_usd=1000,
               entry_price_est=0.5, stop_loss_pct=5.0, take_profit_pct=10.0)
    opt._execute_with_bracket(op, 2000.0, True)
    assert op.executed and op.order_id == "b1"
    mgr.place_bracket.assert_called_once()


def test_execute_with_bracket_live_flatten(opt):
    opt.dry_run = False
    opt._exec_engine = None
    opt.require_approval = False
    mgr = mock.MagicMock()
    mgr.place_bracket.return_value = {
        "status": "INCOMPLETE", "bracket_id": "b1",
        "entry_result": {"success": True, "order_id": "c1"},
    }
    opt._bracket_mgr = mgr
    op = mkopp(P.OpportunityType.STRATEGY_SIGNAL, "BUY", "XRP", size_usd=1000,
               entry_price_est=0.5, stop_loss_pct=5.0, take_profit_pct=10.0)
    opt._execute_with_bracket(op, 2000.0, True)
    mgr.force_flatten_bracket.assert_called_once()


def test_execute_with_bracket_live_fail(opt):
    opt.dry_run = False
    opt._exec_engine = None
    opt.require_approval = False
    mgr = mock.MagicMock()
    mgr.place_bracket.return_value = {
        "status": "REJECTED", "entry_result": {"error": "no liquidity"},
    }
    opt._bracket_mgr = mgr
    op = mkopp(P.OpportunityType.STRATEGY_SIGNAL, "BUY", "XRP", size_usd=1000,
               entry_price_est=0.5, stop_loss_pct=5.0, take_profit_pct=10.0)
    opt._execute_with_bracket(op, 2000.0, True)
    assert not op.executed
