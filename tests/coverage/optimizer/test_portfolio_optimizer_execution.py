"""Coverage tests for PortfolioOptimizer execution / orchestration methods:
process_opportunity, execute_approved, execute_with_bracket, write helpers,
cross-asset risk filter, detection orchestration, pending-approval check,
bracket polling, bear-market + capital-policy overlays.

Network I/O is mocked; each method's decision branches are exercised directly.
"""
from __future__ import annotations

import json
import os
import tempfile
import time
from types import SimpleNamespace
from unittest import mock

import pytest

import portfolio_optimizer as P
from tests.coverage.optimizer.conftest import holding, make_state


def mkopp(opp_type, side, currency, size_usd=1000.0, **kw):
    return P.Opportunity(
        opp_type=opp_type,
        currency=currency,
        side=side,
        size_usd=size_usd,
        reason=kw.get("reason", "test"),
        priority=kw.get("priority", 0.5),
        product_id=kw.get("product_id", f"{currency}-USD"),
        entry_price_est=kw.get("entry_price_est", 0.0),
        stop_loss_pct=kw.get("stop_loss_pct", 0.0),
        take_profit_pct=kw.get("take_profit_pct", 0.0),
        meta=kw.get("meta", {}),
    )


# ----------------------------------------------------- write helpers
def test_write_trade_plans(opt):
    ops = [
        mkopp(P.OpportunityType.TLH, "SELL", "XRP", meta={"trade_style": "tlh"}),
        mkopp(P.OpportunityType.FEE_TIER_VOLUME, "BUY", "SOL"),
        mkopp(P.OpportunityType.STRATEGY_SIGNAL, "BUY", "ETH"),
        mkopp(P.OpportunityType.EVENT_MARKET, "BUY", "BTC"),
    ]
    opt._write_trade_plans(ops)
    assert os.path.exists("trade_plans.json")
    data = json.load(open("trade_plans.json"))
    assert data["total"] == 4
    assert any(p["trade_style"] for p in data["plans"])


def test_write_signal_cache(opt):
    ops = [
        mkopp(P.OpportunityType.TLH, "SELL", "XRP"),
        mkopp(P.OpportunityType.STRATEGY_SIGNAL, "BUY", "ETH",
              meta={"final_confidence": 0.7, "opportunity_score": 0.6}),
    ]
    opt._write_signal_cache(ops)
    data = json.load(open("data/.unified_signal_cache.json"))
    assert data["buy_signals"] == 1 and data["sell_signals"] == 1
    assert data["quality_score"] > 0


def test_write_enhanced_state(opt):
    opt._meta_source_weights = {}
    opt._cross_asset_regime = None
    opt._ensemble_blender = None
    opt._param_opt_results = {}
    opt._wash_sale_cooldown = {}
    opt._order_flow_engine = None
    opt._write_enhanced_state()


# ----------------------------------------------------- cross-asset risk filter
def _regime(allows_new_longs=True, risk_mult=1.0, regime="normal", trend="up"):
    return SimpleNamespace(allows_new_longs=allows_new_longs, risk_multiplier=risk_mult,
                           regime=regime, trend_bias=trend)


def _macro(score=0.0):
    return SimpleNamespace(macro_score=score)


def test_risk_filter_suppress_buy(opt):
    opt._cross_asset_regime = SimpleNamespace(get_state=lambda refresh=False: _regime(False))
    opt._macro_risk = None
    buy = mkopp(P.OpportunityType.STRATEGY_SIGNAL, "BUY", "XRP", size_usd=1000, priority=0.9)
    sell = mkopp(P.OpportunityType.STRATEGY_SIGNAL, "SELL", "XRP", size_usd=1000, priority=0.9)
    out = opt._apply_cross_asset_risk_filter([buy, sell])
    assert out == [sell]


def test_risk_filter_scale_and_macro(opt):
    opt._cross_asset_regime = SimpleNamespace(get_state=lambda refresh=False: _regime(True, 0.5, "crash"))
    opt._macro_risk = SimpleNamespace(get_signal=lambda: _macro(2.0))
    buy = mkopp(P.OpportunityType.STRATEGY_SIGNAL, "BUY", "XRP", size_usd=1000, priority=1.0)
    out = opt._apply_cross_asset_risk_filter([buy])
    assert out[0].priority < 1.0
    assert "macro_penalty" in out[0].meta


def test_risk_filter_empty_and_none(opt):
    opt._cross_asset_regime = None
    opt._macro_risk = None
    assert opt._apply_cross_asset_risk_filter([]) == []
    op = mkopp(P.OpportunityType.TLH, "SELL", "XRP")
    assert opt._apply_cross_asset_risk_filter([op]) == [op]


# ----------------------------------------------------- detection orchestration
def test_detect_opportunities_orchestrates(opt):
    op = mkopp(P.OpportunityType.TLH, "SELL", "XRP")
    detectors = [
        "_detect_enhanced_tlh", "_detect_coinbase_universe_signals", "_detect_stock_opportunities",
        "_detect_fee_tier_volume", "_detect_rebalance", "_detect_rebalance_bot",
        "_detect_stairstep", "_detect_strategy_signals", "_detect_funding_and_onchain_signals",
        "_detect_volume_cycles", "_detect_accumulator_signals", "_detect_aggregator_signals",
        "_detect_event_markets", "_detect_order_flow_signals",
    ]
    with mock.patch.object(opt, "_run_periodic_param_optimization"), \
         mock.patch.object(opt, "_apply_optimized_params"), \
         mock.patch.object(opt, "_signal_ensemble_blend", side_effect=lambda x: x), \
         mock.patch.object(opt, "_apply_meta_source_weights", side_effect=lambda x: x):
        for d in detectors:
            setattr(opt, d, lambda: [])
        opt._detect_enhanced_tlh = lambda: [op]
        ops = opt._detect_opportunities()
    assert ops == [op]


# ----------------------------------------------------- pending approvals
def test_check_pending_approvals_skips(opt):
    opt.require_approval = False
    opt._check_pending_approvals()  # no-op


def test_check_pending_approvals_missing_file(opt):
    opt.require_approval = True
    opt.pending_file = "/nonexistent/path/pending.json"
    opt._check_pending_approvals()  # missing file handled


def test_check_pending_approvals_executes(opt):
    opt.require_approval = True
    path = os.path.join(tempfile.mkdtemp(), "pending.json")
    opt.pending_file = path
    entry = {"status": "approved", "side": "BUY", "currency": "XRP", "size_usd": 100.0,
             "product_id": "XRP-USD", "reason": "go"}
    json.dump({"tok": entry}, open(path, "w"))
    with mock.patch.object(opt, "_execute_approved") as ex:
        opt._check_pending_approvals()
        ex.assert_called_once()
    data = json.load(open(path))
    assert "tok" not in data


# ----------------------------------------------------- bracket polling
def test_poll_brackets_none(opt):
    opt._bracket_mgr = None
    opt._poll_brackets()


def test_poll_brackets_dryrun(opt):
    opt._bracket_mgr = mock.MagicMock()
    opt.dry_run = True
    opt._poll_brackets()


def test_poll_brackets_polls(opt):
    opt.dry_run = False
    mgr = mock.MagicMock()
    mgr.active_brackets.return_value = {
        "b1": {"status": "OPEN", "product_id": "BTC-USD", "entry_price": 100.0,
               "initial_stop_dist": 5.0, "side": "BUY", "created_at": time.time()},
    }
    opt._bracket_mgr = mgr
    opt.cli.best_bid_ask.return_value = {"bids": [[100.0, 1]], "asks": [[101.0, 1]]}
    opt._poll_brackets()
    mgr.update_trailing_stop.assert_called_once()
    mgr.update_trailing_take_profit.assert_called_once()


# ----------------------------------------------------- bear / capital policy
def test_bear_market_policy_no_state(opt):
    opt.state = None
    opt._apply_bear_market_policy()


def test_bear_market_policy_overlay(opt):
    opt._portfolio_peak_value = 100000.0
    opt.state = make_state({"BTC": holding("BTC", 60000, "safe", change_24h=-5.0)},
                            total_value=80000.0, usdc=20000.0)
    opt._apply_bear_market_policy()
    assert opt.capital_policy.get("targets")


def test_refresh_capital_policy(opt):
    opt._forced_max_deployable_usd = 0
    pol = opt._refresh_capital_policy()
    assert "max_deployable_usd" in pol


# ----------------------------------------------------- process_opportunity
def test_process_static_currency(opt):
    opt.state = make_state({"BTC": holding("BTC", 60000, "safe")}, total_value=100000.0, usdc=40000.0)
    opt._process_opportunity(mkopp(P.OpportunityType.TLH, "BUY", "BTC"))


def test_process_event_market_no_notifier(opt):
    opt.state = make_state({"BTC": holding("BTC", 60000, "safe")}, total_value=100000.0, usdc=40000.0)
    opt.dry_run = True
    opt.notifier = None
    opt._process_opportunity(mkopp(P.OpportunityType.EVENT_MARKET, "BUY", "BTC",
                                    meta={"platform": "kalshi", "market_question": "q"}))


def test_process_event_market_with_notifier(opt):
    opt.state = make_state({"BTC": holding("BTC", 60000, "safe")}, total_value=100000.0, usdc=40000.0)
    opt.dry_run = False
    notifier = mock.MagicMock()
    opt.notifier = notifier
    opt._process_opportunity(mkopp(P.OpportunityType.EVENT_MARKET, "BUY", "XRP",
                                    meta={"platform": "kalshi", "market_question": "q"}))
    notifier.send_trade_alert.assert_called_once()


def test_process_event_stock_and_arbitrage(opt):
    opt.state = make_state({"BTC": holding("BTC", 60000, "safe")}, total_value=100000.0, usdc=40000.0)
    opt.dry_run = True
    opt.notifier = None
    opt._process_opportunity(mkopp(P.OpportunityType.STOCK_SIGNAL, "BUY", "AAPL",
                                    meta={"signal_type": "m"}))
    opt._process_opportunity(mkopp(P.OpportunityType.EVENT_ARBITRAGE, "BUY", "BTC",
                                    meta={"signal_type": "m"}))


def test_process_buy_capacity_below_min(opt):
    opt.state = make_state({"XRP": holding("XRP", 5000, "growth")}, total_value=100000.0, usdc=40000.0)
    opt.min_value = 1_000_000
    opt._current_price_for_symbol = lambda pid: 0.5
    opt._process_opportunity(mkopp(P.OpportunityType.STRATEGY_SIGNAL, "BUY", "XRP", size_usd=1000))


def test_process_sell_zero_qty(opt):
    opt.state = make_state({"XRP": holding("XRP", 5000, "growth", price=0.0)},
                           total_value=100000.0, usdc=40000.0)
    opt._process_opportunity(mkopp(P.OpportunityType.STRATEGY_SIGNAL, "SELL", "XRP", size_usd=1000))


def test_process_bracket_branch(opt):
    opt.state = make_state({"XRP": holding("XRP", 5000, "growth", price=0.5)},
                           total_value=100000.0, usdc=40000.0)
    opt._current_price_for_symbol = lambda pid: 0.5
    opt.capital_policy = {"max_deployable_usd": 1_000_000.0, "targets": {"reserve": 0.1, "core": 0.1, "opportunity": 0.8}}
    opt._usdc_reserve_amount = lambda: 0.0
    opt._bracket_mgr = mock.MagicMock()
    op = mkopp(P.OpportunityType.STRATEGY_SIGNAL, "BUY", "XRP", size_usd=1000,
               entry_price_est=0.5, stop_loss_pct=5.0, take_profit_pct=10.0)
    with mock.patch.object(opt, "_execute_with_bracket") as ewb:
        opt._process_opportunity(op)
        ewb.assert_called_once()


def test_process_direct_dryrun(opt):
    opt.state = make_state({"XRP": holding("XRP", 5000, "growth", price=0.5)},
                           total_value=100000.0, usdc=40000.0)
    opt._current_price_for_symbol = lambda pid: 0.5
    opt.capital_policy = {"max_deployable_usd": 1_000_000.0, "targets": {"reserve": 0.1, "core": 0.1, "opportunity": 0.8}}
    opt._usdc_reserve_amount = lambda: 0.0
    opt._bracket_mgr = None
    opt.dry_run = True
    opt.require_approval = False
    opt.cli.preview_order.return_value = {"total_fee": 1.0, "total_cost": 1000.0}
    op = mkopp(P.OpportunityType.REBALANCE, "BUY", "XRP", size_usd=1000)
    opt._process_opportunity(op)
    assert op.executed and op.order_id == "dry-run"


def test_process_direct_live(opt):
    opt.state = make_state({"XRP": holding("XRP", 5000, "growth", price=0.5)},
                           total_value=100000.0, usdc=40000.0)
    opt._current_price_for_symbol = lambda pid: 0.5
    opt.capital_policy = {"max_deployable_usd": 1_000_000.0, "targets": {"reserve": 0.1, "core": 0.1, "opportunity": 0.8}}
    opt._usdc_reserve_amount = lambda: 0.0
    opt._bracket_mgr = None
    opt.dry_run = False
    opt.require_approval = False
    opt.cli.preview_order.return_value = {"total_fee": 1.0, "total_cost": 1000.0}
    opt.cli.create_order.return_value = {"id": "live-1"}
    op = mkopp(P.OpportunityType.REBALANCE, "BUY", "XRP", size_usd=1000)
    opt._process_opportunity(op)
    assert op.executed and op.order_id == "live-1"


def test_process_pending_approval(opt):
    opt.state = make_state({"XRP": holding("XRP", 5000, "growth", price=0.5)},
                           total_value=100000.0, usdc=40000.0)
    opt._current_price_for_symbol = lambda pid: 0.5
    opt.capital_policy = {"max_deployable_usd": 1_000_000.0, "targets": {"reserve": 0.1, "core": 0.1, "opportunity": 0.8}}
    opt._usdc_reserve_amount = lambda: 0.0
    opt._bracket_mgr = None
    opt.dry_run = False
    opt.require_approval = True
    opt.cli.preview_order.return_value = {"total_fee": 1.0, "total_cost": 1000.0}
    path = os.path.join(tempfile.mkdtemp(), "pending.json")
    opt.pending_file = path
    op = mkopp(P.OpportunityType.REBALANCE, "BUY", "XRP", size_usd=1000)
    opt._process_opportunity(op)
    data = json.load(open(path))
    assert any(v.get("status") == "pending" for v in data.values())


# ----------------------------------------------------- execute approved / bracket
def test_execute_approved_invalid(opt):
    opt.state = make_state({"XRP": holding("XRP", 5000, "growth")}, total_value=100000.0, usdc=40000.0)
    opt._execute_approved({"side": "BUY", "currency": "XRP", "size_usd": 0})
    opt._execute_approved({"side": "BUY", "currency": "XRP"})  # no size


def test_execute_approved_buy_capacity(opt):
    opt.state = make_state({"XRP": holding("XRP", 5000, "growth")}, total_value=100000.0, usdc=50.0)
    opt.min_value = 100
    opt._execute_approved({"side": "BUY", "currency": "XRP", "size_usd": 1000.0,
                           "product_id": "XRP-USD"})


def test_execute_with_bracket_dryrun(opt):
    opt.state = make_state({"ETH": holding("ETH", 20000, "growth", price=2000.0)},
                           total_value=100000.0, usdc=40000.0)
    opt._exec_engine = None
    opt.dry_run = True
    opt.require_approval = False
    op = mkopp(P.OpportunityType.STRATEGY_SIGNAL, "BUY", "ETH", size_usd=1000,
               entry_price_est=2000.0, stop_loss_pct=5.0, take_profit_pct=10.0)
    opt._execute_with_bracket(op, 0.5, True)
    assert op.executed and op.order_id == "dry-run-bracket"


def test_execute_with_bracket_invalid_size(opt):
    opt._exec_engine = None
    opt.dry_run = True
    op = mkopp(P.OpportunityType.STRATEGY_SIGNAL, "BUY", "ETH", size_usd=1000,
               entry_price_est=0.0)
    opt._execute_with_bracket(op, 0.0, True)


# ----------------------------------------------------- bug #46 side="PAIR" guard
def test_process_opportunity_rejects_pair_side(opt):
    # Bug #46: side="PAIR" is semantically wrong and must not reach any
    # execution path (preview/place). The guard at the top of
    # _process_opportunity must drop the opportunity before any order call.
    from coinbase.src.config import validate_opportunity_side
    with mock.patch("coinbase.src.config.validate_opportunity_side",
                    side_effect=validate_opportunity_side) as spy:
        opt._process_opportunity(
            mkopp(P.OpportunityType.EVENT_ARBITRAGE, "PAIR", "BTC", size_usd=1000,
                  product_id="kalshi:polymarket"))
        spy.assert_called_once()
    # The bad side is rejected (ValueError) and the opp is not executed.
    op = mkopp(P.OpportunityType.STRATEGY_SIGNAL, "PAIR", "XRP", size_usd=1000)
    opt._execute_with_bracket = mock.MagicMock()
    opt.notifier = None
    opt._process_opportunity(op)
    opt._execute_with_bracket.assert_not_called()


def test_process_opportunity_accepts_valid_sides(opt):
    for side in ("BUY", "SELL"):
        op = mkopp(P.OpportunityType.EVENT_MARKET, side, "BTC", size_usd=1000,
                    meta={"platform": "kalshi", "market_question": "q"})
        # Should not raise; notify-only path proceeds.
        opt._process_opportunity(op)
