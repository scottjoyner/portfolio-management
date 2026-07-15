"""Additional coverage tests for portfolio_optimizer.py (round 4).

Targets the large previously-uncovered blocks:
  _execute_approved, _detect_order_flow_signals (branches),
  _detect_coinbase_universe_signals body, _detect_stock_opportunities,
  _run_periodic_param_optimization (with data), _detect_stairstep branches,
  _detect_rebalance_bot orders, _detect_volume_cycles, _process_opportunity
  notify/route branches, _execute_with_bracket preview/exec-engine branches,
  and the various *_write_* / detection error branches.

All collaborators are mocked; no network / Coinbase / DB writes occur.
"""

import os
import time
from types import SimpleNamespace
from unittest import mock

import pytest

import portfolio_optimizer as P
from strategy_engine import Signal as StrategySignal
from strategy_engine import BacktestVerdict


def make_state(holdings, total_value=100000.0, usdc=50000.0,
               fee_volume_30d=0.0, volume_to_next_tier=0.0):
    return P.PortfolioState(
        holdings=holdings,
        total_value=total_value,
        usdc_balance=usdc,
        fee_volume_30d=fee_volume_30d,
        fee_tier=(0, 0.006, 0.012),
        volume_to_next_tier=volume_to_next_tier,
        timestamp="2024-01-01T00:00:00Z",
    )


def holding(currency, value, classification, **kw):
    return {
        "currency": currency,
        "value": value,
        "classification": classification,
        "price": kw.get("price", 100.0),
        "product_id": kw.get("product_id", f"{currency}-USD"),
        "unrealized_pnl_pct": kw.get("pnl", -10.0),
        "volume_24h": kw.get("volume_24h", 1_000_000.0),
        "change_24h": kw.get("change_24h", 1.0),
        "allocation_pct": kw.get("allocation_pct", value / 100000.0 * 100),
        "liquidity_score": kw.get("liquidity_score", 0.8),
        "spread": kw.get("spread", 0.001),
    }


def _rich_state(opt, holdings, total_value=100000.0, usdc=90000.0):
    opt.state = make_state(holdings, total_value=total_value, usdc=usdc)
    opt.capital_policy["max_deployable_usd"] = 1_000_000.0
    opt.capital_policy["targets"] = {"reserve": 0.0, "core": 1.0, "opportunity": 1.0}
    return opt.state


def _sample_candles(n=60, base=100.0):
    return [{"start": i, "open": base + i, "high": base + i + 1,
             "low": base + i - 1, "close": base + i, "volume": 1000}
            for i in range(n)]


def _of_sig(action, confidence):
    """OrderFlowEngine-style signal needs spread_z/spread_tight/volume_24h."""
    return SimpleNamespace(action=action, confidence=confidence, reason="of r",
                           strategy="order_flow", spread_z=1.0, spread_tight=True,
                           volume_24h=2_000_000.0, spread_bps=5.0)


def _approved_targets(opt):
    opt.capital_policy["max_deployable_usd"] = 1_000_000.0
    opt.capital_policy["targets"] = {"reserve": 0.0, "core": 1.0, "opportunity": 1.0}


# ---------------------------------------------------------------------------
# _execute_approved
# ---------------------------------------------------------------------------

def test_execute_approved_invalid(opt):
    assert opt._execute_approved({"side": "BUY", "currency": "SOL",
                                   "size_usd": 0, "product_id": "SOL-USD"}) is None
    assert opt._execute_approved({"side": "BUY", "currency": "SOL",
                                   "size_usd": 1000, "product_id": ""}) is None


def test_execute_approved_buy_dryrun(opt):
    opt.state = make_state({}, total_value=100000.0, usdc=50000.0)
    _approved_targets(opt)
    opt.dry_run = True
    opt.cli.preview_order.return_value = {"total_fee": 1.0, "total_cost": 1000.0}
    n0 = len(opt.trade_log)
    opt._execute_approved({"side": "BUY", "currency": "SOL", "size_usd": 1000.0,
                           "product_id": "SOL-USD", "reason": "r"})
    assert len(opt.trade_log) == n0 + 1
    assert opt.trade_log[-1]["order_id"] == "dry-run"


def test_execute_approved_buy_live(opt):
    opt.state = make_state({}, total_value=100000.0, usdc=50000.0)
    _approved_targets(opt)
    opt.dry_run = False
    opt.cli.preview_order.return_value = {"total_fee": 1.0, "total_cost": 1000.0}
    opt.cli.create_order.return_value = {"id": "X9"}
    opt._execute_approved({"side": "BUY", "currency": "SOL", "size_usd": 1000.0,
                           "product_id": "SOL-USD", "reason": "r"})
    assert opt.cli.create_order.called
    assert opt.trade_log[-1]["order_id"] == "X9"


def test_execute_approved_sell(opt):
    opt.state = make_state(
        {"SOL": holding("SOL", 5000, "growth", price=100.0, product_id="SOL-USD")},
        total_value=100000.0, usdc=50000.0)
    opt.dry_run = False
    opt.cli.preview_order.return_value = {"total_fee": 1.0, "total_cost": 1000.0}
    opt.cli.create_order.return_value = {"id": "S9"}
    opt._execute_approved({"side": "SELL", "currency": "SOL", "size_usd": 1000.0,
                           "product_id": "SOL-USD", "reason": "r"})
    assert opt.cli.create_order.called


def test_execute_approved_preview_failed(opt):
    opt.state = make_state({}, total_value=100000.0, usdc=50000.0)
    _approved_targets(opt)
    opt.dry_run = False
    opt.cli.preview_order.return_value = None
    n0 = len(opt.trade_log)
    opt._execute_approved({"side": "BUY", "currency": "SOL", "size_usd": 1000.0,
                           "product_id": "SOL-USD", "reason": "r"})
    assert len(opt.trade_log) == n0


def test_execute_approved_create_failed(opt):
    opt.state = make_state({}, total_value=100000.0, usdc=50000.0)
    _approved_targets(opt)
    opt.dry_run = False
    opt.cli.preview_order.return_value = {"total_fee": 1.0, "total_cost": 1000.0}
    opt.cli.create_order.return_value = None
    n0 = len(opt.trade_log)
    opt._execute_approved({"side": "BUY", "currency": "SOL", "size_usd": 1000.0,
                           "product_id": "SOL-USD", "reason": "r"})
    assert len(opt.trade_log) == n0


def test_execute_approved_buy_below_min(opt):
    opt.state = make_state({}, total_value=100000.0, usdc=50000.0)
    opt.capital_policy["targets"] = {"reserve": 1.0, "core": 0.0, "opportunity": 0.0}
    n0 = len(opt.trade_log)
    opt._execute_approved({"side": "BUY", "currency": "SOL", "size_usd": 5.0,
                           "product_id": "SOL-USD", "reason": "r",
                           "capital_bucket": "opportunity"})
    assert len(opt.trade_log) == n0


def test_execute_approved_route(opt):
    opt.state = make_state({}, total_value=100000.0, usdc=50000.0)
    _approved_targets(opt)
    opt.dry_run = False
    step = SimpleNamespace(product_id="SOL-USD", from_currency="USDC",
                           to_currency="SOL", direction="BUY", price=100.0,
                           effective_rate=1.0)
    payload = {"source": "USDC", "target": "SOL", "effective_rate": 1.0,
               "score": 0.9, "steps": [
                   {"product_id": "SOL-USD", "from_currency": "USDC",
                    "to_currency": "SOL", "direction": "BUY", "price": 100.0,
                    "effective_rate": 1.0}]}
    opt._execute_route_decision = mock.MagicMock(return_value=True)
    n0 = len(opt.trade_log)
    opt._execute_approved({"side": "BUY", "currency": "SOL", "size_usd": 1000.0,
                           "product_id": "SOL-USD", "reason": "r",
                           "type": "strategy", "route_decision": payload})
    assert opt._execute_route_decision.called
    assert len(opt.trade_log) == n0 + 1


def test_execute_approved_route_failed(opt):
    opt.state = make_state({}, total_value=100000.0, usdc=50000.0)
    _approved_targets(opt)
    opt.dry_run = False
    payload = {"source": "USDC", "target": "SOL", "effective_rate": 1.0,
               "score": 0.9, "steps": [
                   {"product_id": "SOL-USD", "from_currency": "USDC",
                    "to_currency": "SOL", "direction": "BUY", "price": 100.0,
                    "effective_rate": 1.0}]}
    opt._execute_route_decision = mock.MagicMock(return_value=False)
    n0 = len(opt.trade_log)
    opt._execute_approved({"side": "BUY", "currency": "SOL", "size_usd": 1000.0,
                           "product_id": "SOL-USD", "reason": "r",
                           "type": "strategy", "route_decision": payload})
    assert len(opt.trade_log) == n0


def test_execute_approved_bracket_open(opt):
    opt.state = make_state({}, total_value=100000.0, usdc=50000.0)
    _approved_targets(opt)
    opt._bracket_mgr = mock.MagicMock()
    opt._bracket_mgr.place_bracket.return_value = {
        "status": "OPEN", "bracket_id": "b1",
        "entry_result": {"success": True, "client_order_id": "y"}}
    opt._save_brackets = mock.MagicMock()
    opt.dry_run = False
    opt._execute_approved({"side": "BUY", "currency": "SOL", "size_usd": 1000.0,
                           "product_id": "SOL-USD", "reason": "r",
                           "bracket": True, "stop_price": 95.0,
                           "target_price": 110.0, "entry_price_est": 100.0,
                           "base_qty": 10.0})
    assert opt._bracket_mgr.place_bracket.called


def test_execute_approved_bracket_failed(opt):
    opt.state = make_state({}, total_value=100000.0, usdc=50000.0)
    _approved_targets(opt)
    opt._bracket_mgr = mock.MagicMock()
    opt._bracket_mgr.place_bracket.return_value = {
        "status": "REJECTED",
        "entry_result": {"error": "boom", "success": True, "order_id": "o1"}}
    opt._bracket_mgr.force_flatten_bracket = mock.MagicMock()
    opt.dry_run = False
    opt._execute_approved({"side": "BUY", "currency": "SOL", "size_usd": 1000.0,
                           "product_id": "SOL-USD", "reason": "r",
                           "bracket": True, "stop_price": 95.0,
                           "target_price": 110.0, "entry_price_est": 100.0,
                           "base_qty": 10.0})
    assert opt._bracket_mgr.force_flatten_bracket.called


# ---------------------------------------------------------------------------
# _detect_order_flow_signals branches
# ---------------------------------------------------------------------------

def test_detect_order_flow_buy_sell(opt):
    _rich_state(opt, {"SOL": holding("SOL", 5000, "growth", price=100.0,
                                     volume_24h=2_000_000.0, product_id="SOL-USD")})
    opt.cli.best_product.side_effect = lambda c, s: "SOL-USD"
    eng = mock.MagicMock()
    eng.evaluate.return_value = _of_sig("BUY", 0.6)
    opt._order_flow_engine = eng
    opt._smart_money_flow = None
    ops = opt._detect_order_flow_signals()
    assert any(o.meta.get("source") == "order_flow" for o in ops)


def test_detect_order_flow_sell(opt):
    _rich_state(opt, {"SOL": holding("SOL", 5000, "growth", price=100.0,
                                     volume_24h=2_000_000.0, product_id="SOL-USD")})
    opt.cli.best_product.side_effect = lambda c, s: "SOL-USD"
    eng = mock.MagicMock()
    eng.evaluate.return_value = _of_sig("SELL", 0.6)
    opt._order_flow_engine = eng
    opt._smart_money_flow = None
    ops = opt._detect_order_flow_signals()
    assert any(o.side == "SELL" for o in ops)


def test_detect_order_flow_low_conf(opt):
    _rich_state(opt, {"SOL": holding("SOL", 5000, "growth", price=100.0,
                                     volume_24h=2_000_000.0, product_id="SOL-USD")})
    opt.cli.best_product.side_effect = lambda c, s: "SOL-USD"
    eng = mock.MagicMock()
    eng.evaluate.return_value = _of_sig("BUY", 0.1)
    opt._order_flow_engine = eng
    opt._smart_money_flow = None
    # Low confidence -> no opportunity emitted.
    assert opt._detect_order_flow_signals() == []


def test_detect_order_flow_smart_money(opt):
    from tests.coverage.optimizer.test_portfolio_optimizer_cov3 import _fake_market
    _rich_state(opt, {"SOL": holding("SOL", 5000, "growth", price=100.0,
                                     volume_24h=2_000_000.0, product_id="SOL-USD")})
    opt.cli.best_product.side_effect = lambda c, s: "SOL-USD"
    opt._order_flow_engine = None
    smf = mock.MagicMock()
    setup = SimpleNamespace(direction=SimpleNamespace(value="long"), confidence=0.6,
                            entry=100.0, stop=95.0, target=110.0,
                            expected_return_pct=5.0, risk_pct=5.0)
    smf.detect_setup.return_value = setup
    opt._smart_money_flow = smf
    ops = opt._detect_order_flow_signals()
    assert isinstance(ops, list)


def test_detect_order_flow_smart_money_no_setup(opt):
    _rich_state(opt, {"SOL": holding("SOL", 5000, "growth", price=100.0,
                                     volume_24h=2_000_000.0, product_id="SOL-USD")})
    opt.cli.best_product.side_effect = lambda c, s: "SOL-USD"
    opt._order_flow_engine = None
    smf = mock.MagicMock()
    smf.detect_setup.return_value = None
    opt._smart_money_flow = smf
    assert opt._detect_order_flow_signals() == []


def test_detect_order_flow_smart_money_full(opt):
    _rich_state(opt, {"SOL": holding("SOL", 5000, "growth", price=100.0,
                                     volume_24h=2_000_000.0, product_id="SOL-USD")})
    opt.cli.best_product.side_effect = lambda c, s: "SOL-USD"
    opt._order_flow_engine = None
    opt._feed_mgr = mock.MagicMock()
    candles = [{"start": i, "open": 100 + i, "high": 101 + i, "low": 99 + i,
                "close": 100 + i, "volume": 1000, "time": i}
               for i in range(50)]
    opt._feed_mgr.get_candles_batch.return_value = {"SOL-USD": candles}
    smf = mock.MagicMock()
    setup = SimpleNamespace(direction=SimpleNamespace(value="long"), confidence=0.6,
                            entry=100.0, stop=95.0, target=110.0,
                            expected_return_pct=5.0, risk_pct=5.0, reason="abs")
    smf.detect_setup.return_value = setup
    opt._smart_money_flow = smf
    ops = opt._detect_order_flow_signals()
    assert isinstance(ops, list)


def test_detect_order_flow_no_engine_no_smf(opt):
    _rich_state(opt, {"SOL": holding("SOL", 5000, "growth", price=100.0,
                                     volume_24h=2_000_000.0, product_id="SOL-USD")})
    opt._order_flow_engine = None
    opt._smart_money_flow = None
    assert opt._detect_order_flow_signals() == []


def test_detect_order_flow_no_state(opt):
    opt.state = None
    assert opt._detect_order_flow_signals() == []


def test_detect_volume_cycles_static_skip(opt):
    _rich_state(opt, {"BTC": holding("BTC", 40000, "safe", price=40000.0,
                                     product_id="BTC-USD")})
    opt.position_ages["BTC"] = time.time() - 200 * 3600
    ops = opt._detect_volume_cycles()
    assert ops == []


def test_detect_stairstep_engine_import_fail(opt):
    opt.stairstep_enabled = True
    opt.state = make_state(
        {"XRP": holding("XRP", 1000, "growth", price=0.6, product_id="XRP-USD")},
        total_value=100000.0)
    opt._stairstep_engine = None
    import builtins
    real = builtins.__import__
    def fake(name, *a, **k):
        if name.endswith("rebalance_engine"):
            raise ImportError("nope")
        return real(name, *a, **k)
    with mock.patch.object(builtins, "__import__", side_effect=fake):
        assert opt._detect_stairstep() == []


def test_execute_with_bracket_dryrun_no_mgr(opt):
    opt.state = make_state({}, total_value=100000.0, usdc=50000.0)
    opt._bracket_mgr = mock.MagicMock()
    opt._bracket_mgr.place_bracket.return_value = {
        "status": "OPEN", "bracket_id": "bX",
        "entry_result": {"success": True, "client_order_id": "y"}}
    opt._save_brackets = mock.MagicMock()
    opt.dry_run = True
    opt.require_approval = False
    opp = P.Opportunity(P.OpportunityType.STRATEGY_SIGNAL, "SOL", "BUY", 1000, "r",
                        entry_price_est=100.0, stop_loss_pct=5.0, product_id="SOL-USD")
    opt._execute_with_bracket(opp, 10.0, True)
    assert opp.executed is True


def test_process_buy_dryrun_full(opt):
    _rich_state(opt, {"SOL": holding("SOL", 5000, "growth", price=100.0,
                                     product_id="SOL-USD")})
    opt.cli.best_product.side_effect = lambda c, s: "SOL-USD"
    opt.cli.preview_order.return_value = {"total_fee": 1.0, "total_cost": 1000.0}
    opt.cli.create_order.return_value = {"id": "X1"}
    opt.dry_run = True
    opt.require_approval = False
    opt._best_route_decision_for_opportunity = mock.MagicMock(return_value=None)
    opp = P.Opportunity(P.OpportunityType.STRATEGY_SIGNAL, "SOL", "BUY", 1000, "r",
                        entry_price_est=100.0, product_id="SOL-USD")
    opt._process_opportunity(opp)
    assert opp.executed is True
    assert opp.order_id == "dry-run"


def test_process_approval_pending_route_write(opt):
    _rich_state(opt, {"SOL": holding("SOL", 5000, "growth", price=100.0,
                                     product_id="SOL-USD")})
    opt.cli.best_product.side_effect = lambda c, s: "SOL-USD"
    opt.cli.preview_order.return_value = {"total_fee": 1.0, "total_cost": 1000.0}
    opt.dry_run = False
    opt.require_approval = True
    opt.pending_file = os.path.join("data", "pending_route2.json")
    opt.notifier = mock.MagicMock()
    opp = P.Opportunity(P.OpportunityType.STRATEGY_SIGNAL, "SOL", "BUY", 1000, "r",
                        entry_price_est=100.0, product_id="SOL-USD")
    step = SimpleNamespace(product_id="SOL-USD", from_currency="USDC",
                           to_currency="SOL", direction="BUY", price=100.0,
                           effective_rate=1.0)
    plan = SimpleNamespace(source="USDC", target="SOL", effective_rate=1.0,
                           steps=[step, step], path=["USDC", "SOL"])
    decision = SimpleNamespace(plan=plan, score=0.9, expected_tax_impact_usd=0.0,
                               opportunity_bonus=0.0, drawdown_bonus=0.0,
                               regime_bonus=0.0, hop_penalty=0.0, liquidity_bonus=0.0,
                               factor_breakdown={})
    opt._best_route_decision_for_opportunity = mock.MagicMock(return_value=decision)
    opt._process_opportunity(opp)
    assert os.path.exists(opt.pending_file)
    assert opt.notifier.send_trade_alert.called


def test_process_sell_dryrun(opt):
    _rich_state(opt, {"SOL": holding("SOL", 5000, "growth", price=100.0,
                                     product_id="SOL-USD")})
    opt.cli.best_product.return_value = "SOL-USD"
    opt.cli.preview_order.return_value = {"total_fee": 1.0, "total_cost": 1000.0}
    opt.dry_run = True
    opt.require_approval = False
    opt._best_route_decision_for_opportunity = mock.MagicMock(return_value=None)
    opp = P.Opportunity(P.OpportunityType.STRATEGY_SIGNAL, "SOL", "SELL", 1000, "r",
                        entry_price_est=100.0, product_id="SOL-USD")
    opt._process_opportunity(opp)
    assert opp.executed is True
    assert opp.order_id == "dry-run"



# ---------------------------------------------------------------------------
# _detect_coinbase_universe_signals (exercise the full body)
# ---------------------------------------------------------------------------

def test_detect_coinbase_universe_full_body(opt):
    candles = [{"start": i, "open": 100 + i, "high": 101 + i, "low": 99 + i,
                "close": 100 + i, "volume": 1000} for i in range(50)]
    opt.cli.get_products.return_value = {
        "SOL-USD": {"product_id": "SOL-USD", "volume_24h": 5_000_000.0,
                    "trading_disabled": False},
        "USDC-USD": {"product_id": "USDC-USD", "volume_24h": 1.0},
        "BTC-USD": {"product_id": "BTC-USD", "volume_24h": 9_000_000.0,
                    "trading_disabled": False},
    }
    opt.cli.get_candles.return_value = candles
    _rich_state(opt, {}, total_value=100000.0, usdc=90000.0)
    opt.cli.best_product.side_effect = lambda c, s: f"{c}-USD"
    ops = opt._detect_coinbase_universe_signals()
    assert isinstance(ops, list)


def test_detect_coinbase_universe_trading_disabled(opt):
    candles = [{"start": i, "open": 100 + i, "high": 101 + i, "low": 99 + i,
                "close": 100 + i, "volume": 1000} for i in range(50)]
    opt.cli.get_products.return_value = {
        "SOL-USD": {"product_id": "SOL-USD", "volume_24h": 5_000_000.0,
                    "trading_disabled": True},
    }
    opt.cli.get_candles.return_value = candles
    _rich_state(opt, {}, total_value=100000.0, usdc=90000.0)
    opt.cli.best_product.side_effect = lambda c, s: f"{c}-USD"
    assert opt._detect_coinbase_universe_signals() == []


# ---------------------------------------------------------------------------
# _detect_stock_opportunities branches
# ---------------------------------------------------------------------------

def test_detect_stock_opportunities_buy(opt):
    if P.UnifiedMarketDataAdapter is None:
        pytest.skip("stock adapter unavailable")
    bars = [{"close": 100 + i, "volume": 1000} for i in range(80)]
    adapter = mock.MagicMock()
    adapter.fetch_historical_data.return_value = bars
    adapter.yfinance.get_stock_info.return_value = {"market_capital": 1_000_000_000_000}
    fake = mock.MagicMock(return_value=adapter)
    with mock.patch.object(P, "UnifiedMarketDataAdapter", fake):
        ops = opt._detect_stock_opportunities()
    assert isinstance(ops, list)


# ---------------------------------------------------------------------------
# _run_periodic_param_optimization (with sufficient data)
# ---------------------------------------------------------------------------

def test_run_periodic_param_optimization_with_data(opt):
    P._HAS_WALK_FORWARD = True
    FakeWF = mock.MagicMock()
    old = P._WalkForwardOptimizer
    P._WalkForwardOptimizer = FakeWF
    opt._feed_mgr = mock.MagicMock()
    opt._feed_mgr.get_candles_batch.return_value = {
        "BTC-USD": [[0, 99.0, 101.0, 100.0, 100.0 + (i % 3), 1000.0]
                    for i in range(220)]
    }
    opt._last_param_opt_ts = 0
    try:
        res = opt._run_periodic_param_optimization()
        assert isinstance(res, dict)
    finally:
        P._WalkForwardOptimizer = old
        P._HAS_WALK_FORWARD = False


def test_run_periodic_param_optimization_recent(opt):
    opt._last_param_opt_ts = time.time()
    P._HAS_WALK_FORWARD = True
    FakeWF = mock.MagicMock()
    old = P._WalkForwardOptimizer
    P._WalkForwardOptimizer = FakeWF
    opt._feed_mgr = mock.MagicMock()
    try:
        res = opt._run_periodic_param_optimization()
        assert res == opt._param_opt_results
    finally:
        P._WalkForwardOptimizer = old
        P._HAS_WALK_FORWARD = False


def test_run_periodic_param_optimization_no_walkforward(opt):
    opt._last_param_opt_ts = 0
    P._HAS_WALK_FORWARD = False
    res = opt._run_periodic_param_optimization()
    assert res == {}


# ---------------------------------------------------------------------------
# _detect_stairstep branches
# ---------------------------------------------------------------------------

def test_stairstep_no_signal(opt):
    opt.stairstep_enabled = True
    opt.state = make_state(
        {"XRP": holding("XRP", 1000, "growth", price=0.6, product_id="XRP-USD")},
        total_value=100000.0)
    opt._stairstep_symbols = ["XRP-USD"]
    opt.cli.best_product.side_effect = lambda c, s: "XRP-USD"
    engine = mock.MagicMock()
    engine._symbols = {}
    engine.add_symbol.return_value = None
    engine.on_price.return_value = None
    opt._stairstep_engine = engine
    assert opt._detect_stairstep() == []


def test_stairstep_no_state(opt):
    opt.stairstep_enabled = True
    opt.state = None
    assert opt._detect_stairstep() == []


# ---------------------------------------------------------------------------
# _detect_rebalance_bot with real orders
# ---------------------------------------------------------------------------

def test_rebalance_bot_drift_below_threshold(opt):
    opt.state = make_state(
        {"BTC": holding("BTC", 40000, "safe", price=40000.0, product_id="BTC-USD")},
        total_value=100000.0)
    opt.cli.best_product.side_effect = lambda c, s: "BTC-USD"
    engine = mock.MagicMock()
    engine.targets = ["BTC-USD"]
    rec = mock.MagicMock()
    rec.max_drift = 0.01
    rec.turnover = 0.0
    rec.orders = []
    engine.compute.return_value = rec
    bot = mock.MagicMock()
    bot.engine = engine
    opt._rebalance_bot = bot
    assert opt._detect_rebalance_bot() == []


def test_rebalance_bot_no_price(opt):
    opt.state = make_state(
        {"BTC": holding("BTC", 40000, "safe", price=40000.0, product_id="BTC-USD")},
        total_value=100000.0)
    opt.cli.best_product.side_effect = lambda c, s: None
    engine = mock.MagicMock()
    engine.targets = ["BTC-USD"]
    rec = mock.MagicMock()
    rec.max_drift = 0.10
    rec.turnover = 0.2
    order = SimpleNamespace(asset="BTC-USD", side="BUY", notional=1000.0,
                            drift=0.05, target_weight=0.4, current_weight=0.35)
    rec.orders = [order]
    engine.compute.return_value = rec
    bot = mock.MagicMock()
    bot.engine = engine
    opt._rebalance_bot = bot
    assert opt._detect_rebalance_bot() == []


# ---------------------------------------------------------------------------
# _detect_volume_cycles fresh position
# ---------------------------------------------------------------------------

def test_detect_volume_cycles_fresh(opt):
    _rich_state(opt, {"SOL": holding("SOL", 5000, "growth", price=100.0,
                                     product_id="SOL-USD")})
    opt.cli.best_product.side_effect = lambda c, s: "SOL-USD"
    opt.position_ages.clear()
    ops = opt._detect_volume_cycles()
    assert ops == []


# ---------------------------------------------------------------------------
# _process_opportunity extra branches (dry-run notify for non-tradeable)
# ---------------------------------------------------------------------------

def test_process_event_market_dryrun_notify(opt):
    opt.state = make_state({}, total_value=100000.0, usdc=50000.0)
    opt.dry_run = True
    opt.notifier = mock.MagicMock()
    opp = P.Opportunity(P.OpportunityType.EVENT_MARKET, "?", "NONE", 0, "r",
                        product_id="kalshi:1",
                        meta={"platform": "kalshi", "market_question": "q?",
                              "signal_type": "x"})
    n0 = len(opt.trade_log)
    opt._process_opportunity(opp)
    assert len(opt.trade_log) == n0 + 1
    assert not opt.notifier.send_trade_alert.called


def test_process_stock_signal_notify(opt):
    opt.state = make_state({}, total_value=100000.0, usdc=50000.0)
    opt.dry_run = False
    opt.notifier = mock.MagicMock()
    opp = P.Opportunity(P.OpportunityType.STOCK_SIGNAL, "NVDA", "BUY", 100, "r",
                        product_id="NVDA")
    opt._process_opportunity(opp)
    assert opt.notifier.send_trade_alert.called


def test_process_sell_preview_fail(opt):
    opt.state = make_state(
        {"SOL": holding("SOL", 5000, "growth", price=100.0, product_id="SOL-USD")},
        total_value=100000.0, usdc=50000.0)
    opt.cli.best_product.return_value = "SOL-USD"
    opt.cli.preview_order.return_value = None
    opt.dry_run = False
    opt.require_approval = False
    opt._best_route_decision_for_opportunity = mock.MagicMock(return_value=None)
    opp = P.Opportunity(P.OpportunityType.STRATEGY_SIGNAL, "SOL", "SELL", 1000, "r",
                        entry_price_est=100.0, product_id="SOL-USD")
    with mock.patch.object(P.logger, "warning") as w:
        opt._process_opportunity(opp)
    assert any("Preview failed" in str(a) for a in w.call_args_list)


def test_process_execution_no_order(opt):
    opt.state = make_state(
        {"SOL": holding("SOL", 5000, "growth", price=100.0, product_id="SOL-USD")},
        total_value=100000.0, usdc=50000.0)
    opt.cli.best_product.return_value = "SOL-USD"
    opt.cli.preview_order.return_value = {"total_fee": 1.0, "total_cost": 1000.0}
    opt.cli.create_order.return_value = None
    opt.dry_run = False
    opt.require_approval = False
    opt._best_route_decision_for_opportunity = mock.MagicMock(return_value=None)
    opp = P.Opportunity(P.OpportunityType.STRATEGY_SIGNAL, "SOL", "BUY", 1000, "r",
                        entry_price_est=100.0, product_id="SOL-USD")
    n0 = len(opt.trade_log)
    opt._process_opportunity(opp)
    assert len(opt.trade_log) == n0


# ---------------------------------------------------------------------------
# _execute_with_bracket preview / exec-engine branches
# ---------------------------------------------------------------------------

def test_execute_with_bracket_preview_fail(opt):
    opt.state = make_state({}, total_value=100000.0, usdc=50000.0)
    opt._exec_engine = mock.MagicMock()
    preview = mock.MagicMock()
    preview.success = False
    preview.error = "no liquidity"
    opt._exec_engine._preview.return_value = preview
    opp = P.Opportunity(P.OpportunityType.STRATEGY_SIGNAL, "SOL", "BUY", 1000, "r",
                        entry_price_est=100.0, stop_loss_pct=5.0, product_id="SOL-USD")
    with mock.patch.object(P.logger, "warning") as w:
        opt._execute_with_bracket(opp, 10.0, True)
    assert any("preview failed" in str(a).lower() for a in w.call_args_list)


def test_execute_with_bracket_fee_too_high(opt):
    opt.state = make_state({}, total_value=100000.0, usdc=50000.0)
    opt._exec_engine = mock.MagicMock()
    P._OrderIntent = mock.MagicMock()
    P._OrderType = mock.MagicMock()
    preview = mock.MagicMock()
    preview.success = True
    preview.raw = {"preview": {"total_fee": 500.0}}
    opt._exec_engine._preview.return_value = preview
    opp = P.Opportunity(P.OpportunityType.STRATEGY_SIGNAL, "SOL", "BUY", 1000, "r",
                        entry_price_est=100.0, stop_loss_pct=5.0, product_id="SOL-USD")
    with mock.patch.object(P.logger, "warning") as w:
        opt._execute_with_bracket(opp, 10.0, True)
    assert any("fee too high" in str(a).lower() for a in w.call_args_list)


def test_execute_with_bracket_pending_live(opt):
    opt.state = make_state({}, total_value=100000.0, usdc=50000.0)
    opt._exec_engine = None
    opt.dry_run = False
    opt.require_approval = True
    opt.pending_file = os.path.join("data", "pending_bracket2.json")
    opp = P.Opportunity(P.OpportunityType.STRATEGY_SIGNAL, "SOL", "BUY", 1000, "r",
                        entry_price_est=100.0, stop_loss_pct=5.0, product_id="SOL-USD")
    opt._execute_with_bracket(opp, 10.0, True)
    assert os.path.exists(opt.pending_file)
    with open(opt.pending_file) as f:
        data = __import__("json").load(f)
    assert any(v.get("bracket") for v in data.values())


# ---------------------------------------------------------------------------
# _detect_funding_and_onchain extra branches
# ---------------------------------------------------------------------------

def test_detect_funding_only(opt):
    _rich_state(opt, {"BTC": holding("BTC", 30000, "safe", price=30000.0,
                                     product_id="BTC-USD")})
    opt.cli.get_price.return_value = {"price": 30000.0}
    opt.cli.best_product.return_value = "BTC-USD"
    fund = mock.MagicMock()
    fund.on_bar.return_value = StrategySignal(action="BUY", confidence=0.6,
                                              strategy="funding", reason="fund r")
    opt._funding_contrarian = fund
    opt._onchain_flow = None
    ops = opt._detect_funding_and_onchain_signals()
    assert any(o.meta.get("source") == "funding_rate" for o in ops)


def test_detect_onchain_only(opt):
    _rich_state(opt, {"BTC": holding("BTC", 30000, "safe", price=30000.0,
                                     product_id="BTC-USD")})
    opt.cli.get_price.return_value = {"price": 30000.0}
    opt.cli.best_product.return_value = "BTC-USD"
    opt._funding_contrarian = None
    onchain = mock.MagicMock()
    onchain.get_signals.return_value = [{
        "action": "SELL", "product_id": "BTC-USD", "currency": "BTC",
        "confidence": 0.5, "price": 30000.0, "volume_anomaly": 2.0,
        "price_trend": 0.1, "reason": "oc r"}]
    opt._onchain_flow = onchain
    ops = opt._detect_funding_and_onchain_signals()
    assert any(o.meta.get("source") == "onchain_flow" for o in ops)


# ---------------------------------------------------------------------------
# _detect_event_markets extra branches
# ---------------------------------------------------------------------------

def test_detect_event_markets_non_actionable(opt):
    opt._pm_client = mock.MagicMock()
    opt._pm_client.search_all_categories.return_value = {
        "sports": [SimpleNamespace(category="sports", volume=5000, is_open=True,
                                   probability_extremity=0.9, liquidity_score=0.8,
                                   mid_price=0.6, question="who wins", platform="kalshi",
                                   market_id="m1", spread=0.01)],
    }
    opt._knowledge_gap = mock.MagicMock()
    opt._knowledge_gap.analyze.return_value = None
    opt._arb_scanner = mock.MagicMock()
    opt._arb_scanner.scan.return_value = []
    opt.cli.best_product.return_value = "BTC-USD"
    opt.cli.get_price.return_value = {"price": 50000.0}
    ops = opt._detect_event_markets()
    assert isinstance(ops, list)


def test_detect_event_markets_low_extremity(opt):
    opt._pm_client = mock.MagicMock()
    opt._pm_client.search_all_categories.return_value = {
        "crypto": [SimpleNamespace(category="crypto", volume=5000, is_open=True,
                                   probability_extremity=0.1, liquidity_score=0.8,
                                   mid_price=0.6, question="will btc rise", platform="kalshi",
                                   market_id="m1", spread=0.01)],
    }
    opt._knowledge_gap = mock.MagicMock()
    opt._knowledge_gap.analyze.return_value = None
    opt._arb_scanner = mock.MagicMock()
    opt._arb_scanner.scan.return_value = []
    opt.cli.best_product.return_value = "BTC-USD"
    opt.cli.get_price.return_value = {"price": 50000.0}
    ops = opt._detect_event_markets()
    assert isinstance(ops, list)


def test_detect_event_markets_event_engine_fallback(opt):
    opt._pm_client = None
    eng = mock.MagicMock()
    sig = SimpleNamespace(outcome="BUY YES", reason="r", position_size=100,
                          platform="kalshi", market_ticker="T", market_question="q",
                          probability=0.6, signal_type="x", confidence=0.7)
    eng.find_opportunities.return_value = [sig]
    opt.event_engine = eng
    opt.state = make_state(
        {"SOL": holding("SOL", 5000, "growth", price=100.0, product_id="SOL-USD")},
        total_value=100000.0)
    ops = opt._detect_event_markets()
    assert isinstance(ops, list)


# ---------------------------------------------------------------------------
# _write_* error-tolerance branches
# ---------------------------------------------------------------------------

def test_write_trade_plans_empty(opt):
    opt._write_trade_plans([])
    assert os.path.exists("trade_plans.json")
    os.remove("trade_plans.json")


def test_write_signal_cache_empty(opt):
    opt._write_signal_cache([])
    assert os.path.exists("data/.unified_signal_cache.json")
    os.remove("data/.unified_signal_cache.json")


def test_write_enhanced_state_minimal(opt):
    opt.state = make_state(
        {"SOL": holding("SOL", 5000, "growth", price=100.0, product_id="SOL-USD")},
        total_value=100000.0, usdc=50000.0)
    opt._cross_asset_regime = None
    opt._ensemble_blender = None
    opt._param_opt_results = {}
    opt._wash_sale_cooldown = {}
    opt._order_flow_engine = None
    opt._meta_source_weights = {"x": 0.5}
    opt._write_enhanced_state()
    assert os.path.exists("data/meta_source_weights.json")
    for p in ("data/meta_source_weights.json", "data/cross_asset_regime.json",
              "data/signal_ensemble.json", "data/param_opt_results.json",
              "data/wash_sale_state.json", "data/order_flow_signals.json"):
        if os.path.exists(p):
            os.remove(p)
