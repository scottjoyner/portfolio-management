"""Extra coverage + critical-evaluation tests for portfolio_optimizer.py.

Heavy collaborators are mocked so the process exits cleanly (no non-daemon
threads). Each test exercises a single method / code path directly.
"""

import json
import os
import time
from unittest import mock

import pytest

# Disable the SmartFeed background thread for clean process exit.
import portfolio_optimizer as P
P._HAS_SMART_FEED = False
from conftest import make_state, holding, opt  # noqa: F401


# ---------------------------------------------------------------------------
# Build a clean optimizer and disable any background threads.
# ---------------------------------------------------------------------------

@pytest.fixture
def po(opt):
    """Optimizer with all background engines neutralised for safe unit tests."""
    o = opt
    # Neutralise engines that could spawn threads / do network I/O.
    o._feed_mgr = None
    if o._bracket_mgr is not None and hasattr(o._bracket_mgr, "stop_polling"):
        try:
            o._bracket_mgr.stop_polling()
        except Exception:
            pass
    o._bracket_mgr = None
    o._exec_engine = None
    o._order_flow_engine = None
    o._smart_money_flow = None
    if o._health_server is not None and hasattr(o._health_server, "stop"):
        try:
            o._health_server.stop()
        except Exception:
            pass
    o._health_server = None
    o._pm_client = None
    o._arb_scanner = None
    o._knowledge_gap = None
    o._cross_asset_regime = None
    o._macro_risk = None
    o._ensemble_blender = None
    o.notifier = None
    return o


# ===========================================================================
# TLH
# ===========================================================================

def test_detect_tlh_finds_loss(po):
    po.state = make_state({"SOL": holding("SOL", 1000, "growth", pnl=-12.0)},
                            total_value=100000)
    po.cli.best_product.return_value = "SOL-USD"
    ops = po._detect_tlh()
    assert len(ops) == 1
    assert ops[0].opp_type == P.OpportunityType.TLH
    assert ops[0].side == "SELL"


def test_detect_tlh_cooldown_and_no_state(po):
    po.state = None
    assert po._detect_tlh() == []
    po.state = make_state({"SOL": holding("SOL", 1000, "growth", pnl=-12.0)})
    po.cli.best_product.return_value = "SOL-USD"
    po.last_execution["tlh"] = time.time()
    assert po._detect_tlh() == []


def test_detect_tlh_skips_static_and_minor_loss(po):
    po.state = make_state({
        "BTC": holding("BTC", 50000, "safe", pnl=-20.0),
        "SOL": holding("SOL", 1000, "growth", pnl=-1.0),
    }, total_value=100000)
    po.cli.best_product.return_value = "SOL-USD"
    ops = po._detect_tlh()
    # Only non-static currencies with >5% loss qualify.
    assert all(o.currency != "BTC" for o in ops)


# ===========================================================================
# Enhanced TLH
# ===========================================================================

def test_detect_enhanced_tlh(po):
    po.state = make_state({"SOL": holding("SOL", 1000, "growth", pnl=-15.0)},
                            total_value=100000)
    po.cli.best_product.return_value = "SOL-USD"
    ops = po._detect_enhanced_tlh()
    assert len(ops) == 1
    assert "tax_savings" in ops[0].meta
    assert "replacement" in ops[0].meta


def test_detect_enhanced_tlh_wash_sale(po):
    po._wash_sale_cooldown["SOL"] = time.time()
    po.state = make_state({"SOL": holding("SOL", 1000, "growth", pnl=-15.0)},
                            total_value=100000)
    assert po._detect_enhanced_tlh() == []


# ===========================================================================
# Fee-tier volume
# ===========================================================================

def test_detect_fee_tier_volume(po):
    po.state = make_state({"SOL": holding("SOL", 1000, "growth", volume_24h=2_000_000.0)},
                            total_value=100000, fee_volume_30d=0.0, volume_to_next_tier=5000.0)
    po.cli.best_product.return_value = "SOL-USD"
    ops = po._detect_fee_tier_volume()
    assert len(ops) == 1
    assert ops[0].opp_type == P.OpportunityType.FEE_TIER_VOLUME


def test_detect_fee_tier_volume_no_need(po):
    po.state = make_state({"SOL": holding("SOL", 1000, "growth")},
                            total_value=100000, fee_volume_30d=0.0, volume_to_next_tier=0.0)
    assert po._detect_fee_tier_volume() == []


# ===========================================================================
# Rebalance
# ===========================================================================

def test_detect_rebalance_overweight_sell(po):
    # Heavy growth over-allocation -> SELL to rebalance.
    po.state = make_state({
        "SOL": holding("SOL", 60000, "growth", allocation_pct=60.0),
        "USDC": holding("USDC", 40000, "safe", allocation_pct=40.0),
    }, total_value=100000)
    po.cli.best_product.return_value = "SOL-USD"
    ops = po._detect_rebalance()
    assert any(o.side == "SELL" for o in ops)


def test_detect_rebalance_underweight_buy(po):
    # Growth underweight with a held non-static growth asset -> BUY to rebalance.
    # (Core assets BTC/ETH/SOL are static, so rebalancing INTO them is
    #  intentionally blocked; use a non-static growth asset here.)
    po.state = make_state({
        "BTC": holding("BTC", 45000, "safe", allocation_pct=45.0),
        "USDC": holding("USDC", 50000, "safe", allocation_pct=50.0),
        "AVAX": holding("AVAX", 5000, "growth", allocation_pct=5.0),
    }, total_value=100000)
    po.cli.best_product.return_value = "AVAX-USD"
    ops = po._detect_rebalance()
    assert any(o.side == "BUY" and o.currency == "AVAX" for o in ops)


# ===========================================================================
# Volume cycles
# ===========================================================================

def test_detect_volume_cycles_stale(po):
    po.state = make_state({"SOL": holding("SOL", 1000, "growth")},
                            total_value=100000)
    po.cli.best_product.return_value = "SOL-USD"
    # Fake a stale position age (> 168h).
    po.position_ages["SOL"] = time.time() - 200 * 3600
    ops = po._detect_volume_cycles()
    assert len(ops) == 1
    assert ops[0].opp_type == P.OpportunityType.VOLUME_CYCLE


def test_detect_volume_cycles_fresh(po):
    po.state = make_state({"SOL": holding("SOL", 1000, "growth")},
                            total_value=100000)
    po.position_ages["SOL"] = time.time()
    assert po._detect_volume_cycles() == []


# ===========================================================================
# Strategy signals (heavily mocked)
# ===========================================================================

def _fake_candles(n=40, start=100.0):
    return [{"close": start + i, "high": start + i + 1, "low": start + i - 1,
             "volume": 1000.0} for i in range(n)]


def test_detect_strategy_signals(po):
    po.state = make_state({"SOL": holding("SOL", 1000, "growth", price=140.0)},
                            total_value=100000)
    po.last_execution.clear()
    po._min_pulse_count = 1  # relax quality gate for the single-tick test
    po._feed_mgr = mock.MagicMock()
    po._feed_mgr.get_candles_batch.return_value = {"SOL-USD": _fake_candles()}

    sig = P.StrategySignal(strategy="ema_cross", action="BUY", confidence=0.6, reason="r")
    with mock.patch.object(P, "_run_strategies", return_value=[sig]), \
         mock.patch.object(P, "_batch_signals_fast", return_value={}), \
         mock.patch.object(P, "ConfidenceMatrix") as CM:
        # Fake aggregated signal result.
        class Agg:
            direction = "BUY"
            confidence = 0.7
            best_reason = "ema_cross"
            strategy_count = 1
            strategies = ["ema_cross"]
            agreeing_groups = ["trend"]
        CM.return_value.aggregate.return_value = [Agg()]

        po._bt_cache = {
            "ema_cross/SOL": P.BacktestVerdict(
                strategy="ema_cross", currency="SOL", regime="trending",
                passed=True, win_rate=0.7, sharpe_ratio=1.0, profit_factor=1.5,
                max_drawdown_pct=5.0, total_return_pct=10.0, total_trades=10,
                winning_trades=7, losing_trades=3, reason="ok"),
        }
        ops = po._detect_strategy_signals()
    assert any(o.currency == "SOL" and o.side == "BUY" for o in ops)


def test_detect_strategy_signals_no_state(po):
    po.state = None
    assert po._detect_strategy_signals() == []


# ===========================================================================
# Funding / on-chain
# ===========================================================================

def test_detect_funding_and_onchain_buy(po):
    po.last_execution.clear()
    po.state = make_state({"BTC": holding("BTC", 5000, "safe", price=30000)})
    po.cli.get_price.return_value = {"price": 30000.0}
    f = mock.MagicMock()
    f.action = "BUY"
    f.confidence = 0.6
    f.reason = "funding"
    po._funding_contrarian.on_bar = mock.MagicMock(return_value=f)
    po._onchain_flow.get_signals = mock.MagicMock(return_value=[])
    ops = po._detect_funding_and_onchain_signals()
    assert any(o.currency == "BTC" and o.side == "BUY" for o in ops)


# ===========================================================================
# Order flow (already partially covered; add SELL branch + capacity)
# ===========================================================================

def test_detect_order_flow_sell(po):
    po.last_execution.clear()
    po._order_flow_engine = mock.MagicMock()
    of = mock.MagicMock()
    of.confidence = 0.6
    of.action = "SELL"
    of.spread_z = 3.0
    of.spread_tight = True
    of.spread_bps = 4.0
    of.volume_24h = 1000.0
    po._order_flow_engine.evaluate = mock.MagicMock(return_value=of)
    po._smart_money_flow = None
    po._feed_mgr = None
    po.state = make_state({"SOL": holding("SOL", 1000, "growth", price=100,
                                        spread=0.004, volume_24h=1000.0)})
    ops = po._detect_order_flow_signals()
    assert any(o.side == "SELL" for o in ops)


# ===========================================================================
# Event markets (PM client mock)
# ===========================================================================

class _FakeMarket:
    def __init__(self, **kw):
        self.category = kw.get("category", "crypto")
        self.platform = kw.get("platform", "kalshi")
        self.question = kw.get("question", "Will BTC reach $100k?")
        self.market_id = kw.get("market_id", "m1")
        self.volume = kw.get("volume", 5000.0)
        self.is_open = kw.get("is_open", True)
        self.probability_extremity = kw.get("probability_extremity", 0.8)
        self.mid_price = kw.get("mid_price", 0.8)
        self.liquidity_score = kw.get("liquidity_score", 0.9)
        self.spread = kw.get("spread", 0.02)


def test_detect_event_markets_actionable(po):
    po._pm_client = mock.MagicMock()
    po._pm_client.search_all_categories.return_value = {
        "crypto": [_FakeMarket(question="will bitcoin hit 100k", mid_price=0.8)]
    }
    po._arb_scanner = None
    po._knowledge_gap = None
    po._feed_mgr = None
    po.state = make_state({"BTC": holding("BTC", 5000, "safe", price=30000)})
    ops = po._detect_event_markets()
    assert any(o.opp_type == P.OpportunityType.STRATEGY_SIGNAL for o in ops)


def test_detect_event_markets_notification(po):
    po._pm_client = mock.MagicMock()
    po._pm_client.search_all_categories.return_value = {
        "sports": [_FakeMarket(category="sports", question="who wins the super bowl",
                               mid_price=0.7, volume=2000.0)]
    }
    po._arb_scanner = None
    po._knowledge_gap = None
    po._feed_mgr = None
    ops = po._detect_event_markets()
    assert any(o.opp_type == P.OpportunityType.EVENT_MARKET for o in ops)


def test_detect_event_markets_empty(po):
    po._pm_client = mock.MagicMock()
    po._pm_client.search_all_categories.return_value = {}
    po._arb_scanner = None
    po._knowledge_gap = None
    assert po._detect_event_markets() == []


# ===========================================================================
# Coinbase universe scan
# ===========================================================================

def test_detect_coinbase_universe_signals(po):
    candles = _fake_candles()
    po._feed_mgr = mock.MagicMock()
    po._feed_mgr.get_candles_batch.return_value = {"SOL-USD": candles}
    po.state = make_state({"USDC": holding("USDC", 90000, "safe")}, total_value=100000)
    po.cli.get_products.return_value = {
        "SOL-USD": {"trading_disabled": False, "volume_24h": 200_000_000.0},
    }
    po.cli.get_candles.return_value = candles
    # Mock the compute backend so batch metrics are available.
    cb_mod = mock.MagicMock()
    cb_mod.get_compute_backend.return_value = None
    with mock.patch.dict("sys.modules", {"trading_system.core.compute_backend": cb_mod}):
        ops = po._detect_coinbase_universe_signals()
    assert isinstance(ops, list)


def test_detect_coinbase_universe_signals_no_products(po):
    po.cli.get_products.return_value = {}
    assert po._detect_coinbase_universe_signals() == []


# ===========================================================================
# Signal ensemble / meta learning
# ===========================================================================

def test_signal_ensemble_blend(po):
    from coinbase.src.protocols import Direction, InstrumentType, Opportunity as EOpp
    po._ensemble_blender = mock.MagicMock()
    eo = EOpp(product_id="BTC-USD", direction=Direction.LONG,
                instrument_type=InstrumentType.SPOT, entry_price=100.0,
                stop_price=95.0, target_price=110.0, risk_reward=2.0,
                confidence=0.7, reason="r", strategy_name="x", score=0.7, meta={})
    po._ensemble_blender.blend_signals.return_value = [eo]
    opp = P.Opportunity(P.OpportunityType.STRATEGY_SIGNAL, "BTC", "BUY", 100, "r",
                         entry_price_est=100.0, stop_loss_pct=5.0, take_profit_pct=10.0)
    out = po._signal_ensemble_blend([opp])
    assert out[0].meta.get("ensemble_weight") is not None


def test_signal_ensemble_blend_no_blender(po):
    po._ensemble_blender = None
    opp = P.Opportunity(P.OpportunityType.STRATEGY_SIGNAL, "BTC", "BUY", 100, "r")
    assert po._signal_ensemble_blend([opp]) == [opp]


def test_meta_source_weights(po):
    opp = P.Opportunity(P.OpportunityType.STRATEGY_SIGNAL, "BTC", "BUY", 100, "r",
                         priority=0.8)
    opp.meta["source"] = "order_flow"
    po._update_meta_source_weights([opp])
    assert po._meta_source_weights.get("order_flow") is not None
    out = po._apply_meta_source_weights([opp])
    assert out[0].priority == 0.8  # weight ~1.0 initially


# ===========================================================================
# Cross-asset risk filter
# ===========================================================================

def test_apply_cross_asset_risk_filter_suppress(po):
    class Reg:
        regime = "risk_off"
        allows_new_longs = False
        risk_multiplier = 0.5
        trend_bias = 0.0
    po._cross_asset_regime = mock.MagicMock()
    po._cross_asset_regime.get_state.return_value = Reg()
    po._macro_risk = None
    buy = P.Opportunity(P.OpportunityType.STRATEGY_SIGNAL, "SOL", "BUY", 100, "r")
    sell = P.Opportunity(P.OpportunityType.STRATEGY_SIGNAL, "SOL", "SELL", 100, "r")
    out = po._apply_cross_asset_risk_filter([buy, sell])
    assert all(o.side != "BUY" for o in out)


def test_apply_cross_asset_risk_filter_empty(po):
    assert po._apply_cross_asset_risk_filter([]) == []


# ===========================================================================
# fetch_state
# ===========================================================================

def test_fetch_state(po):
    po.cli.get_balances.return_value = [
        {"currency": "BTC", "available_balance": {"value": 0.1},
         "hold": {"value": 0.0}},
        {"currency": "USDC", "available_balance": {"value": 80000.0},
         "hold": {"value": 0.0}},
    ]
    po.cli.get_products.return_value = {"BTC-USD": {"product_id": "BTC-USD"}}
    po.cli.get_fees.return_value = {"advanced_trade_only_volume": 5000.0}
    po.cli.get_price.return_value = {"price": 30000.0,
                                       "price_percentage_change_24h": 2.0,
                                       "volume_24h": 1_000_000.0}
    po.cli.get_fills.return_value = []
    po._fetch_state()
    assert po.state is not None
    assert "BTC" in po.state.holdings
    assert po.state.usdc_balance == 80000.0


def test_fetch_state_no_balances(po):
    po.cli.get_balances.side_effect = RuntimeError("down")
    po._fetch_state()
    assert po.state is not None
    assert po.state.total_value == 0.0


# ===========================================================================
# Pending approvals / execute_approved
# ===========================================================================

def test_check_pending_approvals_executes(po, tmp_path):
    po.require_approval = True
    po.pending_file = str(tmp_path / "pending.json")
    entry = {
        "status": "approved", "side": "BUY", "currency": "SOL",
        "size_usd": 100.0, "product_id": "SOL-USD", "reason": "r",
        "type": "strategy", "priority": 0.5,
    }
    with open(po.pending_file, "w") as f:
        json.dump({"tok": entry}, f)
    po.cli.preview_order.return_value = {"total_fee": 0.5}
    po.cli.create_order.return_value = {"id": "ord1"}
    po.state = make_state({"USDC": holding("USDC", 90000, "safe")}, total_value=100000)
    po._check_pending_approvals()
    assert "strategy" in po.last_execution
    assert len(po.trade_log) >= 1


def test_execute_approved_invalid(po):
    start = len(po.trade_log)
    po._execute_approved({"side": "BUY", "currency": "SOL", "size_usd": 0.0, "product_id": ""})
    assert len(po.trade_log) == start


# ===========================================================================
# process_opportunity / execute_with_bracket / record_trade
# ===========================================================================

def test_process_opportunity_static_skipped(po):
    po.state = make_state({"BTC": holding("BTC", 50000, "safe")}, total_value=100000)
    opp = P.Opportunity(P.OpportunityType.REBALANCE, "BTC", "BUY", 100, "r")
    before = len(po.trade_log)
    po._process_opportunity(opp)
    assert len(po.trade_log) == before  # static holding skipped


def test_record_trade_buy_updates_state(po):
    po.state = make_state({"USDC": holding("USDC", 90000, "safe")}, total_value=100000)
    opp = P.Opportunity(P.OpportunityType.STRATEGY_SIGNAL, "SOL", "BUY", 100, "r",
                         entry_price_est=100.0, product_id="SOL-USD")
    po._record_trade(opp, 1.0)
    assert po.state.usdc_balance < 90000.0
    assert "SOL" in po.state.holdings


def test_record_trade_tlh_pops_cost_basis(po):
    po.state = make_state({"SOL": holding("SOL", 1000, "growth")}, total_value=100000)
    po.cost_bases["SOL"] = 100.0
    opp = P.Opportunity(P.OpportunityType.TLH, "SOL", "SELL", 100, "r")
    po._record_trade(opp, 0.0)
    assert "SOL" not in po.cost_bases


def test_execute_with_bracket_dry_run(po):
    po.state = make_state({"USDC": holding("USDC", 90000, "safe")}, total_value=100000)
    po.dry_run = True
    opp = P.Opportunity(P.OpportunityType.STRATEGY_SIGNAL, "SOL", "BUY", 100, "r",
                         entry_price_est=100.0, stop_loss_pct=5.0, take_profit_pct=10.0,
                         product_id="SOL-USD")
    po._execute_with_bracket(opp, 1.0, is_quote=False)
    assert opp.executed is True
    assert opp.order_id == "dry-run-bracket"


# ===========================================================================
# Writers
# ===========================================================================

def test_write_trade_plans(po):
    opp = P.Opportunity(P.OpportunityType.TLH, "SOL", "SELL", 100, "r")
    opp.meta["trade_style"] = "tax_loss"
    with mock.patch.object(P, "open", mock.mock_open()) as m, \
         mock.patch.object(P.os, "replace") as rp:
        po._write_trade_plans([opp])
        assert m.called
        rp.assert_called()


def test_write_enhanced_state(po):
    po._meta_source_weights = {"order_flow": 0.8}
    po._param_opt_results = {"atr": {"best_params": {}}}
    po._wash_sale_cooldown = {"SOL": time.time()}
    po._cross_asset_regime = None
    po._ensemble_blender = None
    po._order_flow_engine = None
    with mock.patch.object(P, "open", mock.mock_open()) as m, \
         mock.patch.object(P.os, "replace"):
        po._write_enhanced_state()
        assert m.called


def test_write_signal_cache(po):
    opp = P.Opportunity(P.OpportunityType.STRATEGY_SIGNAL, "SOL", "BUY", 100, "r")
    opp.meta["trade_style"] = "momentum"
    with mock.patch.object(P, "open", mock.mock_open()) as m, \
         mock.patch.object(P.os, "replace"):
        po._write_signal_cache([opp])
        assert m.called


def test_event_signals_to_ops(po):
    class Sig:
        outcome = "BUY YES"
        position_size = 100.0
        reason = "r"
        confidence = 0.6
        platform = "kalshi"
        market_ticker = "ABC"
        market_question = "q"
        probability = 0.6
        signal_type = "event"
    ops = po._event_signals_to_ops([Sig()])
    assert len(ops) == 1
    assert ops[0].opp_type == P.OpportunityType.EVENT_MARKET


def test_normalize_product_id(po):
    po.cli.best_product.return_value = "SOL-USD"
    assert po._normalize_product_id("SOL", "BUY", "") == "SOL-USD"
    assert po._normalize_product_id("ZZZ", "BUY", "ZZZ-USD") == "ZZZ-USD"


# ===========================================================================
# Routing (multi-hop)
# ===========================================================================

def test_route_helpers(po):
    # With multi-hop available, context is built (not None unless disabled).
    ctx = po._route_context_for_opportunity(
        P.Opportunity(P.OpportunityType.STRATEGY_SIGNAL, "SOL", "BUY", 100, "r"))
    assert ctx is not None
    # best_route_decision needs products -> None when none available.
    assert po._best_route_decision_for_opportunity(
        P.Opportunity(P.OpportunityType.STRATEGY_SIGNAL, "SOL", "BUY", 100, "r")) is None
    assert po._route_amount_for_source("USDC", 100.0) == 100.0
    assert po._route_amount_for_source("SOL", 0.0) == 0.0


def test_route_decision_from_payload(po):
    payload = {
        "source": "USDC", "target": "SOL", "score": 0.5,
        "steps": [{"product_id": "SOL-USD", "from_currency": "USDC",
                   "to_currency": "SOL", "direction": "BUY", "price": 100.0,
                   "effective_rate": 0.01}],
        "fee_bps": 5.0, "spread_bps": 3.0,
    }
    dec = po._route_decision_from_payload(payload)
    assert dec is not None
    assert getattr(dec, "score", None) == 0.5


def test_execute_route_decision_dry_run(po):
    po._feed_mgr = None
    step = mock.MagicMock()
    step.product_id = "SOL-USD"
    step.direction = "BUY"
    step.effective_rate = 100.0
    plan = mock.MagicMock()
    plan.source = "USDC"
    plan.steps = [step]
    plan.path = ["USDC", "SOL"]
    dec = mock.MagicMock()
    dec.plan = plan
    dec.score = 0.5
    dec.factor_breakdown = {}
    po.cli.preview_order.return_value = {"total_fee": 0.1}
    po.dry_run = True
    opp = P.Opportunity(P.OpportunityType.STRATEGY_SIGNAL, "SOL", "BUY", 100, "r",
                         product_id="SOL-USD")
    assert po._execute_route_decision(opp, dec) is True
    assert opp.executed is True


# ===========================================================================
# Parameter optimization
# ===========================================================================

def test_apply_optimized_params_empty(po):
    po._param_opt_results = {}
    # should be a no-op
    po._apply_optimized_params()
    assert po._param_opt_results == {}


def test_run_periodic_param_optimization_no_data(po):
    po._feed_mgr = None
    po._last_param_opt_ts = 0
    with mock.patch.object(P, "_WalkForwardOptimizer", None), \
         mock.patch.object(P, "_HAS_WALK_FORWARD", False):
        assert po._run_periodic_param_optimization() == {}


def test_apply_optimized_params_applies(po):
    po._param_opt_results = {
        "stop": {"best_params": {"stop_atr_mult": 2.5}},
        "atr": {"best_params": {"atr_period": 14}},
    }
    po._apply_optimized_params()
    assert po._param_opt_results["_active_stop_mult"] == 2.5
    assert po._param_opt_results["_active_atr_period"] == 14


# ===========================================================================
# tick
# ===========================================================================

def test_tick(po, tmp_path):
    po.require_approval = False
    po.dry_run = True
    po._feed_mgr = None
    po.state = make_state({"USDC": holding("USDC", 90000, "safe")}, total_value=100000)
    po.cli.get_balances.return_value = [
        {"currency": "USDC", "available_balance": {"value": 90000.0}, "hold": {"value": 0.0}},
    ]
    po.cli.get_products.return_value = {}
    po.cli.get_fees.return_value = {"advanced_trade_only_volume": 0.0}
    po.cli.get_price.return_value = {"price": 1.0, "price_percentage_change_24h": 0, "volume_24h": 0}
    po.cli.get_fills.return_value = []
    po.cli.best_product.return_value = "SOL-USD"
    with mock.patch.object(P, "open", mock.mock_open()), \
         mock.patch.object(P.os, "replace"), \
         mock.patch.object(po, "_detect_opportunities", return_value=[]):
        po._tick()
    assert po._last_detected_opportunities == []


# ===========================================================================
# summary
# ===========================================================================

def test_summary(po):
    po.trade_log = [{"type": "tlh", "size_usd": 100.0, "fee": 0.5}]
    s = po.summary()
    assert s["total_trades"] == 1
    assert s["total_volume"] == 100.0
