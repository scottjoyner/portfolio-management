"""Second batch of coverage + critical-evaluation tests for portfolio_optimizer.py.

Heavy collaborators are mocked so the process exits cleanly.
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


@pytest.fixture
def po(opt):
    o = opt
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
# Event markets — arbitrage scanner + knowledge gap + exceptions
# ===========================================================================

class _FakeMarket:
    def __init__(self, **kw):
        self.category = kw.get("category", "crypto")
        self.platform = kw.get("platform", "kalshi")
        self.question = kw.get("question", "Will BTC hit $100k?")
        self.market_id = kw.get("market_id", "m1")
        self.volume = kw.get("volume", 5000.0)
        self.is_open = kw.get("is_open", True)
        self.probability_extremity = kw.get("probability_extremity", 0.8)
        self.mid_price = kw.get("mid_price", 0.8)
        self.liquidity_score = kw.get("liquidity_score", 0.9)
        self.spread = kw.get("spread", 0.02)


def _arb(**kw):
    leg_buy = mock.MagicMock(platform="kalshi", market_id="b", question="q",
                                outcome="YES", side="BUY", price=0.3)
    leg_hedge = mock.MagicMock(platform="polymarket", market_id="h",
                                 question="q", outcome="YES", side="SELL", price=0.4)
    a = mock.MagicMock(edge_pct=kw.get("edge_pct", 0.05), confidence=kw.get("confidence", 0.7),
                       platform_buy="kalshi", platform_hedge="polymarket",
                       leg_buy=leg_buy, leg_hedge=leg_hedge, edge=kw.get("edge", 0.1),
                       category="crypto", reason="arb", event_key="e1")
    return a


def test_event_markets_arb(po):
    po._pm_client = mock.MagicMock()
    po._pm_client.search_all_categories.return_value = {
        "crypto": [_FakeMarket()]
    }
    po._arb_scanner = mock.MagicMock()
    po._arb_scanner.scan.return_value = [_arb()]
    po._knowledge_gap = None
    po._feed_mgr = None
    po.state = make_state({"BTC": holding("BTC", 5000, "safe", price=30000)})
    ops = po._detect_event_markets()
    assert any(o.opp_type == P.OpportunityType.EVENT_ARBITRAGE for o in ops)


def test_event_markets_knowledge_gap_boost(po):
    m = _FakeMarket(mid_price=0.8, probability_extremity=0.9, volume=5000.0)
    po._pm_client = mock.MagicMock()
    po._pm_client.search_all_categories.return_value = {"crypto": [m]}
    po._arb_scanner = None
    kg = mock.MagicMock(is_significant=True, direction="overvalued", gap=0.3,
                         gap_pct=30.0, evidence_score=0.4, evidence_count=3,
                         sentiment_label="bearish", confidence=0.6,
                         sources_used=["wikipedia"])
    po._knowledge_gap = mock.MagicMock()
    po._knowledge_gap.analyze.return_value = kg
    po._feed_mgr = None
    po.state = make_state({"BTC": holding("BTC", 5000, "safe", price=30000)})
    ops = po._detect_event_markets()
    assert any(o.opp_type == P.OpportunityType.STRATEGY_SIGNAL for o in ops)
    arb = [o for o in ops if o.opp_type == P.OpportunityType.STRATEGY_SIGNAL][0]
    assert "knowledge_gap_direction" in arb.meta


def test_event_markets_exception(po):
    po._pm_client = mock.MagicMock()
    po._pm_client.search_all_categories.side_effect = RuntimeError("boom")
    po._arb_scanner = None
    po._knowledge_gap = None
    assert po._detect_event_markets() == []


# ===========================================================================
# Coinbase universe scan — branches
# ===========================================================================

def _candles(n=40, start=100.0, step=1.0):
    return [{"close": start + i * step, "high": start + i * step + 1,
             "low": start + i * step - 1, "volume": 1000.0} for i in range(n)]


def test_coinbase_universe_downtrend_sell(po):
    from datetime import datetime, timezone, timedelta
    candles = _candles(step=-1.0)  # declining -> SELL signal
    # Pre-seed listing age so the new-listing bonus does not mask the downtrend.
    po.store.set_meta("coinbase_first_seen:SOL-USD",
                      (datetime.now(timezone.utc) - timedelta(days=30)).isoformat())
    po._feed_mgr = mock.MagicMock()
    po._feed_mgr.get_candles_batch.return_value = {"SOL-USD": candles}
    po.state = make_state({"USDC": holding("USDC", 90000, "safe"),
                                 "SOL": holding("SOL", 1000, "growth", price=140.0)},
                                total_value=100000)
    po.cli.get_products.return_value = {
        "SOL-USD": {"trading_disabled": False, "volume_24h": 200_000_000.0}}
    po.cli.get_candles.return_value = candles
    with mock.patch.dict("sys.modules",
                          {"trading_system.core.compute_backend": mock.MagicMock()}):
        ops = po._detect_coinbase_universe_signals()
    assert any(o.side == "SELL" for o in ops)


def test_coinbase_universe_low_liquidity_skip(po):
    candles = _candles()
    po._feed_mgr = mock.MagicMock()
    po._feed_mgr.get_candles_batch.return_value = {"PEPE-USD": candles}
    po.state = make_state({"USDC": holding("USDC", 90000, "safe")}, total_value=100000)
    # speculative asset with tiny volume -> filtered out
    po.cli.get_products.return_value = {
        "PEPE-USD": {"trading_disabled": False, "volume_24h": 1_000_000.0}}
    po.cli.get_candles.return_value = candles
    with mock.patch.dict("sys.modules",
                          {"trading_system.core.compute_backend": mock.MagicMock()}):
        ops = po._detect_coinbase_universe_signals()
    assert not any(o.currency == "PEPE" for o in ops)


def test_coinbase_universe_cli_fallback(po):
    # No feed manager -> CLI candle fetch path.
    candles = _candles()
    po._feed_mgr = None
    po.state = make_state({"USDC": holding("USDC", 90000, "safe")}, total_value=100000)
    po.cli.get_products.return_value = {
        "SOL-USD": {"trading_disabled": False, "volume_24h": 200_000_000.0}}
    po.cli.get_candles.return_value = candles
    with mock.patch.dict("sys.modules",
                          {"trading_system.core.compute_backend": mock.MagicMock()}):
        ops = po._detect_coinbase_universe_signals()
    assert isinstance(ops, list)


# ===========================================================================
# Strategy signals — branches
# ===========================================================================

def test_strategy_signals_cli_path(po):
    po.state = make_state({"SOL": holding("SOL", 1000, "growth", price=140.0)},
                            total_value=100000)
    po.last_execution.clear()
    po._min_pulse_count = 1
    po._feed_mgr = None  # force CLI candle path
    po.cli.get_candles.return_value = _candles()
    sig = P.StrategySignal(strategy="ema_cross", action="BUY", confidence=0.6, reason="r")
    with mock.patch.object(P, "_run_strategies", return_value=[sig]), \
         mock.patch.object(P, "_batch_signals_fast", return_value={}), \
         mock.patch.object(P, "ConfidenceMatrix") as CM:
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
                winning_trades=7, losing_trades=3, reason="ok")}
        ops = po._detect_strategy_signals()
    assert any(o.currency == "SOL" and o.side == "BUY" for o in ops)


def test_strategy_signals_bt_cache_miss(po):
    po.state = make_state({"SOL": holding("SOL", 1000, "growth", price=140.0)},
                            total_value=100000)
    po.last_execution.clear()
    po._feed_mgr = mock.MagicMock()
    po._feed_mgr.get_candles_batch.return_value = {"SOL-USD": _candles()}
    sig = P.StrategySignal(strategy="ema_cross", action="BUY", confidence=0.6, reason="r")
    # No bt_cache entry -> verdict is None -> skipped.
    po._bt_cache = {}
    with mock.patch.object(P, "_run_strategies", return_value=[sig]), \
         mock.patch.object(P, "_batch_signals_fast", return_value={}):
        ops = po._detect_strategy_signals()
    assert not any(o.currency == "SOL" for o in ops)


def test_strategy_signals_pulse_invalid(po):
    po.state = make_state({"SOL": holding("SOL", 1000, "growth", price=140.0)},
                            total_value=100000)
    po.last_execution.clear()
    po._min_pulse_count = 5  # require 5 pulses -> recorded 1 is invalid
    po._feed_mgr = mock.MagicMock()
    po._feed_mgr.get_candles_batch.return_value = {"SOL-USD": _candles()}
    sig = P.StrategySignal(strategy="ema_cross", action="BUY", confidence=0.6, reason="r")
    with mock.patch.object(P, "_run_strategies", return_value=[sig]), \
         mock.patch.object(P, "_batch_signals_fast", return_value={}), \
         mock.patch.object(P, "ConfidenceMatrix") as CM:
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
                winning_trades=7, losing_trades=3, reason="ok")}
        ops = po._detect_strategy_signals()
    assert not any(o.currency == "SOL" for o in ops)


# ===========================================================================
# Funding / on-chain — extra branches
# ===========================================================================

def test_funding_sell(po):
    po.last_execution.clear()
    po.state = make_state({"BTC": holding("BTC", 5000, "safe", price=30000)})
    po.cli.get_price.return_value = {"price": 30000.0}
    f = mock.MagicMock(action="SELL", confidence=0.6, reason="funding")
    po._funding_contrarian.on_bar = mock.MagicMock(return_value=f)
    po._onchain_flow.get_signals = mock.MagicMock(return_value=[])
    ops = po._detect_funding_and_onchain_signals()
    assert any(o.currency == "BTC" and o.side == "SELL" for o in ops)


def test_onchain_sell(po):
    po.last_execution.clear()
    po._funding_contrarian.on_bar = mock.MagicMock(return_value=None)
    po.state = make_state({"SOL": holding("SOL", 1000, "growth", price=100)})
    po._onchain_flow.get_signals = mock.MagicMock(return_value=[{
        "action": "SELL", "product_id": "SOL-USD", "currency": "SOL",
        "confidence": 0.6, "price": 100.0, "volume_anomaly": 2.0,
        "price_trend": -0.1}])
    ops = po._detect_funding_and_onchain_signals()
    assert any(o.currency == "SOL" and o.side == "SELL" for o in ops)


def test_funding_contrarian_exception(po):
    po.last_execution.clear()
    po.state = make_state({"BTC": holding("BTC", 5000, "safe", price=30000)})
    po.cli.get_price.return_value = {"price": 30000.0}
    po._funding_contrarian.on_bar = mock.MagicMock(side_effect=RuntimeError("x"))
    po._onchain_flow.get_signals = mock.MagicMock(return_value=[])
    assert po._detect_funding_and_onchain_signals() == []


# ===========================================================================
# Order flow — smart money + edge branches
# ===========================================================================

def test_order_flow_no_engine(po):
    po.last_execution.clear()
    po._order_flow_engine = None
    po._smart_money_flow = None
    po._feed_mgr = None
    po.state = make_state({"SOL": holding("SOL", 1000, "growth", price=100)})
    assert po._detect_order_flow_signals() == []


def test_order_flow_smart_money(po):
    po.last_execution.clear()
    po._order_flow_engine = mock.MagicMock()
    po._order_flow_engine.evaluate = mock.MagicMock(return_value=None)
    sm = mock.MagicMock()
    sm.direction = mock.MagicMock(value="long")
    sm.confidence = 0.6
    sm.reason = "cvd_divergence"
    sm.entry_price = 100.0
    po._smart_money_flow = mock.MagicMock()
    po._smart_money_flow.on_bar.return_value = sm
    po._feed_mgr = mock.MagicMock()
    po._feed_mgr.get_candles_batch.return_value = {"SOL-USD": _candles()}
    po.state = make_state({"SOL": holding("SOL", 1000, "growth", price=100,
                                        spread=0.004, volume_24h=1000.0)})
    ops = po._detect_order_flow_signals()
    assert any(o.side == "BUY" for o in ops)


# ===========================================================================
# Cross-asset risk filter — macro + keep paths
# ===========================================================================

class _Reg:
    regime = "neutral"
    allows_new_longs = True
    risk_multiplier = 1.0
    trend_bias = 0.0


def test_cross_asset_macro_penalty(po):
    po._cross_asset_regime = None
    macro = mock.MagicMock()
    macro.macro_score = 2.0  # extreme risk-off
    po._macro_risk = mock.MagicMock()
    po._macro_risk.get_signal.return_value = macro
    buy = P.Opportunity(P.OpportunityType.STRATEGY_SIGNAL, "SOL", "BUY", 100, "r",
                         priority=0.8)
    out = po._apply_cross_asset_risk_filter([buy])
    assert out[0].priority < 0.8


def test_cross_asset_keep_buy(po):
    po._cross_asset_regime = mock.MagicMock()
    po._cross_asset_regime.get_state.return_value = _Reg()
    po._macro_risk = None
    buy = P.Opportunity(P.OpportunityType.STRATEGY_SIGNAL, "SOL", "BUY", 100, "r",
                         priority=0.8)
    out = po._apply_cross_asset_risk_filter([buy])
    assert any(o.side == "BUY" for o in out)


# ===========================================================================
# execute_with_bracket — exec-engine branches
# ===========================================================================

def _bracket_opp(**kw):
    kw.setdefault("size_usd", 100.0)
    size = kw.pop("size_usd")
    return P.Opportunity(P.OpportunityType.STRATEGY_SIGNAL, "SOL", "BUY", size, "r",
                          entry_price_est=100.0, stop_loss_pct=5.0, take_profit_pct=10.0,
                          product_id="SOL-USD", **kw)


def test_execute_with_bracket_preview_fail(po):
    po._exec_engine = mock.MagicMock()
    bad = mock.MagicMock(success=False, error="nope", raw={})
    po._exec_engine._preview.return_value = bad
    before = len(po.trade_log)
    po._execute_with_bracket(_bracket_opp(), 1.0, is_quote=False)
    assert len(po.trade_log) == before


def test_execute_with_bracket_fee_too_high(po):
    po._exec_engine = mock.MagicMock()
    good = mock.MagicMock(success=True, raw={"preview": {"total_fee": 500.0}})
    po._exec_engine._preview.return_value = good
    before = len(po.trade_log)
    po._execute_with_bracket(_bracket_opp(size_usd=100.0), 1.0, is_quote=False)
    assert len(po.trade_log) == before


def test_execute_with_bracket_approval_live(po, tmp_path):
    po.require_approval = True
    po.dry_run = False
    po.pending_file = str(tmp_path / "pending.json")
    po._exec_engine = mock.MagicMock()
    good = mock.MagicMock(success=True, raw={"preview": {"total_fee": 0.5}})
    po._exec_engine._preview.return_value = good
    po._execute_with_bracket(_bracket_opp(), 1.0, is_quote=False)
    assert os.path.exists(po.pending_file)


def test_execute_with_bracket_live_success(po):
    po.dry_run = False
    po.require_approval = False
    po._exec_engine = mock.MagicMock()
    good = mock.MagicMock(success=True, raw={"preview": {"total_fee": 0.5}})
    po._exec_engine._preview.return_value = good
    po._bracket_mgr = mock.MagicMock()
    po._bracket_mgr.place_bracket.return_value = {"status": "OPEN", "bracket_id": "b1"}
    po._execute_with_bracket(_bracket_opp(), 1.0, is_quote=False)
    assert po.trade_log  # recorded


def test_execute_with_bracket_live_failure(po):
    po.dry_run = False
    po.require_approval = False
    po._exec_engine = mock.MagicMock()
    good = mock.MagicMock(success=True, raw={"preview": {"total_fee": 0.5}})
    po._exec_engine._preview.return_value = good
    po._bracket_mgr = mock.MagicMock()
    po._bracket_mgr.place_bracket.return_value = {
        "status": "failure", "entry_result": {"success": False, "error": "x"}}
    po._bracket_mgr.force_flatten_bracket = mock.MagicMock()
    before = len(po.trade_log)
    po._execute_with_bracket(_bracket_opp(), 1.0, is_quote=False)
    assert len(po.trade_log) == before


# ===========================================================================
# Routing — live multi-hop
# ===========================================================================

def test_process_opportunity_route_dry(po):
    po.dry_run = True
    po.require_approval = False
    po._feed_mgr = None
    po.state = make_state({"USDC": holding("USDC", 90000, "safe")}, total_value=100000)
    step = mock.MagicMock()
    step.product_id = "SOL-USD"
    step.direction = "BUY"
    step.effective_rate = 100.0
    plan = mock.MagicMock()
    plan.source = "USDC"
    plan.target = "SOL"
    plan.steps = [step]
    plan.path = ["USDC", "SOL"]
    dec = mock.MagicMock()
    dec.plan = plan
    dec.score = 0.5
    dec.factor_breakdown = {}
    po.cli.preview_order.return_value = {"total_fee": 0.1}
    with mock.patch.object(P.PortfolioOptimizer, "_best_route_decision_for_opportunity", return_value=dec):
        opp = _bracket_opp()
        po._process_opportunity(opp)
    assert opp.executed is True


def test_process_opportunity_approval_gate(po, tmp_path):
    po.dry_run = False
    po.require_approval = True
    po.pending_file = str(tmp_path / "pending.json")
    po._feed_mgr = None
    po.cli.preview_order.return_value = {"total_fee": 0.5}
    with mock.patch.object(P.PortfolioOptimizer, "_best_route_decision_for_opportunity", return_value=None):
        opp = P.Opportunity(P.OpportunityType.STRATEGY_SIGNAL, "SOL", "BUY", 100, "r",
                              entry_price_est=100.0, product_id="SOL-USD")
        po.state = make_state({"USDC": holding("USDC", 90000, "safe")}, total_value=100000)
        po._process_opportunity(opp)
    assert os.path.exists(po.pending_file)


def test_process_opportunity_preview_fail(po):
    po.dry_run = True
    po.require_approval = False
    po._feed_mgr = None
    po._bracket_mgr = None
    po.cli.preview_order.return_value = None
    with mock.patch.object(P.PortfolioOptimizer, "_best_route_decision_for_opportunity", return_value=None):
        opp = P.Opportunity(P.OpportunityType.STRATEGY_SIGNAL, "SOL", "BUY", 100, "r",
                              entry_price_est=100.0, product_id="SOL-USD")
        po.state = make_state({"USDC": holding("USDC", 90000, "safe")}, total_value=100000)
        before = len(po.trade_log)
        po._process_opportunity(opp)
        assert len(po.trade_log) == before


def test_process_opportunity_live_execute(po):
    po.dry_run = False
    po.require_approval = False
    po._feed_mgr = None
    po._bracket_mgr = None
    po.cli.preview_order.return_value = {"total_fee": 0.5}
    po.cli.create_order.return_value = {"id": "ord1"}
    with mock.patch.object(P.PortfolioOptimizer, "_best_route_decision_for_opportunity", return_value=None):
        opp = P.Opportunity(P.OpportunityType.STRATEGY_SIGNAL, "SOL", "BUY", 100, "r",
                              entry_price_est=100.0, product_id="SOL-USD")
        po.state = make_state({"USDC": holding("USDC", 90000, "safe")}, total_value=100000)
        po._process_opportunity(opp)
    assert opp.executed is True


# ===========================================================================
# execute_approved — route + bracket + live
# ===========================================================================

def test_execute_approved_route(po):
    po._feed_mgr = None
    payload = {
        "source": "USDC", "target": "SOL", "score": 0.5,
        "steps": [{"product_id": "SOL-USD", "from_currency": "USDC",
                   "to_currency": "SOL", "direction": "BUY", "price": 100.0,
                   "effective_rate": 0.01}],
        "fee_bps": 5.0, "spread_bps": 3.0,
    }
    entry = {"side": "BUY", "currency": "SOL", "size_usd": 100.0,
             "product_id": "SOL-USD", "reason": "r", "type": "strategy",
             "route_decision": payload}
    step = mock.MagicMock()
    step.product_id = "SOL-USD"
    step.direction = "BUY"
    step.effective_rate = 100.0
    plan = mock.MagicMock()
    plan.source = "USDC"
    plan.target = "SOL"
    plan.steps = [step]
    plan.path = ["USDC", "SOL"]
    dec = mock.MagicMock()
    dec.plan = plan
    dec.score = 0.5
    dec.factor_breakdown = {}
    with mock.patch.object(P.PortfolioOptimizer, "_route_decision_from_payload", return_value=dec), \
         mock.patch.object(P.PortfolioOptimizer, "_best_route_decision_for_opportunity", return_value=dec):
        po._execute_approved(entry)
    assert po.last_execution.get("strategy")


def test_execute_approved_bracket(po):
    po.dry_run = False
    po.require_approval = False
    po._bracket_mgr = mock.MagicMock()
    po._bracket_mgr.place_bracket.return_value = {"status": "OPEN", "bracket_id": "b1"}
    entry = {"side": "BUY", "currency": "SOL", "size_usd": 100.0,
             "product_id": "SOL-USD", "reason": "r", "type": "strategy",
             "bracket": True, "stop_price": 95.0, "target_price": 110.0,
             "entry_price_est": 100.0, "base_qty": 1.0}
    po._execute_approved(entry)
    assert po.trade_log


def test_execute_approved_live_direct(po):
    po.dry_run = False
    po.require_approval = False
    po._bracket_mgr = None
    po.cli.preview_order.return_value = {"total_fee": 0.5}
    po.cli.create_order.return_value = {"id": "ord1"}
    entry = {"side": "BUY", "currency": "SOL", "size_usd": 100.0,
             "product_id": "SOL-USD", "reason": "r", "type": "strategy"}
    po.state = make_state({"USDC": holding("USDC", 90000, "safe")}, total_value=100000)
    po._execute_approved(entry)
    assert po.trade_log


# ===========================================================================
# Writers — S/R levels + ensemble state
# ===========================================================================

def test_write_enhanced_state_sr(po):
    po._meta_source_weights = {"order_flow": 0.8}
    po._param_opt_results = {}
    po._wash_sale_cooldown = {}
    po._cross_asset_regime = None
    po._ensemble_blender = None
    po._order_flow_engine = None
    po._feed_mgr = mock.MagicMock()
    po._feed_mgr.get_candles_batch.return_value = {"SOL-USD": _candles()}
    po._tick_count = 20  # triggers S/R branch
    po.state = make_state({"SOL": holding("SOL", 1000, "growth", price=140.0)},
                            total_value=100000)
    with mock.patch.object(P, "open", mock.mock_open()) as m, \
         mock.patch.object(P.os, "replace"):
        po._write_enhanced_state()
        assert m.called


def test_write_enhanced_state_ensemble(po):
    po._meta_source_weights = {}
    po._param_opt_results = {}
    po._wash_sale_cooldown = {}
    po._cross_asset_regime = None
    po._order_flow_engine = None
    po._feed_mgr = None
    po._ensemble_blender = mock.MagicMock()
    po._ensemble_blender.to_dict.return_value = {"a": 1}
    po._ensemble_blender.top_strategies.return_value = ["ema_cross"]
    with mock.patch.object(P, "open", mock.mock_open()) as m, \
         mock.patch.object(P.os, "replace"):
        po._write_enhanced_state()
        assert m.called


# ===========================================================================
# Parameter optimization — with feed data
# ===========================================================================

def test_run_param_opt_with_data(po):
    po._feed_mgr = mock.MagicMock()
    closes = [float(100 + i) for i in range(300)]
    po._feed_mgr.get_candles_batch.return_value = {
        "BTC-USD": [[i, i - 1, i + 1, i, i] for i in closes]}
    wf = mock.MagicMock()
    wf.return_value.optimize.return_value = {
        "best_overall": {"atr_period": 14},
        "windows": [mock.MagicMock(train_score=1.0, test_score=0.5)]}
    with mock.patch.object(P, "_WalkForwardOptimizer", wf), \
         mock.patch.object(P, "_HAS_WALK_FORWARD", True):
        po._last_param_opt_ts = 0
        res = po._run_periodic_param_optimization()
    assert "atr" in res


# ===========================================================================
# stop()
# ===========================================================================

def test_stop(po):
    po.stop()  # should not raise
    assert po.running is False
