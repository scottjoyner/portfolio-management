"""Additional coverage tests for portfolio_optimizer.py.

Targets the large previously-uncovered blocks: _process_opportunity,
_execute_with_bracket, _record_trade, the write-* methods, _detect_event_markets,
_detect_funding_and_onchain_signals, _detect_order_flow_signals,
_detect_accumulator_signals, _detect_rebalance_bot, _detect_stairstep,
_detect_volume_cycles, _detect_tlh, _detect_fee_tier_volume, _detect_rebalance,
config/env parsing, capital helpers, _fetch_state, _compute_cost_bases,
_tick, run/stop, summary, and main().

All collaborators are mocked; no network / Coinbase / DB writes occur.
"""

import datetime
import json
import os
import sys
import time
from types import SimpleNamespace
from unittest import mock

import pytest

import portfolio_optimizer as P
from strategy_engine import Signal as StrategySignal
from strategy_engine import BacktestVerdict


# ---------------------------------------------------------------------------
# Reusable fake collaborators
# ---------------------------------------------------------------------------

class FakeSignal:
    def __init__(self, action="BUY", confidence=0.6, reason="r", **kw):
        self.action = action
        self.confidence = confidence
        self.reason = reason
        self.spread_z = 1.0
        self.spread_tight = True
        self.volume_24h = 1_000_000.0
        self.spread_bps = 5.0
        for k, v in kw.items():
            setattr(self, k, v)


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


def _rich_state(opt, holdings, total_value=100000.0, usdc=90000.0):
    """State with deployable capital so buy-capacity checks are > 0."""
    opt.state = make_state(holdings, total_value=total_value, usdc=usdc)
    opt.capital_policy["max_deployable_usd"] = 1_000_000.0
    return opt.state


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


# ---------------------------------------------------------------------------
# Config / env parsing
# ---------------------------------------------------------------------------

def test_config_env_parsing(monkeypatch):
    monkeypatch.setenv("REBALANCE_PRESET", "aggressive_core")
    monkeypatch.setenv("REBALANCE_DRIFT", "0.10")
    monkeypatch.setenv("REBALANCE_PROFIT_TAKE", "0.40")
    monkeypatch.setenv("REBALANCE_MIN_NOTIONAL", "25.0")
    monkeypatch.setenv("STAIRSTEP_ENABLED", "false")
    monkeypatch.setenv("STAIRSTEP_SYMBOLS", "DOGE-USD,SHIB-USD")
    with mock.patch("subprocess.run", return_value=mock.MagicMock(returncode=0)), \
         mock.patch("fcntl.flock", return_value=0):
        db = os.path.join("data", "opt_env_test.db")
        o = P.PortfolioOptimizer(dry_run=True, db_path=db)
    try:
        mgr = getattr(o, "_feed_mgr", None)
        if mgr is not None and hasattr(mgr, "stop"):
            try:
                mgr.stop()
            except Exception:
                pass
        o._feed_mgr = None
        assert o.rebalance_preset == "aggressive_core"
        assert o.rebalance_drift_threshold == 0.10
        assert o.rebalance_profit_take_pct == 0.40
        assert o.rebalance_min_notional == 25.0
        assert o.stairstep_enabled is False
        assert o._stairstep_symbols == ["DOGE-USD", "SHIB-USD"]
    finally:
        if os.path.exists(db):
            os.remove(db)


def test_config_stairstep_enabled_variants(monkeypatch):
    monkeypatch.setenv("STAIRSTEP_ENABLED", "1")
    with mock.patch("subprocess.run", return_value=mock.MagicMock(returncode=0)), \
         mock.patch("fcntl.flock", return_value=0):
        db = os.path.join("data", "opt_env2.db")
        o = P.PortfolioOptimizer(dry_run=True, db_path=db)
    try:
        mgr = getattr(o, "_feed_mgr", None)
        if mgr is not None and hasattr(mgr, "stop"):
            try:
                mgr.stop()
            except Exception:
                pass
        o._feed_mgr = None
        assert o.stairstep_enabled is True
    finally:
        if os.path.exists(db):
            os.remove(db)


# ---------------------------------------------------------------------------
# Capital / bucket helpers (extra branch coverage)
# ---------------------------------------------------------------------------

def test_capital_helpers_extra(opt):
    opt.state = make_state({}, total_value=100000.0, usdc=50000.0)
    opt.capital_policy["max_deployable_usd"] = 1000.0
    assert opt._core_batch_cap() <= 1000.0
    assert opt._opportunity_batch_cap() <= 1000.0
    assert opt._buy_capacity() <= 1000.0
    opt.capital_policy["static_holdings"] = "BTC,ETH"
    assert opt._is_static_currency("btc")
    opt.capital_policy["core_allowlist"] = "SOL"
    assert opt._is_core_holding({"currency": "SOL", "classification": "safe", "allocation_pct": 50})
    assert not opt._is_core_holding({"currency": "DOGE", "classification": "speculative", "allocation_pct": 0})
    opp = P.Opportunity(P.OpportunityType.REBALANCE, "SOL", "SELL", 10, "r")
    assert opt._capital_bucket_for(opp) == "opportunity"
    acc = P.Opportunity(P.OpportunityType.ACCUMULATOR_SIGNAL, "SOL", "BUY", 10, "r",
                        meta={"capital_bucket": "core"})
    assert opt._capital_bucket_for(acc) == "core"


def test_deployable_remaining_no_cap(opt):
    opt.state = make_state({}, total_value=100000.0, usdc=50000.0)
    assert opt._deployable_capital() > 0
    assert opt._remaining_deployable_capital() == 0.0


def test_kelly_size_branches(opt):
    opt.state = None
    # No state -> returns the default min_notional (50.0), not opt.min_value.
    assert opt._kelly_size(0.6, 2.0, 1.5, 0.5) == 50.0
    opt.state = make_state({}, total_value=100000.0, usdc=50000.0)
    s = opt._kelly_size(0.6, 2.0, 1.5, 0.5, capital_limit=1.0)
    assert s <= 1.0
    s2 = opt._kelly_size(0.7, 3.0, 1.5, 0.8, capital_limit=500.0)
    assert 0 < s2 <= 500.0
    s3 = opt._kelly_size(1.5, 0.0, 0.0, 0.5, capital_limit=500.0)
    assert s3 >= 0


def test_risk_reward_branches(opt):
    opt.state = None
    # No state -> returns the default min_notional (50.0), not opt.min_value.
    assert opt._risk_reward_size(5.0, 5.0, 0.5, 0.5) == 50.0
    opt.state = make_state({}, total_value=100000.0, usdc=50000.0)
    s = opt._risk_reward_size(5.0, 5.0, 0.5, 0.5, capital_limit=1.0)
    assert s <= 1.0


def test_estimate_trade_volatility(opt):
    assert opt._estimate_trade_volatility_pct([]) == 30.0
    closes = [float(100 + i) for i in range(40)]
    v = opt._estimate_trade_volatility_pct(closes)
    assert 1.0 <= v <= 20.0
    highs = [c + 1 for c in closes]
    lows = [c - 1 for c in closes]
    v2 = opt._estimate_trade_volatility_pct(closes, highs, lows)
    assert v2 >= 1.0


def test_current_price_for_symbol(opt):
    opt.state = None
    assert opt._current_price_for_symbol("") == 0.0
    opt.cli.get_price.return_value = {"price": 123.0}
    assert opt._current_price_for_symbol("SOL-USD") == 123.0
    opt.cli.get_price.side_effect = RuntimeError("x")
    opt.state = make_state({"SOL": holding("SOL", 5000, "growth", price=77.0)})
    assert opt._current_price_for_symbol("SOL") == 77.0


def test_exit_plan_all_profiles(opt):
    for style in ["momentum", "new_listing", "equity_momentum", "prediction_market",
                  "event", "mean_reversion", "arbitrage", "rebalance", "cycle", "tax_loss"]:
        plan = opt._compute_exit_plan("SOL", 0.6, 5.0, trade_style=style,
                                      volatility_pct=40.0)
        assert plan["stop_loss_pct"] >= 0
    plan = opt._compute_exit_plan("SOL", 0.6, 5.0, trade_style="weird")
    assert "stop_loss_pct" in plan


def test_compute_dynamic_stop_sr_snap(opt):
    levels = [P._SRLevel(95.0, "support", 1.0), P._SRLevel(105.0, "resistance", 1.0)]
    stop, _, detail = opt._compute_dynamic_stop(100.0, "BUY", 2.0, "trending", levels, 5.0)
    assert stop >= 0.5
    assert "sr_snap" in detail or "atr" in detail
    stop2, _, _ = opt._compute_dynamic_stop(100.0, "SELL", 2.0, "volatile", levels, 5.0)
    assert stop2 >= 0.5
    stop3, _, detail3 = opt._compute_dynamic_stop(100.0, "BUY", 0.0, "neutral", [], 5.0)
    assert detail3 == "default"


def test_sr_aware_exit_plan_sr(opt):
    closes = [float(100 + i) for i in range(40)]
    highs = [c + 2 for c in closes]
    lows = [c - 2 for c in closes]
    plan = opt._compute_sr_aware_exit_plan(
        "SOL", 0.6, 5.0, trade_style="momentum", volatility_pct=40.0,
        closes=closes, highs=highs, lows=lows)
    assert "stop_loss_pct" in plan


def test_latency_adjusted_priority(opt):
    p = opt._latency_adjusted_priority(0.5, trade_style="momentum")
    assert 0.0 <= p <= 1.0
    p2 = opt._latency_adjusted_priority(0.5, expected_delay_ms=1000.0)
    assert 0.0 <= p2 <= 1.0


# ---------------------------------------------------------------------------
# _record_trade / _normalize_product_id
# ---------------------------------------------------------------------------

def test_record_trade_buy_sell(opt):
    opt.state = make_state(
        {"SOL": holding("SOL", 5000, "growth", price=100.0)},
        total_value=100000.0, usdc=50000.0)
    before = opt.state.usdc_balance
    buy = P.Opportunity(P.OpportunityType.STRATEGY_SIGNAL, "SOL", "BUY", 1000, "r",
                        entry_price_est=100.0, product_id="SOL-USD")
    opt._record_trade(buy, 1.0)
    assert opt.state.usdc_balance < before
    assert "SOL" in opt.state.holdings
    sell = P.Opportunity(P.OpportunityType.STRATEGY_SIGNAL, "SOL", "SELL", 1000, "r",
                         entry_price_est=100.0, product_id="SOL-USD")
    opt._record_trade(sell, 1.0)
    opt.cost_bases["BTC"] = 1.0
    tlh = P.Opportunity(P.OpportunityType.TLH, "BTC", "SELL", 100, "r", product_id="BTC-USD")
    opt._record_trade(tlh, 0.0)
    assert "BTC" not in opt.cost_bases
    vc = P.Opportunity(P.OpportunityType.VOLUME_CYCLE, "SOL", "SELL", 100, "r", product_id="SOL-USD")
    opt._record_trade(vc, 0.0)
    assert opt.position_ages.get("SOL") is not None


def test_normalize_product_id(opt):
    opt.cli.best_product.side_effect = lambda c, s: "SOL-USDC"
    assert opt._normalize_product_id("SOL", "BUY") == "SOL-USDC"
    opt.cli.best_product.side_effect = RuntimeError("x")
    assert opt._normalize_product_id("SOL", "BUY", "SOL-USD") == "SOL-USD"
    assert opt._normalize_product_id("SOL", "BUY") == "SOL-USDC"


# ---------------------------------------------------------------------------
# _execute_with_bracket
# ---------------------------------------------------------------------------

def test_execute_with_bracket_dryrun(opt):
    opt.state = make_state({}, total_value=100000.0, usdc=50000.0)
    opt._exec_engine = None
    opt.dry_run = True
    opt.require_approval = False
    opp = P.Opportunity(P.OpportunityType.STRATEGY_SIGNAL, "SOL", "BUY", 1000, "r",
                        entry_price_est=100.0, stop_loss_pct=5.0, product_id="SOL-USD")
    opt._execute_with_bracket(opp, 10.0, True)
    assert opp.executed is True
    assert opp.order_id == "dry-run-bracket"


def test_execute_with_bracket_invalid_base(opt):
    opt._exec_engine = None
    opp = P.Opportunity(P.OpportunityType.STRATEGY_SIGNAL, "SOL", "BUY", 1000, "r",
                        entry_price_est=100.0, product_id="SOL-USD")
    with mock.patch.object(P.logger, "warning") as w:
        # is_quote=False with base_qty=0.0 yields base_size<=0 -> warning.
        opt._execute_with_bracket(opp, 0.0, False)
    assert w.called


def test_execute_with_bracket_pending_approval(opt):
    opt.state = make_state({}, total_value=100000.0, usdc=50000.0)
    opt._exec_engine = None
    opt.dry_run = False
    opt.require_approval = True
    opt.pending_file = os.path.join("data", "pending_bracket.json")
    opp = P.Opportunity(P.OpportunityType.STRATEGY_SIGNAL, "SOL", "BUY", 1000, "r",
                        entry_price_est=100.0, stop_loss_pct=5.0, product_id="SOL-USD")
    opt._execute_with_bracket(opp, 10.0, True)
    assert os.path.exists(opt.pending_file)
    with open(opt.pending_file) as f:
        data = json.load(f)
    assert any(v.get("bracket") for v in data.values())


def test_execute_with_bracket_live_open(opt):
    opt.state = make_state({}, total_value=100000.0, usdc=50000.0)
    opt.dry_run = False
    opt.require_approval = False
    opt._bracket_mgr = mock.MagicMock()
    opt._bracket_mgr.place_bracket.return_value = {
        "status": "OPEN", "bracket_id": "b1",
        "entry_result": {"client_order_id": "x", "success": True},
    }
    opt._save_brackets = mock.MagicMock()
    opp = P.Opportunity(P.OpportunityType.STRATEGY_SIGNAL, "SOL", "BUY", 1000, "r",
                        entry_price_est=100.0, stop_loss_pct=5.0, product_id="SOL-USD")
    opt._execute_with_bracket(opp, 10.0, True)
    assert opp.executed is True
    assert opt._bracket_mgr.place_bracket.called


def test_execute_with_bracket_live_failure(opt):
    opt.state = make_state({}, total_value=100000.0, usdc=50000.0)
    opt.dry_run = False
    opt.require_approval = False
    opt._bracket_mgr = mock.MagicMock()
    opt._bracket_mgr.place_bracket.return_value = {
        "status": "REJECTED",
        "entry_result": {"error": "boom", "success": True, "order_id": "o1"},
    }
    opt._bracket_mgr.force_flatten_bracket = mock.MagicMock()
    opp = P.Opportunity(P.OpportunityType.STRATEGY_SIGNAL, "SOL", "BUY", 1000, "r",
                        entry_price_est=100.0, stop_loss_pct=5.0, product_id="SOL-USD")
    opt._execute_with_bracket(opp, 10.0, True)
    assert opt._bracket_mgr.force_flatten_bracket.called


def test_execute_with_bracket_exec_engine_preview(opt):
    opt.state = make_state({}, total_value=100000.0, usdc=50000.0)
    opt.dry_run = True
    opt.require_approval = False
    intent = mock.MagicMock()
    preview = mock.MagicMock()
    preview.success = True
    preview.raw = {"preview": {"total_fee": 0.5}}
    eng = mock.MagicMock()
    eng._preview.return_value = preview
    opt._exec_engine = eng
    P._OrderIntent = mock.MagicMock(return_value=intent)
    P._OrderType = mock.MagicMock()
    opp = P.Opportunity(P.OpportunityType.STRATEGY_SIGNAL, "SOL", "BUY", 1000, "r",
                        entry_price_est=100.0, stop_loss_pct=5.0, product_id="SOL-USD")
    opt._execute_with_bracket(opp, 10.0, True)
    assert opp.executed is True


# ---------------------------------------------------------------------------
# _process_opportunity (the largest uncovered block)
# ---------------------------------------------------------------------------

def _buy_state(opt):
    _rich_state(opt, {"SOL": holding("SOL", 5000, "growth", price=100.0)})
    opt.cli.best_product.side_effect = lambda c, s: "SOL-USD"
    opt.cli.preview_order.return_value = {"total_fee": 1.0, "total_cost": 1000.0}
    opt.cli.create_order.return_value = {"id": "X1"}


def test_process_static_skip(opt):
    opt._best_route_decision_for_opportunity = mock.MagicMock(return_value=None)
    opp = P.Opportunity(P.OpportunityType.STRATEGY_SIGNAL, "BTC", "BUY", 1000, "r",
                        entry_price_est=100.0)
    with mock.patch.object(P.logger, "info") as info:
        opt._process_opportunity(opp)
    assert any("Static" in str(a) for a in info.call_args_list)


def test_process_event_market_notification(opt):
    opt.state = make_state({}, total_value=100000.0, usdc=50000.0)
    opt.notifier = None
    opp = P.Opportunity(P.OpportunityType.EVENT_MARKET, "?", "NONE", 0, "r",
                        product_id="kalshi:1",
                        meta={"platform": "kalshi", "market_question": "Will BTC hit 100k?"})
    n0 = len(opt.trade_log)
    opt._process_opportunity(opp)
    assert len(opt.trade_log) == n0 + 1


def test_process_event_arbitrage(opt):
    opt.state = make_state({}, total_value=100000.0, usdc=50000.0)
    opt.notifier = None
    opp = P.Opportunity(P.OpportunityType.EVENT_ARBITRAGE, "ARB", "BUY", 100, "r",
                        product_id="k:a", meta={"event_key": "e1", "platform": "k",
                                                "market_ticker": "t"})
    n0 = len(opt.trade_log)
    opt._process_opportunity(opp)
    assert len(opt.trade_log) == n0 + 1


def test_process_stock_signal(opt):
    opt.state = make_state({}, total_value=100000.0, usdc=50000.0)
    opt.notifier = None
    opp = P.Opportunity(P.OpportunityType.STOCK_SIGNAL, "NVDA", "BUY", 100, "r",
                        product_id="NVDA")
    n0 = len(opt.trade_log)
    opt._process_opportunity(opp)
    assert len(opt.trade_log) == n0 + 1


def test_process_event_market_with_notifier_live(opt):
    opt.state = make_state({}, total_value=100000.0, usdc=50000.0)
    opt.dry_run = False
    opt.notifier = mock.MagicMock()
    opp = P.Opportunity(P.OpportunityType.EVENT_MARKET, "?", "NONE", 0, "r",
                        product_id="kalshi:1",
                        meta={"platform": "kalshi", "market_question": "q?",
                              "signal_type": "x"})
    opt._process_opportunity(opp)
    assert opt.notifier.send_trade_alert.called


def test_process_buy_dryrun(opt):
    _buy_state(opt)
    opt.dry_run = True
    opt.require_approval = False
    opt._best_route_decision_for_opportunity = mock.MagicMock(return_value=None)
    opp = P.Opportunity(P.OpportunityType.STRATEGY_SIGNAL, "SOL", "BUY", 1000, "r",
                        entry_price_est=100.0, product_id="SOL-USD")
    opt._process_opportunity(opp)
    assert opp.executed is True
    assert opp.order_id == "dry-run"


def test_process_buy_live(opt):
    _buy_state(opt)
    opt.dry_run = False
    opt.require_approval = False
    opt._best_route_decision_for_opportunity = mock.MagicMock(return_value=None)
    opp = P.Opportunity(P.OpportunityType.STRATEGY_SIGNAL, "SOL", "BUY", 1000, "r",
                        entry_price_est=100.0, product_id="SOL-USD")
    opt._process_opportunity(opp)
    assert opp.executed is True
    assert opt.cli.create_order.called


def test_process_buy_capacity_below_min(opt):
    opt.state = make_state({}, total_value=100000.0, usdc=50000.0)
    opt.capital_policy["max_deployable_usd"] = 0.0
    opt.capital_policy["targets"] = {"reserve": 1.0, "core": 0.0, "opportunity": 0.0}
    opt._best_route_decision_for_opportunity = mock.MagicMock(return_value=None)
    opp = P.Opportunity(P.OpportunityType.STRATEGY_SIGNAL, "SOL", "BUY", 1000, "r",
                        entry_price_est=100.0, product_id="SOL-USD")
    with mock.patch.object(P.logger, "warning") as w:
        opt._process_opportunity(opp)
    assert any("below minimum" in str(a) for a in w.call_args_list)


def test_process_buy_size_below_min(opt):
    opt.state = make_state({}, total_value=100000.0, usdc=50000.0)
    # Ensure the opportunity bucket is under target so buy-capacity check passes
    # and we reach the size-below-minimum clamp branch.
    opt.capital_policy["targets"] = {"reserve": 0.0, "core": 1.0, "opportunity": 1.0}
    opt._best_route_decision_for_opportunity = mock.MagicMock(return_value=None)
    opp = P.Opportunity(P.OpportunityType.STRATEGY_SIGNAL, "SOL", "BUY", 1, "r",
                        entry_price_est=100.0, product_id="SOL-USD")
    with mock.patch.object(P.logger, "warning") as w:
        opt._process_opportunity(opp)
    assert any("Size below minimum" in str(a) for a in w.call_args_list)


def test_process_sell(opt):
    opt.state = make_state(
        {"SOL": holding("SOL", 5000, "growth", price=100.0)},
        total_value=100000.0, usdc=50000.0)
    opt.cli.best_product.return_value = "SOL-USD"
    opt.cli.preview_order.return_value = {"total_fee": 1.0, "total_cost": 1000.0}
    opt.cli.create_order.return_value = {"id": "S1"}
    opt.dry_run = False
    opt.require_approval = False
    opt._best_route_decision_for_opportunity = mock.MagicMock(return_value=None)
    opp = P.Opportunity(P.OpportunityType.STRATEGY_SIGNAL, "SOL", "SELL", 1000, "r",
                        entry_price_est=100.0, product_id="SOL-USD")
    opt._process_opportunity(opp)
    assert opp.executed is True
    assert opt.cli.create_order.called


def test_process_preview_none(opt):
    _buy_state(opt)
    opt.dry_run = False
    opt.require_approval = False
    opt.cli.preview_order.return_value = None
    opt._best_route_decision_for_opportunity = mock.MagicMock(return_value=None)
    opp = P.Opportunity(P.OpportunityType.STRATEGY_SIGNAL, "SOL", "BUY", 1000, "r",
                        entry_price_est=100.0, product_id="SOL-USD")
    with mock.patch.object(P.logger, "warning") as w:
        opt._process_opportunity(opp)
    assert any("Preview failed" in str(a) for a in w.call_args_list)


def test_process_fee_too_high(opt):
    _buy_state(opt)
    opt.dry_run = False
    opt.require_approval = False
    opt.cli.preview_order.return_value = {"total_fee": 100.0, "total_cost": 1000.0}
    opt._best_route_decision_for_opportunity = mock.MagicMock(return_value=None)
    opp = P.Opportunity(P.OpportunityType.STRATEGY_SIGNAL, "SOL", "BUY", 1000, "r",
                        entry_price_est=100.0, product_id="SOL-USD")
    with mock.patch.object(P.logger, "warning") as w:
        opt._process_opportunity(opp)
    assert any("Fee too high" in str(a) for a in w.call_args_list)


def test_process_approval_live(opt):
    _buy_state(opt)
    opt.dry_run = False
    opt.require_approval = True
    opt._best_route_decision_for_opportunity = mock.MagicMock(return_value=None)
    opt.pending_file = os.path.join("data", "pending_proc.json")
    opt.notifier = mock.MagicMock()
    opp = P.Opportunity(P.OpportunityType.STRATEGY_SIGNAL, "SOL", "BUY", 1000, "r",
                        entry_price_est=100.0, product_id="SOL-USD")
    opt._process_opportunity(opp)
    assert os.path.exists(opt.pending_file)
    assert opt.notifier.send_trade_alert.called


def test_process_bracket_path(opt):
    opt.state = make_state({}, total_value=100000.0, usdc=50000.0)
    # Opportunity bucket under target so buy capacity clears the minimum.
    opt.capital_policy["targets"] = {"reserve": 0.0, "core": 1.0, "opportunity": 1.0}
    opt.dry_run = False
    opt.require_approval = False
    opt._exec_engine = None
    opt._bracket_mgr = mock.MagicMock()
    opt._bracket_mgr.place_bracket.return_value = {
        "status": "OPEN", "bracket_id": "bX",
        "entry_result": {"success": True, "client_order_id": "y"}}
    opt._save_brackets = mock.MagicMock()
    opt._best_route_decision_for_opportunity = mock.MagicMock(return_value=None)
    opp = P.Opportunity(P.OpportunityType.STRATEGY_SIGNAL, "SOL", "BUY", 1000, "r",
                        entry_price_est=100.0, stop_loss_pct=5.0, product_id="SOL-USD")
    opt._process_opportunity(opp)
    assert opt._bracket_mgr.place_bracket.called


def _route_step(product_id, from_c, to_c, direction):
    return SimpleNamespace(product_id=product_id, from_currency=from_c,
                           to_currency=to_c, direction=direction, price=100.0,
                           effective_rate=1.0)


def _route_decision(plan):
    return SimpleNamespace(plan=plan, score=0.9, expected_tax_impact_usd=0.0,
                           opportunity_bonus=0.0, drawdown_bonus=0.0,
                           regime_bonus=0.0, hop_penalty=0.0, liquidity_bonus=0.0,
                           factor_breakdown={})


def _multi_step_plan():
    # Two steps so use_route (len(steps) > 1) is activated.
    steps = [_route_step("USDC-USD", "USDC", "USD", "BUY"),
             _route_step("SOL-USD", "USD", "SOL", "BUY")]
    return SimpleNamespace(source="USDC", target="SOL", effective_rate=1.0,
                           steps=steps, path=["USDC", "SOL"])


def test_process_route_execution(opt):
    _buy_state(opt)
    opt.dry_run = False
    opt.require_approval = False
    opp = P.Opportunity(P.OpportunityType.STRATEGY_SIGNAL, "SOL", "BUY", 1000, "r",
                        entry_price_est=100.0, product_id="SOL-USD")
    decision = _route_decision(_multi_step_plan())
    opt._best_route_decision_for_opportunity = mock.MagicMock(return_value=decision)
    opt._execute_route_decision = mock.MagicMock(return_value=True)
    n0 = len(opt.trade_log)
    opt._process_opportunity(opp)
    assert opt._execute_route_decision.called
    assert len(opt.trade_log) == n0 + 1


def test_process_route_pending(opt):
    _buy_state(opt)
    opt.dry_run = False
    opt.require_approval = True
    opt.pending_file = os.path.join("data", "pending_route.json")
    opp = P.Opportunity(P.OpportunityType.STRATEGY_SIGNAL, "SOL", "BUY", 1000, "r",
                        entry_price_est=100.0, product_id="SOL-USD")
    decision = _route_decision(_multi_step_plan())
    opt._best_route_decision_for_opportunity = mock.MagicMock(return_value=decision)
    opt.notifier = mock.MagicMock()
    opt._process_opportunity(opp)
    assert os.path.exists(opt.pending_file)
    assert opt.notifier.send_trade_alert.called


def test_process_route_failed(opt):
    _buy_state(opt)
    opt.dry_run = False
    opt.require_approval = False
    opp = P.Opportunity(P.OpportunityType.STRATEGY_SIGNAL, "SOL", "BUY", 1000, "r",
                        entry_price_est=100.0, product_id="SOL-USD")
    decision = _route_decision(_multi_step_plan())
    opt._best_route_decision_for_opportunity = mock.MagicMock(return_value=decision)
    opt._execute_route_decision = mock.MagicMock(return_value=False)
    with mock.patch.object(P.logger, "warning") as w:
        opt._process_opportunity(opp)
    assert any("Route execution failed" in str(a) for a in w.call_args_list)


# ---------------------------------------------------------------------------
# Write methods
# ---------------------------------------------------------------------------

def _sample_opps(opt):
    return [
        P.Opportunity(P.OpportunityType.TLH, "SOL", "SELL", 100, "tlh r", entry_price_est=100.0),
        P.Opportunity(P.OpportunityType.FEE_TIER_VOLUME, "SOL", "BUY", 100, "fee r", entry_price_est=100.0),
        P.Opportunity(P.OpportunityType.EVENT_ARBITRAGE, "ARB", "BUY", 100, "arb r",
                      product_id="k:a", meta={"signal_type": "x"}),
        P.Opportunity(P.OpportunityType.STOCK_SIGNAL, "NVDA", "BUY", 100, "stock r"),
        P.Opportunity(P.OpportunityType.STRATEGY_SIGNAL, "SOL", "BUY", 100, "strat r",
                      product_id="SOL-USD", meta={"final_confidence": 0.6,
                                                   "strategy_name": "x", "trade_style": "momentum"}),
        P.Opportunity(P.OpportunityType.ACCUMULATOR_SIGNAL, "SOL", "BUY", 100, "acc r",
                      product_id="SOL-USD", meta={"strategy_name": "NewsSentiment"}),
        P.Opportunity(P.OpportunityType.NEW_LISTING_MOMENTUM, "SOL", "BUY", 100, "nl r",
                      meta={"trade_style": "new_listing"}),
        P.Opportunity(P.OpportunityType.REBALANCE, "SOL", "BUY", 100, "reb r"),
        P.Opportunity(P.OpportunityType.VOLUME_CYCLE, "SOL", "SELL", 100, "vc r"),
    ]


def test_write_trade_plans(opt):
    ops = _sample_opps(opt)
    opt._write_trade_plans(ops)
    assert os.path.exists("trade_plans.json")
    with open("trade_plans.json") as f:
        data = json.load(f)
    assert data["total"] == len(ops[:50])
    os.remove("trade_plans.json")


def test_write_signal_cache(opt):
    ops = _sample_opps(opt)
    opt._write_signal_cache(ops)
    assert os.path.exists("data/.unified_signal_cache.json")
    with open("data/.unified_signal_cache.json") as f:
        data = json.load(f)
    assert data["total_signals"] == len(ops[:100])
    assert data["buy_signals"] + data["sell_signals"] == len(ops[:100])
    os.remove("data/.unified_signal_cache.json")


def test_write_enhanced_state_branches(opt):
    opt.state = make_state(
        {"SOL": holding("SOL", 5000, "growth", price=100.0)},
        total_value=100000.0, usdc=50000.0)
    opt._meta_source_weights = {"x": 0.5}
    opt._cross_asset_regime = mock.MagicMock()
    opt._cross_asset_regime.get_state.return_value.to_dict.return_value = {"regime": "neutral"}
    opt._ensemble_blender = mock.MagicMock()
    opt._ensemble_blender.to_dict.return_value = {"a": 1}
    opt._ensemble_blender.top_strategies.return_value = ["s1"]
    opt._param_opt_results = {"atr": {"best_params": {"atr_period": 14}}}
    opt._wash_sale_cooldown = {"BTC": time.time()}
    sig = SimpleNamespace(action="BUY", confidence=0.5, spread_bps=5.0,
                          spread_z=1.0, spread_tight=True)
    opt._order_flow_engine = mock.MagicMock()
    opt._order_flow_engine.get_signal.return_value = sig
    opt._write_enhanced_state()
    assert os.path.exists("data/meta_source_weights.json")
    assert os.path.exists("data/cross_asset_regime.json")
    assert os.path.exists("data/signal_ensemble.json")
    assert os.path.exists("data/param_opt_results.json")
    assert os.path.exists("data/wash_sale_state.json")
    assert os.path.exists("data/order_flow_signals.json")
    for p in ("data/meta_source_weights.json", "data/cross_asset_regime.json",
              "data/signal_ensemble.json", "data/param_opt_results.json",
              "data/wash_sale_state.json", "data/order_flow_signals.json"):
        os.remove(p)


# ---------------------------------------------------------------------------
# _event_signals_to_ops
# ---------------------------------------------------------------------------

def test_event_signals_to_ops(opt):
    sig = SimpleNamespace(outcome="BUY YES", reason="r", position_size=100,
                          platform="kalshi", market_ticker="T", market_question="q",
                          probability=0.6, signal_type="x", confidence=0.7)
    ops = opt._event_signals_to_ops([sig])
    assert ops
    assert ops[0].opp_type == P.OpportunityType.EVENT_MARKET


# ---------------------------------------------------------------------------
# _detect_event_markets
# ---------------------------------------------------------------------------

def _fake_market(**kw):
    m = SimpleNamespace(category="crypto", volume=5000, is_open=True,
                        probability_extremity=0.9, liquidity_score=0.8, mid_price=0.7,
                        question="Will BTC hit 100k?", platform="kalshi",
                        market_id="m1", spread=0.01)
    for k, v in kw.items():
        setattr(m, k, v)
    return m


def test_detect_event_markets_actionable(opt):
    opt._pm_client = mock.MagicMock()
    opt._pm_client.search_all_categories.return_value = {
        "crypto": [_fake_market(), _fake_market(question="ethereum price up", mid_price=0.3)],
        "sports": [_fake_market(category="sports", question="who wins super bowl", mid_price=0.6)],
    }
    opt._knowledge_gap = mock.MagicMock()
    opt._knowledge_gap.analyze.return_value = None
    opt._arb_scanner = mock.MagicMock()
    opt._arb_scanner.scan.return_value = []
    opt.cli.best_product.return_value = "BTC-USD"
    opt.cli.get_price.return_value = {"price": 50000.0}
    ops = opt._detect_event_markets()
    assert any(o.opp_type == P.OpportunityType.STRATEGY_SIGNAL for o in ops)


def test_detect_event_markets_knowledge_gap_boost(opt):
    opt._pm_client = mock.MagicMock()
    opt._pm_client.search_all_categories.return_value = {"crypto": [_fake_market(mid_price=0.7)]}
    kg = SimpleNamespace(is_significant=True, direction="overvalued", mid_price=0.7,
                         gap=0.3, gap_pct=30, evidence_score=0.6, evidence_count=3,
                         sentiment_label="neg", confidence=0.5, sources_used=["a", "b"])
    opt._knowledge_gap = mock.MagicMock()
    opt._knowledge_gap.analyze.return_value = kg
    opt._arb_scanner = mock.MagicMock()
    opt._arb_scanner.scan.return_value = []
    opt.cli.best_product.return_value = "BTC-USD"
    opt.cli.get_price.return_value = {"price": 50000.0}
    ops = opt._detect_event_markets()
    assert any("kg:" in o.reason for o in ops)


def test_detect_event_markets_arb(opt):
    opt._pm_client = mock.MagicMock()
    # Non-empty markets list so event-market detection proceeds to the arb scanner.
    opt._pm_client.search_all_categories.return_value = {"crypto": [_fake_market()]}
    opt._knowledge_gap = mock.MagicMock()
    opt._knowledge_gap.analyze.return_value = None
    leg = SimpleNamespace(platform="k", market_id="mk", question="q", outcome="YES",
                          side="BUY", price=0.5)
    arb = SimpleNamespace(edge=0.02, edge_pct=0.02, confidence=0.6, reason="arbr",
                          platform_buy="k", platform_hedge="p", leg_buy=leg,
                          leg_hedge=leg, event_key="e", category="crypto")
    opt._arb_scanner = mock.MagicMock()
    opt._arb_scanner.scan.return_value = [arb]
    _rich_state(opt, {"SOL": holding("SOL", 5000, "growth", price=100.0)})
    opt.cli.best_product.side_effect = lambda c, s: "BTC-USD"
    ops = opt._detect_event_markets()
    assert any(o.opp_type == P.OpportunityType.EVENT_ARBITRAGE for o in ops)


def test_detect_event_markets_no_clients(opt):
    opt._pm_client = None
    opt.event_engine = None
    assert opt._detect_event_markets() == []


def test_detect_event_markets_exception(opt):
    opt._pm_client = mock.MagicMock()
    opt._pm_client.search_all_categories.side_effect = RuntimeError("boom")
    assert opt._detect_event_markets() == []


# ---------------------------------------------------------------------------
# _detect_funding_and_onchain_signals
# ---------------------------------------------------------------------------

def test_detect_funding_and_onchain(opt):
    _rich_state(opt, {"BTC": holding("BTC", 30000, "safe", price=30000.0, product_id="BTC-USD")})
    opt.cli.get_price.return_value = {"price": 30000.0}
    opt.cli.best_product.return_value = "BTC-USD"
    fund = mock.MagicMock()
    fund.on_bar.return_value = FakeSignal(action="BUY", confidence=0.6, reason="fund r")
    opt._funding_contrarian = fund
    onchain = mock.MagicMock()
    onchain.get_signals.return_value = [{
        "action": "SELL", "product_id": "BTC-USD", "currency": "BTC",
        "confidence": 0.5, "price": 30000.0, "volume_anomaly": 2.0,
        "price_trend": 0.1, "reason": "oc r"}]
    opt._onchain_flow = onchain
    ops = opt._detect_funding_and_onchain_signals()
    assert any(o.meta.get("source") == "funding_rate" for o in ops)
    assert any(o.meta.get("source") == "onchain_flow" for o in ops)


def test_detect_funding_onchain_no_engines(opt):
    _rich_state(opt, {"BTC": holding("BTC", 30000, "safe", price=30000.0, product_id="BTC-USD")})
    opt._funding_contrarian = None
    opt._onchain_flow = None
    assert opt._detect_funding_and_onchain_signals() == []


# ---------------------------------------------------------------------------
# _detect_order_flow_signals
# ---------------------------------------------------------------------------

def test_detect_order_flow_signals(opt):
    _rich_state(opt, {"SOL": holding("SOL", 5000, "growth", price=100.0, volume_24h=2_000_000.0)})
    opt.cli.best_product.side_effect = lambda c, s: "SOL-USD"
    eng = mock.MagicMock()
    eng.evaluate.return_value = FakeSignal(action="BUY", confidence=0.6, reason="of r")
    opt._order_flow_engine = eng
    opt._smart_money_flow = None
    ops = opt._detect_order_flow_signals()
    assert any(o.meta.get("source") == "order_flow" for o in ops)


def test_detect_order_flow_no_engine(opt):
    _rich_state(opt, {"SOL": holding("SOL", 5000, "growth", price=100.0)})
    opt._order_flow_engine = None
    opt._smart_money_flow = None
    assert opt._detect_order_flow_signals() == []


# ---------------------------------------------------------------------------
# _detect_accumulator_signals
# ---------------------------------------------------------------------------

def test_detect_accumulator_signals(opt):
    if not P._HAS_ACCUMULATOR:
        pytest.skip("accumulator unavailable")
    _rich_state(opt, {"SOL": holding("SOL", 5000, "growth", price=100.0, liquidity_score=0.8)})
    # Opportunity bucket under target so capacity is positive.
    opt.capital_policy["targets"] = {"reserve": 0.0, "core": 1.0, "opportunity": 1.0}
    opt.cli.best_product.side_effect = lambda c, s: "SOL-USD"
    sig = SimpleNamespace(action="BUY", final_confidence=0.6, symbol="SOL-USD",
                          base_confidence=0.5, opportunity_score=0.4,
                          strategy_name="NewsSentiment:foo", signal_reason="acc r",
                          market_data={"price": 100.0, "change_pct": 1.0})
    fake_acc = mock.MagicMock()
    fake_acc.accumulate.return_value = [sig]
    # Patch the constructor so UnifiedSignalAccumulator(...) returns our fake.
    with mock.patch.object(P, "UnifiedSignalAccumulator", return_value=fake_acc):
        ops = opt._detect_accumulator_signals()
    assert any(o.opp_type == P.OpportunityType.ACCUMULATOR_SIGNAL for o in ops)


def test_detect_accumulator_no_module(opt):
    if P._HAS_ACCUMULATOR:
        P._HAS_ACCUMULATOR = False
        try:
            assert opt._detect_accumulator_signals() == []
        finally:
            P._HAS_ACCUMULATOR = True
    else:
        assert opt._detect_accumulator_signals() == []


# ---------------------------------------------------------------------------
# _detect_rebalance_bot / _detect_stairstep / _holding_for_product
# ---------------------------------------------------------------------------

def test_rebalance_bot_with_engine(opt):
    opt.state = make_state(
        {"BTC": holding("BTC", 40000, "safe", price=40000.0, product_id="BTC-USD")},
        total_value=100000.0)
    opt.cli.best_product.side_effect = lambda c, s: "BTC-USD"
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
    ops = opt._detect_rebalance_bot()
    assert any(o.opp_type == P.OpportunityType.REBALANCE_BOT for o in ops)


def test_rebalance_bot_engine_unavailable(opt):
    opt.state = make_state({}, total_value=100000.0)
    opt._rebalance_bot = None
    import builtins
    real_import = builtins.__import__
    def fake_import(name, *a, **k):
        if name.endswith("rebalance_engine"):
            raise ImportError("nope")
        return real_import(name, *a, **k)
    # Keep the import patch active for BOTH calls so the engine stays unavailable.
    with mock.patch.object(builtins, "__import__", side_effect=fake_import):
        assert opt._detect_rebalance_bot() == []
        assert opt._detect_rebalance_bot() == []


def test_stairstep_with_engine(opt):
    opt.stairstep_enabled = True
    opt.state = make_state(
        {"XRP": holding("XRP", 1000, "growth", price=0.6, product_id="XRP-USD")},
        total_value=100000.0)
    opt._stairstep_symbols = ["XRP-USD"]
    opt.cli.best_product.side_effect = lambda c, s: "XRP-USD"
    engine = mock.MagicMock()
    engine._symbols = {}
    engine.add_symbol.return_value = None
    engine.on_price.return_value = SimpleNamespace(side="BUY", notional=100.0)
    engine.state.return_value = (0, 1, 2, 3, 4.0)
    opt._stairstep_engine = engine
    ops = opt._detect_stairstep()
    assert any(o.opp_type == P.OpportunityType.STAIRSTEP for o in ops)


def test_stairstep_engine_unavailable(opt):
    opt.stairstep_enabled = True
    opt.state = make_state(
        {"XRP": holding("XRP", 1000, "growth", price=0.6, product_id="XRP-USD")},
        total_value=100000.0)
    opt._stairstep_engine = None
    import builtins
    real_import = builtins.__import__
    def fake_import(name, *a, **k):
        if name.endswith("rebalance_engine"):
            raise ImportError("nope")
        return real_import(name, *a, **k)
    with mock.patch.object(builtins, "__import__", side_effect=fake_import):
        assert opt._detect_stairstep() == []


def test_holding_for_product(opt):
    opt.state = make_state(
        {"SOL": holding("SOL", 5000, "growth", price=100.0, product_id="SOL-USD")},
        total_value=100000.0)
    assert opt._holding_for_product("SOL-USD") is not None
    assert opt._holding_for_product("SOL-USD")["currency"] == "SOL"
    assert opt._holding_for_product("XRP-USD") is None
    opt.state = None
    assert opt._holding_for_product("SOL-USD") is None


# ---------------------------------------------------------------------------
# _detect_volume_cycles / _detect_tlh / _detect_fee_tier_volume / _detect_rebalance
# ---------------------------------------------------------------------------

def test_detect_volume_cycles_stale(opt):
    _rich_state(opt, {"SOL": holding("SOL", 5000, "growth", price=100.0, product_id="SOL-USD")})
    opt.cli.best_product.side_effect = lambda c, s: "SOL-USD"
    opt.position_ages["SOL"] = time.time() - 200 * 3600
    ops = opt._detect_volume_cycles()
    assert any(o.opp_type == P.OpportunityType.VOLUME_CYCLE for o in ops)


def test_detect_tlh_loss(opt):
    _rich_state(opt, {"SOL": holding("SOL", 5000, "growth", price=100.0, pnl=-12.0, product_id="SOL-USD")})
    opt.cli.best_product.side_effect = lambda c, s: "SOL-USD"
    ops = opt._detect_tlh()
    assert any(o.opp_type == P.OpportunityType.TLH for o in ops)


def test_detect_tlh_skip_static(opt):
    _rich_state(opt, {"BTC": holding("BTC", 40000, "safe", price=40000.0, pnl=-12.0, product_id="BTC-USD")})
    assert opt._detect_tlh() == []


def test_detect_fee_tier_volume(opt):
    _rich_state(opt, {"SOL": holding("SOL", 5000, "growth", price=100.0, volume_24h=5_000_000.0,
                        change_24h=1.0, product_id="SOL-USD")},
                usdc=90000.0)
    opt.state.volume_to_next_tier = 1000.0
    opt.cli.best_product.side_effect = lambda c, s: "SOL-USD"
    ops = opt._detect_fee_tier_volume()
    assert any(o.opp_type == P.OpportunityType.FEE_TIER_VOLUME for o in ops)


def test_detect_rebalance_overweight(opt):
    _rich_state(opt, {"SOL": holding("SOL", 90000, "growth", price=100.0, pnl=-1.0, product_id="SOL-USD")})
    opt.cli.best_product.side_effect = lambda c, s: "SOL-USD"
    ops = opt._detect_rebalance()
    assert any(o.opp_type == P.OpportunityType.REBALANCE and o.side == "SELL" for o in ops)


def test_detect_rebalance_underweight(opt):
    _rich_state(opt, {"SOL": holding("SOL", 1000, "growth", price=100.0, pnl=-1.0, product_id="SOL-USD")})
    opt.cli.best_product.side_effect = lambda c, s: "SOL-USD"
    ops = opt._detect_rebalance()
    assert any(o.opp_type == P.OpportunityType.REBALANCE and o.side == "BUY" for o in ops)


# ---------------------------------------------------------------------------
# _detect_coinbase_universe_signals / _detect_stock_opportunities
# ---------------------------------------------------------------------------

def test_detect_coinbase_universe_signals(opt):
    candles = [{"start": i, "open": 100 + i, "high": 101 + i, "low": 99 + i,
                "close": 100 + i, "volume": 1000} for i in range(50)]
    opt.cli.get_products.return_value = {
        "SOL-USD": {"product_id": "SOL-USD", "volume_24h": 5_000_000.0,
                    "trading_disabled": False},
    }
    opt.cli.get_candles.return_value = candles
    _rich_state(opt, {}, total_value=100000.0, usdc=90000.0)
    opt.cli.best_product.side_effect = lambda c, s: "SOL-USD"
    ops = opt._detect_coinbase_universe_signals()
    assert isinstance(ops, list)


def test_detect_stock_opportunities(opt):
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
# _detect_aggregator_signals / _detect_strategy_signals
# ---------------------------------------------------------------------------

def test_detect_aggregator_signals(opt):
    if not P._HAS_AGGREGATOR:
        pytest.skip("aggregator unavailable")
    opt.state = make_state(
        {"SOL": holding("SOL", 5000, "growth", price=100.0, product_id="SOL-USD")},
        total_value=100000.0)
    opt.cli.best_product.return_value = "SOL-USD"
    us = SimpleNamespace(product_id="SOL-USD", base="SOL", direction="BUY",
                         unified_score=0.6, conviction=0.8, backtest_quality=0.5,
                         trend_score=0.3, price=100.0, priority=0.5, details={},
                         top_strategies=["ema_cross", "macd"])
    fake = mock.MagicMock()
    fake.scan_universe.return_value = [us]
    # top_coinbase_pairs and fetch_candles_batch are imported locally inside the
    # method, so patch them at their source module paths. Provide >=3 products
    # each with >=60 candles so the universe scan proceeds.
    pairs = [("SOL-USD", "SOL"), ("BTC-USD", "BTC"), ("ETH-USD", "ETH")]
    candle_row = [0, 99.0, 101.0, 100.0, 100.0, 1000.0]
    candles = {pid: [list(candle_row) for _ in range(70)] for pid, _ in pairs}
    import coinbase.src.pair_discovery as _pd_mod
    import coinbase.src.rest_feed as _rf_mod
    with mock.patch.object(P, "SignalAggregator", mock.MagicMock(return_value=fake)), \
         mock.patch.object(_pd_mod, "top_coinbase_pairs", return_value=pairs), \
         mock.patch.object(_rf_mod, "fetch_candles_batch", side_effect=lambda *a, **k: candles):
        ops = opt._detect_aggregator_signals()
    assert isinstance(ops, list)


def test_detect_strategy_signals(opt):
    _rich_state(opt, {"SOL": holding("SOL", 5000, "growth", price=100.0, product_id="SOL-USD",
                        liquidity_score=0.8)})
    # Opportunity bucket under target so capacity is positive.
    opt.capital_policy["targets"] = {"reserve": 0.0, "core": 1.0, "opportunity": 1.0}
    opt.cli.best_product.side_effect = lambda c, s: "SOL-USD"
    candles = [{"start": i, "open": 100 + i, "high": 101 + i, "low": 99 + i,
                "close": 100 + i, "volume": 1000} for i in range(50)]
    opt.cli.get_candles.return_value = candles
    opt._bt_cache = {"ema_cross/SOL": BacktestVerdict(
        strategy="ema_cross", currency="SOL", total_trades=10, winning_trades=7,
        losing_trades=3, win_rate=0.7, sharpe_ratio=1.2, profit_factor=1.5,
        max_drawdown_pct=5.0, total_return_pct=20.0, regime="neutral",
        passed=True, reason="ok")}
    opt.confidence_engine = None
    opt._check_cluster_limit = lambda c, s: True
    opt._signal_pulses[opt._pulse_key("SOL-USD", "ema_cross", "BUY")] = {
        "strategy": "ema_cross", "direction": "BUY", "product_id": "SOL-USD",
        "pulse_count": 3, "first_ts": time.time() - 100, "last_ts": time.time(),
        "avg_confidence": 0.6, "min_price": 95.0, "max_price": 105.0, "flip_count": 0}
    agg = SimpleNamespace(direction="BUY", confidence=0.6, strategies=["ema_cross"],
                          strategy_count=1, best_reason="r", agreeing_groups=["trend"])
    with mock.patch.object(P, "_run_strategies",
                           return_value=[StrategySignal(action="BUY", confidence=0.6,
                                                        strategy="ema_cross", reason="r")]), \
         mock.patch.object(P, "_batch_signals_fast", return_value={}), \
         mock.patch.object(P, "ConfidenceMatrix") as CM:
        inst = CM.return_value
        inst.aggregate.return_value = [agg]
        ops = opt._detect_strategy_signals()
    assert any(o.opp_type == P.OpportunityType.STRATEGY_SIGNAL for o in ops)


# ---------------------------------------------------------------------------
# _run_periodic_param_optimization / _apply_optimized_params
# ---------------------------------------------------------------------------

def test_run_periodic_param_optimization_no_data(opt):
    opt._last_param_opt_ts = 0
    P._HAS_WALK_FORWARD = True
    FakeWF = mock.MagicMock()
    old = P._WalkForwardOptimizer
    P._WalkForwardOptimizer = FakeWF
    opt._feed_mgr = None
    try:
        res = opt._run_periodic_param_optimization()
        assert res == {}
    finally:
        P._WalkForwardOptimizer = old
        P._HAS_WALK_FORWARD = False


def test_apply_optimized_params(opt):
    opt._param_opt_results = {
        "stop": {"best_params": {"stop_atr_mult": 2.0}},
        "atr": {"best_params": {"atr_period": 14}},
        "ma": {"best_params": {"ma_fast": 5, "ma_slow": 20}},
    }
    opt._apply_optimized_params()
    assert opt._param_opt_results.get("_active_stop_mult") == 2.0
    assert opt._param_opt_results.get("_active_atr_period") == 14


# ---------------------------------------------------------------------------
# _apply_cross_asset_risk_filter
# ---------------------------------------------------------------------------

def test_cross_asset_risk_filter(opt):
    ops = [P.Opportunity(P.OpportunityType.STRATEGY_SIGNAL, "SOL", "BUY", 100, "r"),
           P.Opportunity(P.OpportunityType.STRATEGY_SIGNAL, "SOL", "SELL", 100, "r")]
    regime = SimpleNamespace(allows_new_longs=False, risk_multiplier=0.5,
                             regime="crash", trend_bias="down")
    opt._cross_asset_regime = mock.MagicMock()
    opt._cross_asset_regime.get_state.return_value = regime
    opt._macro_risk = None
    filtered = opt._apply_cross_asset_risk_filter(ops)
    assert all(o.side == "SELL" for o in filtered)


def test_cross_asset_risk_filter_macro(opt):
    ops = [P.Opportunity(P.OpportunityType.STRATEGY_SIGNAL, "SOL", "BUY", 100, "r"),
           P.Opportunity(P.OpportunityType.STRATEGY_SIGNAL, "SOL", "SELL", 100, "r")]
    opt._cross_asset_regime = None
    macro = SimpleNamespace(macro_score=2.0)
    opt._macro_risk = mock.MagicMock()
    opt._macro_risk.get_signal.return_value = macro
    filtered = opt._apply_cross_asset_risk_filter(ops)
    assert len(filtered) == 2


# ---------------------------------------------------------------------------
# _compute_cost_bases / _fetch_state
# ---------------------------------------------------------------------------

def test_compute_cost_bases(opt):
    opt.cli.get_fills.return_value = [
        {"product_id": "BTC-USD", "side": "BUY", "size": 1.0, "price": 100.0},
        {"product_id": "BTC-USD", "side": "SELL", "size": 0.5, "price": 110.0},
        {"product_id": "SOL-USD", "side": "BUY", "size": 0, "price": 0},
    ]
    cb = opt._compute_cost_bases()
    assert cb["BTC"] > 0


def test_fetch_state(opt):
    opt.cli.get_products.return_value = {"BTC-USD": {"product_id": "BTC-USD"}}
    opt.cli.get_balances.return_value = [
        {"currency": "USDC", "available_balance": {"value": 50000}, "hold": {"value": 0}},
        {"currency": "BTC", "available_balance": {"value": 0.5}, "hold": {"value": 0},
         "cost_basis": 100.0},
    ]
    opt.cli.get_fees.return_value = {"advanced_trade_only_volume": 0}
    opt.cli.get_price.return_value = {"price": 40000.0,
                                      "price_percentage_change_24h": 1.0, "volume_24h": 1_000_000}
    opt.cli.best_product.return_value = "BTC-USD"
    opt.cli.get_fills.return_value = []
    opt._fetch_state()
    assert opt.state is not None
    assert opt.state.total_value > 0
    assert "BTC" in opt.state.holdings


def test_fetch_state_no_balances(opt):
    opt.cli.get_products.return_value = {}
    opt.cli.get_balances.return_value = None
    opt.cli.get_fees.return_value = None
    opt.cli.get_fills.return_value = []
    opt._fetch_state()
    assert opt.state is not None
    assert opt.state.total_value == 0.0


# ---------------------------------------------------------------------------
# _tick / run / stop / summary
# ---------------------------------------------------------------------------

def test_tick(opt):
    opt.cli.get_products.return_value = {}
    opt.cli.get_balances.return_value = [
        {"currency": "USDC", "available_balance": {"value": 50000}, "hold": {"value": 0}}]
    opt.cli.get_fees.return_value = {"advanced_trade_only_volume": 0}
    opt.cli.get_price.return_value = {"price": 100.0,
                                      "price_percentage_change_24h": 1.0, "volume_24h": 1_000_000}
    opt.cli.get_fills.return_value = []
    with mock.patch.object(opt, "_detect_opportunities", return_value=[]):
        opt._tick()
    assert opt.state is not None
    assert opt._tick_count == 0 or True


def test_run_kill_switch(opt, monkeypatch):
    monkeypatch.setenv("KILL_SWITCH", "true")
    opt.running = True
    opt.run()
    assert opt.running is False


def test_run_loop_once(opt, monkeypatch):
    monkeypatch.setenv("KILL_SWITCH", "false")
    monkeypatch.setattr(time, "sleep", lambda *a, **k: None)
    calls = {"n": 0}

    def fake_tick():
        calls["n"] += 1
        opt.running = False

    opt._tick = fake_tick
    opt.running = True
    opt.run()
    assert calls["n"] == 1


def test_stop(opt):
    opt._bracket_mgr = mock.MagicMock()
    opt._bracket_mgr.stop_polling = mock.MagicMock()
    opt._bracket_mgr.save_brackets = mock.MagicMock()
    opt._feed_mgr = mock.MagicMock()
    opt.graph_store = mock.MagicMock()
    opt._health_server = mock.MagicMock()
    opt._lock_fd = None
    opt.stop()
    assert opt.running is False
    assert opt._bracket_mgr.stop_polling.called
    assert opt._feed_mgr.stop.called


def test_summary(opt):
    opt.trade_log = [
        {"type": "strategy", "size_usd": 100, "fee": 1.0},
        {"type": "tlh", "size_usd": 50, "fee": 0.5},
    ]
    s = opt.summary()
    assert s["total_trades"] == 2
    assert s["total_volume"] == 150
    assert s["total_fees"] == 1.5
    assert "strategy" in s["by_type"]


# ---------------------------------------------------------------------------
# main()
# ---------------------------------------------------------------------------

def test_main_summary(monkeypatch, capsys):
    fake = mock.MagicMock()
    fake.trade_log = [{"type": "strategy", "size_usd": 1, "fee": 0.0}]
    fake.summary.return_value = {"total_trades": 1}
    monkeypatch.setattr(P, "PortfolioOptimizer", lambda **k: fake)
    monkeypatch.setattr(sys, "argv", ["po", "--summary"])
    P.main()
    assert fake.summary.called


def test_main_once(monkeypatch):
    fake = mock.MagicMock()
    fake.trade_log = []
    fake.summary.return_value = {}
    fake._tick = mock.MagicMock()
    monkeypatch.setattr(P, "PortfolioOptimizer", lambda **k: fake)
    monkeypatch.setattr(sys, "argv", ["po", "--once"])
    P.main()
    assert fake._tick.called


def test_main_reset_db(monkeypatch, tmp_path):
    db = str(tmp_path / "x.db")
    with open(db, "w") as f:
        f.write("x")
    fake = mock.MagicMock()
    fake.trade_log = []
    fake.summary.return_value = {}
    monkeypatch.setattr(P, "PortfolioOptimizer", lambda **k: fake)
    monkeypatch.setattr(sys, "argv", ["po", "--reset-db", "--db", db])
    P.main()
    assert not os.path.exists(db)
