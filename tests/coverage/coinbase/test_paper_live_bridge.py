"""Coverage tests for coinbase/src/paper_live_bridge.py (target >=90%)."""
from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from coinbase.src import paper_live_bridge as plb


@pytest.fixture
def state_path(tmp_path):
    return str(tmp_path / "strat_perf.json")


def test_performance_tracker_record_win_loss(state_path):
    t = plb.PerformanceTracker(state_path)
    # a win
    t.record_trade("s1", "BTC-USD", "BUY", 100, 110, 10.0, 1.0, fees=0.1)
    # a loss
    t.record_trade("s1", "BTC-USD", "SELL", 100, 90, -10.0, -1.0, fees=0.1)
    perf = t.get_performance("s1")
    assert perf["trades"] == 2
    assert perf["wins"] == 1
    assert perf["losses"] == 1
    assert perf["win_rate"] == 0.5
    assert t.get_win_rate("s1") == 0.5


def test_performance_tracker_missing(state_path):
    t = plb.PerformanceTracker(state_path)
    assert t.get_performance("nope") == {}
    # default win rate 0.5 for unknown
    assert t.get_win_rate("nope") == 0.5
    assert t.get_all_performance() == {}


def test_performance_tracker_saves_and_reloads(state_path):
    t = plb.PerformanceTracker(state_path)
    t.record_trade("s1", "BTC-USD", "BUY", 100, 110, 5.0, 0.5)
    # new instance reloads from disk
    t2 = plb.PerformanceTracker(state_path)
    assert t2.get_performance("s1")["trades"] == 1


def test_performance_tracker_sharpe_and_profit_factor(state_path):
    t = plb.PerformanceTracker(state_path)
    # >=5 trades so sharpe/profit_factor computed
    for i in range(6):
        t.record_trade("s2", "BTC-USD", "BUY", 100, 110, 5.0, 1.0 + i * 0.1)
    perf = t.get_performance("s2")
    assert perf["trades"] >= 5
    assert "sharpe" in perf
    assert "profit_factor" in perf


def test_best_strategies(state_path):
    t = plb.PerformanceTracker(state_path)
    for i in range(12):
        t.record_trade("good", "BTC-USD", "BUY", 100, 110, 5.0, 1.0)
    for i in range(3):
        t.record_trade("bad", "BTC-USD", "BUY", 100, 90, -5.0, -1.0)
    best = t.best_strategies(min_trades=10, top_n=5)
    assert best and best[0]["strategy"] == "good"


def test_best_strategies_empty(state_path):
    t = plb.PerformanceTracker(state_path)
    assert t.best_strategies() == []


def test_deployment_pipeline_register_and_backtest(state_path):
    pipe = plb.DeploymentPipeline(plb.PerformanceTracker(state_path))
    pipe.register_strategy("s1", "growth")
    pipe.update_backtest_result("s1", 0.6, 1.2, 1.5, 50, True)
    dep = pipe.get_deployment_status()["s1"]
    assert dep["passed_backtest"] is True
    assert dep["deployed"] is False  # not enough paper trades yet


def test_deployment_pipeline_check_deployment(state_path):
    pipe = plb.DeploymentPipeline(plb.PerformanceTracker(state_path))
    pipe.register_strategy("s1", "growth")
    pipe.update_backtest_result("s1", 0.7, 1.5, 2.0, 50, True)
    # simulate enough paper trades + good win rate
    tracker = pipe.tracker
    for i in range(20):
        tracker.record_trade("s1", "BTC-USD", "BUY", 100, 110, 5.0, 1.0)
    deployed = pipe.check_deployment("s1")
    assert deployed is True
    assert pipe.get_deployment_status()["s1"]["deployed"] is True
    # already deployed -> re-check returns False without re-deploying (162->167)
    assert pipe.check_deployment("s1") is False
    assert pipe.get_deployment_status()["s1"]["deployed"] is True


def test_deployment_pipeline_check_deployment_fails(state_path):
    pipe = plb.DeploymentPipeline(plb.PerformanceTracker(state_path))
    pipe.register_strategy("s1", "growth")
    # not passed backtest -> not deployed
    assert pipe.check_deployment("s1") is False
    # unknown strategy
    assert pipe.check_deployment("ghost") is False


def test_feedback_loop_on_trade_close(state_path):
    tracker = plb.PerformanceTracker(state_path)
    pipe = plb.DeploymentPipeline(tracker)
    orch = MagicMock()
    fb = plb.FeedbackLoop(tracker, pipe, orch)
    pipe.register_strategy("s1", "growth")
    pipe.update_backtest_result("s1", 0.7, 1.5, 2.0, 50, True)
    for i in range(20):
        tracker.record_trade("s1", "BTC-USD", "BUY", 100, 110, 5.0, 1.0)
    fb.on_trade_close("BTC-USD", "BUY", 100, 110, 5.0, 1.0, "s1", mode="paper")
    assert pipe.get_deployment_status()["s1"]["deployed"] is True


def test_performance_tracker_corrupt_state(state_path):
    # write invalid JSON so _load's except branch runs (lines 44-45)
    with open(state_path, "w") as f:
        f.write("{not valid json")
    t = plb.PerformanceTracker(state_path)
    assert t.get_all_performance() == {}


def test_deployment_pipeline_update_unknown(state_path):
    pipe = plb.DeploymentPipeline(plb.PerformanceTracker(state_path))
    # updating an unregistered strategy is a no-op (line 141)
    pipe.update_backtest_result("ghost", 0.6, 1.0, 1.5, 10, True)
    assert "ghost" not in pipe.get_deployment_status()


def test_deployment_pipeline_passed_but_insufficient_trades(state_path):
    pipe = plb.DeploymentPipeline(plb.PerformanceTracker(state_path))
    pipe.register_strategy("s1", "growth")
    pipe.update_backtest_result("s1", 0.7, 1.5, 2.0, 50, True)
    # only 5 paper trades -> passed_backtest True but trades < 20 -> 162->167
    for i in range(5):
        pipe.tracker.record_trade("s1", "BTC-USD", "BUY", 100, 110, 5.0, 1.0)
    assert pipe.check_deployment("s1") is False


def test_feedback_loop_cycle_no_trades(state_path):
    tracker = plb.PerformanceTracker(state_path)
    pipe = plb.DeploymentPipeline(tracker)
    orch = MagicMock()
    fb = plb.FeedbackLoop(tracker, pipe, orch)
    pipe.register_strategy("s1", "growth")  # no trades recorded
    fb.cycle()  # 202->200 (skip inner block)
    assert not orch.update_strategy_performance.called


def test_feedback_loop_cycle_not_deployed(state_path):
    tracker = plb.PerformanceTracker(state_path)
    pipe = plb.DeploymentPipeline(tracker)
    orch = MagicMock()
    fb = plb.FeedbackLoop(tracker, pipe, orch)
    pipe.register_strategy("s1", "growth")
    # 15 trades -> enough to mark passed (>=10) but not to deploy (<20)
    for i in range(15):
        tracker.record_trade("s1", "BTC-USD", "BUY", 100, 110, 5.0, 1.0)
    fb.cycle()  # 202 True -> update; check_deployment False -> 212->200
    assert pipe.get_deployment_status()["s1"]["deployed"] is False


def test_feedback_loop_cycle(state_path):
    tracker = plb.PerformanceTracker(state_path)
    pipe = plb.DeploymentPipeline(tracker)
    orch = MagicMock()
    fb = plb.FeedbackLoop(tracker, pipe, orch)
    pipe.register_strategy("s1", "growth")
    # give the strategy some paper trades with a good win rate
    for i in range(20):
        tracker.record_trade("s1", "BTC-USD", "BUY", 100, 110, 5.0, 1.0)
    fb.cycle()
    # cycle recomputes backtest result and (re)checks deployment; if deployed,
    # it tells the orchestrator about the strategy's performance.
    assert pipe.get_deployment_status()["s1"]["deployed"] is True
    assert orch.update_strategy_performance.called
