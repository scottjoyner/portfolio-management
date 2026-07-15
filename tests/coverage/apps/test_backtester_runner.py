from types import SimpleNamespace
from unittest.mock import MagicMock

from _helpers import install_fakes

install_fakes({
    "research": None,
    "research.experiment_tracking": None,
    "research.experiment_tracking.tracker": {
        "ExperimentRun": MagicMock,
        "ExperimentTracker": MagicMock,
    },
})

from trading_system.apps.backtester import runner


def test_horizon_from_metadata():
    assert runner._horizon_from_metadata({"expected_holding_horizon": "swing"}) == "swing"
    assert runner._horizon_from_metadata({}) == "intraday"


def test_strategy_assumptions_branches():
    # micro + maker
    a = runner._strategy_assumptions("micro_maker_strategy", 0.5, 0.2, 25.0)
    assert a.maker_ratio == 0.7
    # latency but not micro
    b = runner._strategy_assumptions("latency_alpha", 0.5, 0.2, 25.0)
    assert b.latency_ms == 17.0  # 25 - 8
    # plain
    c = runner._strategy_assumptions("trend_follow", 0.5, 0.2, 25.0)
    assert c.maker_ratio == 0.45
    # queue keyword
    d = runner._strategy_assumptions("queue_imbalance", 0.5, 0.2, 25.0)
    assert d.rejection_rate == 0.03
    # non-micro, non-maker decay
    e = runner._strategy_assumptions("mean_rev", 0.5, 0.2, 25.0)
    assert round(e.stale_quote_decay, 2) == 0.18  # 0.2 - 0.02


def _fake_qm():
    class FakeQM:
        def estimate(self, *a, **k):
            return SimpleNamespace(fill_probability=0.5, stale_quote_decay=0.2)
    return FakeQM


def _patch_main(monkeypatch, strategies):
    monkeypatch.setattr(runner, "basic_metrics", lambda r: {"return": 0.01, "sharpe": 1.0})
    monkeypatch.setattr(runner, "SimpleQueueModel", _fake_qm())
    monkeypatch.setattr(runner, "load_strategies", lambda: strategies)
    result = SimpleNamespace(
        live_transfer_confidence=0.5,
        fragility_score=0.3,
        expected_live_return=0.01,
        breakdown=SimpleNamespace(total=0.1),
    )
    runner.BacktestRealismScorer.assess_strategy.return_value = result
    tracker = MagicMock()
    monkeypatch.setattr(runner, "ExperimentTracker", lambda: tracker)
    monkeypatch.setattr(runner.hub, "publish_sync", MagicMock())
    return tracker


def test_main_with_strategies(monkeypatch, tmp_path):
    strat = MagicMock()
    strat.strategy_id = "micro_maker_x"
    strat.metadata.return_value = {"expected_holding_horizon": "swing"}
    _patch_main(monkeypatch, [strat])
    import sys
    monkeypatch.setattr(sys, "argv", [
        "runner", "--config", "c.yaml",
        "--output", str(tmp_path / "out.json"),
        "--validation-report", str(tmp_path / "val.json"),
    ])
    runner.main()
    assert (tmp_path / "out.json").exists()


def test_main_empty_strategies(monkeypatch, tmp_path):
    _patch_main(monkeypatch, [])
    import sys
    monkeypatch.setattr(sys, "argv", [
        "runner", "--config", "c.yaml",
        "--output", str(tmp_path / "out.json"),
        "--validation-report", str(tmp_path / "val.json"),
    ])
    runner.main()  # should not raise ZeroDivisionError
    assert (tmp_path / "out.json").exists()
