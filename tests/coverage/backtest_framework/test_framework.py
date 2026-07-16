"""Tests for the iterative backtesting framework (experiment + comparison)."""
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from scripts.backtest_framework.experiment import (
    Experiment, resolve_strategies, evaluate_verdict, experiments_dir,
)
from scripts.backtest_framework.compare_experiments import compare, cli_main


def _verdict(passed, sharpe=0.5, win_rate=0.5, pf=1.2, dd=10.0, trades=20):
    class V:
        pass
    v = V()
    v.passed = passed
    v.strategy = "ema_cross"
    v.currency = "BTC"
    v.total_trades = trades
    v.win_rate = win_rate
    v.sharpe_ratio = sharpe
    v.profit_factor = pf
    v.max_drawdown_pct = dd
    v.regime = "AUTO"
    return v


def test_experiment_roundtrip(tmp_path):
    p = tmp_path / "exp.json"
    e = Experiment(name="t1", granularity=3600, strategies="rust",
                   universe="BTC-USD", thresholds={"min_sharpe": 0.4})
    e.save(str(p))
    e2 = Experiment.load(str(p))
    assert e2.name == "t1"
    assert e2.thresholds["min_sharpe"] == 0.4
    assert e2.granularity == 3600


def test_experiment_validate_bad_granularity():
    e = Experiment(name="bad", granularity=123)
    try:
        e.validate()
        assert False, "should raise"
    except ValueError:
        pass


def test_resolve_strategies_rust_is_subset_of_all():
    import strategy_engine as S
    rust = resolve_strategies("rust")
    assert rust
    assert all(s in S._RUST_STRATEGIES for s in rust)
    assert set(rust) <= set(S.ALL_STRATEGIES.keys())


def test_evaluate_verdict_applies_thresholds():
    th = {"min_win_rate": 0.40, "min_sharpe": 0.30, "min_profit_factor": 1.05,
          "min_trades": 5, "max_drawdown_pct": 50.0}
    assert evaluate_verdict(_verdict(passed=True, sharpe=0.6, win_rate=0.5), th)
    assert not evaluate_verdict(_verdict(passed=True, sharpe=0.1), th)
    assert not evaluate_verdict(_verdict(passed=True, trades=3), th)
    assert not evaluate_verdict(_verdict(passed=False), th)
    assert not evaluate_verdict(_verdict(passed=True, dd=80.0), th)


def test_compare_detects_regression_and_improvement():
    base = {
        "name": "base", "mean_sharpe_passed": 0.60, "pass_rate": 0.50,
        "results": {
            "ema_cross/BTC": {"strategy": "ema_cross", "currency": "BTC",
                              "passed": True, "sharpe": 0.6},
            "rsi_revert/BTC": {"strategy": "rsi_revert", "currency": "BTC",
                               "passed": True, "sharpe": 0.5},
        },
    }
    cand = {
        "name": "cand", "mean_sharpe_passed": 0.40, "pass_rate": 0.25,
        "results": {
            "ema_cross/BTC": {"strategy": "ema_cross", "currency": "BTC",
                              "passed": True, "sharpe": 0.4},
            "rsi_revert/BTC": {"strategy": "rsi_revert", "currency": "BTC",
                               "passed": False, "sharpe": 0.0},
        },
    }
    rep = compare(base, cand, max_pass_rate_drop=0.05)
    assert not rep["passed"]
    assert "rsi_revert/BTC" in rep["new_failures"]
    assert rep["pass_rate_drop"] == 0.25

    cand2 = {
        "name": "cand2", "mean_sharpe_passed": 0.70, "pass_rate": 0.50,
        "results": {
            "ema_cross/BTC": {"strategy": "ema_cross", "currency": "BTC",
                              "passed": True, "sharpe": 0.8},
            "rsi_revert/BTC": {"strategy": "rsi_revert", "currency": "BTC",
                               "passed": True, "sharpe": 0.6},
        },
    }
    rep2 = compare(base, cand2, max_pass_rate_drop=0.05)
    assert rep2["passed"]
    assert rep2["improved"]


def test_run_experiment_with_synthetic_data(tmp_path, monkeypatch):
    """End-to-end executor test: write synthetic candles to a temp feed_cache,
    run an experiment, and assert a valid scorecard comes out."""
    import math
    import data.feed_cache as fc
    from scripts.backtest_framework import run_experiment as re

    # Point feed_cache at a temp NAS root and reload so _root() resolves there.
    monkeypatch.setenv("NAS_FEED_ROOT", str(tmp_path))
    import importlib
    importlib.reload(fc)
    importlib.reload(re)

    # Build a trending series so at least one strategy produces trades.
    n = 400
    rows = []
    price = 100.0
    for i in range(n):
        price *= 1.0 + 0.001 * math.sin(i / 10.0) + 0.0003
        rows.append([float(i), price * 0.999, price * 1.002, price * 0.997, price, price * 10.0])
    fc.save_candles("coinbase_candles", "BTC-USD", 3600, rows)

    exp = Experiment(name="itest", strategies="rust", universe="BTC-USD",
                     granularity=3600, window_bars=5000,
                     thresholds={"min_win_rate": 0.0, "min_sharpe": -99.0,
                                  "min_profit_factor": 0.0, "min_trades": 1,
                                  "max_drawdown_pct": 100.0})
    scorecard, raw = re.run(exp)
    assert scorecard["name"] == "itest"
    assert scorecard["n_symbols"] == 1
    assert scorecard["n_strategies_tested"] > 0
    # at least one verdict was produced (engine ran without fatal error)
    assert len(raw) > 0
    # scorecard results keyed by strategy/currency
    assert all("/" in k for k in scorecard["results"])


def test_run_experiment_with_walk_forward(tmp_path, monkeypatch):
    import math
    import data.feed_cache as fc
    from scripts.backtest_framework import run_experiment as re

    monkeypatch.setenv("NAS_FEED_ROOT", str(tmp_path))
    import importlib
    importlib.reload(fc)
    importlib.reload(re)

    n = 600
    rows = []
    price = 100.0
    for i in range(n):
        price *= 1.0 + 0.001 * math.sin(i / 8.0) + 0.0004
        rows.append([float(i), price * 0.999, price * 1.002, price * 0.997, price, price * 10.0])
    fc.save_candles("coinbase_candles", "BTC-USD", 3600, rows)

    exp = Experiment(name="itest_wf", strategies="rust", universe="BTC-USD",
                     granularity=3600, window_bars=5000, walk_forward_folds=4,
                     thresholds={"min_win_rate": 0.0, "min_sharpe": -99.0,
                                  "min_profit_factor": 0.0, "min_trades": 1,
                                  "max_drawdown_pct": 100.0})
    scorecard, raw = re.run(exp)
    wf = scorecard["walk_forward"]
    assert wf is not None
    assert wf["folds"] == 4
    assert "oos_mean_sharpe" in wf
    assert wf["n_evaluated"] > 0


def test_compare_cli_missing_experiment(tmp_path, monkeypatch):
    from scripts.backtest_framework import compare_experiments as ce
    monkeypatch.setattr(ce, "experiments_dir", lambda: str(tmp_path))
    try:
        ce.cli_main("nope_a", "nope_b")
        assert False, "should exit"
    except SystemExit as e:
        assert e.code  # non-zero exit on missing scorecard


def test_compare_cli_runs_on_generated():
    """Reuse the real smoke scorecards committed under scripts/experiments."""
    from scripts.backtest_framework import compare_experiments as ce
    src = ce.experiments_dir()
    if not os.path.isdir(os.path.join(src, "smoke_v1")):
        import pytest
        pytest.skip("no smoke scorecards generated yet")
    # identical-data runs => no regression => exit 0
    ce.cli_main("smoke_v1", "smoke_loose")


def test_walk_forward_make_folds():
    from scripts.backtest_framework.walk_forward import make_folds
    rows = list(range(1000))
    folds = make_folds(rows, n_folds=4)
    assert len(folds) == 4
    for train, test in folds:
        assert len(train) >= 40 and len(test) >= 40
        assert max(train) < min(test)
    assert make_folds(list(range(50)), n_folds=4) == []


def test_walk_forward_runs_on_synthetic():
    import math
    import strategy_engine as S
    from scripts.backtest_framework.walk_forward import walk_forward, aggregate_walk_forward
    rows = []
    price = 100.0
    for i in range(600):
        price *= 1.0 + 0.001 * math.sin(i / 8.0) + 0.0004
        rows.append([float(i), price * 0.999, price * 1.002, price * 0.997, price, price * 10.0])
    res = walk_forward(S, "ema_cross", "BTC", rows, n_folds=4)
    assert res["strategy"] == "ema_cross"
    assert res["n_folds"] == 4
    assert "oos_sharpe" in res and "is_sharpe" in res
    agg = aggregate_walk_forward([res])
    assert agg["n_evaluated"] == 1
    assert 0.0 <= agg["stable_rate"] <= 1.0


def test_promote_writes_yaml_and_json(tmp_path):
    from scripts.backtest_framework import promote
    scorecard = {
        "name": "smoke_v1",
        "pass_rate": 0.1,
        "mean_sharpe_passed": 0.2,
        "thresholds": {"min_sharpe": 0.3},
        "results": {
            "ema_cross/BTC": {"strategy": "ema_cross", "currency": "BTC", "passed": True},
            "rsi_revert/BTC": {"strategy": "rsi_revert", "currency": "BTC", "passed": False},
        },
    }
    sc_path = tmp_path / "scorecard.json"
    sc_path.write_text(json.dumps(scorecard))
    out = tmp_path / "configs"
    res = promote.promote("promo_x", str(sc_path), str(out))
    assert res["n_strategies"] == 1
    yaml_text = open(res["yaml"]).read()
    assert "enabled_strategies: [ema_cross]" in yaml_text
    assert "mode: PAPER" in yaml_text
    assert "promoted_from_experiment: smoke_v1" in yaml_text
    rec = json.loads(open(res["json"]).read())
    assert rec["passing_strategies"] == ["ema_cross"]
    assert rec["thresholds"]["min_sharpe"] == 0.3

    res2 = promote.promote("promo_live", str(sc_path), str(out), mode="LIVE")
    assert "mode: LIVE" in open(res2["yaml"]).read()


def test_ensemble_consensus_detects_multi_group():
    from scripts.backtest_framework import ensemble
    # Two strategies in DIFFERENT independence groups both pass on BTC.
    results = {
        "ema_cross/BTC": {"strategy": "ema_cross", "currency": "BTC", "passed": True, "sharpe": 0.8},
        "rsi_revert/BTC": {"strategy": "rsi_revert", "currency": "BTC", "passed": True, "sharpe": 0.6},
        "macd/BTC": {"strategy": "macd", "currency": "BTC", "passed": False, "sharpe": -0.2},
    }
    c = ensemble.ensemble_consensus(results, asset_class="growth", min_groups=2)
    assert c["BTC"]["n_passing_groups"] == 2  # trend + momentum
    assert c["BTC"]["consensus"] is True
    assert c["BTC"]["ensemble_sharpe"] > 0

    # Only one group passes -> no consensus.
    results2 = {
        "ema_cross/BTC": {"strategy": "ema_cross", "currency": "BTC", "passed": True, "sharpe": 0.8},
        "macd/BTC": {"strategy": "macd", "currency": "BTC", "passed": True, "sharpe": 0.5},
    }
    c2 = ensemble.ensemble_consensus(results2, asset_class="growth", min_groups=2)
    assert c2["BTC"]["n_passing_groups"] == 1  # both trend
    assert c2["BTC"]["consensus"] is False

    summary = ensemble.ensemble_summary(c)
    assert summary["consensus_coverage"] == 1.0
    assert summary["n_consensus"] == 1


def test_run_experiment_with_ensemble(tmp_path, monkeypatch):
    import math
    import data.feed_cache as fc
    from scripts.backtest_framework import run_experiment as re

    monkeypatch.setenv("NAS_FEED_ROOT", str(tmp_path))
    import importlib
    importlib.reload(fc)
    importlib.reload(re)

    n = 500
    rows = []
    price = 100.0
    for i in range(n):
        price *= 1.0 + 0.001 * math.sin(i / 8.0) + 0.0004
        rows.append([float(i), price * 0.999, price * 1.002, price * 0.997, price, price * 10.0])
    fc.save_candles("coinbase_candles", "BTC-USD", 3600, rows)

    exp = Experiment(name="itest_ens", strategies="rust", universe="BTC-USD",
                     granularity=3600, window_bars=5000, ensemble=True,
                     thresholds={"min_win_rate": 0.0, "min_sharpe": -99.0,
                                  "min_profit_factor": 0.0, "min_trades": 1,
                                  "max_drawdown_pct": 100.0})
    scorecard, raw = re.run(exp)
    assert scorecard["ensemble"] is not None
    assert "consensus_coverage" in scorecard["ensemble"]
    assert scorecard["ensemble"]["n_symbols"] == 1


def _make_rows(n, freq=0.0006, slope=0.0004):
    import math
    rows = []
    price = 100.0
    for i in range(n):
        price *= 1.0 + freq * math.sin(i / 8.0) + slope
        rows.append([float(i), price * 0.999, price * 1.002, price * 0.997, price, price * 10.0])
    return rows


def test_mtf_backtest_and_summary_direct():
    import strategy_engine as S
    from scripts.backtest_framework import multitimeframe as mtf

    rows = _make_rows(600)
    names = ["ema_cross", "rsi_revert", "macd"]
    res = mtf.mtf_backtest(S, names, "BTC", rows, rows,
                           {"min_win_rate": 0.0, "min_sharpe": -99.0,
                            "min_profit_factor": 0.0, "min_trades": 1,
                            "max_drawdown_pct": 100.0})
    assert set(res) == set(names)
    for d in res.values():
        assert {"primary_passed", "confirm_passed", "mtf_passed",
                "primary_sharpe", "confirm_sharpe"} <= d.keys()
        assert d["mtf_passed"] == (d["primary_passed"] and d["confirm_passed"])
    summ = mtf.mtf_summary(res)
    assert summ["n_strategies"] == 3
    assert summ["lift"] == 0.0  # identical rows => no lift variance
    # flat shape (single symbol) handled identically
    flat_summ = mtf.mtf_summary(res)
    assert flat_summ["n_strategies"] == 3


def test_mtf_summary_nested_shape():
    from scripts.backtest_framework import multitimeframe as mtf

    per_symbol = {
        "BTC-USD": {
            "ema_cross": {"primary_passed": True, "confirm_passed": True,
                          "mtf_passed": True, "primary_sharpe": 0.5, "confirm_sharpe": 0.6},
            "rsi_revert": {"primary_passed": True, "confirm_passed": False,
                           "mtf_passed": False, "primary_sharpe": 0.4, "confirm_sharpe": -0.1},
        },
    }
    summ = mtf.mtf_summary(per_symbol)
    assert summ["n_strategies"] == 2
    assert summ["n_mtf_passed"] == 1
    assert summ["mtf_pass_rate"] == 0.5
    assert summ["n_single_passed"] == 2
    assert summ["single_pass_rate"] == 1.0
    assert summ["lift"] == -0.5


def test_run_experiment_with_multitimeframe(tmp_path, monkeypatch):
    import math
    import data.feed_cache as fc
    from scripts.backtest_framework import run_experiment as re

    monkeypatch.setenv("NAS_FEED_ROOT", str(tmp_path))
    import importlib
    importlib.reload(fc)
    importlib.reload(re)

    # Save BTC-USD at TWO granularities (3600 + 86400) with trending rows.
    rows_3600 = _make_rows(500, freq=0.001, slope=0.0003)
    rows_86400 = _make_rows(500, freq=0.0008, slope=0.0006)
    fc.save_candles("coinbase_candles", "BTC-USD", 3600, rows_3600)
    fc.save_candles("coinbase_candles", "BTC-USD", 86400, rows_86400)

    exp = Experiment(name="itest_mtf", strategies="rust", universe="BTC-USD",
                     granularity=3600, confirm_granularity=86400, window_bars=5000,
                     thresholds={"min_win_rate": 0.0, "min_sharpe": -99.0,
                                  "min_profit_factor": 0.0, "min_trades": 1,
                                  "max_drawdown_pct": 100.0})
    scorecard, raw = re.run(exp)
    assert "multitimeframe" in scorecard
    mtf = scorecard["multitimeframe"]
    assert mtf is not None
    assert mtf["confirm_granularity"] == 86400
    assert mtf["n_strategies"] > 0
    # confirmation is a STRICTER filter: mtf pass rate cannot exceed single.
    assert mtf["mtf_pass_rate"] <= mtf["single_pass_rate"]


def _write_trending_candles(fc, n=400, drift=0.0006):
    import math
    rows = []
    price = 100.0
    for i in range(n):
        price *= 1.0 + drift + 0.001 * math.sin(i / 10.0)
        rows.append([float(i), price * 0.999, price * 1.002, price * 0.997, price, price * 10.0])
    fc.save_candles("coinbase_candles", "BTC-USD", 3600, rows)
    return rows


def test_detect_regime_slices_splits_and_merges():
    from scripts.backtest_framework import regime_experiment as rg

    # Two clearly-separated regimes: sustained uptrend, then sustained downtrend,
    # each >> min_bars so the transition is not noise-merged.
    up = [[float(i), 0, 0, 0, 100.0 * (1.0 + 0.005 * i), 10.0] for i in range(150)]
    down = [[float(150 + i), 0, 0, 0, 400.0 * (1.0 - 0.005 * i), 10.0] for i in range(150)]
    rows = up + down

    # Force the heuristic detector so behavior is deterministic without Rust.
    def detect_fn(c, h, l, v):
        return rg._heuristic_regime(c)

    slices = rg.detect_regime_slices(rows, detect_fn=detect_fn, min_bars=60)
    assert len(slices) >= 2
    regimes = [s[0] for s in slices]
    assert "uptrend" in regimes
    assert "downtrend" in regimes
    # min_bars guard: every slice has at least 60 bars
    assert all(len(s[1]) >= 60 for s in slices)


def test_regime_ensemble_per_regime_consensus():
    from scripts.backtest_framework import regime_experiment as rg

    rows = [[float(i), 0, 0, 0, 100.0 * (1.0 + 0.01 * i), 10.0] for i in range(200)]
    results = {
        "ema_cross/BTC": {"strategy": "ema_cross", "currency": "BTC",
                          "passed": True, "sharpe": 0.8},
        "rsi_revert/BTC": {"strategy": "rsi_revert", "currency": "BTC",
                           "passed": True, "sharpe": 0.6},
        "macd/BTC": {"strategy": "macd", "currency": "BTC",
                     "passed": False, "sharpe": -0.2},
    }

    def detect_fn(c, h, l, v):
        return "uptrend"

    out = rg.regime_ensemble(results, classify_fn=lambda c: "growth",
                             detect_fn=detect_fn, rows_by_currency={"BTC": rows},
                             min_groups=2, min_bars=60)
    assert out["n_regimes"] >= 1
    assert "uptrend" in out["regimes"]
    assert out["regimes"]["uptrend"]["n_consensus"] >= 1
    assert out["best_regime"] == "uptrend"


def test_run_experiment_with_regime(tmp_path, monkeypatch):
    import math
    import data.feed_cache as fc
    from scripts.backtest_framework import run_experiment as re

    monkeypatch.setenv("NAS_FEED_ROOT", str(tmp_path))
    import importlib
    importlib.reload(fc)
    importlib.reload(re)

    _write_trending_candles(fc, n=400)

    exp = Experiment(name="itest_reg", strategies="rust", universe="BTC-USD",
                     granularity=3600, window_bars=5000, ensemble=True, regime=True,
                     thresholds={"min_win_rate": 0.0, "min_sharpe": -99.0,
                                 "min_profit_factor": 0.0, "min_trades": 1,
                                 "max_drawdown_pct": 100.0})
    scorecard, raw = re.run(exp)
    assert scorecard["regime"] is not None
    assert "n_regimes" in scorecard["regime"]
    assert "regimes" in scorecard["regime"]
    assert isinstance(scorecard["regime"]["regimes"], dict)
    assert scorecard["regime"]["best_regime"] is not None



def test_run_suite_produces_scorecards_and_ledger(tmp_path, monkeypatch):
    """Drive run_suite against a tiny in-memory suite over synthetic BTC-USD
    data and assert scorecards + ledger entries are written under tmp_path."""
    import math
    import data.feed_cache as fc
    from scripts.backtest_framework import run_experiment as re

    monkeypatch.setenv("NAS_FEED_ROOT", str(tmp_path))
    import importlib
    importlib.reload(fc)
    importlib.reload(re)
    # Point the experiments dir at tmp_path so scorecards/ledger land there.
    from scripts.backtest_framework import experiment as exp_mod
    monkeypatch.setattr(exp_mod, "experiments_dir", lambda: str(tmp_path / "experiments"))
    importlib.reload(re)

    n = 500
    rows = []
    price = 100.0
    for i in range(n):
        price *= 1.0 + 0.001 * math.sin(i / 8.0) + 0.0004
        rows.append([float(i), price * 0.999, price * 1.002, price * 0.997, price, price * 10.0])
    fc.save_candles("coinbase_candles", "BTC-USD", 3600, rows)

    from scripts.backtest_framework import run_suite
    specs = [
        {"name": "baseline_1h", "strategies": "rust", "universe": "BTC-USD",
         "granularity": 3600, "window_bars": 5000,
         "thresholds": {"min_win_rate": 0.0, "min_sharpe": -99.0,
                        "min_profit_factor": 0.0, "min_trades": 1, "max_drawdown_pct": 100.0}},
        {"name": "ensemble_1h", "strategies": "rust", "universe": "BTC-USD",
         "granularity": 3600, "window_bars": 5000, "ensemble": True,
         "thresholds": {"min_win_rate": 0.0, "min_sharpe": -99.0,
                        "min_profit_factor": 0.0, "min_trades": 1, "max_drawdown_pct": 100.0}},
    ]
    result = run_suite.run_suite(specs, "baseline_1h", fail_on_regression=True)
    assert result["n_ran"] == 2
    assert result["passed"]

    exp_dir = tmp_path / "experiments"
    assert (exp_dir / "baseline_1h" / "scorecard.json").exists()
    assert (exp_dir / "ensemble_1h" / "scorecard.json").exists()
    ledger = exp_dir / "ledger.jsonl"
    assert ledger.exists()
    lines = [l for l in ledger.read_text().splitlines() if l.strip()]
    assert len(lines) == 2
    for l in lines:
        json.loads(l)  # each entry is valid json


def test_report_ledger_parses_fake_ledger(tmp_path, monkeypatch, capsys):
    """report_ledger parses a ledger (with a corrupt line) gracefully."""
    from scripts.backtest_framework import experiment as exp_mod
    from scripts.backtest_framework import run_suite

    ledger = tmp_path / "ledger.jsonl"
    ledger.write_text(
        '{"name": "baseline_1h", "pass_rate": 0.5, "n_passed": 10, "mean_sharpe_passed": 0.7}\n'
        "this is not json\n"
        '{"name": "ensemble_1h", "pass_rate": 0.4, "n_passed": 8, "mean_sharpe_passed": 0.6}\n'
    )
    monkeypatch.setattr(exp_mod, "experiments_dir", lambda: str(tmp_path))
    run_suite.report_ledger()
    out = capsys.readouterr().out
    assert "baseline_1h" in out
    assert "ensemble_1h" in out
    assert "not json" not in out


def test_run_suite_regression_fails(tmp_path, monkeypatch):
    """A candidate that drops pass_rate below baseline is flagged + exits 1."""
    import math
    import data.feed_cache as fc
    from scripts.backtest_framework import run_experiment as re

    monkeypatch.setenv("NAS_FEED_ROOT", str(tmp_path))
    import importlib
    importlib.reload(fc)
    importlib.reload(re)
    from scripts.backtest_framework import experiment as exp_mod
    monkeypatch.setattr(exp_mod, "experiments_dir", lambda: str(tmp_path / "experiments"))
    importlib.reload(re)

    n = 500
    rows = []
    price = 100.0
    for i in range(n):
        price *= 1.0 + 0.001 * math.sin(i / 8.0) + 0.0004
        rows.append([float(i), price * 0.999, price * 1.002, price * 0.997, price, price * 10.0])
    fc.save_candles("coinbase_candles", "BTC-USD", 3600, rows)

    from scripts.backtest_framework import run_suite
    base_spec = {"name": "baseline_1h", "strategies": "rust", "universe": "BTC-USD",
                 "granularity": 3600, "window_bars": 5000,
                 "thresholds": {"min_win_rate": 0.0, "min_sharpe": -99.0,
                                "min_profit_factor": 0.0, "min_trades": 1, "max_drawdown_pct": 100.0}}
    re._persist(run_experiment_run(base_spec), _exp(base_spec))

    cand_spec = dict(base_spec)
    cand_spec["name"] = "candidate_worse"
    # worse thresholds => fewer passes => regression vs baseline
    cand_spec["thresholds"] = {"min_win_rate": 0.99, "min_sharpe": 99.0,
                               "min_profit_factor": 99.0, "min_trades": 1, "max_drawdown_pct": 0.0001}
    re._persist(run_experiment_run(cand_spec), _exp(cand_spec))

    specs = [base_spec, cand_spec]
    try:
        run_suite.run_suite(specs, "baseline_1h", fail_on_regression=True)
        assert False, "expected SystemExit(1)"
    except SystemExit as e:
        assert e.code == 1


def _exp(spec):
    import data.feed_cache as fc  # noqa: F401
    from scripts.backtest_framework import experiment as exp_mod
    thresholds = dict(exp_mod.DEFAULT_THRESHOLDS)
    thresholds.update(spec.get("thresholds", {}))
    return exp_mod.Experiment(
        name=spec["name"], strategies=spec.get("strategies", "rust"),
        universe=spec.get("universe", "all-harvested"),
        granularity=spec.get("granularity", 3600),
        window_bars=spec.get("window_bars", 5000),
        thresholds=thresholds, ensemble=spec.get("ensemble", False),
        ensemble_min_groups=spec.get("ensemble_min_groups", 2) or 2,
        walk_forward_folds=spec.get("walk_forward_folds", 0) or 0,
    )


def run_experiment_run(spec):
    from scripts.backtest_framework import run_experiment as re
    sc, _ = re.run(_exp(spec))
    return sc


# ----------------------------- replay_trades (E20) -----------------------------


def _write_trade_events(events_dir, product, lines):
    os.makedirs(events_dir, exist_ok=True)
    fp = os.path.join(events_dir, f"{product}.jsonl")
    with open(fp, "w", encoding="utf-8") as fh:
        fh.write("\n".join(lines) + "\n")
    return fp


def test_load_trade_events_pairs_entry_exit_and_open(tmp_path):
    from scripts.backtest_framework import replay_trades as rt

    # entry1 -> exit1 (win), entry2 -> no exit (open)
    lines = [
        json.dumps({"ts": 1.0, "kind": "entry", "product_id": "TEST-USD",
                    "price": 100.0, "side": "LONG", "strategy": "ema_cross",
                    "qty": 1.0, "notional": 100.0}),
        json.dumps({"ts": 2.0, "kind": "exit", "product_id": "TEST-USD",
                    "price": 110.0, "side": "SELL", "strategy": "ema_cross",
                    "pnl": 10.0, "reason": "target"}),
        json.dumps({"ts": 3.0, "kind": "entry", "product_id": "TEST-USD",
                    "price": 105.0, "side": "LONG", "strategy": "rsi_revert",
                    "qty": 1.0, "notional": 105.0}),
    ]
    _write_trade_events(str(tmp_path), "TEST-USD", lines)

    trades = rt.load_trade_events(str(tmp_path), product="TEST-USD")
    assert len(trades) == 2
    closed = [t for t in trades if t["pnl_usd"] is not None]
    open_t = [t for t in trades if t["pnl_usd"] is None]
    assert len(closed) == 1 and len(open_t) == 1
    assert closed[0]["product"] == "TEST-USD"
    assert closed[0]["entry_price"] == 100.0
    assert closed[0]["exit_price"] == 110.0
    assert closed[0]["pnl_usd"] == 10.0
    assert closed[0]["strategy"] == "ema_cross"
    assert open_t[0]["pnl_usd"] is None
    assert open_t[0]["exit_price"] is None


def test_score_replay_computes_winrate_pnl(tmp_path):
    from scripts.backtest_framework import replay_trades as rt

    lines = [
        json.dumps({"ts": 1.0, "kind": "entry", "product_id": "TEST-USD",
                    "price": 100.0, "side": "LONG", "strategy": "ema_cross",
                    "qty": 1.0, "notional": 100.0}),
        json.dumps({"ts": 2.0, "kind": "exit", "product_id": "TEST-USD",
                    "price": 110.0, "side": "SELL", "strategy": "ema_cross",
                    "pnl": 10.0, "reason": "target"}),
        json.dumps({"ts": 3.0, "kind": "entry", "product_id": "TEST-USD",
                    "price": 50.0, "side": "LONG", "strategy": "rsi_revert",
                    "qty": 1.0, "notional": 50.0}),
        json.dumps({"ts": 4.0, "kind": "exit", "product_id": "TEST-USD",
                    "price": 40.0, "side": "SELL", "strategy": "rsi_revert",
                    "pnl": -10.0, "reason": "stop"}),
        json.dumps({"ts": 5.0, "kind": "entry", "product_id": "TEST-USD",
                    "price": 20.0, "side": "LONG", "strategy": "rsi_revert",
                    "qty": 1.0, "notional": 20.0}),  # open
    ]
    _write_trade_events(str(tmp_path), "TEST-USD", lines)

    trades = rt.load_trade_events(str(tmp_path), product="TEST-USD")
    score = rt.score_replay(trades)
    assert score["n_trades"] == 3
    assert score["n_with_exit"] == 2
    assert score["n_open"] == 1
    assert abs(score["win_rate"] - 0.5) < 1e-9
    assert abs(score["total_pnl_usd"] - 0.0) < 1e-9
    assert abs(score["mean_pnl_usd"] - 0.0) < 1e-9
    assert "ema_cross" in score["by_strategy"]
    assert score["by_strategy"]["ema_cross"]["win_rate"] == 1.0
    assert score["by_strategy"]["rsi_revert"]["win_rate"] == 0.0


def test_load_trade_events_handles_empty_and_missing(tmp_path):
    from scripts.backtest_framework import replay_trades as rt
    # missing dir -> empty list, no raise
    assert rt.load_trade_events(os.path.join(str(tmp_path), "nope")) == []
    # empty dir -> empty list
    d = os.path.join(str(tmp_path), "trade_events")
    os.makedirs(d, exist_ok=True)
    assert rt.load_trade_events(str(tmp_path)) == []


def test_compare_to_backtest_flags_consistency(tmp_path):
    from scripts.backtest_framework import replay_trades as rt

    lines = [
        json.dumps({"ts": 1.0, "kind": "entry", "product_id": "TEST-USD",
                    "price": 100.0, "side": "LONG", "strategy": "ema_cross",
                    "qty": 1.0, "notional": 100.0}),
        json.dumps({"ts": 2.0, "kind": "exit", "product_id": "TEST-USD",
                    "price": 110.0, "side": "SELL", "strategy": "ema_cross",
                    "pnl": 10.0, "reason": "target"}),
        json.dumps({"ts": 3.0, "kind": "entry", "product_id": "TEST-USD",
                    "price": 50.0, "side": "LONG", "strategy": "rsi_revert",
                    "qty": 1.0, "notional": 50.0}),
        json.dumps({"ts": 4.0, "kind": "exit", "product_id": "TEST-USD",
                    "price": 55.0, "side": "SELL", "strategy": "rsi_revert",
                    "pnl": 5.0, "reason": "target"}),
    ]
    _write_trade_events(str(tmp_path), "TEST-USD", lines)
    trades = rt.load_trade_events(str(tmp_path), product="TEST-USD")

    scorecard = {
        "ema_cross/BTC": {"strategy": "ema_cross", "currency": "BTC",
                          "passed": True, "win_rate": 0.50},
        "rsi_revert/BTC": {"strategy": "rsi_revert", "currency": "BTC",
                           "passed": False, "win_rate": 0.05},
    }
    comp = rt.compare_to_backtest(trades, scorecard)
    by_s = {r["strategy"]: r for r in comp}
    assert "ema_cross" in by_s and "rsi_revert" in by_s
    # live win_rate 1.0 vs bt 0.50 -> 50pp divergence -> not consistent
    assert by_s["ema_cross"]["live_win_rate"] == 1.0
    assert by_s["ema_cross"]["consistent"] is False
    # live win_rate 1.0 vs bt 0.05 -> divergent; bt_passed False
    assert by_s["rsi_revert"]["bt_passed"] is False
    assert by_s["rsi_revert"]["consistent"] is False

    # Tight tolerance; both still flagged (live beats backtest widely).
    # Use a synthetic near-match to prove consistency True path.
    sc2 = {
        "ema_cross/BTC": {"strategy": "ema_cross", "currency": "BTC",
                          "passed": True, "win_rate": 0.95},
    }
    comp2 = rt.compare_to_backtest(trades, sc2, tol_pp=20.0)
    assert comp2[0]["consistent"] is True


# --------------------- run_experiment.py coverage boosters ---------------------


import math as _re_math
import importlib as _re_importlib


def _re_write(tmp_path, monkeypatch, symbols=("BTC-USD",), n=500, gran=3600):
    monkeypatch.setenv("NAS_FEED_ROOT", str(tmp_path))
    import data.feed_cache as fc
    _re_importlib.reload(fc)
    import scripts.backtest_framework.run_experiment as re
    _re_importlib.reload(re)
    rows = []
    price = 100.0
    for i in range(n):
        price *= 1.0 + 0.001 * _re_math.sin(i / 8.0) + 0.0004
        rows.append([float(i), price * 0.999, price * 1.002, price * 0.997, price, price * 10.0])
    for s in symbols:
        fc.save_candles("coinbase_candles", s, gran, rows)
    return re


_LOOSE = {"min_win_rate": 0.0, "min_sharpe": -99.0, "min_profit_factor": 0.0,
          "min_trades": 1, "max_drawdown_pct": 100.0}


def test_re_discover_symbols_all_harvested_and_list(tmp_path, monkeypatch):
    re = _re_write(tmp_path, monkeypatch, symbols=("BTC-USD", "ETH-USD"), n=100)
    # all-harvested scans the feed_cache dir
    found = re._discover_symbols(3600, "all-harvested")
    assert set(found) == {"BTC-USD", "ETH-USD"}
    # comma list path
    assert re._discover_symbols(3600, "SOL-USD, DOGE-USD") == ["SOL-USD", "DOGE-USD"]
    # empty universe also treated as all-harvested
    assert set(re._discover_symbols(3600, "")) == {"BTC-USD", "ETH-USD"}
    # granularity with no parquet -> empty
    assert re._discover_symbols(999, "all-harvested") == []


def test_re_discover_symbols_missing_dir(tmp_path, monkeypatch):
    # Point NAS root at an empty dir with no coinbase_candles subdir.
    empty = tmp_path / "empty_root"
    empty.mkdir()
    monkeypatch.setenv("NAS_FEED_ROOT", str(empty))
    import data.feed_cache as fc
    _re_importlib.reload(fc)
    import scripts.backtest_framework.run_experiment as re
    _re_importlib.reload(re)
    assert re._discover_symbols(3600, "all-harvested") == []


def test_re_load_window_truncation_and_ts_filter(tmp_path, monkeypatch):
    re = _re_write(tmp_path, monkeypatch, symbols=("BTC-USD",), n=500)
    exp = Experiment(name="w", strategies="rust", universe="BTC-USD",
                     granularity=3600, window_bars=100, thresholds=dict(_LOOSE))
    rows = re._load_window("BTC-USD", 3600, exp)
    assert len(rows) == 100  # truncated to window_bars

    # start/end ts filtering
    exp2 = Experiment(name="w2", strategies="rust", universe="BTC-USD",
                      granularity=3600, window_bars=5000,
                      start_ts=10, end_ts=50, thresholds=dict(_LOOSE))
    rows2 = re._load_window("BTC-USD", 3600, exp2)
    assert rows2
    assert all(10 <= r[0] <= 50 for r in rows2)


def test_re_run_asset_class_and_short_series_skip(tmp_path, monkeypatch):
    # A too-short series (< min_trades+20) should be skipped in run().
    re = _re_write(tmp_path, monkeypatch, symbols=("BTC-USD",), n=15)
    exp = Experiment(name="short", strategies="rust", universe="BTC-USD",
                     granularity=3600, window_bars=5000, thresholds=dict(_LOOSE))
    scorecard, raw = re.run(exp)
    # series too short -> no verdicts produced
    assert raw == {}
    assert scorecard["n_strategies_tested"] == 0


def test_re_write_bt_cache_persists_rows(tmp_path, monkeypatch):
    re = _re_write(tmp_path, monkeypatch, symbols=("BTC-USD",), n=500)
    exp = Experiment(name="btc_cache", strategies="rust", universe="BTC-USD",
                     granularity=3600, window_bars=5000, thresholds=dict(_LOOSE))
    scorecard, raw = re.run(exp)
    assert raw

    db_path = str(tmp_path / "opt_state.db")
    n = re._write_bt_cache(db_path, raw, only_passed=False)
    assert n == len(raw)

    from state_store import StateStore
    store = StateStore(db_path=db_path)
    cached = store.load_bt_cache(ttl=10 ** 9)
    assert len(cached) == n

    # only_passed filter path
    db2 = str(tmp_path / "opt_state2.db")
    n_passed = re._write_bt_cache(db2, raw, only_passed=True)
    passed_ct = sum(1 for v in raw.values() if getattr(v, "passed", False))
    assert n_passed == passed_ct


def _run_cli(re, monkeypatch, argv):
    monkeypatch.setattr(sys, "argv", ["run_experiment.py"] + argv)
    return re.cli()


def test_re_cli_basic(tmp_path, monkeypatch, capsys):
    re = _re_write(tmp_path, monkeypatch, symbols=("BTC-USD",), n=500)
    from scripts.backtest_framework import experiment as exp_mod
    monkeypatch.setattr(exp_mod, "experiments_dir", lambda: str(tmp_path / "exps"))
    _re_importlib.reload(re)

    sc = _run_cli(re, monkeypatch, [
        "--name", "cli_basic", "--strategies", "rust", "--universe", "BTC-USD",
        "--granularity", "3600", "--window-bars", "5000",
        "--min-sharpe", "-99", "--min-profit-factor", "0", "--min-win-rate", "0",
        "--min-trades", "1", "--max-drawdown-pct", "100",
    ])
    out = capsys.readouterr().out
    assert sc["name"] == "cli_basic"
    assert "=== Experiment cli_basic ===" in out
    assert "Scorecard ->" in out


def test_re_cli_all_branches(tmp_path, monkeypatch, capsys):
    re = _re_write(tmp_path, monkeypatch, symbols=("BTC-USD",), n=600)
    import data.feed_cache as fc
    # add a confirm granularity dataset
    rows = []
    price = 100.0
    for i in range(600):
        price *= 1.0 + 0.0008 * _re_math.sin(i / 8.0) + 0.0006
        rows.append([float(i), price * 0.999, price * 1.002, price * 0.997, price, price * 10.0])
    fc.save_candles("coinbase_candles", "BTC-USD", 86400, rows)

    from scripts.backtest_framework import experiment as exp_mod
    monkeypatch.setattr(exp_mod, "experiments_dir", lambda: str(tmp_path / "exps"))
    _re_importlib.reload(re)

    db_path = str(tmp_path / "cli_state.db")
    sc = _run_cli(re, monkeypatch, [
        "--name", "cli_full", "--strategies", "rust", "--universe", "BTC-USD",
        "--granularity", "3600", "--window-bars", "5000",
        "--min-sharpe", "-99", "--min-profit-factor", "0", "--min-win-rate", "0",
        "--min-trades", "1", "--max-drawdown-pct", "100",
        "--walk-forward", "4", "--ensemble", "--regime",
        "--confirm-granularity", "86400",
        "--bt-cache-db", db_path, "--only-passed-cache",
    ])
    out = capsys.readouterr().out
    assert sc["name"] == "cli_full"
    assert "bt_cache: wrote" in out
    assert "walk_forward:" in out
    assert "ensemble:" in out
    # regime / multitimeframe branches may or may not print depending on data;
    # scorecard fields should be populated for wf & ensemble.
    assert sc["walk_forward"] is not None
    assert sc["ensemble"] is not None


# ----------------------------- regime_experiment (E16) coverage -----------------------------


def test_heuristic_regime_empty_returns_unknown():
    from scripts.backtest_framework import regime_experiment as rg
    assert rg._heuristic_regime([]) == "unknown"


def test_detect_regime_slices_empty_returns_empty():
    from scripts.backtest_framework import regime_experiment as rg
    assert rg.detect_regime_slices([]) == []


def test_detect_regime_slices_single_regime_window():
    from scripts.backtest_framework import regime_experiment as rg
    # Monotonic uptrend only (forced heuristic) -> a single merged slice.
    def detect_fn(c, h, l, v):
        return rg._heuristic_regime(c)
    rows = [[float(i), 0, 0, 0, 100.0 * (1.0 + 0.02 * i), 10.0] for i in range(150)]
    slices = rg.detect_regime_slices(rows, detect_fn=detect_fn, min_bars=60)
    assert len(slices) == 1
    assert slices[0][0] == "uptrend"
    assert len(slices[0][1]) == 150


def test_detect_regime_slices_tiny_window_under_min_bars():
    from scripts.backtest_framework import regime_experiment as rg
    # Window shorter than min_bars -> returns a single merged slice, no error.
    rows = [[float(i), 0, 0, 0, 100.0 * (1.0 + 0.004 * i), 10.0] for i in range(10)]
    slices = rg.detect_regime_slices(rows, min_bars=60)
    assert len(slices) == 1
    assert len(slices[0][1]) == 10


def test_detect_regime_slices_default_detect_fn(monkeypatch):
    from scripts.backtest_framework import regime_experiment as rg
    # Force the heuristic path by disabling Rust so _make_detect_fn returns lambda.
    monkeypatch.setattr(rg, "_HAS_RUST", False)
    rows = [[float(i), 0, 0, 0, 100.0 * (1.0 + 0.004 * i), 10.0] for i in range(120)]
    slices = rg.detect_regime_slices(rows, min_bars=60)
    assert len(slices) >= 1
    # heuristic => uptrend since net change > 2%
    assert slices[0][0] == "uptrend"


def test_regime_ensemble_default_rows_by_currency():
    from scripts.backtest_framework import regime_experiment as rg
    # rows_by_currency=None default -> empty per_symbol, best_regime="" gracefully.
    out = rg.regime_ensemble({}, classify_fn=lambda c: "growth")
    assert out["n_regimes"] == 0
    assert out["best_regime"] == ""


def test_regime_ensemble_skips_slice_without_verdicts():
    from scripts.backtest_framework import regime_experiment as rg
    # A currency in rows whose verdicts belong to a DIFFERENT currency => the
    # single slice yields no verdicts and is skipped (continue branch).
    rows_eth = [[float(i), 0, 0, 0, 100.0 * (1.0 + 0.004 * i), 10.0] for i in range(120)]
    results = {
        "ema_cross/BTC": {"strategy": "ema_cross", "currency": "BTC",
                          "passed": True, "sharpe": 0.8},
    }
    out = rg.regime_ensemble(results, classify_fn=lambda c: "growth",
                             detect_fn=lambda c, h, l, v: "uptrend",
                             rows_by_currency={"ETH": rows_eth}, min_bars=60)
    assert out["n_regimes"] == 0
    assert out["best_regime"] == ""


def test_verdicts_in_slice_empty_and_currency_filter():
    from scripts.backtest_framework import regime_experiment as rg
    assert rg._verdicts_in_slice({}, "BTC", []) == {}
    results = {
        "ema_cross/BTC": {"strategy": "ema_cross", "currency": "BTC", "passed": True},
        "rsi_revert/ETH": {"strategy": "rsi_revert", "currency": "ETH", "passed": True},
    }
    out = rg._verdicts_in_slice(results, "BTC", [[0.0, 0, 0, 0, 1.0, 1.0]])
    assert set(out) == {"ema_cross/BTC"}  # ETH filtered out


def test_regime_ensemble_no_consensus_branches():
    from scripts.backtest_framework import regime_experiment as rg
    # Only ONE passing group across two symbols => no consensus, but summary +
    # best_regime selection still runs (mean_ensemble_sharpe summed, coverage 0).
    rows_a = [[float(i), 0, 0, 0, 100.0 * (1.0 + 0.004 * i), 10.0] for i in range(120)]
    rows_b = [[float(i), 0, 0, 0, 100.0 * (1.0 + 0.004 * i), 10.0] for i in range(120)]
    results = {
        "ema_cross/A": {"strategy": "ema_cross", "currency": "A",
                        "passed": True, "sharpe": 0.7},
        "macd/A": {"strategy": "macd", "currency": "A",
                   "passed": True, "sharpe": 0.5},  # both trend -> 1 group
        "ema_cross/B": {"strategy": "ema_cross", "currency": "B",
                        "passed": True, "sharpe": 0.3},
    }
    out = rg.regime_ensemble(results, classify_fn=lambda c: "growth",
                             detect_fn=lambda c, h, l, v: "uptrend",
                             rows_by_currency={"A": rows_a, "B": rows_b},
                             min_groups=2, min_bars=60)
    assert out["n_regimes"] == 1
    reg = out["regimes"]["uptrend"]
    assert reg["n_consensus"] == 0
    assert reg["consensus_coverage"] == 0.0
    assert reg["symbols"] == ["A", "B"]
    assert reg["mean_ensemble_sharpe"] > 0  # summed across both, no consensus
    assert out["best_regime"] == "uptrend"


def test_load_trade_events_corrupt_line_skipped(tmp_path):
    from scripts.backtest_framework import replay_trades as rt

    # corrupt (non-JSON) line between two valid records
    lines = [
        json.dumps({"ts": 1.0, "kind": "entry", "product_id": "TEST-USD",
                    "price": 100.0, "side": "LONG", "strategy": "ema_cross",
                    "qty": 1.0, "notional": 100.0}),
        "not valid json {{{",
        "   ",  # blank line
        json.dumps({"ts": 2.0, "kind": "exit", "product_id": "TEST-USD",
                    "price": 110.0, "side": "SELL", "strategy": "ema_cross",
                    "pnl": 10.0, "reason": "target"}),
        json.dumps({"entry_only": 1, "no_kind": True}),  # missing kind -> skipped
    ]
    _write_trade_events(str(tmp_path), "TEST-USD", lines)
    trades = rt.load_trade_events(str(tmp_path), product="TEST-USD")
    assert len(trades) == 1
    assert trades[0]["pnl_usd"] == 10.0


def test_load_trade_events_exit_without_entry(tmp_path):
    from scripts.backtest_framework import replay_trades as rt

    # exit with no preceding entry -> handled gracefully
    lines = [
        json.dumps({"ts": 2.0, "kind": "exit", "product_id": "TEST-USD",
                    "price": 110.0, "side": "SELL", "strategy": "ema_cross",
                    "pnl": 10.0, "reason": "target"}),
    ]
    _write_trade_events(str(tmp_path), "TEST-USD", lines)
    trades = rt.load_trade_events(str(tmp_path), product="TEST-USD")
    assert len(trades) == 1
    # entry values None, exit values present, strategy from exit
    assert trades[0]["entry_ts"] is None
    assert trades[0]["exit_price"] == 110.0
    assert trades[0]["pnl_usd"] == 10.0
    assert trades[0]["strategy"] == "ema_cross"


def test_load_trade_events_multi_product(tmp_path):
    from scripts.backtest_framework import replay_trades as rt

    lines_a = [
        json.dumps({"ts": 1.0, "kind": "entry", "product_id": "AAA-USD",
                    "price": 100.0, "side": "LONG", "strategy": "ema_cross",
                    "qty": 1.0, "notional": 100.0}),
        json.dumps({"ts": 2.0, "kind": "exit", "product_id": "AAA-USD",
                    "price": 120.0, "side": "SELL", "strategy": "ema_cross",
                    "pnl": 20.0, "reason": "target"}),
    ]
    lines_b = [
        json.dumps({"ts": 3.0, "kind": "entry", "product_id": "BBB-USD",
                    "price": 50.0, "side": "LONG", "strategy": "rsi_revert",
                    "qty": 1.0, "notional": 50.0}),
        json.dumps({"ts": 4.0, "kind": "exit", "product_id": "BBB-USD",
                    "price": 40.0, "side": "SELL", "strategy": "rsi_revert",
                    "pnl": -10.0, "reason": "stop"}),
    ]
    _write_trade_events(str(tmp_path), "AAA-USD", lines_a)
    _write_trade_events(str(tmp_path), "BBB-USD", lines_b)

    # load all (no product filter)
    trades = rt.load_trade_events(str(tmp_path))
    assert len(trades) == 2
    prods = sorted(t["product"] for t in trades)
    assert prods == ["AAA-USD", "BBB-USD"]

    # product filter restricts to one file
    trades_a = rt.load_trade_events(str(tmp_path), product="AAA-USD")
    assert len(trades_a) == 1
    assert trades_a[0]["product"] == "AAA-USD"


def test_load_trade_events_missing_product_file(tmp_path):
    from scripts.backtest_framework import replay_trades as rt

    _write_trade_events(str(tmp_path), "AAA-USD", [
        json.dumps({"ts": 1.0, "kind": "entry", "product_id": "AAA-USD",
                    "price": 100.0, "side": "LONG", "strategy": "ema_cross",
                    "qty": 1.0, "notional": 100.0}),
    ])
    # product requested that has no file -> empty, no crash
    trades = rt.load_trade_events(str(tmp_path), product="ZZZ-USD")
    assert trades == []


def test_load_trade_events_env_root_resolution(tmp_path, monkeypatch):
    from scripts.backtest_framework import replay_trades as rt

    # NAS_FEED_ROOT/<trade_events> dir resolution branch
    nas_root = tmp_path / "nas"
    te_dir = nas_root / "trade_events"
    te_dir.mkdir(parents=True)
    _write_trade_events(str(te_dir), "TEST-USD", [
        json.dumps({"ts": 1.0, "kind": "entry", "product_id": "TEST-USD",
                    "price": 100.0, "side": "LONG", "strategy": "ema_cross",
                    "qty": 1.0, "notional": 100.0}),
        json.dumps({"ts": 2.0, "kind": "exit", "product_id": "TEST-USD",
                    "price": 110.0, "side": "SELL", "strategy": "ema_cross",
                    "pnl": 10.0, "reason": "target"}),
    ])
    monkeypatch.setenv("NAS_FEED_ROOT", str(nas_root))
    trades = rt.load_trade_events(None)
    assert len(trades) == 1
    assert trades[0]["pnl_usd"] == 10.0

    # NAS_FEED_ROOT set but no trade_events dir -> falls back to default root
    monkeypatch.setenv("NAS_FEED_ROOT", str(tmp_path / "no_te"))
    assert rt.load_trade_events(None) == []


def test_score_replay_no_trades():
    from scripts.backtest_framework import replay_trades as rt

    score = rt.score_replay([])
    assert score["n_trades"] == 0
    assert score["win_rate"] == 0.0
    assert score["total_pnl_usd"] == 0.0
    assert score["mean_pnl_usd"] == 0.0
    assert score["by_strategy"] == {}


def test_compare_to_backtest_strategy_not_in_backtest(tmp_path):
    from scripts.backtest_framework import replay_trades as rt

    lines = [
        json.dumps({"ts": 1.0, "kind": "entry", "product_id": "TEST-USD",
                    "price": 100.0, "side": "LONG", "strategy": "ema_cross",
                    "qty": 1.0, "notional": 100.0}),
        json.dumps({"ts": 2.0, "kind": "exit", "product_id": "TEST-USD",
                    "price": 110.0, "side": "SELL", "strategy": "ema_cross",
                    "pnl": 10.0, "reason": "target"}),
    ]
    _write_trade_events(str(tmp_path), "TEST-USD", lines)
    trades = rt.load_trade_events(str(tmp_path), product="TEST-USD")

    # scorecard with no matching strategy -> empty comparison
    assert rt.compare_to_backtest(trades, {}) == []
    # empty scorecard_results -> empty, no crash
    assert rt.compare_to_backtest(trades, None) == []
    # result missing 'strategy' key -> skipped
    assert rt.compare_to_backtest(trades, {"x/BTC": {"passed": True}}) == []


def test_compare_to_backtest_open_trade_excluded(tmp_path):
    from scripts.backtest_framework import replay_trades as rt

    # one closed win + one open (pnl None) for same strategy
    lines = [
        json.dumps({"ts": 1.0, "kind": "entry", "product_id": "TEST-USD",
                    "price": 100.0, "side": "LONG", "strategy": "ema_cross",
                    "qty": 1.0, "notional": 100.0}),
        json.dumps({"ts": 2.0, "kind": "exit", "product_id": "TEST-USD",
                    "price": 110.0, "side": "SELL", "strategy": "ema_cross",
                    "pnl": 10.0, "reason": "target"}),
        json.dumps({"ts": 3.0, "kind": "entry", "product_id": "TEST-USD",
                    "price": 50.0, "side": "LONG", "strategy": "ema_cross",
                    "qty": 1.0, "notional": 50.0}),
    ]
    _write_trade_events(str(tmp_path), "TEST-USD", lines)
    trades = rt.load_trade_events(str(tmp_path), product="TEST-USD")
    sc = {"ema_cross/BTC": {"strategy": "ema_cross", "currency": "BTC",
                            "passed": True, "win_rate": 1.0}}
    comp = rt.compare_to_backtest(trades, sc)
    assert len(comp) == 1
    assert comp[0]["live_win_rate"] == 1.0  # only the closed trade counts


def test_cli_main_runs_and_summarizes(tmp_path, capsys):
    from scripts.backtest_framework import replay_trades as rt

    lines = [
        json.dumps({"ts": 1.0, "kind": "entry", "product_id": "TEST-USD",
                    "price": 100.0, "side": "LONG", "strategy": "ema_cross",
                    "qty": 1.0, "notional": 100.0}),
        json.dumps({"ts": 2.0, "kind": "exit", "product_id": "TEST-USD",
                    "price": 110.0, "side": "SELL", "strategy": "ema_cross",
                    "pnl": 10.0, "reason": "target"}),
    ]
    _write_trade_events(str(tmp_path), "TEST-USD", lines)
    rc = rt.cli_main(events_dir=str(tmp_path), product="TEST-USD")
    assert rc == 0
    out = capsys.readouterr().out
    assert "REPLAY SCORING" in out
    assert "win_rate" in out
    assert "by_strategy" in out


def test_cli_main_no_events(tmp_path, capsys):
    from scripts.backtest_framework import replay_trades as rt

    rc = rt.cli_main(events_dir=str(tmp_path / "empty"))
    assert rc == 0
    out = capsys.readouterr().out
    assert "No trade events found" in out


def test_cli_main_with_scorecard(tmp_path, capsys):
    from scripts.backtest_framework import replay_trades as rt

    lines = [
        json.dumps({"ts": 1.0, "kind": "entry", "product_id": "TEST-USD",
                    "price": 100.0, "side": "LONG", "strategy": "ema_cross",
                    "qty": 1.0, "notional": 100.0}),
        json.dumps({"ts": 2.0, "kind": "exit", "product_id": "TEST-USD",
                    "price": 110.0, "side": "SELL", "strategy": "ema_cross",
                    "pnl": 10.0, "reason": "target"}),
    ]
    _write_trade_events(str(tmp_path), "TEST-USD", lines)

    sc = {"results": {"ema_cross/BTC": {"strategy": "ema_cross",
                                        "currency": "BTC", "passed": True,
                                        "win_rate": 0.95}}}
    sc_path = tmp_path / "scorecard.json"
    sc_path.write_text(json.dumps(sc))

    rc = rt.cli_main(events_dir=str(tmp_path), scorecard_path=str(sc_path))
    assert rc == 0
    out = capsys.readouterr().out
    assert "LIVE vs BACKTEST" in out
    assert "consistent=True" in out

    # corrupt scorecard -> graceful message, no crash
    bad = tmp_path / "bad.json"
    bad.write_text("not json")
    rc2 = rt.cli_main(events_dir=str(tmp_path), scorecard_path=str(bad))
    assert rc2 == 0
    out2 = capsys.readouterr().out
    assert "Could not parse scorecard" in out2


def test_cli_argv_handling(tmp_path, monkeypatch, capsys):
    from scripts.backtest_framework import replay_trades as rt

    lines = [
        json.dumps({"ts": 1.0, "kind": "entry", "product_id": "TEST-USD",
                    "price": 100.0, "side": "LONG", "strategy": "ema_cross",
                    "qty": 1.0, "notional": 100.0}),
        json.dumps({"ts": 2.0, "kind": "exit", "product_id": "TEST-USD",
                    "price": 110.0, "side": "SELL", "strategy": "ema_cross",
                    "pnl": 10.0, "reason": "target"}),
    ]
    _write_trade_events(str(tmp_path), "TEST-USD", lines)

    monkeypatch.setattr(sys, "argv", [
        "replay_trades.py", "--events-dir", str(tmp_path),
        "--product", "TEST-USD",
    ])
    try:
        rt.cli()
    except SystemExit as e:
        assert e.code == 0
    out = capsys.readouterr().out
    assert "REPLAY SCORING" in out

    # also works with the committed smoke scorecard
    sc_path = os.path.join(
        os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
        "scripts", "experiments", "smoke_v1", "scorecard.json")
    if os.path.isfile(sc_path):
        monkeypatch.setattr(sys, "argv", [
            "replay_trades.py", "--events-dir", str(tmp_path),
            "--scorecard", sc_path, "--product", "TEST-USD",
        ])
        try:
            rt.cli()
        except SystemExit as e:
            assert e.code == 0



# ----------------------- coverage: run_suite / compare / experiment / wf / mtf -----------------------
import math  # noqa: E402,F401  (appended coverage tests use math directly)


def _setup(tmp_path, monkeypatch, symbols=("BTC-USD",), n=500, gran=3600):
    import math
    import importlib
    import data.feed_cache as fc
    importlib.reload(fc)
    import scripts.backtest_framework.run_experiment as re
    importlib.reload(re)
    import scripts.backtest_framework.run_suite as rs
    importlib.reload(rs)
    rows = []
    price = 100.0
    for i in range(n):
        price *= 1.0 + 0.001 * math.sin(i / 8.0) + 0.0004
        rows.append([float(i), price * 0.999, price * 1.002, price * 0.997, price, price * 10.0])
    for s in symbols:
        fc.save_candles("coinbase_candles", s, gran, rows)
    return re, rs


def test_run_suite_no_baseline_warns(tmp_path, monkeypatch, capsys):
    """run_suite with no baseline found -> warning path, passes."""
    import math
    import importlib
    import data.feed_cache as fc
    import scripts.backtest_framework.run_experiment as re
    monkeypatch.setenv("NAS_FEED_ROOT", str(tmp_path))
    importlib.reload(fc)
    importlib.reload(re)
    from scripts.backtest_framework import experiment as exp_mod
    monkeypatch.setattr(exp_mod, "experiments_dir", lambda: str(tmp_path / "experiments"))
    importlib.reload(re)
    from scripts.backtest_framework import run_suite

    n = 500
    rows = []
    price = 100.0
    for i in range(n):
        price *= 1.0 + 0.001 * math.sin(i / 8.0) + 0.0004
        rows.append([float(i), price * 0.999, price * 1.002, price * 0.997, price, price * 10.0])
    fc.save_candles("coinbase_candles", "BTC-USD", 3600, rows)

    specs = [{"name": "solo_a", "strategies": "rust", "universe": "BTC-USD",
              "granularity": 3600, "window_bars": 5000,
              "thresholds": {"min_win_rate": 0.0, "min_sharpe": -99.0,
                             "min_profit_factor": 0.0, "min_trades": 1, "max_drawdown_pct": 100.0}}]
    # explicitly request a baseline name that does not exist -> None baseline
    result = run_suite.run_suite(specs, "missing_baseline", fail_on_regression=True)
    assert result["baseline"] is None
    assert result["passed"]


def test_run_suite_universe_override_applied(tmp_path, monkeypatch):
    """universe_override rewrites every spec's universe before running."""
    import math
    import importlib
    import data.feed_cache as fc
    import scripts.backtest_framework.run_experiment as re
    monkeypatch.setenv("NAS_FEED_ROOT", str(tmp_path))
    importlib.reload(fc)
    importlib.reload(re)
    from scripts.backtest_framework import experiment as exp_mod
    monkeypatch.setattr(exp_mod, "experiments_dir", lambda: str(tmp_path / "experiments"))
    importlib.reload(re)
    from scripts.backtest_framework import run_suite

    n = 500
    rows = []
    price = 100.0
    for i in range(n):
        price *= 1.0 + 0.001 * math.sin(i / 8.0) + 0.0004
        rows.append([float(i), price * 0.999, price * 1.002, price * 0.997, price, price * 10.0])
    fc.save_candles("coinbase_candles", "BTC-USD", 3600, rows)

    specs = [{"name": "override_me", "strategies": "rust", "universe": "WILL-BE-OVERRIDDEN",
              "granularity": 3600, "window_bars": 5000,
              "thresholds": {"min_win_rate": 0.0, "min_sharpe": -99.0,
                             "min_profit_factor": 0.0, "min_trades": 1, "max_drawdown_pct": 100.0}}]
    run_suite.run_suite(specs, None, fail_on_regression=False, universe_override="BTC-USD")
    assert specs[0]["universe"] == "BTC-USD"


def test_run_suite_cli_json_override(tmp_path, monkeypatch):
    """cli() with --suite JSON file + --baseline-name + --fail-on-regression."""
    import math
    import json
    import importlib
    import data.feed_cache as fc
    import scripts.backtest_framework.run_experiment as re
    monkeypatch.setenv("NAS_FEED_ROOT", str(tmp_path))
    importlib.reload(fc)
    importlib.reload(re)
    from scripts.backtest_framework import experiment as exp_mod
    monkeypatch.setattr(exp_mod, "experiments_dir", lambda: str(tmp_path / "experiments"))
    importlib.reload(re)

    n = 500
    rows = []
    price = 100.0
    for i in range(n):
        price *= 1.0 + 0.001 * math.sin(i / 8.0) + 0.0004
        rows.append([float(i), price * 0.999, price * 1.002, price * 0.997, price, price * 10.0])
    fc.save_candles("coinbase_candles", "BTC-USD", 3600, rows)

    suite_file = tmp_path / "my_suite.json"
    suite_file.write_text(json.dumps([
        {"name": "cli_base", "strategies": "rust", "universe": "BTC-USD",
         "granularity": 3600, "window_bars": 5000,
         "thresholds": {"min_win_rate": 0.0, "min_sharpe": -99.0,
                        "min_profit_factor": 0.0, "min_trades": 1, "max_drawdown_pct": 100.0}},
        {"name": "cli_cand", "strategies": "rust", "universe": "BTC-USD",
         "granularity": 3600, "window_bars": 5000,
         "thresholds": {"min_win_rate": 0.0, "min_sharpe": -99.0,
                        "min_profit_factor": 0.0, "min_trades": 1, "max_drawdown_pct": 100.0}},
    ]))

    from scripts.backtest_framework import run_suite
    from scripts.backtest_framework.run_suite import cli
    import argparse
    ns = argparse.Namespace(suite=str(suite_file), baseline_name="cli_base",
                            fail_on_regression=True, universe=None, report=False)
    # monkeypatch parse_args
    monkeypatch.setattr(run_suite.argparse, "ArgumentParser", lambda *a, **k: _FakeParser(ns))
    cli()


class _FakeParser:
    def __init__(self, ns):
        self._ns = ns

    def add_argument(self, *a, **k):
        pass

    def parse_args(self):
        return self._ns


def test_run_suite_cli_report_only(tmp_path, monkeypatch, capsys):
    from scripts.backtest_framework import experiment as exp_mod
    from scripts.backtest_framework import run_suite
    ledger = tmp_path / "ledger.jsonl"
    ledger.write_text(
        '{"name": "x", "pass_rate": 0.5, "n_passed": 3, "mean_sharpe_passed": 0.5}\n')
    monkeypatch.setattr(exp_mod, "experiments_dir", lambda: str(tmp_path))
    from scripts.backtest_framework.run_suite import cli
    import argparse
    ns = argparse.Namespace(suite="", baseline_name=None, fail_on_regression=True,
                            universe=None, report=True)
    monkeypatch.setattr(run_suite.argparse, "ArgumentParser", lambda *a, **k: _FakeParser(ns))
    cli()
    out = capsys.readouterr().out
    assert "x" in out


def test_run_suite_cli_fail_on_regression(tmp_path, monkeypatch):
    """cli() --fail-on-regression with a worse candidate exits SystemExit(1)."""
    import math
    import importlib
    import data.feed_cache as fc
    import scripts.backtest_framework.run_experiment as re
    monkeypatch.setenv("NAS_FEED_ROOT", str(tmp_path))
    importlib.reload(fc)
    importlib.reload(re)
    from scripts.backtest_framework import experiment as exp_mod
    monkeypatch.setattr(exp_mod, "experiments_dir", lambda: str(tmp_path / "experiments"))
    importlib.reload(re)

    n = 500
    rows = []
    price = 100.0
    for i in range(n):
        price *= 1.0 + 0.001 * math.sin(i / 8.0) + 0.0004
        rows.append([float(i), price * 0.999, price * 1.002, price * 0.997, price, price * 10.0])
    fc.save_candles("coinbase_candles", "BTC-USD", 3600, rows)

    from scripts.backtest_framework import run_suite

    base_spec = {"name": "baseline_1h", "strategies": "rust", "universe": "BTC-USD",
                 "granularity": 3600, "window_bars": 5000,
                 "thresholds": {"min_win_rate": 0.0, "min_sharpe": -99.0,
                                "min_profit_factor": 0.0, "min_trades": 1, "max_drawdown_pct": 100.0}}
    re._persist(run_experiment_run(base_spec), _exp(base_spec))
    cand_spec = dict(base_spec)
    cand_spec["name"] = "candidate_worse"
    cand_spec["thresholds"] = {"min_win_rate": 0.99, "min_sharpe": 99.0,
                               "min_profit_factor": 99.0, "min_trades": 1, "max_drawdown_pct": 0.0001}
    re._persist(run_experiment_run(cand_spec), _exp(cand_spec))

    from scripts.backtest_framework.run_suite import cli
    import argparse
    ns = argparse.Namespace(suite="", baseline_name="baseline_1h",
                            fail_on_regression=True, universe=None, report=False)
    monkeypatch.setattr(run_suite.argparse, "ArgumentParser", lambda *a, **k: _FakeParser(ns))
    try:
        cli()
        assert False, "expected SystemExit(1)"
    except SystemExit as e:
        assert e.code == 1


def test_run_suite_report_ledger_missing_file(tmp_path, monkeypatch, capsys):
    from scripts.backtest_framework import experiment as exp_mod
    from scripts.backtest_framework import run_suite
    monkeypatch.setattr(exp_mod, "experiments_dir", lambda: str(tmp_path))
    run_suite.report_ledger()
    out = capsys.readouterr().out
    assert "No ledger entries found" in out


def test_compare_experiments_branches():
    from scripts.backtest_framework.compare_experiments import compare, _print

    # shared=0 (no overlapping strategies)
    base = {"name": "b", "mean_sharpe_passed": 0.5, "pass_rate": 0.5, "results": {}}
    cand = {"name": "c", "mean_sharpe_passed": 0.5, "pass_rate": 0.5, "results": {}}
    rep = compare(base, cand, 0.05)
    assert rep["shared_strategies"] == 0
    assert rep["passed"]

    # degradation-only case: mean_sharpe drops, nothing new fails
    base2 = {"name": "b2", "mean_sharpe_passed": 0.8, "pass_rate": 0.5,
             "results": {"ema_cross/BTC": {"strategy": "ema_cross", "currency": "BTC",
                                           "passed": True, "sharpe": 0.8}}}
    cand2 = {"name": "c2", "mean_sharpe_passed": 0.3, "pass_rate": 0.5,
             "results": {"ema_cross/BTC": {"strategy": "ema_cross", "currency": "BTC",
                                           "passed": True, "sharpe": 0.3}}}
    rep2 = compare(base2, cand2, 0.05)
    assert not rep2["passed"]
    assert any("mean_sharpe" in r for r in rep2["regressions"])
    assert not rep2["new_failures"]

    # _print with improved + new_failures + regressions branches
    rep3 = compare(base2, cand2, 0.05)
    rep3["improved"] = ["ema_cross/BTC"]
    rep3["new_failures"] = ["rsi_revert/BTC"]
    _print(rep3)


def test_compare_experiments_missing_scorecard(tmp_path, monkeypatch):
    from scripts.backtest_framework import compare_experiments as ce
    monkeypatch.setattr(ce, "experiments_dir", lambda: str(tmp_path))
    try:
        ce._load("does_not_exist")
        assert False, "should exit"
    except SystemExit as e:
        assert e.code


def test_compare_cli_main_fail_on_regression(tmp_path, monkeypatch):
    from scripts.backtest_framework import compare_experiments as ce
    monkeypatch.setattr(ce, "experiments_dir", lambda: str(tmp_path / "experiments"))

    sc = {"name": "a", "mean_sharpe_passed": 0.8, "pass_rate": 0.8,
          "results": {"ema_cross/BTC": {"strategy": "ema_cross", "currency": "BTC",
                                        "passed": True, "sharpe": 0.8}}}
    sc2 = {"name": "b", "mean_sharpe_passed": 0.3, "pass_rate": 0.8,
           "results": {"ema_cross/BTC": {"strategy": "ema_cross", "currency": "BTC",
                                         "passed": True, "sharpe": 0.3}}}
    import os
    os.makedirs(tmp_path / "experiments" / "a")
    os.makedirs(tmp_path / "experiments" / "b")
    ce._load  # ensure attr exists
    import json
    json.dump(sc, open(tmp_path / "experiments" / "a" / "scorecard.json", "w"))
    json.dump(sc2, open(tmp_path / "experiments" / "b" / "scorecard.json", "w"))
    try:
        ce.cli_main("a", "b", fail_on_regression=True)
        assert False, "should exit"
    except SystemExit as e:
        assert e.code == 1


def test_experiment_validate_bad_asset_class():
    from scripts.backtest_framework.experiment import Experiment
    e = Experiment(name="x", asset_classes=["bogus"])
    try:
        e.validate()
        assert False, "should raise"
    except ValueError as ex:
        assert "bogus" in str(ex)


def test_resolve_strategies_all():
    from scripts.backtest_framework.experiment import resolve_strategies
    all_names = resolve_strategies("all")
    assert all_names
    assert "ema_cross" in all_names


def test_evaluate_verdict_edge_cases():
    from scripts.backtest_framework.experiment import evaluate_verdict
    th = {"min_win_rate": 0.40, "min_sharpe": 0.30, "min_profit_factor": 1.05,
          "min_trades": 5, "max_drawdown_pct": 50.0}
    # drawdown exactly at boundary passes
    assert evaluate_verdict(_verdict(passed=True, dd=50.0), th)
    # profit factor below threshold fails
    assert not evaluate_verdict(_verdict(passed=True, pf=1.0), th)
    # win rate below threshold fails
    assert not evaluate_verdict(_verdict(passed=True, win_rate=0.3), th)


def test_experiment_roundtrip_extra_fields(tmp_path):
    from scripts.backtest_framework.experiment import Experiment
    e = Experiment(name="t2", granularity=3600, strategies="rust", universe="BTC-USD",
                   walk_forward_folds=4, ensemble=True, ensemble_min_groups=3,
                   regime=True, regime_min_bars=80, confirm_granularity=86400,
                   config_ref="live_mode_1", notes="a note",
                   thresholds={"min_sharpe": 0.4})
    p = tmp_path / "exp2.json"
    e.save(str(p))
    e2 = Experiment.load(str(p))
    assert e2.walk_forward_folds == 4
    assert e2.ensemble is True
    assert e2.ensemble_min_groups == 3
    assert e2.regime is True
    assert e2.regime_min_bars == 80
    assert e2.confirm_granularity == 86400
    assert e2.config_ref == "live_mode_1"
    assert e2.notes == "a note"
    assert e2.to_dict()["name"] == "t2"


def _args_obj(**kw):
    class A:
        pass
    a = A()
    for k, v in kw.items():
        setattr(a, k, v)
    return a


def test_experiment_from_args_with_config_ref_and_notes():
    from scripts.backtest_framework.experiment import Experiment
    args = _args_obj(name="ea", strategies="rust", universe="BTC-USD",
                     asset_classes="safe,growth", granularity=3600, window_bars=5000,
                     start_ts=None, end_ts=None, min_win_rate=0.5,
                     min_sharpe=0.4, min_profit_factor=1.1, min_trades=10,
                     max_drawdown_pct=40.0, walk_forward_folds=3, ensemble=True,
                     ensemble_min_groups=2, regime=True, regime_min_bars=70,
                     confirm_granularity=86400, config_ref="ref1", notes="note1")
    e = Experiment.from_args(args)
    assert e.name == "ea"
    assert e.config_ref == "ref1"
    assert e.notes == "note1"
    assert e.walk_forward_folds == 3
    assert e.min_trades_default if hasattr(e, "min_trades_default") else e.thresholds["min_trades"] == 10
    assert e.thresholds["min_sharpe"] == 0.4


def test_walk_forward_make_folds_short():
    from scripts.backtest_framework.walk_forward import make_folds
    # n_folds < 2
    assert make_folds(list(range(200)), n_folds=1) == []
    # too-short rows -> n_folds=0 path returns []
    assert make_folds(list(range(10)), n_folds=4) == []


def test_walk_forward_empty_aggregate():
    from scripts.backtest_framework.walk_forward import aggregate_walk_forward
    agg = aggregate_walk_forward([])
    assert agg["n_evaluated"] == 0
    assert agg["n_with_folds"] == 0
    assert agg["oos_mean_sharpe"] == 0.0
    assert agg["stable_rate"] == 0.0


def test_walk_forward_nzero_oos_passed():
    import strategy_engine as S
    from scripts.backtest_framework.walk_forward import walk_forward
    rows = []
    price = 100.0
    for i in range(400):
        price *= 1.0 + 0.001 * math.sin(i / 8.0) + 0.0004
        rows.append([float(i), price * 0.999, price * 1.002, price * 0.997, price, price * 10.0])
    # single fold that fails OOS -> oos_passed != n_folds -> stable False
    res = walk_forward(S, "ema_cross", "BTC", rows, n_folds=4)
    assert res["n_folds"] == 4
    assert res["oos_passed"] <= res["n_folds"]
    assert res["stable"] == (res["oos_passed"] == res["n_folds"])


def test_mtf_backtest_panics_skipped():
    from scripts.backtest_framework import multitimeframe as mtf

    class _BadEngine:
        def backtest_strategy(self, name, currency, closes, volumes, **kw):
            raise RuntimeError("boom")

    class _GoodEngine:
        def __init__(self):
            import strategy_engine as S
            self._S = S

        def backtest_strategy(self, name, currency, closes, volumes, **kw):
            return self._S.backtest_strategy(name, currency, closes, volumes, **kw)

    rows = _make_rows(200)
    res = mtf.mtf_backtest(_BadEngine(), ["ema_cross"], "BTC", rows, rows,
                           {"min_win_rate": 0.0, "min_sharpe": -99.0,
                            "min_profit_factor": 0.0, "min_trades": 1, "max_drawdown_pct": 100.0})
    assert res["ema_cross"]["primary_passed"] is False
    assert res["ema_cross"]["confirm_passed"] is False
    assert res["ema_cross"]["mtf_passed"] is False


def test_mtf_summary_empty():
    from scripts.backtest_framework import multitimeframe as mtf
    summ = mtf.mtf_summary({})
    assert summ["n_strategies"] == 0
    assert summ["mtf_pass_rate"] == 0.0
    assert summ["lift"] == 0.0


def test_mtf_confirm_insufficient_rows():
    import strategy_engine as S
    from scripts.backtest_framework import multitimeframe as mtf
    # primary has enough, confirm has < 40 rows -> confirm skipped -> False
    rows_p = _make_rows(300)
    rows_c = [[float(i), 100.0, 100.0, 100.0, 100.0 + i * 0.001, 10.0] for i in range(10)]
    res = mtf.mtf_backtest(S, ["ema_cross", "rsi_revert"], "BTC", rows_p, rows_c,
                           {"min_win_rate": 0.0, "min_sharpe": -99.0,
                            "min_profit_factor": 0.0, "min_trades": 1, "max_drawdown_pct": 100.0})
    # confirm rows too short -> confirm verdict None -> confirm_passed False
    assert res["ema_cross"]["confirm_passed"] is False


def test_walk_forward_make_folds_fold_too_small():
    from scripts.backtest_framework.walk_forward import make_folds
    # len in [160,199] => fold_size < 40 => line 35 returns []
    assert make_folds(list(range(180)), n_folds=4) == []


class _Verdict:
    def __init__(self, sharpe=0.5, pf=1.2, passed=True):
        self.sharpe_ratio = sharpe
        self.profit_factor = pf
        self.passed = passed


class _RaisingEngine:
    def backtest_strategy(self, name, currency, closes, volumes, **kw):
        raise RuntimeError("panic")


class _FlakyEngine:
    """Returns a valid verdict for in-sample, None-ish (raises) for out-of-sample."""

    def __init__(self, oos_raises=False, oos_none=False):
        self.oos_raises = oos_raises
        self.oos_none = oos_none
        self._call = 0

    def backtest_strategy(self, name, currency, closes, volumes, **kw):
        self._call += 1
        if self._call % 2 == 0:  # out-of-sample calls
            if self.oos_raises:
                raise RuntimeError("panic")
            if self.oos_none:
                return None
        return _Verdict()


def test_walk_forward_with_raising_engine():
    from scripts.backtest_framework.walk_forward import walk_forward, aggregate_walk_forward
    rows = []
    price = 100.0
    for i in range(600):
        price *= 1.0 + 0.001 * math.sin(i / 8.0) + 0.0004
        rows.append([float(i), price * 0.999, price * 1.002, price * 0.997, price, price * 10.0])
    # folds non-empty; engine raises on every _bt call => _bt returns None,
    # catches BaseException (line 57-58), stable False, n==0 => line 90 branch.
    res = walk_forward(_RaisingEngine(), "ema_cross", "BTC", rows, n_folds=4)
    assert res["n_folds"] == 4
    assert res["stable"] is False
    assert res["is_sharpe"] == 0.0
    agg = aggregate_walk_forward([res])
    assert agg["n_evaluated"] == 1


def test_walk_forward_oos_none_skips():
    from scripts.backtest_framework.walk_forward import walk_forward
    rows = []
    price = 100.0
    for i in range(600):
        price *= 1.0 + 0.001 * math.sin(i / 8.0) + 0.0004
        rows.append([float(i), price * 0.999, price * 1.002, price * 0.997, price, price * 10.0])
    # OOS returns None => stable False (line 79-80), n could still be >0 if IS ok
    res = walk_forward(_FlakyEngine(oos_none=True), "ema_cross", "BTC", rows, n_folds=4)
    assert res["stable"] is False
    assert res["oos_passed"] == 0


def test_walk_forward_short_rows_no_folds():
    from scripts.backtest_framework.walk_forward import walk_forward
    rows = [[float(i), 0, 0, 0, 100.0 + i, 10.0] for i in range(50)]
    res = walk_forward(_RaisingEngine(), "ema_cross", "BTC", rows, n_folds=4)
    assert res["n_folds"] == 0
    assert res["stable"] is False


def test_experiment_validate_blank_strategies():
    from scripts.backtest_framework.experiment import Experiment
    e = Experiment(name="x", strategies="")
    try:
        e.validate()
        assert False, "should raise"
    except ValueError as ex:
        assert "strategies" in str(ex)


def test_experiment_from_args_threshold_none():
    from scripts.backtest_framework.experiment import Experiment
    args = _args_obj(name="ea2", strategies="rust", universe="BTC-USD",
                     asset_classes="safe", granularity=3600, window_bars=5000,
                     start_ts=None, end_ts=None, min_win_rate=None,
                     min_sharpe=None, min_profit_factor=None, min_trades=None,
                     max_drawdown_pct=None, walk_forward_folds=0, ensemble=False,
                     ensemble_min_groups=2, regime=False, regime_min_bars=60,
                     confirm_granularity=0, config_ref="", notes="")
    e = Experiment.from_args(args)
    assert e.thresholds["min_sharpe"] == 0.30  # default retained when None


def test_compare_experiments_cli_function(tmp_path, monkeypatch):
    from scripts.backtest_framework import compare_experiments as ce
    import json, os
    monkeypatch.setattr(ce, "experiments_dir", lambda: str(tmp_path / "experiments"))
    os.makedirs(tmp_path / "experiments" / "a")
    os.makedirs(tmp_path / "experiments" / "b")
    sc = {"name": "a", "mean_sharpe_passed": 0.8, "pass_rate": 0.8,
          "results": {"ema_cross/BTC": {"strategy": "ema_cross", "currency": "BTC",
                                        "passed": True, "sharpe": 0.8}}}
    json.dump(sc, open(tmp_path / "experiments" / "a" / "scorecard.json", "w"))
    json.dump(sc, open(tmp_path / "experiments" / "b" / "scorecard.json", "w"))
    from scripts.backtest_framework.compare_experiments import cli
    import argparse
    ns = argparse.Namespace(baseline="a", candidate="b", max_pass_rate_drop=0.05,
                            fail_on_regression=False)
    monkeypatch.setattr(ce.argparse, "ArgumentParser", lambda *a, **k: _FakeParser(ns))
    cli()
