from __future__ import annotations

import copy

import pytest

from scripts.alpha_validation import build_alpha_validation_evidence, stable_hash
from scripts.backtest_framework.canonical_replay import snapshot_from_rows
from scripts.learning_lineage import LineageStore
from scripts.research_tournament import (
    ResearchTournament,
    TerminalHoldoutPolicy,
    build_experiment_plan_from_snapshot,
    verify_terminal_holdout_evidence,
)


def _rows(count: int = 600) -> list[list[float]]:
    rows: list[list[float]] = []
    for index in range(count):
        close = 100.0 + index * 0.05
        rows.append([
            1_700_000_000.0 + index * 60.0,
            close - 0.02,
            close + 0.10,
            close - 0.10,
            close,
            1000.0 + index,
        ])
    return rows


def _snapshot(count: int = 600):
    return snapshot_from_rows(
        _rows(count),
        kind="coinbase_candles",
        symbol="BTC-USD",
        granularity=60,
    )


def _alpha(plan, candidate_id: str, *, pnl: float, config=None):
    config = config or {"period": 14, "oversold": 30.0, "overbought": 70.0}
    evidence = build_alpha_validation_evidence(
        candidate_id=candidate_id,
        candidate_source_sha="a" * 40,
        candidate_config=config,
        dataset_id=plan["search_dataset"]["dataset_id"],
        dataset_hash=plan["search_dataset"]["dataset_hash"],
        fold_returns=[
            [0.02, 0.01, 0.015],
            [0.01, 0.02, 0.012],
            [0.015, 0.011, 0.018],
            [0.014, 0.013, 0.016],
        ],
        net_pnl_after_cost_usd=pnl,
        cost_coverage_ratio=2.0,
        regimes_tested=["bull", "bear", "sideways"],
        accounting_invariants_ok=True,
        lineage_verified=True,
        parameter_stability_score=0.9,
        bootstrap_samples=100,
        bootstrap_seed=7,
    )
    evidence.pop("evidence_hash")
    evidence["replay_provenance_bound"] = True
    evidence["replay_attestation"] = {
        "strategy_name": "rsi_revert",
        "strategy_config": config,
        "strategy_config_hash": stable_hash(config),
        "warmup": 30,
        "fee_bps": 0.0,
        "max_hold_bars": 0,
    }
    evidence["evidence_hash"] = stable_hash(evidence)
    return evidence


def _tournament(tmp_path):
    lineage = LineageStore(tmp_path / "lineage.jsonl")
    tournament = ResearchTournament(tmp_path / "experiments.json", lineage=lineage)
    experiment = tournament.create_from_snapshot(
        _snapshot(),
        strategy_name="rsi_revert",
        holdout_bars=80,
        embargo_bars=10,
        min_search_bars=200,
        experiment_id="experiment-test",
    )
    return tournament, lineage, experiment


def test_experiment_plan_commits_non_overlapping_terminal_without_exposing_rows():
    plan = build_experiment_plan_from_snapshot(
        _snapshot(),
        experiment_id="experiment-plan",
        strategy_name="rsi_revert",
        holdout_bars=80,
        embargo_bars=10,
        min_search_bars=200,
        created_at="2026-09-11T12:00:00+00:00",
    )

    assert plan["search_dataset"]["row_count"] == 510
    assert plan["terminal_holdout_commitment"]["row_count"] == 80
    assert plan["embargo_bars"] == 10
    assert plan["search_dataset"]["end_ts"] < plan["terminal_holdout_commitment"]["start_ts"]
    assert "rows" not in plan
    assert "rows" not in plan["terminal_holdout_commitment"]
    unhashed = dict(plan)
    supplied = unhashed.pop("experiment_hash")
    assert stable_hash(unhashed) == supplied


def test_all_candidate_trials_are_lineaged_and_selection_is_pre_holdout(monkeypatch, tmp_path):
    tournament, lineage, experiment = _tournament(tmp_path)
    monkeypatch.setattr(
        "scripts.research_tournament.verify_evidence_replay_binding",
        lambda evidence, reverify_source=True: (True, []),
    )

    weaker = _alpha(experiment["plan"], "candidate-weaker", pnl=100.0)
    stronger = _alpha(experiment["plan"], "candidate-stronger", pnl=150.0)
    bad_dataset = _alpha(experiment["plan"], "candidate-bad-dataset", pnl=1000.0)
    bad_dataset["dataset_hash"] = "b" * 64
    bad_dataset.pop("evidence_hash")
    bad_dataset["evidence_hash"] = stable_hash(bad_dataset)

    weak_trial = tournament.register_candidate("experiment-test", weaker, reverify_source=False)
    strong_trial = tournament.register_candidate("experiment-test", stronger, reverify_source=False)
    rejected_trial = tournament.register_candidate("experiment-test", bad_dataset, reverify_source=False)

    assert weak_trial["eligible"] is True
    assert strong_trial["eligible"] is True
    assert rejected_trial["eligible"] is False
    assert "candidate_search_dataset_hash_mismatch" in rejected_trial["reasons"]

    selection = tournament.seal_selection("experiment-test")
    assert selection["candidate_id"] == "candidate-stronger"
    assert selection["trial_count"] == 3
    assert selection["eligible_trial_count"] == 2
    assert selection["ranked_candidate_ids"] == ["candidate-stronger", "candidate-weaker"]

    events = lineage.events()
    assert [row["type"] for row in events].count("candidate_trial") == 3
    assert [row["type"] for row in events].count("candidate_selection") == 1
    assert [row["type"] for row in events].count("terminal_holdout") == 0
    assert lineage.verify()["ok"] is True

    with pytest.raises(ValueError, match="no longer accepting"):
        tournament.register_candidate("experiment-test", _alpha(experiment["plan"], "late", pnl=999.0))


def test_terminal_holdout_is_one_shot_and_source_reverifiable(monkeypatch, tmp_path):
    tournament, lineage, experiment = _tournament(tmp_path)
    monkeypatch.setattr(
        "scripts.research_tournament.verify_evidence_replay_binding",
        lambda evidence, reverify_source=True: (True, []),
    )
    tournament.register_candidate(
        "experiment-test",
        _alpha(experiment["plan"], "candidate-selected", pnl=150.0),
        reverify_source=False,
    )
    tournament.seal_selection("experiment-test")

    terminal_rows = _rows()[-80:]
    terminal_snapshot = snapshot_from_rows(
        terminal_rows,
        kind="coinbase_candles",
        symbol="BTC-USD",
        granularity=60,
    )
    terminal_returns = [0.02, 0.01, -0.005, 0.015, 0.01, 0.012]

    monkeypatch.setattr(
        "scripts.research_tournament.load_canonical_snapshot",
        lambda **kwargs: terminal_snapshot,
    )
    monkeypatch.setattr(
        "scripts.research_tournament.replay_trade_returns_rust",
        lambda *args, **kwargs: list(terminal_returns),
    )

    evidence = tournament.run_terminal_holdout(
        "experiment-test",
        policy=TerminalHoldoutPolicy(min_trades=5),
    )
    assert evidence["candidate_id"] == "candidate-selected"
    assert evidence["passed"] is True
    assert evidence["returns"] == terminal_returns
    assert evidence["terminal_dataset"] == experiment["plan"]["terminal_holdout_commitment"]

    valid, reasons = verify_terminal_holdout_evidence(
        evidence,
        lineage=lineage,
        reverify_source=True,
    )
    assert valid is True, reasons
    assert [row["type"] for row in lineage.events()].count("terminal_holdout") == 1

    with pytest.raises(ValueError, match="one-shot"):
        tournament.run_terminal_holdout("experiment-test")


def test_terminal_evidence_rejects_rehashed_return_or_lineage_tampering(monkeypatch, tmp_path):
    tournament, lineage, experiment = _tournament(tmp_path)
    monkeypatch.setattr(
        "scripts.research_tournament.verify_evidence_replay_binding",
        lambda evidence, reverify_source=True: (True, []),
    )
    tournament.register_candidate(
        "experiment-test",
        _alpha(experiment["plan"], "candidate-selected", pnl=150.0),
        reverify_source=False,
    )
    tournament.seal_selection("experiment-test")

    terminal_snapshot = snapshot_from_rows(
        _rows()[-80:],
        kind="coinbase_candles",
        symbol="BTC-USD",
        granularity=60,
    )
    monkeypatch.setattr("scripts.research_tournament.load_canonical_snapshot", lambda **kwargs: terminal_snapshot)
    monkeypatch.setattr(
        "scripts.research_tournament.replay_trade_returns_rust",
        lambda *args, **kwargs: [0.02, 0.01, -0.005, 0.015, 0.01, 0.012],
    )
    evidence = tournament.run_terminal_holdout("experiment-test")

    tampered = copy.deepcopy(evidence)
    tampered["returns"][0] = 0.50
    tampered["returns_hash"] = stable_hash(tampered["returns"])
    tampered["evidence_hash"] = stable_hash({k: v for k, v in tampered.items() if k != "evidence_hash"})
    valid, reasons = verify_terminal_holdout_evidence(tampered, lineage=lineage, reverify_source=False)
    assert valid is False
    assert "terminal_result_hash_mismatch" in reasons
    assert "terminal_metrics_mismatch" in reasons

    lineage_tampered = copy.deepcopy(evidence)
    lineage_tampered["terminal_lineage_event_hash"] = "0" * 64
    lineage_tampered["evidence_hash"] = stable_hash(
        {k: v for k, v in lineage_tampered.items() if k != "evidence_hash"}
    )
    valid, reasons = verify_terminal_holdout_evidence(
        lineage_tampered,
        lineage=lineage,
        reverify_source=False,
    )
    assert valid is False
    assert "terminal_holdout_lineage_hash_mismatch" in reasons


def test_terminal_failure_is_final_for_the_experiment(monkeypatch, tmp_path):
    tournament, _lineage, experiment = _tournament(tmp_path)
    monkeypatch.setattr(
        "scripts.research_tournament.verify_evidence_replay_binding",
        lambda evidence, reverify_source=True: (True, []),
    )
    tournament.register_candidate(
        "experiment-test",
        _alpha(experiment["plan"], "candidate-selected", pnl=150.0),
        reverify_source=False,
    )
    tournament.seal_selection("experiment-test")

    terminal_snapshot = snapshot_from_rows(
        _rows()[-80:],
        kind="coinbase_candles",
        symbol="BTC-USD",
        granularity=60,
    )
    monkeypatch.setattr("scripts.research_tournament.load_canonical_snapshot", lambda **kwargs: terminal_snapshot)
    monkeypatch.setattr(
        "scripts.research_tournament.replay_trade_returns_rust",
        lambda *args, **kwargs: [-0.02, -0.01, -0.005, 0.001, -0.01],
    )

    evidence = tournament.run_terminal_holdout("experiment-test")
    assert evidence["passed"] is False
    assert "terminal_nonpositive_return" in evidence["reasons"]
    persisted = tournament.load()["experiments"][0]
    assert persisted["status"] == "terminal_failed"
    with pytest.raises(ValueError, match="one-shot"):
        tournament.run_terminal_holdout("experiment-test")
