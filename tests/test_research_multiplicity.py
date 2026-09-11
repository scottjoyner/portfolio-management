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


def _snapshot():
    return snapshot_from_rows(
        _rows(), kind="coinbase_candles", symbol="BTC-USD", granularity=60
    )


def _evidence(plan, candidate_id: str, fold_returns, *, pnl: float = 100.0):
    config = {"period": 14, "oversold": 30.0, "overbought": 70.0}
    evidence = build_alpha_validation_evidence(
        candidate_id=candidate_id,
        candidate_source_sha="a" * 40,
        candidate_config=config,
        dataset_id=plan["search_dataset"]["dataset_id"],
        dataset_hash=plan["search_dataset"]["dataset_hash"],
        fold_returns=fold_returns,
        net_pnl_after_cost_usd=pnl,
        cost_coverage_ratio=3.0,
        regimes_tested=["bull", "bear", "sideways"],
        accounting_invariants_ok=True,
        lineage_verified=True,
        parameter_stability_score=0.9,
        bootstrap_samples=200,
        bootstrap_seed=7,
    )
    evidence.pop("evidence_hash")
    evidence["replay_provenance_bound"] = True
    evidence["replay_attestation"] = {
        "strategy_name": "rsi_revert",
        "strategy_config": config,
        "strategy_config_hash": stable_hash(config),
        "execution_config_bound": True,
        "warmup": 30,
        "fee_bps": 0.0,
        "max_hold_bars": 0,
    }
    evidence["evidence_hash"] = stable_hash(evidence)
    return evidence


def _tournament(tmp_path, *, max_trials: int = 20):
    lineage = LineageStore(tmp_path / "lineage.jsonl")
    tournament = ResearchTournament(tmp_path / "experiments.json", lineage=lineage)
    experiment = tournament.create_from_snapshot(
        _snapshot(),
        strategy_name="rsi_revert",
        holdout_bars=80,
        embargo_bars=10,
        min_search_bars=200,
        max_candidate_trials=max_trials,
        familywise_alpha=0.05,
        min_sign_test_trades=10,
        dependence_block_size=5,
        min_nonzero_blocks=5,
        experiment_id="experiment-multiple-testing",
    )
    return tournament, lineage, experiment


def test_experiment_hash_commits_multiple_testing_policy_before_search():
    base = build_experiment_plan_from_snapshot(
        _snapshot(),
        experiment_id="exp",
        strategy_name="rsi_revert",
        holdout_bars=80,
        embargo_bars=10,
        max_candidate_trials=20,
        familywise_alpha=0.05,
        dependence_block_size=5,
        min_nonzero_blocks=5,
        created_at="2026-09-11T12:00:00+00:00",
    )
    changed = build_experiment_plan_from_snapshot(
        _snapshot(),
        experiment_id="exp",
        strategy_name="rsi_revert",
        holdout_bars=80,
        embargo_bars=10,
        max_candidate_trials=100,
        familywise_alpha=0.05,
        dependence_block_size=5,
        min_nonzero_blocks=5,
        created_at="2026-09-11T12:00:00+00:00",
    )
    changed_block = build_experiment_plan_from_snapshot(
        _snapshot(),
        experiment_id="exp",
        strategy_name="rsi_revert",
        holdout_bars=80,
        embargo_bars=10,
        max_candidate_trials=20,
        familywise_alpha=0.05,
        dependence_block_size=10,
        min_nonzero_blocks=5,
        created_at="2026-09-11T12:00:00+00:00",
    )
    assert base["multiple_testing_policy"]["max_candidate_trials"] == 20
    assert base["multiple_testing_policy"]["per_trial_alpha"] == 0.0025
    assert base["multiple_testing_policy"]["dependence_block_size"] == 5
    assert changed["multiple_testing_policy"]["per_trial_alpha"] == 0.0005
    assert base["experiment_hash"] != changed["experiment_hash"]
    assert base["experiment_hash"] != changed_block["experiment_hash"]


def test_every_registered_candidate_consumes_precommitted_budget(monkeypatch, tmp_path):
    tournament, _lineage, experiment = _tournament(tmp_path, max_trials=2)
    monkeypatch.setattr(
        "scripts.research_tournament.verify_evidence_replay_binding",
        lambda evidence, reverify_source=True: (True, []),
    )
    strong = [[0.02, 0.015, 0.01, 0.012, 0.011] for _ in range(10)]
    first = tournament.register_candidate(
        experiment["id"], _evidence(experiment["plan"], "candidate-1", strong), reverify_source=False
    )
    second = tournament.register_candidate(
        experiment["id"], _evidence(experiment["plan"], "candidate-2", strong), reverify_source=False
    )
    assert first["trial_index"] == 1
    assert second["trial_index"] == 2
    with pytest.raises(ValueError, match="budget exhausted"):
        tournament.register_candidate(
            experiment["id"], _evidence(experiment["plan"], "candidate-3", strong), reverify_source=False
        )


def test_alpha_pass_can_still_fail_familywise_selection_gate(monkeypatch, tmp_path):
    tournament, _lineage, experiment = _tournament(tmp_path, max_trials=20)
    monkeypatch.setattr(
        "scripts.research_tournament.verify_evidence_replay_binding",
        lambda evidence, reverify_source=True: (True, []),
    )
    mixed = [
        [0.02] * 6 + [-0.005] * 4,
        [0.02] * 6 + [-0.005] * 4,
        [0.02] * 5 + [-0.005] * 5,
    ]
    evidence = _evidence(experiment["plan"], "candidate-profitable-but-selected", mixed)
    assert evidence["walk_forward_passed"] is True, evidence["fail_reasons"]

    trial = tournament.register_candidate(
        experiment["id"], evidence, reverify_source=False
    )
    assert trial["multiple_testing"]["passed"] is False
    assert trial["eligible"] is False
    assert "multiple_testing_not_familywise_significant" in trial["reasons"]
    with pytest.raises(ValueError, match="no eligible candidate"):
        tournament.seal_selection(experiment["id"])


def test_terminal_evidence_recomputes_selected_search_significance(monkeypatch, tmp_path):
    tournament, lineage, experiment = _tournament(tmp_path, max_trials=20)
    monkeypatch.setattr(
        "scripts.research_tournament.verify_evidence_replay_binding",
        lambda evidence, reverify_source=True: (True, []),
    )
    strong = [[0.02, 0.015, 0.01, 0.012, 0.011] for _ in range(10)]
    tournament.register_candidate(
        experiment["id"], _evidence(experiment["plan"], "candidate-strong", strong), reverify_source=False
    )
    tournament.seal_selection(experiment["id"])

    terminal_snapshot = snapshot_from_rows(
        _rows()[-80:], kind="coinbase_candles", symbol="BTC-USD", granularity=60
    )
    monkeypatch.setattr(
        "scripts.research_tournament.load_canonical_snapshot",
        lambda **kwargs: terminal_snapshot,
    )
    monkeypatch.setattr(
        "scripts.research_tournament.replay_trade_returns_rust",
        lambda *args, **kwargs: [0.02, 0.01, -0.005, 0.015, 0.01, 0.012],
    )
    evidence = tournament.run_terminal_holdout(
        experiment["id"], policy=TerminalHoldoutPolicy(min_trades=5)
    )
    assert evidence["selected_multiple_testing"]["passed"] is True
    assert evidence["selected_multiple_testing"]["dependence"]["nonzero_blocks"] == 10
    assert evidence["trial_count"] == 1
    assert evidence["max_candidate_trials"] == 20

    tampered = copy.deepcopy(evidence)
    tampered["selected_multiple_testing"]["dependence"]["raw_p_value"] = 0.0
    tampered["selected_multiple_testing"]["dependence"]["adjusted_p_value"] = 0.0
    core_names = {
        "schema_version", "method", "experiment_id", "experiment_hash", "selection_hash",
        "selection_policy_version", "candidate_id", "candidate_source_sha", "candidate_config",
        "candidate_config_hash", "alpha_validation_evidence_hash", "multiple_testing_policy",
        "selected_multiple_testing", "search_oos_returns", "search_oos_returns_hash", "trial_count",
        "max_candidate_trials", "terminal_dataset", "strategy_name", "strategy_config",
        "strategy_config_hash", "warmup", "fee_bps", "max_hold_bars", "periods_per_year",
        "returns", "returns_hash", "metrics", "policy", "passed", "reasons", "evaluated_at",
    }
    tampered["terminal_result_hash"] = stable_hash(
        {name: tampered.get(name) for name in core_names}
    )
    tampered["evidence_hash"] = stable_hash(
        {key: value for key, value in tampered.items() if key != "evidence_hash"}
    )
    valid, reasons = verify_terminal_holdout_evidence(
        tampered, lineage=lineage, reverify_source=False
    )
    assert valid is False
    assert "terminal_multiple_testing_assessment_mismatch" in reasons
