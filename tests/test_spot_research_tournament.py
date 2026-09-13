from __future__ import annotations

from pathlib import Path

from scripts.alpha_validation import build_alpha_validation_evidence, stable_hash
from scripts.backtest_framework.canonical_replay import snapshot_from_rows
from scripts.learning_lineage import LineageStore
from scripts.research_tournament import ResearchTournament


def _rows(count: int = 700) -> list[list[float]]:
    rows = []
    for index in range(count):
        close = 100.0 + index * 0.05
        rows.append([
            1_700_000_000.0 + index * 60.0,
            close,
            close + 0.2,
            close - 0.2,
            close,
            1000.0,
        ])
    return rows


def _snapshot(count: int = 700):
    return snapshot_from_rows(
        _rows(count), kind="coinbase_candles", symbol="BTC-USD", granularity=60
    )


def _alpha(plan: dict, candidate_id: str) -> dict:
    config = {"period": 14, "oversold": 30.0, "overbought": 70.0}
    evidence = build_alpha_validation_evidence(
        candidate_id=candidate_id,
        candidate_source_sha="a" * 40,
        candidate_config=config,
        dataset_id=plan["search_dataset"]["dataset_id"],
        dataset_hash=plan["search_dataset"]["dataset_hash"],
        fold_returns=[[0.01] * 25 for _ in range(4)],
        net_pnl_after_cost_usd=100.0,
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
        "execution_config_bound": True,
        "dataset": plan["search_dataset"],
        "n_folds": 4,
        "purge_size": 0,
        "embargo_size": 0,
        "warmup": 30,
        "fee_bps": 10.0,
        "max_hold_bars": 12,
    }
    evidence["evidence_hash"] = stable_hash(evidence)
    return evidence


def _spot_search(alpha: dict, *, passed: bool = True) -> dict:
    return {
        "evidence_hash": stable_hash({"candidate": alpha["candidate_id"], "passed": passed}),
        "candidate_id": alpha["candidate_id"],
        "passed": passed,
        "reasons": [] if passed else ["spot_search_nonpositive_return"],
        "multiple_testing": {
            "adjusted_p_value": 0.0001 if passed else 1.0,
            "passed": passed,
        },
    }


def _tournament(tmp_path: Path):
    lineage = LineageStore(tmp_path / "lineage.jsonl")
    tournament = ResearchTournament(tmp_path / "experiments.json", lineage=lineage)
    experiment = tournament.create_from_snapshot(
        _snapshot(),
        strategy_name="rsi_revert",
        holdout_bars=80,
        embargo_bars=10,
        min_search_bars=300,
        max_candidate_trials=20,
        familywise_alpha=0.05,
        min_sign_test_trades=10,
        dependence_block_size=5,
        min_nonzero_blocks=5,
        execution_lifecycle="spot_long_only_v1",
        experiment_id="experiment-spot",
    )
    return tournament, lineage, experiment


def test_experiment_hash_precommits_spot_execution_lifecycle(tmp_path):
    tournament, lineage, experiment = _tournament(tmp_path)
    commitment = experiment["plan"]["execution_lifecycle_commitment"]
    assert commitment["lifecycle_key"] == "spot_long_only_v1"
    assert commitment["required_for_promotion"] is True
    event = lineage.events()[0]
    assert event["payload"]["execution_lifecycle_commitment"] == commitment

    legacy = tournament.create_from_snapshot(
        _snapshot(),
        strategy_name="rsi_revert",
        holdout_bars=80,
        embargo_bars=10,
        min_search_bars=300,
        experiment_id="experiment-legacy",
    )
    assert "execution_lifecycle_commitment" not in legacy["plan"]
    assert experiment["plan"]["experiment_hash"] != legacy["plan"]["experiment_hash"]


def test_committed_candidate_must_pass_spot_search_gate(monkeypatch, tmp_path):
    tournament, _lineage, experiment = _tournament(tmp_path)
    alpha = _alpha(experiment["plan"], "candidate-spot")
    monkeypatch.setattr(
        "scripts.research_tournament.verify_evidence_replay_binding",
        lambda evidence, reverify_source=True: (True, []),
    )
    monkeypatch.setattr(
        "scripts.research_tournament.build_spot_search_evidence",
        lambda **kwargs: _spot_search(kwargs["validation_evidence"], passed=True),
    )
    monkeypatch.setattr(
        "scripts.research_tournament.verify_spot_search_evidence",
        lambda *args, **kwargs: (True, []),
    )

    trial = tournament.register_candidate(
        experiment["id"], alpha, reverify_source=False
    )
    assert trial["eligible"] is True, trial["reasons"]
    assert trial["spot_execution_search_evidence"]["passed"] is True
    assert trial["spot_execution_search_evidence_hash"] == trial["spot_execution_search_evidence"]["evidence_hash"]

    persisted_trial = tournament.load()["experiments"][0]["trials"][0]
    assert persisted_trial["spot_execution_search_evidence_hash"] == trial["spot_execution_search_evidence_hash"]


def test_spot_search_failure_consumes_trial_but_is_ineligible(monkeypatch, tmp_path):
    tournament, _lineage, experiment = _tournament(tmp_path)
    alpha = _alpha(experiment["plan"], "candidate-bad-spot")
    monkeypatch.setattr(
        "scripts.research_tournament.verify_evidence_replay_binding",
        lambda evidence, reverify_source=True: (True, []),
    )
    monkeypatch.setattr(
        "scripts.research_tournament.build_spot_search_evidence",
        lambda **kwargs: _spot_search(kwargs["validation_evidence"], passed=False),
    )
    monkeypatch.setattr(
        "scripts.research_tournament.verify_spot_search_evidence",
        lambda *args, **kwargs: (True, []),
    )

    trial = tournament.register_candidate(
        experiment["id"], alpha, reverify_source=False
    )
    assert trial["trial_index"] == 1
    assert trial["eligible"] is False
    assert "spot_execution_search_failed:spot_search_nonpositive_return" in trial["reasons"]


def test_selection_and_terminal_bind_spot_search_and_terminal_artifacts(monkeypatch, tmp_path):
    tournament, lineage, experiment = _tournament(tmp_path)
    alpha = _alpha(experiment["plan"], "candidate-spot")
    spot_search = _spot_search(alpha, passed=True)
    monkeypatch.setattr(
        "scripts.research_tournament.verify_evidence_replay_binding",
        lambda evidence, reverify_source=True: (True, []),
    )
    monkeypatch.setattr(
        "scripts.research_tournament.build_spot_search_evidence",
        lambda **kwargs: spot_search,
    )
    monkeypatch.setattr(
        "scripts.research_tournament.verify_spot_search_evidence",
        lambda *args, **kwargs: (True, []),
    )
    monkeypatch.setattr(
        "scripts.research_tournament.verify_spot_terminal_evidence",
        lambda *args, **kwargs: (True, []),
    )

    tournament.register_candidate(experiment["id"], alpha, reverify_source=False)
    selection = tournament.seal_selection(experiment["id"])
    assert selection["execution_lifecycle_commitment"] == experiment["plan"]["execution_lifecycle_commitment"]
    assert selection["spot_execution_search_evidence_hash"] == spot_search["evidence_hash"]

    terminal_snapshot = snapshot_from_rows(
        _rows(80), kind="coinbase_candles", symbol="BTC-USD", granularity=60
    )
    monkeypatch.setattr(
        "scripts.research_tournament.load_canonical_snapshot",
        lambda **kwargs: terminal_snapshot,
    )
    monkeypatch.setattr(
        "scripts.research_tournament.replay_trade_returns_rust",
        lambda *args, **kwargs: [0.01] * 8,
    )
    spot_terminal = {
        "evidence_hash": "f" * 64,
        "passed": True,
        "reasons": [],
    }
    monkeypatch.setattr(
        "scripts.research_tournament.build_spot_terminal_evidence",
        lambda **kwargs: spot_terminal,
    )

    evidence = tournament.run_terminal_holdout(experiment["id"])
    assert evidence["research_terminal_passed"] is True
    assert evidence["spot_execution_search_evidence"] == spot_search
    assert evidence["spot_execution_terminal_evidence"] == spot_terminal
    assert evidence["passed"] is True
    assert evidence["execution_lifecycle_commitment"] == experiment["plan"]["execution_lifecycle_commitment"]

    terminal_event = [row for row in lineage.events() if row["type"] == "terminal_holdout"][0]
    assert terminal_event["payload"]["spot_execution_terminal_evidence_hash"] == spot_terminal["evidence_hash"]


def test_spot_terminal_failure_makes_committed_experiment_terminal_failed(monkeypatch, tmp_path):
    tournament, _lineage, experiment = _tournament(tmp_path)
    alpha = _alpha(experiment["plan"], "candidate-spot")
    spot_search = _spot_search(alpha, passed=True)
    monkeypatch.setattr(
        "scripts.research_tournament.verify_evidence_replay_binding",
        lambda evidence, reverify_source=True: (True, []),
    )
    monkeypatch.setattr(
        "scripts.research_tournament.build_spot_search_evidence",
        lambda **kwargs: spot_search,
    )
    monkeypatch.setattr(
        "scripts.research_tournament.verify_spot_search_evidence",
        lambda *args, **kwargs: (True, []),
    )
    monkeypatch.setattr(
        "scripts.research_tournament.verify_spot_terminal_evidence",
        lambda *args, **kwargs: (True, []),
    )
    tournament.register_candidate(experiment["id"], alpha, reverify_source=False)
    tournament.seal_selection(experiment["id"])

    terminal_snapshot = snapshot_from_rows(
        _rows(80), kind="coinbase_candles", symbol="BTC-USD", granularity=60
    )
    monkeypatch.setattr("scripts.research_tournament.load_canonical_snapshot", lambda **kwargs: terminal_snapshot)
    monkeypatch.setattr("scripts.research_tournament.replay_trade_returns_rust", lambda *args, **kwargs: [0.01] * 8)
    monkeypatch.setattr(
        "scripts.research_tournament.build_spot_terminal_evidence",
        lambda **kwargs: {
            "evidence_hash": "e" * 64,
            "passed": False,
            "reasons": ["spot_terminal_nonpositive_return"],
        },
    )

    evidence = tournament.run_terminal_holdout(experiment["id"])
    assert evidence["research_terminal_passed"] is True
    assert evidence["passed"] is False
    assert "spot_execution:spot_terminal_nonpositive_return" in evidence["reasons"]
    assert tournament.load()["experiments"][0]["status"] == "terminal_failed"
