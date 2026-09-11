from __future__ import annotations

import json
import multiprocessing as mp
import time
from pathlib import Path

import pytest

from scripts.alpha_validation import build_alpha_validation_evidence, stable_hash
from scripts.backtest_framework.canonical_replay import snapshot_from_rows
from scripts.learning_lineage import LineageStore
from scripts.research_tournament import ResearchTournament, _registry_mutation


def _locked_increment_worker(lock_path: str, counter_path: str) -> None:
    probe = _LockedProbe(Path(lock_path))
    probe.increment(Path(counter_path))


class _LockedProbe:
    def __init__(self, lock_path: Path):
        self.registry_lock_path = lock_path

    @_registry_mutation
    def increment(self, counter_path: Path) -> None:
        current = int(counter_path.read_text()) if counter_path.exists() else 0
        time.sleep(0.03)
        counter_path.write_text(str(current + 1))


def _rows(count: int = 360) -> list[list[float]]:
    rows = []
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


def _tournament(tmp_path: Path, *, max_candidate_trials: int = 2):
    snapshot = snapshot_from_rows(
        _rows(), kind="coinbase_candles", symbol="BTC-USD", granularity=60
    )
    lineage = LineageStore(tmp_path / "lineage.jsonl")
    tournament = ResearchTournament(tmp_path / "experiments.json", lineage=lineage)
    experiment = tournament.create_from_snapshot(
        snapshot,
        strategy_name="rsi_revert",
        holdout_bars=40,
        embargo_bars=5,
        min_search_bars=200,
        max_candidate_trials=max_candidate_trials,
        experiment_id="experiment-hardening",
    )
    return tournament, lineage, experiment


def _valid_alpha(plan: dict, candidate_id: str) -> dict:
    config = {"period": 14, "oversold": 30.0, "overbought": 70.0}
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
        "warmup": 30,
        "fee_bps": 0.0,
        "max_hold_bars": 0,
    }
    evidence["evidence_hash"] = stable_hash(evidence)
    return evidence


def test_registry_mutation_lock_serializes_processes(tmp_path: Path):
    lock_path = tmp_path / ".experiments.json.lock"
    counter_path = tmp_path / "counter.txt"
    counter_path.write_text("0")
    ctx = mp.get_context("spawn")
    processes = [
        ctx.Process(
            target=_locked_increment_worker,
            args=(str(lock_path), str(counter_path)),
        )
        for _ in range(5)
    ]

    for process in processes:
        process.start()
    for process in processes:
        process.join(timeout=30)
        assert process.exitcode == 0

    assert counter_path.read_text() == "5"


def test_noncanonical_candidate_consumes_budget_and_persists_rejection(tmp_path: Path):
    tournament, lineage, experiment = _tournament(tmp_path, max_candidate_trials=2)
    malformed = _valid_alpha(experiment["plan"], "candidate-malformed")
    malformed["net_pnl_after_cost_usd"] = float("nan")
    malformed["unserializable"] = {"not-json"}

    trial = tournament.register_candidate(
        "experiment-hardening", malformed, reverify_source=False
    )

    assert trial["trial_index"] == 1
    assert trial["candidate_id"] == "candidate-malformed"
    assert trial["eligible"] is False
    assert "candidate_evidence_not_canonical" in trial["reasons"]
    assert trial["validation_evidence"] == {
        "candidate_id": "candidate-malformed",
        "canonicalization_failed": True,
    }
    assert trial["validation_evidence_hash"] is None
    assert trial["selection_metrics"] == {
        "net_pnl_after_cost_usd": 0.0,
        "profit_factor": 0.0,
        "max_drawdown_pct": 1e30,
        "annualized_return_pct": 0.0,
        "out_of_sample_trades": 0,
    }

    persisted = json.loads((tmp_path / "experiments.json").read_text())
    persisted_trial = persisted["experiments"][0]["trials"][0]
    assert persisted_trial["candidate_id"] == "candidate-malformed"
    assert persisted_trial["eligible"] is False
    assert len([
        row for row in lineage.events() if row["type"] == "candidate_trial"
    ]) == 1
    assert lineage.verify()["ok"] is True

    second = _valid_alpha(experiment["plan"], "candidate-second")
    second["bad_metric"] = object()
    trial2 = tournament.register_candidate(
        "experiment-hardening", second, reverify_source=False
    )
    assert trial2["trial_index"] == 2

    with pytest.raises(ValueError, match="candidate search budget exhausted"):
        tournament.register_candidate(
            "experiment-hardening",
            _valid_alpha(experiment["plan"], "candidate-third"),
            reverify_source=False,
        )
