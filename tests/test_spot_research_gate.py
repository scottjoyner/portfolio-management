from __future__ import annotations

import copy

from scripts.alpha_validation import stable_hash
from scripts.backtest_framework.canonical_replay import snapshot_from_rows
from scripts.selection_bias import SearchMultiplicityPolicy
from scripts import spot_research_gate as gate


def _rows(count: int = 600) -> list[list[float]]:
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


def _snapshot():
    return snapshot_from_rows(
        _rows(), kind="coinbase_candles", symbol="BTC-USD", granularity=60
    )


def _policy():
    return SearchMultiplicityPolicy(
        max_candidate_trials=20,
        familywise_alpha=0.05,
        min_nonzero_trades=10,
        dependence_block_size=5,
        min_nonzero_blocks=5,
    ).artifact()


def _validation(dataset: dict) -> dict:
    config = {"period": 14, "oversold": 30.0, "overbought": 70.0}
    return {
        "candidate_id": "candidate-spot",
        "candidate_source_sha": "a" * 40,
        "candidate_config": config,
        "periods_per_year": 365 * 24,
        "evidence_hash": "b" * 64,
        "replay_attestation": {
            "dataset": dataset,
            "strategy_name": "rsi_revert",
            "strategy_config": config,
            "n_folds": 4,
            "purge_size": 2,
            "embargo_size": 2,
            "warmup": 30,
            "fee_bps": 10.0,
            "max_hold_bars": 12,
        },
    }


def _spot_attestation(dataset: dict, config: dict, fold_returns: list[list[float]]) -> dict:
    lifecycle = {
        "method": "spot_long_only_opposite_signal_or_max_hold_v1",
        "position_mode": "spot_long_only",
        "dataset_kind": dataset["kind"],
        "dataset_symbol": dataset["symbol"],
        "granularity_seconds": dataset["granularity"],
        "warmup_bars": 30,
        "fee_bps": 10.0,
        "max_hold_bars": 12,
        "buy_while_flat": "open_long",
        "buy_while_long": "noop",
        "sell_while_flat": "noop",
        "sell_while_long": "close_long",
        "max_hold_on_quiet_bar": True,
        "same_bar_reentry_after_forced_exit": False,
        "close_open_trade_at_observation_end": True,
    }
    core = {
        "schema_version": 1,
        "attestation_type": "canonical_spot_long_only_rust_replay_v1",
        "runner": "scripts.backtest_framework.spot_execution_replay",
        # Source verification is disabled in these focused unit tests.
        "runner_source_sha256": "c" * 64,
        "dataset": dataset,
        "strategy_name": "rsi_revert",
        "strategy_config": config,
        "strategy_config_hash": stable_hash(config),
        "lifecycle": lifecycle,
        "lifecycle_hash": stable_hash(lifecycle),
        "n_folds": 4,
        "purge_size": 2,
        "embargo_size": 2,
        "folds": [
            {
                "fold": index,
                "test_start": index * 100,
                "test_end": (index + 1) * 100,
                "test_rows_hash": str(index) * 64,
                "return_count": len(returns),
                "returns_hash": stable_hash(returns),
                "events_hash": str(index + 1) * 64,
            }
            for index, returns in enumerate(fold_returns)
        ],
        "fold_returns_hash": stable_hash(fold_returns),
    }
    return {**core, "attestation_hash": stable_hash(core)}


def test_lifecycle_commitment_is_hash_bound_and_precommits_policies():
    commitment = gate.build_execution_lifecycle_commitment("spot_long_only_v1")
    assert commitment["required_for_promotion"] is True
    assert commitment["position_mode"] == "spot_long_only"
    assert commitment["terminal_policy"]["min_trades"] == 5
    ok, reasons = gate.verify_execution_lifecycle_commitment(commitment)
    assert ok is True, reasons

    tampered = copy.deepcopy(commitment)
    tampered["terminal_policy"]["min_trades"] = 1
    ok, reasons = gate.verify_execution_lifecycle_commitment(tampered)
    assert ok is False
    assert "execution_lifecycle_commitment_mismatch" in reasons


def test_spot_search_evidence_must_pass_same_precommitted_family_gate(monkeypatch):
    snapshot = _snapshot()
    dataset = snapshot["manifest"]
    validation = _validation(dataset)
    config = validation["candidate_config"]
    strong = [[0.01] * 25 for _ in range(4)]
    attestation = _spot_attestation(dataset, config, strong)

    monkeypatch.setattr(gate, "load_canonical_snapshot", lambda **kwargs: snapshot)
    monkeypatch.setattr(
        gate,
        "attest_spot_snapshot_replay",
        lambda *args, **kwargs: (strong, attestation),
    )
    # The real attestation verifier is covered in test_spot_execution_replay;
    # this unit isolates research-gate binding semantics.
    monkeypatch.setattr(
        gate,
        "verify_spot_replay_attestation",
        lambda *args, **kwargs: (True, []),
    )

    commitment = gate.build_execution_lifecycle_commitment("spot_long_only_v1")
    evidence = gate.build_spot_search_evidence(
        commitment=commitment,
        plan_search_dataset=dataset,
        multiple_testing_policy=_policy(),
        validation_evidence=validation,
    )
    assert evidence["passed"] is True, evidence["reasons"]
    assert evidence["multiple_testing"]["passed"] is True
    assert evidence["alpha_validation_evidence_hash"] == validation["evidence_hash"]

    ok, reasons = gate.verify_spot_search_evidence(
        evidence,
        commitment=commitment,
        plan_search_dataset=dataset,
        multiple_testing_policy=_policy(),
        validation_evidence=validation,
        reverify_source=False,
    )
    assert ok is True, reasons

    tampered = copy.deepcopy(evidence)
    tampered["candidate_config"]["period"] = 15
    tampered["candidate_config_hash"] = stable_hash(tampered["candidate_config"])
    core = dict(tampered)
    core.pop("evidence_hash")
    tampered["evidence_hash"] = stable_hash(core)
    ok, reasons = gate.verify_spot_search_evidence(
        tampered,
        commitment=commitment,
        plan_search_dataset=dataset,
        multiple_testing_policy=_policy(),
        validation_evidence=validation,
        reverify_source=False,
    )
    assert ok is False
    assert "spot_search_candidate_config_binding_mismatch" in reasons


def test_spot_terminal_binds_strategy_search_artifact_lifecycle_and_precommitted_policy(monkeypatch):
    snapshot = _snapshot()
    dataset = snapshot["manifest"]
    validation = _validation(dataset)
    config = validation["candidate_config"]
    strong = [[0.01] * 25 for _ in range(4)]
    attestation = _spot_attestation(dataset, config, strong)
    commitment = gate.build_execution_lifecycle_commitment("spot_long_only_v1")

    monkeypatch.setattr(gate, "load_canonical_snapshot", lambda **kwargs: snapshot)
    monkeypatch.setattr(
        gate,
        "attest_spot_snapshot_replay",
        lambda *args, **kwargs: (strong, attestation),
    )
    monkeypatch.setattr(
        gate,
        "verify_spot_replay_attestation",
        lambda *args, **kwargs: (True, []),
    )
    search = gate.build_spot_search_evidence(
        commitment=commitment,
        plan_search_dataset=dataset,
        multiple_testing_policy=_policy(),
        validation_evidence=validation,
    )

    terminal_snapshot = snapshot_from_rows(
        _rows(80), kind="coinbase_candles", symbol="BTC-USD", granularity=60
    )
    terminal_returns = [0.01] * 8
    terminal_events = [{"event": "OPEN_LONG"}, {"event": "CLOSE_LONG"}]
    monkeypatch.setattr(
        gate,
        "replay_spot_long_only_rust",
        lambda *args, **kwargs: {"returns": terminal_returns, "events": terminal_events},
    )

    terminal = gate.build_spot_terminal_evidence(
        commitment=commitment,
        search_evidence=search,
        terminal_dataset=terminal_snapshot["manifest"],
        terminal_rows=terminal_snapshot["rows"],
        periods_per_year=365 * 24,
    )
    assert terminal["passed"] is True, terminal["reasons"]
    assert terminal["strategy_name"] == "rsi_revert"
    assert terminal["spot_search_evidence_hash"] == search["evidence_hash"]
    assert terminal["spot_execution_lifecycle_hash"] == attestation["lifecycle_hash"]

    ok, reasons = gate.verify_spot_terminal_evidence(
        terminal,
        search_evidence=search,
        reverify_source=False,
    )
    assert ok is True, reasons

    tampered = copy.deepcopy(terminal)
    tampered["policy"]["min_trades"] = 1
    core = dict(tampered)
    core.pop("evidence_hash")
    tampered["evidence_hash"] = stable_hash(core)
    ok, reasons = gate.verify_spot_terminal_evidence(
        tampered,
        search_evidence=search,
        reverify_source=False,
    )
    assert ok is False
    assert "spot_terminal_policy_not_precommitted" in reasons
