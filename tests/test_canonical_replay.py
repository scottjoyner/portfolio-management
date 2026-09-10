from __future__ import annotations

import copy
import math

import pytest

import strategy_engine as S
from scripts.alpha_validation import build_alpha_validation_evidence
from scripts.backtest_framework import canonical_replay as C


def _require_native_replay() -> None:
    if not C.canonical_rust_backend_available():
        pytest.skip("compiled rust_core replay bindings unavailable")


def _rows(n: int = 360) -> list[list[float]]:
    rows = []
    for i in range(n):
        close = 100.0 + 8.0 * math.sin(i / 5.0) + i * 0.01
        rows.append([
            1_700_000_000.0 + i * 3600.0,
            close,
            close + 2.0,
            close - 2.0,
            close,
            1000.0 + (i % 11) * 10.0,
        ])
    return rows


def _bound_evidence(rows: list[list[float]]) -> tuple[dict, dict]:
    _require_native_replay()
    snapshot = C.snapshot_from_rows(
        rows,
        kind=C.DATASET_KIND,
        symbol="BTC-USD",
        granularity=3600,
    )
    fold_returns, attestation = C.attest_snapshot_replay(
        snapshot,
        strategy_name="ema_cross",
        n_folds=3,
        warmup=21,
        fee_bps=10.0,
    )
    evidence = build_alpha_validation_evidence(
        candidate_id="challenger-canonical",
        candidate_source_sha="a" * 40,
        candidate_config={"strategy": "ema_cross"},
        dataset_id=snapshot["manifest"]["dataset_id"],
        dataset_hash=snapshot["manifest"]["dataset_hash"],
        fold_returns=fold_returns,
        net_pnl_after_cost_usd=100.0,
        cost_coverage_ratio=2.0,
        regimes_tested=["trend_up", "trend_down", "range"],
        accounting_invariants_ok=True,
        lineage_verified=True,
        parameter_stability_score=0.9,
        bootstrap_samples=50,
        bootstrap_seed=7,
        created_at="2026-09-10T12:00:00+00:00",
    )
    return C.bind_evidence_to_replay(evidence, attestation), snapshot


def test_snapshot_hash_is_bound_to_exact_replayed_rows():
    rows = _rows()
    first = C.snapshot_from_rows(
        rows, kind=C.DATASET_KIND, symbol="BTC-USD", granularity=3600
    )
    mutated = copy.deepcopy(rows)
    mutated[123][4] += 0.25
    mutated[123][2] += 0.25  # preserve high >= close invariant
    second = C.snapshot_from_rows(
        mutated, kind=C.DATASET_KIND, symbol="BTC-USD", granularity=3600
    )
    assert first["manifest"]["dataset_id"] == second["manifest"]["dataset_id"]
    assert first["manifest"]["rows_hash"] != second["manifest"]["rows_hash"]
    assert first["manifest"]["dataset_hash"] != second["manifest"]["dataset_hash"]


def test_canonical_replay_trade_returns_match_rust_backtester():
    _require_native_replay()
    assert S._HAS_RUST, "strategy_engine must recognize the compiled native backend"

    rows = _rows()
    snapshot = C.snapshot_from_rows(
        rows, kind=C.DATASET_KIND, symbol="BTC-USD", granularity=3600
    )
    fold_returns, attestation = C.attest_snapshot_replay(
        snapshot,
        strategy_name="ema_cross",
        n_folds=3,
        warmup=21,
        fee_bps=10.0,
    )

    assert len(fold_returns) == 3
    for fold, returns in zip(attestation["folds"], fold_returns):
        test_rows = snapshot["rows"][fold["test_start"]:fold["test_end"]]
        closes = [row[4] for row in test_rows]
        volumes = [row[5] for row in test_rows]
        highs = [row[2] for row in test_rows]
        lows = [row[3] for row in test_rows]
        verdict = S._rust_backtest_strategy(
            "ema_cross",
            "BTC",
            closes,
            volumes,
            highs=highs,
            lows=lows,
            warmup=21,
            fee_bps=10.0,
            min_trades=1,
        )
        assert verdict is not None
        assert verdict.total_trades == len(returns)
        equity = 1.0
        for value in returns:
            equity *= 1.0 + value
        assert (equity - 1.0) * 100.0 == pytest.approx(
            verdict.total_return_pct, abs=1e-9
        )


def test_bound_evidence_rejects_a_different_dataset_even_with_same_label():
    evidence, snapshot = _bound_evidence(_rows())
    valid, reasons = C.verify_evidence_replay_binding(evidence, reverify_source=False)
    assert valid, reasons

    mutated = copy.deepcopy(snapshot["rows"])
    mutated[80][4] += 0.5
    mutated[80][2] += 0.5
    other = C.snapshot_from_rows(
        mutated, kind=C.DATASET_KIND, symbol="BTC-USD", granularity=3600
    )
    assert other["manifest"]["dataset_id"] == evidence["dataset_id"]
    assert other["manifest"]["dataset_hash"] != evidence["dataset_hash"]


def test_source_reverification_regenerates_rows_and_returns(monkeypatch):
    rows = _rows()
    evidence, _ = _bound_evidence(rows)

    monkeypatch.setattr(C, "load_candles", lambda kind, symbol, granularity: copy.deepcopy(rows))
    valid, reasons = C.verify_evidence_replay_binding(evidence, reverify_source=True)
    assert valid, reasons

    mutated = copy.deepcopy(rows)
    mutated[180][4] += 0.75
    mutated[180][2] += 0.75
    monkeypatch.setattr(C, "load_candles", lambda kind, symbol, granularity: copy.deepcopy(mutated))
    valid, reasons = C.verify_evidence_replay_binding(evidence, reverify_source=True)
    assert not valid
    assert "replay_dataset_source_mismatch" in reasons


def test_binding_rejects_attestation_for_other_returns():
    _require_native_replay()
    rows = _rows()
    snapshot = C.snapshot_from_rows(
        rows, kind=C.DATASET_KIND, symbol="BTC-USD", granularity=3600
    )
    fold_returns, attestation = C.attest_snapshot_replay(
        snapshot, strategy_name="ema_cross", n_folds=3, warmup=21
    )
    changed_returns = copy.deepcopy(fold_returns)
    if changed_returns[0]:
        changed_returns[0][0] += 0.001
    else:
        changed_returns[0].append(0.001)
    evidence = build_alpha_validation_evidence(
        candidate_id="challenger-canonical",
        candidate_source_sha="a" * 40,
        candidate_config={"strategy": "ema_cross"},
        dataset_id=snapshot["manifest"]["dataset_id"],
        dataset_hash=snapshot["manifest"]["dataset_hash"],
        fold_returns=changed_returns,
        net_pnl_after_cost_usd=1.0,
        cost_coverage_ratio=2.0,
        regimes_tested=["up", "down", "range"],
        accounting_invariants_ok=True,
        lineage_verified=True,
        parameter_stability_score=0.9,
        bootstrap_samples=20,
        created_at="2026-09-10T12:00:00+00:00",
    )
    with pytest.raises(ValueError, match="replay_fold_returns_hash_mismatch"):
        C.bind_evidence_to_replay(evidence, attestation)


def test_runner_source_is_part_of_attestation_integrity():
    evidence, _ = _bound_evidence(_rows())
    tampered = copy.deepcopy(evidence)
    tampered["replay_attestation"]["runner_source_sha256"] = "0" * 64
    valid, reasons = C.verify_evidence_replay_binding(tampered, reverify_source=False)
    assert not valid
    assert "replay_runner_source_mismatch" in reasons


def test_configured_rsi_defaults_match_legacy_replay():
    _require_native_replay()
    rows = _rows()
    legacy = C.replay_trade_returns_rust("rsi_revert", rows, warmup=14, fee_bps=10.0)
    configured = C.replay_trade_returns_rust(
        "rsi_revert", rows, warmup=14, fee_bps=10.0,
        strategy_config={"period": 14, "oversold": 30.0, "overbought": 70.0},
    )
    assert configured == legacy


def test_configured_rsi_attestation_binds_exact_execution_config():
    _require_native_replay()
    snapshot = C.snapshot_from_rows(
        _rows(), kind=C.DATASET_KIND, symbol="BTC-USD", granularity=3600
    )
    config = {"period": 10, "oversold": 25.0, "overbought": 75.0}
    fold_returns, attestation = C.attest_snapshot_replay(
        snapshot, strategy_name="rsi_revert", strategy_config=config, n_folds=3, warmup=10
    )
    assert len(fold_returns) == 3
    assert attestation["execution_config_bound"] is True
    assert attestation["strategy_config"] == config
    assert attestation["strategy_config_hash"] == C.stable_hash(config)


def test_strategy_config_normalization_fails_closed():
    assert C.normalize_strategy_config("rsi_revert", None) == {}
    assert C.normalize_strategy_config(
        "rsi_revert", {"period": 14, "oversold": 30, "overbought": 70}
    ) == {"period": 14, "oversold": 30.0, "overbought": 70.0}
    with pytest.raises(ValueError, match="keys mismatch"):
        C.normalize_strategy_config("rsi_revert", {"period": 14, "oversold": 30.0})
    with pytest.raises(ValueError, match="integer"):
        C.normalize_strategy_config(
            "rsi_revert", {"period": 14.5, "oversold": 30.0, "overbought": 70.0}
        )
    with pytest.raises(ValueError, match="thresholds"):
        C.normalize_strategy_config(
            "rsi_revert", {"period": 14, "oversold": 80.0, "overbought": 70.0}
        )
    with pytest.raises(ValueError, match="not supported"):
        C.normalize_strategy_config(
            "ema_cross", {"period": 14, "oversold": 30.0, "overbought": 70.0}
        )


def test_binding_rejects_candidate_metadata_different_from_executed_config():
    _require_native_replay()
    snapshot = C.snapshot_from_rows(
        _rows(), kind=C.DATASET_KIND, symbol="BTC-USD", granularity=3600
    )
    config = {"period": 14, "oversold": 30.0, "overbought": 70.0}
    fold_returns, attestation = C.attest_snapshot_replay(
        snapshot, strategy_name="rsi_revert", strategy_config=config, n_folds=3, warmup=14
    )
    evidence = build_alpha_validation_evidence(
        candidate_id="challenger-config-mismatch",
        candidate_source_sha="a" * 40,
        candidate_config={"period": 7, "oversold": 30.0, "overbought": 70.0},
        dataset_id=snapshot["manifest"]["dataset_id"],
        dataset_hash=snapshot["manifest"]["dataset_hash"],
        fold_returns=fold_returns,
        net_pnl_after_cost_usd=1.0,
        cost_coverage_ratio=2.0,
        regimes_tested=["up", "down", "range"],
        accounting_invariants_ok=True,
        lineage_verified=True,
        parameter_stability_score=0.9,
        bootstrap_samples=20,
        created_at="2026-09-10T12:00:00+00:00",
    )
    with pytest.raises(ValueError, match="replay_candidate_execution_config_mismatch"):
        C.bind_evidence_to_replay(evidence, attestation)
