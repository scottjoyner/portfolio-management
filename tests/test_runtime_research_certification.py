from __future__ import annotations

import copy
import json
import sys
from pathlib import Path
from types import SimpleNamespace

from scripts.alpha_validation import build_alpha_validation_evidence, stable_hash
from scripts.challenger_manager import ChallengerRegistry
from scripts.runtime_research_certification import (
    _identity_core,
    bind_certification_to_runtime,
    build_runtime_research_certification,
    load_active_research_certification,
    run_certified_current_signal,
    verify_static_certification,
)


class FakeLineage:
    def __init__(self):
        self.rows = []
    def append(self, event_type, payload, *, actor, parents):
        row = {"id": f"lineage-{len(self.rows)+1}", "event_type": event_type, "payload": payload, "parents": parents}
        self.rows.append(row)
        return row


def _alpha(candidate_id: str, config: dict) -> dict:
    evidence = build_alpha_validation_evidence(
        candidate_id=candidate_id,
        candidate_source_sha="a" * 40,
        candidate_config=config,
        dataset_id="btc-hourly-search",
        dataset_hash="b" * 64,
        fold_returns=[[0.02] * 12, [0.015] * 12, [0.01] * 12],
        net_pnl_after_cost_usd=125.0,
        cost_coverage_ratio=2.5,
        regimes_tested=["up", "down", "range"],
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
        "strategy_config": copy.deepcopy(config),
        "strategy_config_hash": stable_hash(config),
        "execution_config_bound": True,
        "runner_id": "fixture",
    }
    evidence["evidence_hash"] = stable_hash(evidence)
    return evidence


def _terminal(evidence: dict) -> dict:
    config = copy.deepcopy(evidence["candidate_config"])
    terminal = {
        "schema_version": 2,
        "candidate_id": evidence["candidate_id"],
        "candidate_source_sha": evidence["candidate_source_sha"],
        "candidate_config": config,
        "candidate_config_hash": evidence["candidate_config_hash"],
        "alpha_validation_evidence_hash": evidence["evidence_hash"],
        "strategy_name": "rsi_revert",
        "strategy_config": config,
        "strategy_config_hash": stable_hash(config),
        "terminal_dataset": {"kind": "coinbase_candles", "symbol": "BTC-USD", "granularity": 3600},
        "metrics": {"trade_count": 12, "total_return_pct": 8.5, "profit_factor": 1.6, "max_drawdown_pct": 4.0},
        "experiment_id": "experiment-runtime",
        "experiment_hash": "c" * 64,
        "selection_hash": "d" * 64,
        "terminal_result_hash": "e" * 64,
        "passed": True,
        "reasons": [],
        "terminal_lineage_id": "fixture-terminal-lineage",
    }
    terminal["evidence_hash"] = stable_hash(terminal)
    return terminal


def _registry(tmp_path: Path):
    registry = ChallengerRegistry(tmp_path / "challengers.json", tmp_path / "active.json", lineage=FakeLineage())
    config = {"period": 14, "oversold": 30.0, "overbought": 70.0}
    challenger = registry.propose(config, rationale="runtime certification fixture", model_request_id="request-1")
    return registry, challenger


def _patch(monkeypatch):
    monkeypatch.setattr("scripts.challenger_manager.verify_evidence_replay_binding", lambda evidence, reverify_source=True: (True, []))
    monkeypatch.setattr("scripts.challenger_manager.verify_terminal_holdout_evidence", lambda evidence, lineage=None, reverify_source=True: (True, []))


def _promoted(tmp_path, monkeypatch):
    registry, challenger = _registry(tmp_path)
    evidence = _alpha(challenger["id"], challenger["parameters"])
    terminal = _terminal(evidence)
    _patch(monkeypatch)
    result = registry.evaluate(
        challenger["id"], {"net_pnl_after_cost_usd": 0, "max_drawdown_pct": 0},
        validation_evidence=evidence, terminal_holdout_evidence=terminal,
    )
    assert result["approved"] is True, result["reasons"]
    config = registry.promote(challenger["id"], canary_fraction=0.05)
    return registry, challenger, evidence, terminal, config


def test_executable_certification_binds_exact_promoted_scope(tmp_path, monkeypatch):
    registry, challenger, evidence, terminal, config = _promoted(tmp_path, monkeypatch)
    certification = config["research_certification"]
    assert certification is not None
    valid, reasons = verify_static_certification(certification)
    assert valid is True, reasons
    assert certification["candidate_id"] == challenger["id"]
    assert certification["strategy_name"] == "rsi_revert"
    assert certification["candidate_config"] == challenger["parameters"]
    assert certification["symbol"] == "BTC-USD"
    assert certification["granularity"] == "3600"
    assert certification["alpha_validation_evidence_hash"] == evidence["evidence_hash"]
    assert certification["terminal_holdout_evidence_hash"] == terminal["evidence_hash"]
    assert config["runtime_certification_hash"] == certification["certification_hash"]
    verified, verify_reasons, recovered = registry.verify_active_runtime_config(reverify_source=False)
    assert verified is True, verify_reasons
    assert recovered == certification
    assert load_active_research_certification(tmp_path / "active.json") == certification


def test_executable_certification_rejects_rehashed_identity_tampering(tmp_path, monkeypatch):
    registry, _, _, _, config = _promoted(tmp_path, monkeypatch)
    tampered = copy.deepcopy(config)
    cert = tampered["research_certification"]
    cert["candidate_source_sha"] = "f" * 40
    cert["certification_hash"] = stable_hash(_identity_core(cert))
    tampered["runtime_certification_hash"] = cert["certification_hash"]
    valid, reasons, recovered = registry.verify_active_runtime_config(tampered, reverify_source=False)
    assert valid is False
    assert "active_runtime_certification_identity_mismatch" in reasons
    assert recovered is None


def test_runtime_binding_requires_exact_symbol_and_granularity(tmp_path, monkeypatch):
    _, _, _, _, config = _promoted(tmp_path, monkeypatch)
    certification = config["research_certification"]
    assert bind_certification_to_runtime(certification, product_id="BTC-USD", granularity="ONE_HOUR") is not None
    assert bind_certification_to_runtime(certification, product_id="ETH-USD", granularity="ONE_HOUR") is None
    assert bind_certification_to_runtime(certification, product_id="BTC-USD", granularity="FIFTEEN_MINUTE") is None


def test_certified_signal_uses_exact_configured_rust_path(tmp_path, monkeypatch):
    _, _, _, _, config = _promoted(tmp_path, monkeypatch)
    bound = bind_certification_to_runtime(config["research_certification"], product_id="BTC-USD", granularity=3600)
    assert bound is not None
    observed = {}
    def runner(closes, opens, volumes, highs, lows, *, period, oversold, overbought):
        observed.update(period=period, oversold=oversold, overbought=overbought, opens=opens, closes=closes)
        return ("BUY", 0.73, "configured RSI")
    monkeypatch.setitem(sys.modules, "rust_core", SimpleNamespace(run_rsi_revert_opens_configured_py=runner))
    signal = run_certified_current_signal(
        bound,
        closes=[100.0 + i for i in range(40)], volumes=[10.0] * 40,
        highs=[101.0 + i for i in range(40)], lows=[99.0 + i for i in range(40)],
    )
    assert signal is not None
    assert signal["action"] == "BUY"
    assert signal["research_certification"]["certification_hash"] == config["runtime_certification_hash"]
    assert observed == {"period": 14, "oversold": 30.0, "overbought": 70.0, "opens": observed["closes"], "closes": observed["closes"]}
