from __future__ import annotations

from types import SimpleNamespace

import scripts.strategy_signal_scanner as scanner
from scripts.certified_strategy_runtime import CertifiedRuntimeError


def _context():
    return {
        "identity": {"candidate_id": "challenger-1"},
        "runtime_identity_hash": "identity-hash",
        "deployment": "canary",
        "canary_fraction": 0.5,
    }


def _args():
    return {
        "currency": "BTC-USD",
        "asset_class": "safe",
        "closes": [100.0] * 40,
        "volumes": [1.0] * 40,
        "current_price": 100.0,
        "highs": [101.0] * 40,
        "lows": [99.0] * 40,
    }


def test_uncertified_scanner_preserves_legacy_screen(monkeypatch):
    marker = [SimpleNamespace(strategy="legacy")]
    monkeypatch.setattr(scanner, "_get_runtime_context", lambda: None)
    monkeypatch.setattr(scanner, "_LEGACY_RUN_STRATEGIES", lambda *args, **kwargs: marker)
    assert scanner.run_strategies(**_args()) is marker


def test_selected_canary_uses_certified_evaluator_only(monkeypatch):
    marker = [SimpleNamespace(strategy="rsi_revert")]
    legacy_calls = []
    monkeypatch.setattr(scanner, "_get_runtime_context", _context)
    monkeypatch.setattr(scanner, "canary_selected", lambda identity, key, fraction: True)
    monkeypatch.setattr(scanner, "_LEGACY_RUN_STRATEGIES", lambda *args, **kwargs: legacy_calls.append(True))
    monkeypatch.setattr(scanner, "run_certified_strategy", lambda identity, **kwargs: marker)

    assert scanner.run_strategies(**_args()) is marker
    assert legacy_calls == []


def test_selected_canary_never_falls_back_to_defaults_after_runtime_failure(monkeypatch):
    legacy_calls = []
    monkeypatch.setattr(scanner, "_get_runtime_context", _context)
    monkeypatch.setattr(scanner, "canary_selected", lambda identity, key, fraction: True)
    monkeypatch.setattr(scanner, "_LEGACY_RUN_STRATEGIES", lambda *args, **kwargs: legacy_calls.append(True))

    def fail(identity, **kwargs):
        raise CertifiedRuntimeError("binary_drift")

    monkeypatch.setattr(scanner, "run_certified_strategy", fail)
    assert scanner.run_strategies(**_args()) == []
    assert legacy_calls == []


def test_certified_trade_plan_is_explicitly_signal_only(monkeypatch):
    signal = SimpleNamespace(
        runtime_certification={
            "certified": True,
            "runtime_identity_hash": "runtime-1",
            "candidate_id": "challenger-1",
        }
    )
    monkeypatch.setattr(
        scanner,
        "_LEGACY_BUILD_TRADE_PLAN",
        lambda *args, **kwargs: {"plan_type": "entry"},
    )
    plan = scanner._build_trade_plan(signal, object(), 100.0, [100.0], [101.0], [99.0], 0.0)
    certification = plan["runtime_certification"]
    assert certification["certified"] is True
    assert certification["certification_scope"] == "configured_signal_only_v1"
    assert certification["execution_lifecycle_certified"] is False
    assert certification["execution_lifecycle_blocker"] == "trade_lifecycle_not_canonical_replay_bound"
