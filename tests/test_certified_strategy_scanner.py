from __future__ import annotations

from types import SimpleNamespace

import scripts.strategy_signal_scanner as scanner
from scripts.certified_strategy_runtime import CertifiedRuntimeError


def _identity():
    return {
        "candidate_id": "challenger-1",
        "strategy_name": "rsi_revert",
        "replay_execution_config": {
            "dataset_symbol": "BTC-USD",
            "granularity_seconds": 3600,
            "warmup_bars": 30,
            "fee_bps": 10.0,
            "max_hold_bars": 12,
        },
    }


def _context():
    return {
        "identity": _identity(),
        "runtime_identity_hash": "identity-hash",
        "deployment": "canary",
        "canary_fraction": 1.0,
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


def _series():
    candles = []
    for index in range(80):
        close = 100.0 + index * 0.1
        candles.append({
            "open": close,
            "high": close + 0.5,
            "low": close - 0.5,
            "close": close,
            "volume": 1000.0,
        })
    return SimpleNamespace(source="fixture", candles=candles)


def _signal(action="BUY"):
    return SimpleNamespace(
        action=action,
        confidence=0.72,
        reason="configured RSI",
        strategy="rsi_revert",
        runtime_certification={
            "certified": True,
            "runtime_identity_hash": "identity-hash",
            "candidate_id": "challenger-1",
            "candidate_source_sha": "a" * 40,
            "strategy_name": "rsi_revert",
            "strategy_config_hash": "b" * 64,
            "alpha_validation_evidence_hash": "c" * 64,
            "terminal_holdout_evidence_hash": "d" * 64,
            "replay_attestation_hash": "e" * 64,
            "runtime_binary_sha256": "f" * 64,
            "runtime_evaluator": "rust_core.run_rsi_revert_opens_configured_py",
            "replay_execution_config": _identity()["replay_execution_config"],
            "replay_execution_config_hash": "1" * 64,
        },
    )


def test_uncertified_direct_dispatch_preserves_legacy_screen(monkeypatch):
    marker = [SimpleNamespace(strategy="legacy")]
    monkeypatch.setattr(scanner, "_get_runtime_context", lambda: None)
    monkeypatch.setattr(scanner._legacy, "run_strategies", lambda *args, **kwargs: marker)
    assert scanner.run_strategies(**_args()) is marker


def test_selected_canary_direct_dispatch_uses_certified_evaluator_only(monkeypatch):
    marker = [SimpleNamespace(strategy="rsi_revert")]
    legacy_calls = []
    monkeypatch.setattr(scanner, "_get_runtime_context", _context)
    monkeypatch.setattr(scanner, "canary_selected", lambda identity, key, fraction: True)
    monkeypatch.setattr(scanner._legacy, "run_strategies", lambda *args, **kwargs: legacy_calls.append(True))
    monkeypatch.setattr(scanner, "run_certified_strategy", lambda identity, **kwargs: marker)

    assert scanner.run_strategies(**_args()) is marker
    assert legacy_calls == []


def test_selected_canary_never_falls_back_after_runtime_failure(monkeypatch):
    legacy_calls = []
    monkeypatch.setattr(scanner, "_get_runtime_context", _context)
    monkeypatch.setattr(scanner, "canary_selected", lambda identity, key, fraction: True)
    monkeypatch.setattr(scanner._legacy, "run_strategies", lambda *args, **kwargs: legacy_calls.append(True))

    def fail(identity, **kwargs):
        raise CertifiedRuntimeError("binary_drift")

    monkeypatch.setattr(scanner, "run_certified_strategy", fail)
    assert scanner.run_strategies(**_args()) == []
    assert legacy_calls == []


def test_canary_scope_requires_exact_symbol_and_granularity(monkeypatch):
    monkeypatch.setattr(scanner, "_get_runtime_context", _context)
    monkeypatch.setattr(scanner, "canary_selected", lambda identity, key, fraction: key == "BTC-USD")

    assert scanner._canary_context_for("BTC-USD", "ONE_HOUR") is not None
    assert scanner._canary_context_for("BTC-USD", "FIVE_MINUTE") is None
    assert scanner._canary_context_for("ETH-USD", "ONE_HOUR") is None


def test_certified_scan_bypasses_legacy_default_backtest(monkeypatch):
    legacy_calls = []
    monkeypatch.setattr(scanner, "_canary_context_for", lambda product, granularity: _context())
    monkeypatch.setattr(scanner, "_LEGACY_SCAN_PRODUCT", lambda *args, **kwargs: legacy_calls.append(True))
    monkeypatch.setattr(scanner._legacy, "_fetch_live_candles", lambda *args, **kwargs: _series())
    monkeypatch.setattr(scanner, "run_certified_strategy", lambda identity, **kwargs: [_signal("BUY")])

    rows = scanner.scan_product(
        "BTC-USD", "ONE_HOUR", 30, 0.99, 0.99, cache_ttl_seconds=1, refresh=False
    )

    assert legacy_calls == []
    assert len(rows) == 1
    row = rows[0]
    assert row["strategy"] == "rsi_revert"
    assert row["trade_intent"] == "entry"
    assert row["same_window_backtest_applied"] is False
    assert row["backtest_reason"] == "certified_runtime_bypasses_same_window_default_backtest"
    # Even impossible legacy screening thresholds must not suppress the exact
    # promoted signal.
    assert row["weighted_confidence"] == 0.72
    certification = row["trade_plan"]["runtime_certification"]
    assert certification["execution_lifecycle_certified"] is False
    assert certification["execution_lifecycle_blocker"] == "fresh_spot_long_only_attestation_required"
    assert row["trade_plan"]["take_profit_price"] is None
    assert row["trade_plan"]["stop_loss_price"] is None


def test_certified_sell_is_close_long_signal_not_short_entry(monkeypatch):
    monkeypatch.setattr(scanner, "_canary_context_for", lambda product, granularity: _context())
    monkeypatch.setattr(scanner._legacy, "_fetch_live_candles", lambda *args, **kwargs: _series())
    monkeypatch.setattr(scanner, "run_certified_strategy", lambda identity, **kwargs: [_signal("SELL")])

    rows = scanner.scan_product("BTC-USD", "ONE_HOUR", 30, 0.55, 0.55)

    assert len(rows) == 1
    assert rows[0]["trade_intent"] == "exit"
    assert rows[0]["execution_purpose"] == "close_long"
    assert rows[0]["trade_plan"]["position_side"] == "long"


def test_noncanary_scan_still_uses_legacy_discovery(monkeypatch):
    marker = [{"strategy": "legacy"}]
    monkeypatch.setattr(scanner, "_canary_context_for", lambda product, granularity: None)
    monkeypatch.setattr(scanner, "_LEGACY_SCAN_PRODUCT", lambda *args, **kwargs: marker)

    assert scanner.scan_product("ETH-USD", "ONE_HOUR", 30, 0.55, 0.55) is marker
