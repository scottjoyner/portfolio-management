from __future__ import annotations

from types import SimpleNamespace

import pytest

import scripts.certified_shadow_signal_probe as probe


IDENTITY = {
    "candidate_id": "challenger-shadow-probe",
    "candidate_source_sha": "a" * 40,
    "strategy_name": "rsi_revert",
    "strategy_config": {"period": 14, "oversold": 30.0, "overbought": 70.0},
    "strategy_config_hash": "b" * 64,
    "replay_execution_config": {
        "method": "canonical_replay_lifecycle_v1",
        "dataset_kind": "spot",
        "dataset_symbol": "BTC-USD",
        "granularity_seconds": 3600,
        "warmup_bars": 30,
        "fee_bps": 7.5,
        "max_hold_bars": 12,
        "exit_on_opposite_signal": True,
        "close_open_trade_at_observation_end": True,
    },
    "replay_execution_config_hash": "c" * 64,
    "deployment_bundle_hash": "d" * 64,
}
CONTEXT = {
    "identity": IDENTITY,
    "runtime_identity_hash": "e" * 64,
    "deployment": "canary",
    # Deliberately tiny: the raw shadow probe must ignore execution routing.
    "canary_fraction": 0.01,
}


def _series():
    candles = []
    for index in range(40):
        price = 100.0 + index
        candles.append({
            "open": price - 0.5,
            "high": price + 1.0,
            "low": price - 1.0,
            "close": price,
            "volume": 10.0,
        })
    return SimpleNamespace(product_id="BTC-USD", source="test-feed", candles=candles)


def _signal(action="SELL"):
    return SimpleNamespace(
        action=action,
        price=139.0,
        confidence=0.37,
        reason="raw certified probe signal",
        strategy="rsi_revert",
        runtime_certification={
            "certified": True,
            "runtime_identity_hash": CONTEXT["runtime_identity_hash"],
            "candidate_id": IDENTITY["candidate_id"],
            "candidate_source_sha": IDENTITY["candidate_source_sha"],
            "strategy_name": "rsi_revert",
            "strategy_config_hash": IDENTITY["strategy_config_hash"],
            "alpha_validation_evidence_hash": "f" * 64,
            "terminal_holdout_evidence_hash": "1" * 64,
            "replay_attestation_hash": "2" * 64,
            "runtime_binary_sha256": "3" * 64,
            "runtime_evaluator": "rust_core.run_rsi_revert_opens_configured_py",
            "replay_execution_config": IDENTITY["replay_execution_config"],
            "replay_execution_config_hash": IDENTITY["replay_execution_config_hash"],
            "deployment_bundle_hash": IDENTITY["deployment_bundle_hash"],
        },
    )


def test_probe_observes_certified_signal_without_same_window_screen_or_canary(monkeypatch):
    monkeypatch.setattr(probe.scanner, "_fetch_live_candles", lambda *args, **kwargs: _series())
    calls = []

    def run(identity, **kwargs):
        calls.append((identity, kwargs))
        return [_signal("SELL")]

    monkeypatch.setattr(probe, "run_certified_strategy", run)
    result = probe.probe_certified_shadow_signals(
        runtime_context=CONTEXT,
        observed_at="2026-09-12T20:30:00+00:00",
    )

    assert result["ok"] is True
    assert result["canary_fraction_ignored_for_shadow"] is True
    assert result["same_window_screen_applied"] is False
    assert result["symbol"] == "BTC-USD"
    assert len(result["signals"]) == 1
    signal = result["signals"][0]
    assert signal["action"] == "SELL"
    assert signal["shadow_only"] is True
    assert signal["execution_canary_applied"] is False
    assert signal["measurement_scope"] == probe.MEASUREMENT_SCOPE
    assert signal["weighted_confidence"] == 0.37
    assert "win_rate" not in signal
    certification = signal["trade_plan"]["runtime_certification"]
    assert certification["execution_lifecycle_certified"] is False
    assert certification["shadow_measurement_scope"] == probe.MEASUREMENT_SCOPE
    assert calls and calls[0][0] is IDENTITY


def test_probe_uses_only_replayed_market_and_granularity(monkeypatch):
    captured = []

    def fetch(product, granularity, days_back, **kwargs):
        captured.append((product, granularity, days_back))
        return _series()

    monkeypatch.setattr(probe.scanner, "_fetch_live_candles", fetch)
    monkeypatch.setattr(probe, "run_certified_strategy", lambda identity, **kwargs: [])
    result = probe.probe_certified_shadow_signals(runtime_context=CONTEXT, days_back=7)

    assert result["signals"] == []
    assert captured == [("BTC-USD", "ONE_HOUR", 7)]


def test_probe_never_uses_legacy_backtest_filter(monkeypatch):
    monkeypatch.setattr(probe.scanner, "_fetch_live_candles", lambda *args, **kwargs: _series())
    monkeypatch.setattr(
        probe.scanner,
        "backtest_strategy",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("legacy backtest filter called")),
    )
    monkeypatch.setattr(probe, "run_certified_strategy", lambda identity, **kwargs: [_signal("BUY")])

    result = probe.probe_certified_shadow_signals(runtime_context=CONTEXT)
    assert len(result["signals"]) == 1
    assert result["signals"][0]["action"] == "BUY"


def test_probe_rejects_missing_runtime_certification(monkeypatch):
    monkeypatch.setattr(probe.scanner, "_fetch_live_candles", lambda *args, **kwargs: _series())
    bad = _signal()
    bad.runtime_certification = None
    monkeypatch.setattr(probe, "run_certified_strategy", lambda identity, **kwargs: [bad])

    with pytest.raises(probe.CertifiedRuntimeError, match="shadow_probe_certification_missing"):
        probe.probe_certified_shadow_signals(runtime_context=CONTEXT)


def test_probe_reports_insufficient_data_without_fabricating_signal(monkeypatch):
    short = SimpleNamespace(
        product_id="BTC-USD",
        source="short-feed",
        candles=_series().candles[:10],
    )
    monkeypatch.setattr(probe.scanner, "_fetch_live_candles", lambda *args, **kwargs: short)
    monkeypatch.setattr(
        probe,
        "run_certified_strategy",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("should not run")),
    )

    result = probe.probe_certified_shadow_signals(runtime_context=CONTEXT)
    assert result["signals"] == []
    assert result["errors"] == ["insufficient_certified_shadow_candles:10<16"]
