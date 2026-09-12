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
    # Deliberately tiny: raw shadow measurement must ignore execution routing.
    "canary_fraction": 0.01,
}


def _candles(count: int = 40):
    start = 1_700_000_000
    rows = []
    for index in range(count):
        price = 100.0 + index
        rows.append({
            "start": float(start + index * 3600),
            "open": price - 0.5,
            "high": price + 1.0,
            "low": price - 1.0,
            "close": price,
            "volume": 10.0,
        })
    return rows


def _signal(action="SELL", confidence=0.37):
    return SimpleNamespace(
        action=action,
        price=139.0,
        confidence=confidence,
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
    calls = []

    def run(identity, **kwargs):
        calls.append((identity, kwargs))
        # Confidence would be below the legacy scanner's normal threshold; it
        # must still be measured because research certification already exists.
        return [_signal("SELL", confidence=0.01)]

    monkeypatch.setattr(probe, "run_certified_strategy", run)
    result = probe.probe_certified_shadow_signals(
        runtime_context=CONTEXT,
        candle_rows=_candles(),
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
    assert signal["weighted_confidence"] == 0.01
    assert "win_rate" not in signal
    certification = signal["trade_plan"]["runtime_certification"]
    assert certification["execution_lifecycle_certified"] is False
    assert certification["shadow_measurement_scope"] == probe.MEASUREMENT_SCOPE
    assert calls and calls[0][0] is IDENTITY


def test_probe_fetches_only_replayed_market_at_replay_granularity(monkeypatch):
    captured = []

    def fetch(symbol, *, granularity_seconds, now_epoch=None, max_bars=300):
        captured.append((symbol, granularity_seconds, max_bars))
        return _candles()

    monkeypatch.setattr(probe, "_fetch_completed_coinbase_candles", fetch)
    monkeypatch.setattr(probe, "run_certified_strategy", lambda identity, **kwargs: [])
    result = probe.probe_certified_shadow_signals(runtime_context=CONTEXT)

    assert result["signals"] == []
    assert captured == [("BTC-USD", 3600, 100)]


def test_probe_observation_id_source_is_canonical_bar_close_not_poll_time(monkeypatch):
    monkeypatch.setattr(probe, "run_certified_strategy", lambda identity, **kwargs: [_signal("BUY")])
    rows = _candles()
    result = probe.probe_certified_shadow_signals(
        runtime_context=CONTEXT,
        candle_rows=rows,
        now_epoch=9_999_999_999,
    )
    expected = probe._iso_timestamp(rows[-1]["start"] + 3600)
    assert result["observed_at"] == expected
    assert result["signals"][0]["observed_at"] == expected
    assert result["signals"][0]["canonical_bar_closed_at"] == expected


def test_completed_candle_normalizer_sorts_deduplicates_and_drops_forming_bar():
    now = 10_000.0
    rows = [
        [7200, 99, 103, 100, 102, 5],      # still forming at t=10,000
        [3600, 98, 102, 99, 101, 4],       # completed
        [0, 97, 101, 98, 100, 3],          # completed
        [3600, 98, 102, 99, 101, 4],       # duplicate
    ]
    normalized = probe._normalize_completed_rows(
        rows,
        granularity_seconds=3600,
        now_epoch=now,
    )
    assert [row["start"] for row in normalized] == [0.0, 3600.0]


def test_probe_rejects_missing_runtime_certification(monkeypatch):
    bad = _signal()
    bad.runtime_certification = None
    monkeypatch.setattr(probe, "run_certified_strategy", lambda identity, **kwargs: [bad])

    with pytest.raises(probe.CertifiedRuntimeError, match="shadow_probe_certification_missing"):
        probe.probe_certified_shadow_signals(runtime_context=CONTEXT, candle_rows=_candles())


def test_probe_rejects_untimestamped_injected_rows():
    rows = [{"open": 1, "high": 1, "low": 1, "close": 1, "volume": 1}] * 40
    with pytest.raises(probe.CertifiedRuntimeError, match="shadow_probe_candle_timestamp_required"):
        probe.probe_certified_shadow_signals(runtime_context=CONTEXT, candle_rows=rows)


def test_probe_reports_insufficient_data_without_fabricating_signal(monkeypatch):
    monkeypatch.setattr(
        probe,
        "run_certified_strategy",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("should not run")),
    )

    result = probe.probe_certified_shadow_signals(
        runtime_context=CONTEXT,
        candle_rows=_candles(10),
    )
    assert result["signals"] == []
    assert result["errors"] == ["insufficient_certified_shadow_candles:10<32"]
