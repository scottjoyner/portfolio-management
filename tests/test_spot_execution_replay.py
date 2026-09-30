from __future__ import annotations

import pytest

from scripts.backtest_framework import canonical_replay
from scripts.backtest_framework import spot_execution_replay as spot


class FakeRust:
    def __init__(self, actions: dict[int, str]):
        self.actions = actions

    def run_strategy_opens_py(self, _name, closes, _opens, _volumes, _highs, _lows):
        action = self.actions.get(len(closes) - 1)
        return None if action is None else (action, 0.8, f"fake-{action.lower()}")


def _rows(count: int = 60):
    rows = []
    for i in range(count):
        close = 100.0 + i
        rows.append([
            1_700_000_000.0 + i * 60,
            close,
            close + 0.5,
            close - 0.5,
            close,
            1000.0,
        ])
    return rows


def test_spot_lifecycle_rejects_short_pyramiding_and_closes_on_sell(monkeypatch):
    fake = FakeRust({5: "SELL", 6: "BUY", 7: "BUY", 8: "SELL", 9: "SELL"})
    monkeypatch.setattr(spot, "_require_rust_core", lambda: fake)

    replay = spot.replay_spot_long_only_rust(
        "fake_strategy", _rows(), warmup=5, fee_bps=0.0, max_hold_bars=0
    )

    assert [row["event"] for row in replay["events"][:5]] == [
        "NOOP", "OPEN_LONG", "NOOP", "CLOSE_LONG", "NOOP"
    ]
    assert replay["events"][0]["reason"] == "sell_while_flat"
    assert replay["events"][2]["reason"] == "buy_while_long"
    assert replay["events"][4]["reason"] == "sell_while_flat"
    assert len(replay["returns"]) == 1
    assert replay["returns"][0] == pytest.approx((108.0 - 106.0) / 106.0)


def test_max_hold_closes_on_quiet_bar_and_prohibits_same_bar_reentry(monkeypatch):
    # BUY at 6 opens. Bar 9 is exactly three bars later. Even though the fake
    # evaluator would emit BUY there, max-hold closes first and the bar is not
    # reused for another entry.
    fake = FakeRust({6: "BUY", 9: "BUY"})
    monkeypatch.setattr(spot, "_require_rust_core", lambda: fake)

    replay = spot.replay_spot_long_only_rust(
        "fake_strategy", _rows(), warmup=5, fee_bps=10.0, max_hold_bars=3
    )

    assert [row["event"] for row in replay["events"]] == ["OPEN_LONG", "CLOSE_LONG"]
    assert replay["events"][1]["bar"] == 9
    assert replay["events"][1]["reason"] == "max_hold"
    expected = (109.0 - 106.0) / 106.0 - 0.002
    assert replay["returns"] == pytest.approx([expected])


def test_observation_end_closes_historical_long(monkeypatch):
    fake = FakeRust({55: "BUY"})
    monkeypatch.setattr(spot, "_require_rust_core", lambda: fake)

    replay = spot.replay_spot_long_only_rust(
        "fake_strategy", _rows(), warmup=5, fee_bps=0.0, max_hold_bars=0
    )

    assert replay["events"][-1]["event"] == "CLOSE_LONG"
    assert replay["events"][-1]["reason"] == "observation_end"
    assert replay["events"][-1]["bar"] == 59


def test_legacy_v1_short_semantics_remain_unchanged(monkeypatch):
    fake = FakeRust({5: "SELL", 8: "BUY"})
    monkeypatch.setattr(canonical_replay, "_require_rust_core", lambda: fake)
    monkeypatch.setattr(spot, "_require_rust_core", lambda: fake)

    legacy = canonical_replay.replay_trade_returns_rust(
        "fake_strategy", _rows(), warmup=5, fee_bps=0.0, max_hold_bars=0
    )
    executable = spot.replay_spot_long_only_returns_rust(
        "fake_strategy", _rows(), warmup=5, fee_bps=0.0, max_hold_bars=0
    )

    # v1 opens a short at 105 and closes it at 108. The new spot lifecycle
    # intentionally refuses that uncovered short and later opens long at 108.
    assert legacy[0] == pytest.approx((105.0 - 108.0) / 105.0)
    assert executable[0] == pytest.approx((159.0 - 108.0) / 108.0)


def test_spot_attestation_binds_lifecycle_and_rejects_tampering(monkeypatch):
    fake = FakeRust({45: "BUY", 50: "SELL", 85: "BUY", 90: "SELL", 125: "BUY", 130: "SELL", 165: "BUY", 170: "SELL"})
    monkeypatch.setattr(spot, "_require_rust_core", lambda: fake)
    snapshot = canonical_replay.snapshot_from_rows(
        _rows(240), kind="coinbase_candles", symbol="BTC-USD", granularity=60
    )

    returns, attestation = spot.attest_spot_snapshot_replay(
        snapshot,
        strategy_name="fake_strategy",
        n_folds=4,
        warmup=5,
        fee_bps=10.0,
        max_hold_bars=12,
    )

    assert attestation["lifecycle"]["position_mode"] == "spot_long_only"
    assert attestation["lifecycle"]["sell_while_flat"] == "noop"
    assert attestation["lifecycle"]["max_hold_on_quiet_bar"] is True
    ok, reasons = spot.verify_spot_replay_attestation(
        attestation, returns, reverify_source=False
    )
    assert ok is True, reasons

    tampered = dict(attestation)
    tampered["lifecycle"] = dict(attestation["lifecycle"])
    tampered["lifecycle"]["sell_while_flat"] = "open_short"
    ok, reasons = spot.verify_spot_replay_attestation(
        tampered, returns, reverify_source=False
    )
    assert ok is False
    assert "spot_replay_lifecycle_hash_mismatch" in reasons
