import hashlib
import json
import os
from pathlib import Path

import pytest

from scripts import reconcile_trader_ledger as reconcile


def _state(cash=1.0):
    return {
        "mode": "paper",
        "state_schema_version": 2,
        "paper_starting_capital": 10_000.0,
        "paper_cash": cash,
        "paper_realized_pnl": 125.0,
        "paper_positions": [
            {
                "product_id": "BTC-USD",
                "qty": 0.2,
                "entry_price": 20_000.0,
                "entry_notional": 4_000.0,
                "leverage": 2.0,
                "fees_paid": 4.0,
                "cum_funding": 1.5,
            }
        ],
        "paper_trades": [],
    }


def _write(path: Path, state: dict) -> bytes:
    raw = json.dumps(state, indent=2).encode()
    path.write_bytes(raw)
    return raw


def test_dry_run_is_default_and_does_not_touch_state_or_sentinel(tmp_path, capsys):
    state_path = tmp_path / "state.json"
    original = _write(state_path, _state())
    sentinel = tmp_path / "trader_state_corrupt"
    sentinel.write_text("blocked\n")

    assert reconcile.main(["--state", str(state_path), "--sentinel", str(sentinel)]) == 0

    report = json.loads(capsys.readouterr().out)
    assert report["dry_run"] is True
    assert report["formula"] == {
        "paper_starting_capital": 10_000.0,
        "paper_realized_pnl": 125.0,
        "open_margin": 2_000.0,
        "open_entry_fees": 4.0,
        "open_funding": 1.5,
        "expected_cash": 8_119.5,
        "input_cash": 1.0,
        "difference": -8_118.5,
    }
    assert state_path.read_bytes() == original
    assert sentinel.exists()
    assert not list(tmp_path.glob("state.json.pre-repair.*"))
    assert not list(tmp_path.glob("state.json.reconcile-audit.*.json"))


@pytest.mark.parametrize(
    "mutate",
    [
        lambda s: s.update(paper_cash="not-a-number"),
        lambda s: s.update(paper_cash=float("nan")),
        lambda s: s.update(paper_positions="wrong"),
        lambda s: s["paper_positions"][0].update(leverage=0),
        lambda s: s["paper_positions"][0].update(entry_notional=float("inf")),
        lambda s: s["paper_positions"][0].pop("fees_paid"),
    ],
)
def test_refuses_malformed_or_non_finite_state_without_side_effects(tmp_path, mutate):
    state = _state()
    mutate(state)
    state_path = tmp_path / "state.json"
    original = _write(state_path, state)
    sentinel = tmp_path / "sentinel"
    sentinel.write_text("blocked")

    with pytest.raises(reconcile.ReconciliationError):
        reconcile.reconcile(state_path, sentinel, write=True)

    assert state_path.read_bytes() == original
    assert sentinel.exists()
    assert len(list(tmp_path.iterdir())) == 2


def test_write_backs_up_audits_atomically_verifies_then_clears_sentinel(tmp_path):
    state_path = tmp_path / "state.json"
    input_raw = _write(state_path, _state())
    sentinel = tmp_path / "sentinel"
    sentinel.write_text("blocked")

    report = reconcile.reconcile(state_path, sentinel, write=True)

    output_raw = state_path.read_bytes()
    repaired = json.loads(output_raw)
    assert repaired["paper_cash"] == 8_119.5
    assert not sentinel.exists()
    backup = Path(report["backup_path"])
    audit_path = Path(report["audit_path"])
    assert backup.read_bytes() == input_raw
    assert backup.stat().st_mode & 0o222 == 0
    assert audit_path.stat().st_mode & 0o222 == 0
    audit = json.loads(audit_path.read_text())
    assert audit["input_sha256"] == hashlib.sha256(input_raw).hexdigest()
    assert audit["output_sha256"] == hashlib.sha256(output_raw).hexdigest()
    assert audit["formula"] == report["formula"]
    assert audit["verified"] is True
    assert audit["sentinel_clear_authorized"] is True


def test_failed_post_write_verification_keeps_sentinel(tmp_path, monkeypatch):
    state_path = tmp_path / "state.json"
    _write(state_path, _state())
    sentinel = tmp_path / "sentinel"
    sentinel.write_text("blocked")
    monkeypatch.setattr(reconcile, "verify_written_state", lambda *a, **k: False)

    with pytest.raises(reconcile.ReconciliationError, match="verification"):
        reconcile.reconcile(state_path, sentinel, write=True)

    assert sentinel.exists()
