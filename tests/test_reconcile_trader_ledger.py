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


def _with_core_holdings(cash=1.0, total_cost=381.6786686073287):
    """A state shaped like the one that hard-blocked the trader on 2026-10-04.

    Core buys debit paper_cash in full and are booked to
    ``core_holdings[].total_cost``; the old formula ignored that bucket entirely
    and reported the whole thing as a missing-cash corruption.
    """
    state = _state(cash)
    state["core_holdings"] = [
        {"product_id": "SOL-USD", "qty": 0.668, "total_cost": 80.35866860732868,
         "total_qty": 0.668, "trades": 2},
        {"product_id": "BTC-USD", "qty": 0.00236, "total_cost": 200.88,
         "total_qty": 0.00236, "trades": 2},
        {"product_id": "ETH-USD", "qty": 0.03728, "total_cost": 100.44,
         "total_qty": 0.03728, "trades": 1},
    ]
    return state


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
        "paper_core_realized_pnl": 0.0,
        "open_margin": 2_000.0,
        "open_entry_fees": 4.0,
        "open_funding": 1.5,
        "core_cost": 0.0,
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


# ---------------------------------------------------------------------------
# Core (DCA/rebalance) bucket. Regression cover for the defect that hard-blocked
# trader-v4 on 2026-10-04: both copies of the invariant summed only
# paper_positions, so a book holding BTC/ETH/SOL looked short by the entire core
# bucket. The gate refused to start, and the recovery tool would have "fixed" it
# by crediting that shortfall as cash which _paper_equity then counted a second
# time via _core_holdings_value.
# ---------------------------------------------------------------------------
def test_core_holdings_are_part_of_the_cash_invariant():
    components = reconcile.formula_components(_with_core_holdings())

    assert components["core_cost"] == pytest.approx(381.6786686073287)
    # 10_000 + 125 - 2_000 - 4 - 1.5 - 381.678...
    assert components["expected_cash"] == pytest.approx(7_737.821331392671, abs=1e-6)


def test_a_balanced_book_with_core_holdings_reports_no_difference():
    # What the blocked book should have looked like: cash already reduced by the
    # core bucket, so the invariant balances instead of demanding phantom cash.
    state = _with_core_holdings()
    components = reconcile.formula_components(state)
    state["paper_cash"] = components["expected_cash"]

    assert reconcile.formula_components(state)["difference"] == 0.0


def test_recovery_does_not_invent_the_core_bucket_as_cash(tmp_path):
    """The repair must be a rounding fix, not a ~$382 windfall.

    Regression: with the old formula this wrote paper_cash up by the whole core
    cost, and equity (cash + core_holdings_value) then counted the BTC/ETH/SOL
    twice.
    """
    state_path = tmp_path / "state.json"
    _write(state_path, _with_core_holdings())
    sentinel = tmp_path / "trader_state_corrupt"
    sentinel.write_text("blocked\n")

    report = reconcile.reconcile(state_path, sentinel, write=True)

    repaired = json.loads(state_path.read_text())
    # report["formula"] describes the INPUT state, which was genuinely short.
    assert report["formula"]["difference"] < -7_000.0
    assert repaired["paper_cash"] == pytest.approx(
        report["formula"]["expected_cash"], abs=1e-6
    )
    # The repaired book balances.
    assert reconcile.formula_components(repaired)["difference"] == 0.0
    # And the repair did not credit the core bucket as cash: the old formula
    # would have produced a cash figure higher by exactly core_cost.
    buggy = report["formula"]["expected_cash"] + report["formula"]["core_cost"]
    assert repaired["paper_cash"] == pytest.approx(buggy - report["formula"]["core_cost"])
    assert not sentinel.exists()


def test_core_trim_pnl_is_a_first_class_term():
    # A trim credits cash and releases cost; the difference is realized P&L.
    # It is tracked apart from paper_realized_pnl because that accumulator is
    # cross-checked against the paper_trades ledger, which has no trim records.
    state = _with_core_holdings()
    before = reconcile.formula_components(state)["expected_cash"]
    state["paper_core_realized_pnl"] = 12.5

    assert reconcile.formula_components(state)["expected_cash"] == pytest.approx(
        before + 12.5
    )


def test_absent_core_fields_default_to_zero_not_an_error():
    """A pre-core book has no core_holdings key at all; that is zero, not corrupt."""
    state = _state()
    assert "core_holdings" not in state
    components = reconcile.formula_components(state)
    assert components["core_cost"] == 0.0
    assert components["paper_core_realized_pnl"] == 0.0


@pytest.mark.parametrize(
    "mutate",
    [
        lambda s: s.update(core_holdings="wrong"),
        lambda s: s.update(core_holdings=[{"total_cost": 10.0}]),
        lambda s: s["core_holdings"][0].pop("total_cost"),
        lambda s: s["core_holdings"][0].update(total_cost=-1.0),
        lambda s: s["core_holdings"][0].update(total_cost=float("nan")),
        lambda s: s.update(paper_core_realized_pnl=float("inf")),
    ],
)
def test_malformed_core_bucket_is_refused_without_side_effects(tmp_path, mutate):
    state = _with_core_holdings()
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


def test_startup_gate_shares_this_formula(tmp_path):
    """The gate and the recovery tool must not drift apart again.

    They each carried their own copy of the invariant, and both were wrong the
    same way. This pins the gate to the shared implementation.
    """
    from coinbase.src import run_trader_v4 as trader

    assert trader.ledger_formula_components is reconcile.formula_components
    assert trader.ledger_TOLERANCE == 1.0


def test_gate_accepts_a_core_holding_book(tmp_path):
    """The exact shape that blocked the trader must now pass."""
    from coinbase.src import run_trader_v4 as trader

    state = _with_core_holdings()
    state["paper_cash"] = reconcile.formula_components(state)["expected_cash"]
    difference = trader.ledger_formula_components(state)["difference"]

    assert abs(difference) <= trader.ledger_TOLERANCE


def test_gate_still_fails_on_a_genuinely_short_book():
    """Accuracy fix only: a real shortfall must still block trading."""
    from coinbase.src import run_trader_v4 as trader

    state = _with_core_holdings(cash=1.0)
    difference = trader.ledger_formula_components(state)["difference"]

    assert abs(difference) > trader.ledger_TOLERANCE
