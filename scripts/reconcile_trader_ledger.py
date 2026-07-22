#!/usr/bin/env python3
"""Audit and reconcile the trader-v4 paper cash ledger.

The command is observational by default.  ``--write`` is intentionally required
before it creates artifacts, replaces the state file, or removes the corruption
sentinel.  This is recovery tooling, not a distributed fencing mechanism.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import sys
import tempfile
import time
import uuid
from pathlib import Path
from typing import Any


class ReconciliationError(ValueError):
    """The state cannot be safely reconciled or verified."""


def _number(value: Any, field: str, *, positive: bool = False) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ReconciliationError(f"{field} must be a JSON number")
    number = float(value)
    if not math.isfinite(number):
        raise ReconciliationError(f"{field} must be finite")
    if positive and number <= 0:
        raise ReconciliationError(f"{field} must be greater than zero")
    return number


def formula_components(state: Any) -> dict[str, float]:
    """Validate *state* and return every component of the cash invariant."""
    if not isinstance(state, dict):
        raise ReconciliationError("state root must be an object")
    starting = _number(state.get("paper_starting_capital"), "paper_starting_capital")
    cash = _number(state.get("paper_cash"), "paper_cash")
    realized = _number(state.get("paper_realized_pnl"), "paper_realized_pnl")
    positions = state.get("paper_positions")
    if not isinstance(positions, list):
        raise ReconciliationError("paper_positions must be an array")

    open_margin = 0.0
    open_entry_fees = 0.0
    open_funding = 0.0
    for index, position in enumerate(positions):
        prefix = f"paper_positions[{index}]"
        if not isinstance(position, dict):
            raise ReconciliationError(f"{prefix} must be an object")
        product = position.get("product_id")
        if not isinstance(product, str) or not product.strip():
            raise ReconciliationError(f"{prefix}.product_id must be a non-empty string")
        notional = _number(position.get("entry_notional"), f"{prefix}.entry_notional")
        if notional < 0:
            raise ReconciliationError(f"{prefix}.entry_notional must not be negative")
        leverage = _number(position.get("leverage"), f"{prefix}.leverage", positive=True)
        fees = _number(position.get("fees_paid"), f"{prefix}.fees_paid")
        funding = _number(position.get("cum_funding"), f"{prefix}.cum_funding")
        if fees < 0:
            raise ReconciliationError(f"{prefix}.fees_paid must not be negative")
        open_margin += notional / leverage
        open_entry_fees += fees
        open_funding += funding

    expected = starting + realized - open_margin - open_entry_fees - open_funding
    components = {
        "paper_starting_capital": starting,
        "paper_realized_pnl": realized,
        "open_margin": open_margin,
        "open_entry_fees": open_entry_fees,
        "open_funding": open_funding,
        "expected_cash": expected,
        "input_cash": cash,
        "difference": cash - expected,
    }
    for name, value in components.items():
        if not math.isfinite(value):
            raise ReconciliationError(f"computed {name} is non-finite")
    return components


def _sha256(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def _fsync_directory(path: Path) -> None:
    fd = os.open(path, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def _immutable_create(path: Path, raw: bytes) -> None:
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o400)
    try:
        view = memoryview(raw)
        while view:
            written = os.write(fd, view)
            if written <= 0:  # pragma: no cover - defensive OS contract guard
                raise OSError("short write")
            view = view[written:]
        os.fsync(fd)
        os.fchmod(fd, 0o400)
    finally:
        os.close(fd)
    _fsync_directory(path.parent)


def _atomic_replace(path: Path, raw: bytes) -> None:
    fd, temp_name = tempfile.mkstemp(prefix=f".{path.name}.reconcile.", dir=path.parent)
    temp_path = Path(temp_name)
    try:
        with os.fdopen(fd, "wb") as handle:
            handle.write(raw)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temp_path, path)
        _fsync_directory(path.parent)
    except BaseException:
        temp_path.unlink(missing_ok=True)
        raise


def verify_written_state(path: Path, expected_raw: bytes, expected_cash: float) -> bool:
    try:
        actual_raw = path.read_bytes()
        if _sha256(actual_raw) != _sha256(expected_raw):
            return False
        actual = json.loads(actual_raw)
        formula = formula_components(actual)
        return formula["input_cash"] == expected_cash and formula["difference"] == 0.0
    except (OSError, json.JSONDecodeError, ReconciliationError):
        return False


def reconcile(state_path: Path, sentinel_path: Path, *, write: bool = False) -> dict[str, Any]:
    state_path = Path(state_path)
    sentinel_path = Path(sentinel_path)
    try:
        input_raw = state_path.read_bytes()
        state = json.loads(input_raw)
    except (OSError, json.JSONDecodeError) as exc:
        raise ReconciliationError(f"cannot read valid JSON state: {exc}") from exc

    formula = formula_components(state)
    report: dict[str, Any] = {
        "dry_run": not write,
        "state_path": str(state_path),
        "input_sha256": _sha256(input_raw),
        "formula": formula,
    }
    if not write:
        return report

    repaired = dict(state)
    repaired["paper_cash"] = formula["expected_cash"]
    output_raw = (json.dumps(repaired, indent=2, ensure_ascii=False, allow_nan=False) + "\n").encode()
    output_sha = _sha256(output_raw)
    token = f"{time.time_ns()}.{os.getpid()}.{uuid.uuid4().hex}"
    backup_path = state_path.with_name(f"{state_path.name}.pre-repair.{token}")
    audit_path = state_path.with_name(f"{state_path.name}.reconcile-audit.{token}.json")

    # Preserve the exact input before replacing anything. O_EXCL avoids ever
    # overwriting a prior recovery artifact, and mode 0400 makes it immutable to
    # ordinary accidental writes by the owner.
    _immutable_create(backup_path, input_raw)
    _atomic_replace(state_path, output_raw)
    if not verify_written_state(state_path, output_raw, formula["expected_cash"]):
        raise ReconciliationError(
            "post-write verification failed; corruption sentinel was not cleared"
        )

    audit = {
        "schema_version": 1,
        "operation": "trader-v4-paper-ledger-reconciliation",
        "created_unix_ns": time.time_ns(),
        "state_path": str(state_path.resolve()),
        "backup_path": str(backup_path.resolve()),
        "input_sha256": _sha256(input_raw),
        "output_sha256": output_sha,
        "formula": formula,
        "verified": True,
        "sentinel_clear_authorized": True,
    }
    audit_raw = (json.dumps(audit, indent=2, sort_keys=True, allow_nan=False) + "\n").encode()
    _immutable_create(audit_path, audit_raw)

    # Clearing is the final operation: state bytes and invariant are verified,
    # and both immutable recovery artifacts are already durable.
    try:
        sentinel_path.unlink(missing_ok=True)
        _fsync_directory(sentinel_path.parent)
    except OSError as exc:
        raise ReconciliationError(f"repaired state verified but sentinel clear failed: {exc}") from exc

    report.update(
        dry_run=False,
        output_sha256=output_sha,
        backup_path=str(backup_path),
        audit_path=str(audit_path),
        verified=True,
        sentinel_cleared=True,
    )
    return report


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--state", type=Path, default=Path("data/paper_trader_v4_state.json"))
    parser.add_argument("--sentinel", type=Path, default=Path("data/trader_state_corrupt"))
    parser.add_argument("--write", action="store_true", help="perform the audited repair (default: dry-run)")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        report = reconcile(args.state, args.sentinel, write=args.write)
    except ReconciliationError as exc:
        print(f"reconciliation refused: {exc}", file=sys.stderr)
        return 2
    print(json.dumps(report, indent=2, sort_keys=True, allow_nan=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
