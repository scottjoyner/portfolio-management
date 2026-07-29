#!/usr/bin/env python3
"""Versioned, symmetric paper accounting for the paid trading agent.

Accounting v2 uses one representation for both long and short positions:

* ``quantity`` is the leveraged asset quantity.
* ``margin_usd`` is cash reserved by the position.
* ``notional_usd = margin_usd * leverage`` at entry.
* P&L is price movement times ``quantity`` exactly once.
* Entry and exit fees are charged on traded notional exactly once.

Compatibility aliases (``base``, ``cost_basis``, and ``exposure``) are persisted
so the existing Hermes strategy/risk modules can read the v2 ledger while the
canonical fields remain explicit.
"""
from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timezone
from math import isfinite
from typing import Any

ACCOUNTING_VERSION = 2
EPSILON = 1e-8


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def new_ledger(starting_capital: float = 10_000.0) -> dict[str, Any]:
    capital = _positive(starting_capital, "starting_capital")
    now = utc_now()
    return {
        "schema_version": 2,
        "accounting_version": ACCOUNTING_VERSION,
        "history_valid_from": now,
        "created_at": now,
        "starting_capital": capital,
        "cash": capital,
        "realized_pnl": 0.0,
        "fees_paid": 0.0,
        "positions": {},
        "trades": [],
        "equity": capital,
        "peak_equity": capital,
        "equity_curve": [capital],
        "return_pct": 0.0,
        "ranking_eligible": True,
    }


def _number(value: Any, name: str) -> float:
    result = float(value)
    if not isfinite(result):
        raise ValueError(f"{name} must be finite")
    return result


def _positive(value: Any, name: str) -> float:
    result = _number(value, name)
    if result <= 0:
        raise ValueError(f"{name} must be > 0")
    return result


def _non_negative(value: Any, name: str) -> float:
    result = _number(value, name)
    if result < 0:
        raise ValueError(f"{name} must be >= 0")
    return result


def _side(side: str) -> str:
    normalized = str(side).upper()
    if normalized not in {"LONG", "SHORT"}:
        raise ValueError("side must be LONG or SHORT")
    return normalized


def require_v2(ledger: dict[str, Any]) -> None:
    if int(ledger.get("accounting_version", 0) or 0) != ACCOUNTING_VERSION:
        raise ValueError("legacy_agent_ledger_requires_archive_and_reset")
    if ledger.get("ranking_eligible") is not True:
        raise ValueError("agent_ledger_not_ranking_eligible")


def position_key(product_id: str, side: str) -> str:
    """Use legacy-readable keys while keeping an explicit ``side`` field."""
    product = str(product_id).upper()
    return product if _side(side) == "LONG" else f"SHORT:{product}"


def _sync_aliases(position: dict[str, Any]) -> None:
    position["base"] = float(position["quantity"])
    position["cost_basis"] = float(position["margin_usd"])
    position["exposure"] = float(position["notional_usd"])


def _trade_side(direction: str, opening: bool, existing: bool = False) -> str:
    if direction == "LONG":
        return "BUY_ADD" if opening and existing else "BUY_OPEN" if opening else "BUY_CLOSE"
    return "SHORT_ADD" if opening and existing else "SHORT_OPEN" if opening else "SHORT_CLOSE"


def open_position(
    ledger: dict[str, Any],
    *,
    product_id: str,
    side: str,
    margin_usd: float,
    entry_price: float,
    leverage: float,
    fee_rate: float = 0.0012,
    note: str = "",
    regime: str = "",
    setup: str = "",
    timestamp: str | None = None,
) -> dict[str, Any]:
    """Open or add to a position without applying leverage twice."""
    require_v2(ledger)
    margin = _positive(margin_usd, "margin_usd")
    price = _positive(entry_price, "entry_price")
    lev = _positive(leverage, "leverage")
    rate = _non_negative(fee_rate, "fee_rate")
    direction = _side(side)
    notional = margin * lev
    quantity = notional / price
    entry_fee = notional * rate
    if float(ledger["cash"]) + EPSILON < margin + entry_fee:
        raise ValueError("insufficient_cash_for_margin_and_fee")

    key = position_key(product_id, direction)
    existing = ledger.get("positions", {}).get(key)
    if existing and existing.get("side") != direction:
        raise ValueError("position_side_mismatch")

    ts = timestamp or utc_now()
    updated = deepcopy(ledger)
    pos = deepcopy(existing) if existing else {
        "product_id": str(product_id).upper(),
        "side": direction,
        "quantity": 0.0,
        "margin_usd": 0.0,
        "notional_usd": 0.0,
        "entry_price": price,
        "leverage": lev,
        "entry_fees_usd": 0.0,
        "entries": 0,
        "adds": 0,
        "entry_ts": ts,
        "accounting_version": ACCOUNTING_VERSION,
    }
    if float(pos["quantity"]) > EPSILON and abs(float(pos["leverage"]) - lev) > EPSILON:
        raise ValueError("cannot_mix_leverage_within_position")

    prior_quantity = float(pos["quantity"])
    combined_quantity = prior_quantity + quantity
    pos["entry_price"] = (
        float(pos["entry_price"]) * prior_quantity + price * quantity
    ) / combined_quantity
    pos["quantity"] = combined_quantity
    pos["margin_usd"] = float(pos["margin_usd"]) + margin
    pos["notional_usd"] = float(pos["notional_usd"]) + notional
    pos["entry_fees_usd"] = float(pos["entry_fees_usd"]) + entry_fee
    pos["entries"] = int(pos["entries"]) + 1
    if existing:
        pos["adds"] = int(pos.get("adds", 0)) + 1
    pos["leverage"] = lev
    pos["regime"] = regime
    pos["setup"] = setup
    _sync_aliases(pos)

    updated["cash"] = float(updated["cash"]) - margin - entry_fee
    updated["fees_paid"] = float(updated.get("fees_paid", 0.0)) + entry_fee
    updated["positions"][key] = pos
    updated["trades"].append({
        "ts": ts,
        "product_id": str(product_id).upper(),
        "side": _trade_side(direction, True, bool(existing)),
        "direction": direction,
        "quote_size": margin,
        "margin_usd": margin,
        "exposure": notional,
        "notional_usd": notional,
        "base_size": quantity,
        "quantity": quantity,
        "fill_price": price,
        "leverage": lev,
        "commission": entry_fee,
        "entry_fee_usd": entry_fee,
        "accounting_version": ACCOUNTING_VERSION,
        "note": note,
        "regime": regime,
        "setup": setup,
        "live": False,
    })
    assert_invariants(updated)
    return updated


def close_position(
    ledger: dict[str, Any],
    *,
    product_id: str,
    side: str,
    exit_price: float,
    fraction: float = 1.0,
    fee_rate: float = 0.0012,
    note: str = "close",
    regime: str = "",
    timestamp: str | None = None,
) -> dict[str, Any]:
    """Close all or part of a position and realize net P&L including both fees."""
    require_v2(ledger)
    direction = _side(side)
    price = _positive(exit_price, "exit_price")
    frac = _positive(fraction, "fraction")
    if frac > 1.0:
        raise ValueError("fraction must be <= 1")
    rate = _non_negative(fee_rate, "fee_rate")
    key = position_key(product_id, direction)
    pos = ledger.get("positions", {}).get(key)
    if not pos or float(pos.get("quantity", 0.0)) <= EPSILON:
        raise ValueError("position_not_found")

    quantity = float(pos["quantity"]) * frac
    margin_returned = float(pos["margin_usd"]) * frac
    close_notional = quantity * price
    exit_fee = close_notional * rate
    allocated_entry_fee = float(pos.get("entry_fees_usd", 0.0)) * frac
    entry = float(pos["entry_price"])
    gross_pnl = (price - entry) * quantity if direction == "LONG" else (entry - price) * quantity
    net_realized = gross_pnl - allocated_entry_fee - exit_fee
    ts = timestamp or utc_now()

    updated = deepcopy(ledger)
    next_pos = deepcopy(pos)
    next_pos["quantity"] = max(0.0, float(next_pos["quantity"]) - quantity)
    next_pos["margin_usd"] = max(0.0, float(next_pos["margin_usd"]) - margin_returned)
    next_pos["notional_usd"] = max(0.0, float(next_pos["notional_usd"]) * (1.0 - frac))
    next_pos["entry_fees_usd"] = max(
        0.0, float(next_pos.get("entry_fees_usd", 0.0)) - allocated_entry_fee
    )
    _sync_aliases(next_pos)

    # Entry fees were already debited when the position opened. Cash receives
    # margin + gross price P&L - exit fee. Realized P&L includes the allocated
    # entry fee so the closed round trip reconciles exactly to cash.
    updated["cash"] = float(updated["cash"]) + margin_returned + gross_pnl - exit_fee
    updated["realized_pnl"] = float(updated.get("realized_pnl", 0.0)) + net_realized
    updated["fees_paid"] = float(updated.get("fees_paid", 0.0)) + exit_fee
    if next_pos["quantity"] <= EPSILON:
        updated["positions"].pop(key, None)
    else:
        updated["positions"][key] = next_pos
    updated["trades"].append({
        "ts": ts,
        "product_id": str(product_id).upper(),
        "side": _trade_side(direction, False),
        "direction": direction,
        "fraction": frac,
        "margin_returned_usd": margin_returned,
        "quote_size": margin_returned,
        "exposure": close_notional,
        "notional_usd": close_notional,
        "base_size": quantity,
        "quantity": quantity,
        "fill_price": price,
        "leverage": float(pos["leverage"]),
        "commission": exit_fee,
        "entry_fee_allocated_usd": allocated_entry_fee,
        "fees_total_usd": allocated_entry_fee + exit_fee,
        "gross_pnl": gross_pnl,
        "realized_pnl": net_realized,
        "accounting_version": ACCOUNTING_VERSION,
        "note": note,
        "regime": regime or pos.get("regime", ""),
        "live": False,
    })
    assert_invariants(updated)
    return updated


def mark_to_market(ledger: dict[str, Any], marks: dict[str, float]) -> dict[str, Any]:
    require_v2(ledger)
    unrealized = 0.0
    rows: dict[str, Any] = {}
    missing: list[str] = []
    for key, pos in ledger.get("positions", {}).items():
        product_id = pos["product_id"]
        mark = marks.get(product_id)
        if mark is None or float(mark) <= 0:
            missing.append(product_id)
            continue
        price = float(mark)
        entry = float(pos["entry_price"])
        quantity = float(pos["quantity"])
        pnl = (price - entry) * quantity if pos["side"] == "LONG" else (entry - price) * quantity
        unrealized += pnl
        rows[key] = {**pos, "mark_price": price, "unrealized_pnl": pnl}
    reserved_margin = sum(
        float(position["margin_usd"]) for position in ledger.get("positions", {}).values()
    )
    equity = float(ledger["cash"]) + reserved_margin + unrealized
    return {
        "positions": rows,
        "missing_marks": sorted(set(missing)),
        "total_unrealized_pnl": unrealized,
        "reserved_margin_usd": reserved_margin,
        "equity": equity if not missing else None,
    }


def apply_marks(ledger: dict[str, Any], marks: dict[str, float]) -> dict[str, Any]:
    snapshot = mark_to_market(ledger, marks)
    if snapshot["equity"] is None:
        raise ValueError("missing_position_marks")
    updated = deepcopy(ledger)
    equity = float(snapshot["equity"])
    updated["equity"] = equity
    updated["peak_equity"] = max(float(updated.get("peak_equity", equity)), equity)
    curve = list(updated.get("equity_curve", []))
    curve.append(equity)
    updated["equity_curve"] = curve[-5000:]
    updated["return_pct"] = (
        (equity - float(updated["starting_capital"]))
        / float(updated["starting_capital"])
        * 100.0
    )
    updated["last_marked_at"] = utc_now()
    assert_invariants(updated)
    return updated


def assert_invariants(ledger: dict[str, Any]) -> None:
    require_v2(ledger)
    for name in ("starting_capital", "cash", "realized_pnl", "fees_paid"):
        _number(ledger.get(name, 0.0), name)
    if float(ledger["cash"]) < -EPSILON:
        raise AssertionError("cash_below_zero")
    for key, pos in ledger.get("positions", {}).items():
        if int(pos.get("accounting_version", 0)) != ACCOUNTING_VERSION:
            raise AssertionError(f"position_accounting_version_invalid:{key}")
        margin = _non_negative(pos.get("margin_usd", 0.0), f"{key}.margin_usd")
        notional = _non_negative(pos.get("notional_usd", 0.0), f"{key}.notional_usd")
        quantity = _non_negative(pos.get("quantity", 0.0), f"{key}.quantity")
        leverage = _positive(pos.get("leverage", 0.0), f"{key}.leverage")
        entry = _positive(pos.get("entry_price", 0.0), f"{key}.entry_price")
        if abs(notional - margin * leverage) > max(0.01, notional * 1e-8):
            raise AssertionError(f"notional_margin_leverage_mismatch:{key}")
        if abs(quantity - notional / entry) > max(1e-8, quantity * 1e-8):
            raise AssertionError(f"quantity_notional_price_mismatch:{key}")
        if abs(float(pos.get("base", quantity)) - quantity) > EPSILON:
            raise AssertionError(f"base_quantity_alias_mismatch:{key}")
        if abs(float(pos.get("cost_basis", margin)) - margin) > EPSILON:
            raise AssertionError(f"cost_basis_margin_alias_mismatch:{key}")
        if abs(float(pos.get("exposure", notional)) - notional) > EPSILON:
            raise AssertionError(f"exposure_notional_alias_mismatch:{key}")
