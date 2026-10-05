"""Canonical cash invariant for the trader-v4 paper ledger.

Both consumers of this invariant import it from here:

* ``coinbase/src/run_trader_v4.py`` — the startup gate that refuses to trade on a
  corrupt book.
* ``scripts/reconcile_trader_ledger.py`` — the recovery tool that rewrites
  ``paper_cash`` and clears the corruption sentinel.

They used to carry separate copies of the formula, and both copies were wrong in
the same way: they summed only ``paper_positions``. Core holdings (the DCA /
rebalance bucket for BTC/ETH/SOL) debit ``paper_cash`` in full and are booked to
``core_holdings[].total_cost``, so any run that had bought a core asset made the
invariant report a shortfall equal to the whole core bucket. That blocked paper
trading outright, and the recovery tool would have "fixed" it by crediting the
shortfall as phantom cash — which ``_paper_equity`` then counted a second time,
because it adds core-holdings value on top of cash.

The invariant, derived from the paths that actually move cash:

* entry debits ``margin + fee`` (``run_trader_v4._paper_open_position``)
* exit credits ``margin + realized - fee`` and books the same P&L, net of the
  entry fee and funding, into ``paper_realized_pnl`` (``_paper_close_position``)
* a core buy debits ``notional + fee`` and books it into
  ``core_holdings[].total_cost`` (``_dca_execute_buy``)
* a core trim credits the proceeds and releases proportional cost; the difference
  is booked into ``paper_core_realized_pnl`` (``_rebalance_trim``)

Margin is debited and credited symmetrically, so it cancels across closed trades
and survives only for open positions::

    paper_cash == paper_starting_capital
                + paper_realized_pnl
                + paper_core_realized_pnl
                - sum(open entry_notional / leverage)
                - sum(open fees_paid)
                - sum(open cum_funding)
                - sum(core_holdings[].total_cost)

``total_cost`` is already fee-inclusive (``CoreHolding.add_buy`` adds
``qty * price + fee``; the constructor uses ``notional + fee``), so core fees need
no separate term.
"""

from __future__ import annotations

import math
from typing import Any

__all__ = [
    "PaperLedgerError",
    "ReconciliationError",
    "TOLERANCE",
    "formula_components",
    "soft_difference",
    "describe",
]

# The startup gate allows a dollar of slack for float accumulation across
# hundreds of round-trips. Anything larger is treated as a real fault.
TOLERANCE = 1.0


class PaperLedgerError(ValueError):
    """The state cannot be safely reconciled or verified."""


# The recovery tool's historical name for the same failure; kept so existing
# callers and tests that catch ``ReconciliationError`` keep working.
ReconciliationError = PaperLedgerError


def _number(value: Any, field: str, *, positive: bool = False) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise PaperLedgerError(f"{field} must be a JSON number")
    number = float(value)
    if not math.isfinite(number):
        raise PaperLedgerError(f"{field} must be finite")
    if positive and number <= 0:
        raise PaperLedgerError(f"{field} must be greater than zero")
    return number


def formula_components(state: Any) -> dict[str, float]:
    """Validate *state* and return every component of the cash invariant.

    Strict: raises :class:`PaperLedgerError` on a malformed or non-finite state
    rather than guessing, because the recovery tool rewrites ``paper_cash`` from
    this result and clears the sentinel afterwards.
    """
    if not isinstance(state, dict):
        raise PaperLedgerError("state root must be an object")
    starting = _number(state.get("paper_starting_capital"), "paper_starting_capital")
    cash = _number(state.get("paper_cash"), "paper_cash")
    realized = _number(state.get("paper_realized_pnl"), "paper_realized_pnl")
    # Absent means the book predates core holdings, i.e. it never DCA'd. Present
    # but empty is likewise zero. Only the individual records are strict.
    core_realized = _number(
        state.get("paper_core_realized_pnl", 0.0) or 0.0, "paper_core_realized_pnl"
    )
    positions = state.get("paper_positions")
    if not isinstance(positions, list):
        raise PaperLedgerError("paper_positions must be an array")

    open_margin = 0.0
    open_entry_fees = 0.0
    open_funding = 0.0
    for index, position in enumerate(positions):
        prefix = f"paper_positions[{index}]"
        if not isinstance(position, dict):
            raise PaperLedgerError(f"{prefix} must be an object")
        product = position.get("product_id")
        if not isinstance(product, str) or not product.strip():
            raise PaperLedgerError(f"{prefix}.product_id must be a non-empty string")
        notional = _number(position.get("entry_notional"), f"{prefix}.entry_notional")
        if notional < 0:
            raise PaperLedgerError(f"{prefix}.entry_notional must not be negative")
        leverage = _number(position.get("leverage"), f"{prefix}.leverage", positive=True)
        fees = _number(position.get("fees_paid"), f"{prefix}.fees_paid")
        funding = _number(position.get("cum_funding"), f"{prefix}.cum_funding")
        if fees < 0:
            raise PaperLedgerError(f"{prefix}.fees_paid must not be negative")
        open_margin += notional / leverage
        open_entry_fees += fees
        open_funding += funding

    core_cost = _core_cost(state.get("core_holdings"))

    expected = (
        starting
        + realized
        + core_realized
        - open_margin
        - open_entry_fees
        - open_funding
        - core_cost
    )
    components = {
        "paper_starting_capital": starting,
        "paper_realized_pnl": realized,
        "paper_core_realized_pnl": core_realized,
        "open_margin": open_margin,
        "open_entry_fees": open_entry_fees,
        "open_funding": open_funding,
        "core_cost": core_cost,
        "expected_cash": expected,
        "input_cash": cash,
        "difference": cash - expected,
    }
    for name, value in components.items():
        if not math.isfinite(value):
            raise PaperLedgerError(f"computed {name} is non-finite")
    return components


def _core_cost(holdings: Any) -> float:
    """Sum ``total_cost`` across the core (DCA) bucket.

    This is cash already spent and not represented by any open position, so it
    has to leave the expected-cash side of the invariant.
    """
    if holdings is None:
        return 0.0
    if not isinstance(holdings, list):
        raise PaperLedgerError("core_holdings must be an array")
    total = 0.0
    for index, holding in enumerate(holdings):
        prefix = f"core_holdings[{index}]"
        if not isinstance(holding, dict):
            raise PaperLedgerError(f"{prefix} must be an object")
        product = holding.get("product_id")
        if not isinstance(product, str) or not product.strip():
            raise PaperLedgerError(f"{prefix}.product_id must be a non-empty string")
        cost = _number(holding.get("total_cost"), f"{prefix}.total_cost")
        if cost < 0:
            raise PaperLedgerError(f"{prefix}.total_cost must not be negative")
        total += cost
    return total


def soft_difference(state: Any) -> float | None:
    """Return ``paper_cash - expected_cash``, or ``None`` if unreadable.

    The startup gate prefers this over :func:`formula_components` because a state
    file that is merely *incomplete* is a different fault from one that is
    *arithmetically wrong*, and each has its own message. Returning ``None`` lets
    the caller report the malformed-state problem without also claiming the
    ledger failed to balance.
    """
    try:
        return formula_components(state)["difference"]
    except PaperLedgerError:
        return None


def describe(components: dict[str, float]) -> str:
    """One-line, operator-readable summary of a balanced/unbalanced book."""
    return (
        "cash={cash:.2f} but expected={expected:.2f} "
        "(start={start:.2f} +realized={realized:.2f} +core_realized={core_realized:.2f} "
        "-open_margin={margin:.2f} -open_fees={fees:.2f} -open_funding={funding:.2f} "
        "-core_cost={core:.2f})"
    ).format(
        cash=components["input_cash"],
        expected=components["expected_cash"],
        start=components["paper_starting_capital"],
        realized=components["paper_realized_pnl"],
        core_realized=components["paper_core_realized_pnl"],
        margin=components["open_margin"],
        fees=components["open_entry_fees"],
        funding=components["open_funding"],
        core=components["core_cost"],
    )