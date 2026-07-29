from __future__ import annotations

import math
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

from hermes_agent_accounting import apply_marks, close_position, mark_to_market, new_ledger, open_position


def test_three_x_long_one_percent_move_returns_three_percent_on_margin_before_fees():
    ledger = new_ledger(10_000)
    ledger = open_position(
        ledger,
        product_id="BTC-USD",
        side="LONG",
        margin_usd=100,
        entry_price=100,
        leverage=3,
        fee_rate=0,
    )
    assert ledger["positions"]["BTC-USD"]["quantity"] == 3
    assert ledger["positions"]["BTC-USD"]["base"] == 3
    ledger = close_position(
        ledger,
        product_id="BTC-USD",
        side="LONG",
        exit_price=101,
        fee_rate=0,
    )
    assert ledger["realized_pnl"] == 3
    assert ledger["cash"] == 10_003


def test_three_x_short_one_percent_move_is_symmetric():
    ledger = new_ledger(10_000)
    ledger = open_position(
        ledger,
        product_id="BTC-USD",
        side="SHORT",
        margin_usd=100,
        entry_price=100,
        leverage=3,
        fee_rate=0,
    )
    assert ledger["positions"]["SHORT:BTC-USD"]["quantity"] == 3
    ledger = close_position(
        ledger,
        product_id="BTC-USD",
        side="SHORT",
        exit_price=99,
        fee_rate=0,
    )
    assert ledger["realized_pnl"] == 3
    assert ledger["cash"] == 10_003


def test_entry_and_exit_fees_reconcile_realized_pnl_to_cash():
    ledger = new_ledger(10_000)
    ledger = open_position(
        ledger,
        product_id="ETH-USD",
        side="LONG",
        margin_usd=100,
        entry_price=100,
        leverage=2,
        fee_rate=0.001,
    )
    assert math.isclose(ledger["cash"], 9_899.8)
    ledger = close_position(
        ledger,
        product_id="ETH-USD",
        side="LONG",
        exit_price=100,
        fee_rate=0.001,
    )
    assert math.isclose(ledger["fees_paid"], 0.4)
    assert math.isclose(ledger["realized_pnl"], -0.4)
    assert math.isclose(ledger["cash"], 9_999.6)
    assert math.isclose(ledger["cash"] - ledger["starting_capital"], ledger["realized_pnl"])
    assert math.isclose(ledger["trades"][-1]["entry_fee_allocated_usd"], 0.2)


def test_add_and_partial_close_preserve_margin_notional_quantity_contract():
    ledger = new_ledger(10_000)
    for price in (100, 110):
        ledger = open_position(
            ledger,
            product_id="SOL-USD",
            side="LONG",
            margin_usd=100,
            entry_price=price,
            leverage=2,
            fee_rate=0,
        )
    pos = ledger["positions"]["SOL-USD"]
    assert math.isclose(pos["notional_usd"], pos["margin_usd"] * pos["leverage"])
    assert math.isclose(pos["quantity"], pos["notional_usd"] / pos["entry_price"])
    assert math.isclose(pos["base"], pos["quantity"])
    assert math.isclose(pos["cost_basis"], pos["margin_usd"])
    assert math.isclose(pos["exposure"], pos["notional_usd"])
    ledger = close_position(
        ledger,
        product_id="SOL-USD",
        side="LONG",
        exit_price=120,
        fraction=0.5,
        fee_rate=0,
    )
    remaining = ledger["positions"]["SOL-USD"]
    assert math.isclose(remaining["margin_usd"], 100)
    assert math.isclose(remaining["notional_usd"], 200)
    assert math.isclose(remaining["base"], remaining["quantity"])


def test_mark_to_market_requires_all_open_position_marks():
    ledger = new_ledger(10_000)
    ledger = open_position(
        ledger,
        product_id="BTC-USD",
        side="LONG",
        margin_usd=100,
        entry_price=100,
        leverage=3,
        fee_rate=0,
    )
    missing = mark_to_market(ledger, {})
    assert missing["equity"] is None
    assert missing["missing_marks"] == ["BTC-USD"]
    marked = apply_marks(ledger, {"BTC-USD": 101})
    assert marked["equity"] == 10_003
