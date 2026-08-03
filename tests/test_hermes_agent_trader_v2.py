from __future__ import annotations

import json
import math

import pytest

from scripts import hermes_agent_trader as trader
from scripts.hermes_agent_accounting import new_ledger


def configure_ledger(tmp_path, monkeypatch, *, fee_rate=0.0):
    ledger_path = tmp_path / "hermes_agent_ledger.json"
    monkeypatch.setattr(trader, "LEDGER", ledger_path)
    monkeypatch.setattr(trader, "LOCK_FILE", tmp_path / "hermes_agent_ledger.lock")
    monkeypatch.setattr(trader, "FEE_RATE", fee_rate)
    monkeypatch.setattr(trader, "KILL_SWITCH", False)
    trader.save_ledger(new_ledger(10_000))
    return ledger_path


def test_active_facade_long_round_trip_uses_leverage_once(tmp_path, monkeypatch):
    configure_ledger(tmp_path, monkeypatch)

    opened = trader.record_signal(
        "BTC-USD",
        "BUY",
        quote_size=100,
        price=100,
        leverage=3,
    )
    assert opened["action"] == "signal_recorded"
    assert opened["position"]["base"] == 3
    assert opened["position"]["cost_basis"] == 100
    assert opened["position"]["exposure"] == 300

    closed = trader.close_position("BTC-USD", price=101)
    assert closed["action"] == "closed"
    assert math.isclose(closed["realized_pnl"], 3)

    ledger = trader.load_ledger()
    assert ledger["positions"] == {}
    assert math.isclose(ledger["cash"], 10_003)
    assert ledger["accounting_version"] == 2


def test_active_facade_short_round_trip_is_cash_symmetric(tmp_path, monkeypatch):
    configure_ledger(tmp_path, monkeypatch)

    opened = trader.open_short(
        "BTC-USD",
        100,
        price=100,
        leverage=3,
    )
    assert opened["action"] == "short_opened"
    assert opened["position"]["base"] == 3
    assert math.isclose(trader.load_ledger()["cash"], 9_900)

    closed = trader.close_short("BTC-USD", price=99)
    assert closed["action"] == "short_closed"
    assert math.isclose(trader.load_ledger()["cash"], 10_003)
    assert math.isclose(closed["realized_pnl"], 3)


def test_facade_adds_using_the_existing_position_leverage(tmp_path, monkeypatch):
    configure_ledger(tmp_path, monkeypatch)
    trader.record_signal("SOL-USD", "BUY", quote_size=100, price=100, leverage=2)

    added = trader.add_to_position("SOL-USD", 50, price=110)
    assert added["action"] == "position_added"
    position = added["position"]
    assert position["leverage"] == 2
    assert math.isclose(position["margin_usd"], 150)
    assert math.isclose(position["notional_usd"], 300)
    assert position["adds"] == 1


def test_legacy_ledger_is_never_silently_reinterpreted(tmp_path, monkeypatch):
    ledger_path = tmp_path / "hermes_agent_ledger.json"
    monkeypatch.setattr(trader, "LEDGER", ledger_path)
    monkeypatch.setattr(trader, "LOCK_FILE", tmp_path / "hermes_agent_ledger.lock")
    monkeypatch.setattr(trader, "FEE_RATE", 0.0)
    monkeypatch.setattr(trader, "KILL_SWITCH", False)
    ledger_path.write_text(json.dumps({
        "cash": 10_000,
        "positions": {},
        "trades": [],
    }), encoding="utf-8")

    with pytest.raises(trader.LegacyLedgerError):
        trader.load_ledger()

    attempted = trader.record_signal("BTC-USD", "BUY", quote_size=100, price=100)
    assert attempted["action"] == "refused"
    assert "reset_agent_competition.py" in attempted["reason"]
    assert json.loads(ledger_path.read_text(encoding="utf-8"))["cash"] == 10_000


def test_fee_adjusted_closed_pnl_matches_cash_change(tmp_path, monkeypatch):
    configure_ledger(tmp_path, monkeypatch, fee_rate=0.001)
    trader.record_signal("ETH-USD", "BUY", quote_size=100, price=100, leverage=2)
    trader.close_position("ETH-USD", price=100)

    ledger = trader.load_ledger()
    assert math.isclose(ledger["fees_paid"], 0.4)
    assert math.isclose(ledger["realized_pnl"], -0.4)
    assert math.isclose(ledger["cash"] - ledger["starting_capital"], -0.4)
