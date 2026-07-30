from __future__ import annotations

import json
import sys
import types
from datetime import datetime, timezone

import pytest

from scripts import hermes_agent_trader as trader


@pytest.fixture
def ledger_env(tmp_path, monkeypatch):
    monkeypatch.setattr(trader, "ROOT", tmp_path)
    monkeypatch.setattr(trader, "LEDGER", tmp_path / "hermes_agent_ledger.json")
    monkeypatch.setattr(trader, "LOCK_FILE", tmp_path / "hermes_agent_ledger.lock")
    monkeypatch.setattr(trader, "KILL_SWITCH", False)
    monkeypatch.setattr(trader, "REQUIRE_MANUAL_APPROVAL", False)
    monkeypatch.setattr(trader, "HERMES_AGENT_LIVE", False)
    monkeypatch.setattr(trader, "MAX_NOTIONAL", 250.0)
    monkeypatch.setattr(trader, "AGENT_LEVERAGE", 3.0)
    monkeypatch.setattr(trader, "FEE_RATE", 0.0)
    trader.save_ledger(trader.new_ledger(10_000))
    return tmp_path


def install_fake_coinbase(monkeypatch, responses):
    queue = list(responses)

    class FakeCBClient:
        def __init__(self, dry_run_cli=True):
            self.dry_run_cli = dry_run_cli

        def _cli_json(self, *_args):
            return queue.pop(0)

    coinbase = types.ModuleType("coinbase")
    coinbase.__path__ = []
    src = types.ModuleType("coinbase.src")
    src.__path__ = []
    module = types.ModuleType("coinbase.src.cb_client")
    module.CBClient = FakeCBClient
    monkeypatch.setitem(sys.modules, "coinbase", coinbase)
    monkeypatch.setitem(sys.modules, "coinbase.src", src)
    monkeypatch.setitem(sys.modules, "coinbase.src.cb_client", module)


def test_preview_price_resolution_and_current_marks(ledger_env, monkeypatch):
    monkeypatch.setattr(trader, "KILL_SWITCH", True)
    assert trader.quote("BTC-USD", "BUY", quote_size=10)["action"] == "refused"
    monkeypatch.setattr(trader, "KILL_SWITCH", False)
    assert trader.quote("BTC-USD", "BUY", quote_size=251)["action"] == "refused"

    class FakeClient:
        def __init__(self):
            self.responses = [
                {"status": "preview_error", "error": "preview failed"},
                {"est_average_filled_price": "101.5"},
            ]

        def preview_order(self, *_args, **_kwargs):
            return self.responses.pop(0)

    client = FakeClient()
    monkeypatch.setattr(trader, "get_client", lambda: client)
    assert trader.quote("BTC-USD", "BUY", quote_size=10)["action"] == "quote_error"
    quoted = trader.quote("BTC-USD", "BUY", quote_size=10)
    assert quoted["action"] == "quote"
    assert quoted["side"] == "BUY"

    assert trader._resolve_price("BTC-USD", "BUY", 10, None, 99)[0] == 99
    monkeypatch.setattr(trader, "quote", lambda *_args, **_kwargs: {"action": "quote_error", "error": "no quote"})
    resolved, error = trader._resolve_price("BTC-USD", "BUY", 10, None, None)
    assert resolved == 0
    assert error["action"] == "quote_error"
    monkeypatch.setattr(
        trader,
        "quote",
        lambda *_args, **_kwargs: {"action": "quote", "quote": {"est_average_filled_price": "123.5"}},
    )
    assert trader._resolve_price("BTC-USD", "BUY", 10, None, None)[0] == 123.5

    install_fake_coinbase(
        monkeypatch,
        [
            {"candles": [{"close": ""}, {"close": "102.25"}]},
            {"candles": []},
        ],
    )
    assert trader._current_price("BTC-USD") == 102.25
    assert trader._current_price("BTC-USD") == 0.0


def test_signal_guards_and_partial_sell_paths(ledger_env, monkeypatch):
    monkeypatch.setattr(trader, "KILL_SWITCH", True)
    assert trader.record_signal("BTC-USD", "BUY", quote_size=10, price=100)["action"] == "refused"
    monkeypatch.setattr(trader, "KILL_SWITCH", False)

    assert trader.record_signal("BTC-USD", "BUY")["action"] == "refused"
    assert trader.record_signal("BTC-USD", "BUY", quote_size=0, price=100)["action"] == "refused"
    assert trader.record_signal("BTC-USD", "HOLD", quote_size=10, price=100)["action"] == "refused"

    with monkeypatch.context() as scoped:
        scoped.setattr(trader, "_resolve_price", lambda *_args, **_kwargs: (0.0, None))
        assert trader.record_signal("BTC-USD", "BUY", quote_size=10)["action"] == "quote_error"

    missing = trader.record_signal("BTC-USD", "SELL", base_size=1, price=100)
    assert missing["action"] == "refused"
    assert "position_not_found" in missing["reason"]

    opened = trader.record_signal("BTC-USD", "BUY", quote_size=100, price=100, leverage=2)
    assert opened["action"] == "signal_recorded"
    partial = trader.record_signal("BTC-USD", "SELL", base_size=1, price=101)
    assert partial["action"] == "signal_recorded"
    assert partial["position"] is not None

    by_margin = trader.record_signal("BTC-USD", "SELL", quote_size=25, price=101)
    assert by_margin["action"] == "signal_recorded"

    base_only = trader.record_signal("SOL-USD", "BUY", base_size=1, price=50, leverage=2)
    assert base_only["action"] == "signal_recorded"


def test_position_guardrails_and_missing_positions(ledger_env, monkeypatch):
    monkeypatch.setattr(trader, "KILL_SWITCH", True)
    assert trader.close_position("BTC-USD", price=100)["action"] == "refused"
    assert trader.open_short("BTC-USD", 10, price=100)["action"] == "refused"
    assert trader.close_short("BTC-USD", price=100)["action"] == "refused"
    assert trader.add_to_position("BTC-USD", 10, price=100)["action"] == "refused"
    monkeypatch.setattr(trader, "KILL_SWITCH", False)

    monkeypatch.setattr(trader, "_current_price", lambda _product: 0.0)
    assert trader.close_position("BTC-USD")["action"] == "quote_error"
    assert trader.open_short("BTC-USD", 10)["action"] == "quote_error"
    assert trader.close_short("BTC-USD")["action"] == "quote_error"
    assert trader.add_to_position("BTC-USD", 10)["action"] == "quote_error"

    assert trader.close_position("BTC-USD", price=100)["action"] == "no_position"
    assert trader.close_short("BTC-USD", price=100)["action"] == "no_short"
    assert trader.open_short("BTC-USD", 0, price=100)["action"] == "refused"
    assert trader.add_to_position("BTC-USD", 0, price=100)["action"] == "refused"
    assert trader.add_to_position("BTC-USD", 10, price=100)["action"] == "refused"

    assert trader.open_short("ETH-USD", 100, price=100, leverage=2)["action"] == "short_opened"
    assert trader.led_short_key("ETH-USD") is True
    assert trader.led_short_key("BTC-USD") is False


def test_stats_bot_fills_and_drawdown_controls(ledger_env, monkeypatch):
    assert trader._timestamp(123.5) == 123.5
    assert trader._timestamp("2026-01-01T00:00:00Z") > 0
    assert trader._timestamp("not-a-time") == 0.0

    data_dir = ledger_env / "data"
    data_dir.mkdir()
    now = datetime.now(timezone.utc).timestamp()
    state_path = data_dir / "paper_trader_v4_state.json"
    state_path.write_text(
        json.dumps(
            {
                "paper_trades": [
                    {"product_id": "BTC-USD", "side": "buy", "ts": now, "strategy": "ema"},
                    {"product_id": "ETH-USD", "side": "sell", "ts": now - 60, "strategy": "breakout"},
                    {"product_id": "OLD-USD", "side": "buy", "ts": now - 7200},
                ]
            }
        ),
        encoding="utf-8",
    )
    fills = trader.bot_recent_fills(minutes=15)
    assert fills["BTC-USD"]["side"] == "BUY"
    assert fills["ETH-USD"]["side"] == "SELL"
    assert "OLD-USD" not in fills

    state_path.write_text("{bad json", encoding="utf-8")
    assert trader.bot_recent_fills() == {}

    ledger = {
        "trades": [
            {"realized_pnl": 2},
            {"realized_pnl": -5},
            {"realized_pnl": 1},
            {"realized_pnl": -1},
            {"status": "open"},
        ],
        "equity": 94,
        "peak_equity": 100,
    }
    stats = trader.recent_stats(ledger)
    assert stats["n"] == 4
    assert stats["max_drawdown"] == -5.0
    assert trader.size_for(0.001, cap=100) == 29.7
    assert trader.size_for(0.04, cap=100) == 99.0
    assert trader.equity_drawdown_pct(ledger) == -6.0
    assert trader.equity_drawdown_pct({"equity": 0, "peak_equity": 0}) == 0.0

    deep = trader.drawdown_circuit({"trades": [], "equity": 80, "peak_equity": 100})
    assert deep["open"] is False
    weak = trader.drawdown_circuit(
        {"trades": [{"realized_pnl": -1} for _ in range(5)], "equity": 94, "peak_equity": 100}
    )
    assert weak["open"] is False
    assert "win_rate" in weak["reason"]
    assert trader.drawdown_circuit({"trades": [{"realized_pnl": 1}], "equity": 100, "peak_equity": 100})["reason"] == "warming_up"
    assert trader.drawdown_circuit(
        {"trades": [{"realized_pnl": 1} for _ in range(4)], "equity": 100, "peak_equity": 100}
    )["reason"] == "ok"

    with monkeypatch.context() as scoped:
        scoped.setattr(trader, "load_ledger", lambda: (_ for _ in ()).throw(trader.LegacyLedgerError("legacy")))
        assert trader.drawdown_circuit()["open"] is False


def test_marks_equity_close_all_and_live_controls(ledger_env, monkeypatch):
    trader.record_signal("BTC-USD", "BUY", quote_size=100, price=100, leverage=2)
    trader.open_short("ETH-USD", 100, price=100, leverage=2)
    marks = {"BTC-USD": 110.0, "ETH-USD": 90.0}
    monkeypatch.setattr(trader, "_current_price", lambda product: marks.get(product, 0.0))

    mtm = trader.mark_to_market()
    assert mtm["total_unrealized_pnl"] > 0
    updated = trader.update_equity()
    assert updated["equity"] > updated["cash"]

    monkeypatch.setattr(trader, "_current_price", lambda product: 110.0 if product == "BTC-USD" else 0.0)
    missing = trader.update_equity()
    assert missing["action"] == "equity_not_updated"
    assert missing["missing_marks"] == ["ETH-USD"]

    monkeypatch.setattr(trader, "KILL_SWITCH", True)
    assert trader.mark_to_market()["error"] == "KILL_SWITCH active"
    monkeypatch.setattr(trader, "KILL_SWITCH", False)

    with monkeypatch.context() as scoped:
        scoped.setattr(trader, "load_ledger", lambda: (_ for _ in ()).throw(trader.LegacyLedgerError("legacy")))
        assert trader.mark_to_market()["error"] == "legacy"
        assert trader.update_equity()["action"] == "equity_not_updated"
        assert trader.close_all()["error"] == "legacy"
        assert trader.ledger_summary()["ranking_eligible"] is False

    monkeypatch.setattr(trader, "_current_price", lambda product: marks[product])
    closed = trader.close_all()
    assert closed["longs"][0]["BTC-USD"] == "closed"
    assert closed["shorts"][0]["ETH-USD"] == "short_closed"

    assert trader.propose_live("BTC-USD", "BUY", 10)["action"] == "refused"
    monkeypatch.setattr(trader, "HERMES_AGENT_LIVE", True)
    monkeypatch.setattr(trader, "KILL_SWITCH", True)
    assert trader.propose_live("BTC-USD", "BUY", 10)["action"] == "refused"
    monkeypatch.setattr(trader, "KILL_SWITCH", False)
    monkeypatch.setattr(trader, "REQUIRE_MANUAL_APPROVAL", True)
    assert trader.propose_live("BTC-USD", "BUY", 10)["action"] == "refused"
    monkeypatch.setattr(trader, "REQUIRE_MANUAL_APPROVAL", False)
    monkeypatch.setattr(trader, "quote", lambda *_args, **_kwargs: {"action": "quote"})
    assert trader.propose_live("BTC-USD", "BUY", 10)["action"] == "live_proposal_blocked"

    summary = trader.ledger_summary()
    assert summary["accounting_version"] == trader.ACCOUNTING_VERSION
    assert trader.status()["mode"].startswith("PAPER/SIM")


@pytest.mark.parametrize(
    ("argv", "target"),
    [
        (["quote", "BTC-USD", "BUY", "--quote-size", "10"], "quote"),
        (["signal", "BTC-USD", "BUY", "--quote-size", "10"], "record_signal"),
        (["ledger"], "ledger_summary"),
        (["close", "BTC-USD"], "close_position"),
        (["short", "BTC-USD", "--quote-size", "10"], "open_short"),
        (["closeshort", "BTC-USD"], "close_short"),
        (["closeall"], "close_all"),
        (["mtm"], "mark_to_market"),
        (["propose-live", "BTC-USD", "BUY", "--quote-size", "10"], "propose_live"),
        (["status"], "status"),
    ],
)
def test_cli_dispatches_each_supported_command(monkeypatch, capsys, argv, target):
    monkeypatch.setattr(trader, target, lambda *_args, **_kwargs: {"target": target})
    monkeypatch.setattr(sys, "argv", ["hermes_agent_trader.py", *argv])
    assert trader.main() == 0
    assert json.loads(capsys.readouterr().out)["target"] == target
