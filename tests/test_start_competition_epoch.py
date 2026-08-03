from __future__ import annotations

import json
import os
import time
from pathlib import Path

import pytest

from scripts.start_competition_epoch import start_epoch


def write_fresh(path: Path, payload: dict, now: float) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")
    os.utime(path, (now, now))


def test_start_epoch_archives_agent_and_captures_flat_bot_and_cost_baselines(tmp_path):
    now = time.time()
    agent = tmp_path / "data" / "hermes_agent_ledger.json"
    bot = tmp_path / "data" / "paper_trader_v4_state.json"
    costs = tmp_path / "data" / "agent_cost_ledger.json"
    epoch_path = tmp_path / "data" / "competition_epoch.json"
    snapshot = tmp_path / "data" / "competition_state.json"

    write_fresh(agent, {
        "cash": 9_500,
        "equity": 9_500,
        "positions": {},
        "trades": [],
    }, now)
    write_fresh(bot, {
        "paper_cash": 12_500,
        "paper_positions": [],
        "paper_trades": [],
        "paper_realized_pnl": 2_500,
        "paper_fees_paid": 100,
    }, now)
    write_fresh(costs, {
        "costs": [{"agentId": "openrouter-trader", "remoteApiCost": 12.5}],
    }, now)
    write_fresh(snapshot, {"status": "stale-old-race"}, now)

    epoch = start_epoch(
        agent_ledger=agent,
        agent_archive=tmp_path / "data" / "legacy_agent_ledgers",
        agent_lock=tmp_path / "data" / "hermes_agent_ledger.lock",
        bot_state=bot,
        bot_lock=tmp_path / "data" / "trader-v4.lock",
        cost_ledger=costs,
        epoch_path=epoch_path,
        epoch_archive=tmp_path / "data" / "competition_epochs",
        snapshot_path=snapshot,
        normalized_capital=10_000,
        stale_after=3600,
        now=now,
    )

    reset_agent = json.loads(agent.read_text(encoding="utf-8"))
    assert reset_agent["accounting_version"] == 2
    assert reset_agent["ranking_eligible"] is True
    assert reset_agent["cash"] == 10_000
    assert reset_agent["positions"] == {}
    assert epoch["baselines"]["agent_raw_equity_usd"] == 10_000
    assert epoch["baselines"]["bot_raw_equity_usd"] == 12_500
    assert epoch["baselines"]["agent_cost_usd"] == 12.5
    assert epoch["agent_history_valid_from"] == reset_agent["history_valid_from"]
    assert epoch_path.exists()
    assert not snapshot.exists()
    assert list((tmp_path / "data" / "legacy_agent_ledgers").glob("*.json"))


def test_start_epoch_refuses_open_bot_book_before_resetting_agent(tmp_path):
    now = time.time()
    agent = tmp_path / "agent.json"
    bot = tmp_path / "bot.json"
    costs = tmp_path / "costs.json"
    original_agent = {"cash": 9_500, "positions": {}, "trades": []}
    write_fresh(agent, original_agent, now)
    write_fresh(bot, {
        "paper_cash": 9_000,
        "paper_positions": [{
            "product_id": "BTC-USD",
            "side": "LONG",
            "qty": 0.1,
            "entry_price": 10_000,
            "mark_price": 10_100,
        }],
        "paper_trades": [],
    }, now)
    write_fresh(costs, {
        "costs": [{"agentId": "openrouter-trader", "remoteApiCost": 1}],
    }, now)

    with pytest.raises(RuntimeError, match="bot_book_must_be_flat"):
        start_epoch(
            agent_ledger=agent,
            agent_archive=tmp_path / "legacy",
            agent_lock=tmp_path / "agent.lock",
            bot_state=bot,
            bot_lock=tmp_path / "bot.lock",
            cost_ledger=costs,
            epoch_path=tmp_path / "epoch.json",
            epoch_archive=tmp_path / "epochs",
            snapshot_path=tmp_path / "snapshot.json",
            stale_after=3600,
            now=now,
        )

    assert json.loads(agent.read_text(encoding="utf-8")) == original_agent
