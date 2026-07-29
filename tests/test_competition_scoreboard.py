from __future__ import annotations

import json
import os
import time
from pathlib import Path

from scripts.competition_scoreboard import build_state


def _write(path: Path, payload: dict, now: float) -> None:
    path.write_text(json.dumps(payload), encoding="utf-8")
    os.utime(path, (now, now))


def _agent(*, equity=10_000, realized=0, history="2026-07-29T12:00:00+00:00", trades=None):
    return {
        "schema_version": 2,
        "accounting_version": 2,
        "ranking_eligible": True,
        "history_valid_from": history,
        "starting_capital": 10_000,
        "cash": equity,
        "equity": equity,
        "peak_equity": max(10_000, equity),
        "equity_curve": [10_000, equity],
        "realized_pnl": realized,
        "positions": {},
        "trades": trades or [],
    }


def _bot(*, cash=10_000, realized=0, positions=None, trades=None):
    return {
        "paper_starting_capital": 10_000,
        "paper_cash": cash,
        "paper_realized_pnl": realized,
        "paper_peak_equity": max(10_000, cash),
        "paper_equity_curve": [10_000, cash],
        "paper_positions": positions or [],
        "paper_trades": trades or [],
        "paper_fees_paid": 0,
    }


def _epoch(*, history="2026-07-29T12:00:00+00:00", agent_base=10_000,
           bot_base=10_000, cost_base=0):
    return {
        "schema_version": 1,
        "epoch_id": "epoch-test",
        "started_at": "2026-07-29T12:00:00+00:00",
        "started_epoch": 1785326400,
        "normalized_starting_capital_usd": 10_000,
        "agent_history_valid_from": history,
        "baselines": {
            "agent_raw_equity_usd": agent_base,
            "agent_realized_pnl_usd": 0,
            "agent_cost_usd": cost_base,
            "bot_raw_equity_usd": bot_base,
            "bot_realized_pnl_usd": 0,
            "bot_fees_paid_usd": 0,
        },
    }


def test_bot_equity_does_not_double_count_realized_pnl(tmp_path):
    now = time.time()
    agent = tmp_path / "agent.json"
    bot = tmp_path / "bot.json"
    costs = tmp_path / "costs.json"
    epoch = tmp_path / "epoch.json"

    _write(agent, _agent(
        equity=10_100,
        realized=100,
        trades=[{"ts": now, "side": "BUY_CLOSE", "realized_pnl": 100, "commission": 1}],
    ), now)
    _write(bot, _bot(
        cash=9_800,
        realized=-100,
        trades=[{"ts": now, "pnl": -100, "fee": 1}],
    ), now)
    _write(costs, {"costs": [{"agentId": "openrouter-trader", "remoteApiCost": 25}]}, now)
    _write(epoch, _epoch(), now)

    result = build_state(agent, bot, costs, epoch_path=epoch, now=now, stale_after=3600)

    # paper_realized_pnl is already reflected in paper_cash and is not added again.
    assert result["competitors"]["bot"]["raw_lifetime_equity_usd"] == 9_800
    assert result["competitors"]["bot"]["gross_equity_usd"] == 9_800
    assert result["competitors"]["agent"]["net_equity_usd"] == 10_075
    assert result["standings"]["leader"] == "agent"
    assert result["standings"]["valid_for_ranking"] is True


def test_epoch_normalizes_different_lifetime_balances_and_cost_baseline(tmp_path):
    now = time.time()
    agent = tmp_path / "agent.json"
    bot = tmp_path / "bot.json"
    costs = tmp_path / "costs.json"
    epoch = tmp_path / "epoch.json"

    _write(agent, _agent(equity=10_050, realized=50), now)
    _write(bot, _bot(cash=12_500, realized=2_500), now)
    _write(costs, {"costs": [{"agentId": "openrouter", "remoteApiCost": 25}]}, now)
    _write(epoch, _epoch(agent_base=10_000, bot_base=12_400, cost_base=20), now)

    result = build_state(agent, bot, costs, epoch_path=epoch, now=now, stale_after=3600)

    assert result["competitors"]["agent"]["gross_equity_usd"] == 10_050
    assert result["competitors"]["agent"]["operating_cost_usd"] == 5
    assert result["competitors"]["agent"]["net_equity_usd"] == 10_045
    assert result["competitors"]["bot"]["gross_equity_usd"] == 10_100
    assert result["standings"]["leader"] == "bot"


def test_unmarked_open_bot_positions_block_ranking(tmp_path):
    now = time.time()
    agent = tmp_path / "agent.json"
    bot = tmp_path / "bot.json"
    costs = tmp_path / "costs.json"
    epoch = tmp_path / "epoch.json"

    _write(agent, _agent(), now)
    _write(bot, _bot(
        cash=9_900,
        positions=[{"product_id": "BTC-USD", "side": "LONG", "qty": 0.01, "entry_price": 10_000}],
    ), now)
    _write(costs, {"costs": [{"agentId": "openrouter", "remoteApiCost": 0}]}, now)
    _write(epoch, _epoch(), now)

    result = build_state(agent, bot, costs, epoch_path=epoch, now=now, stale_after=3600)

    assert "bot_open_positions_missing_current_marks" in result["warnings"]
    assert result["standings"]["valid_for_ranking"] is False
    assert result["standings"]["leader"] == "unknown"


def test_missing_epoch_blocks_ranking(tmp_path):
    now = time.time()
    agent = tmp_path / "agent.json"
    bot = tmp_path / "bot.json"
    costs = tmp_path / "costs.json"
    missing_epoch = tmp_path / "missing-epoch.json"

    _write(agent, _agent(), now)
    _write(bot, _bot(), now)
    _write(costs, {"costs": [{"agentId": "openrouter", "remoteApiCost": 0}]}, now)

    result = build_state(agent, bot, costs, epoch_path=missing_epoch, now=now, stale_after=3600)

    assert "competition_epoch_missing" in result["warnings"]
    assert result["standings"]["valid_for_ranking"] is False
    assert result["standings"]["leader"] == "unknown"


def test_legacy_agent_history_is_never_ranked(tmp_path):
    now = time.time()
    agent = tmp_path / "agent.json"
    bot = tmp_path / "bot.json"
    costs = tmp_path / "costs.json"
    epoch = tmp_path / "epoch.json"

    legacy = _agent()
    legacy.pop("accounting_version")
    _write(agent, legacy, now)
    _write(bot, _bot(), now)
    _write(costs, {"costs": [{"agentId": "openrouter", "remoteApiCost": 0}]}, now)
    _write(epoch, _epoch(), now)

    result = build_state(agent, bot, costs, epoch_path=epoch, now=now, stale_after=3600)

    assert "agent_accounting_version_invalid" in result["warnings"]
    assert result["standings"]["valid_for_ranking"] is False
