import json
import os
import time
from pathlib import Path

from scripts.competition_scoreboard import build_state


def _write(path: Path, payload: dict) -> None:
    path.write_text(json.dumps(payload), encoding="utf-8")
    os.utime(path, (time.time(), time.time()))


def test_bot_equity_does_not_double_count_realized_pnl(tmp_path):
    agent = tmp_path / "agent.json"
    bot = tmp_path / "bot.json"
    costs = tmp_path / "costs.json"

    _write(agent, {
        "starting_capital": 10000,
        "equity": 10100,
        "cash": 10100,
        "realized_pnl": 100,
        "peak_equity": 10120,
        "equity_curve": [10000, 10120, 10100],
        "positions": {},
        "trades": [{"side": "SELL", "realized_pnl": 100, "commission": 1}],
    })
    _write(bot, {
        "paper_starting_capital": 10000,
        "paper_cash": 9800,
        "paper_realized_pnl": -100,
        "paper_peak_equity": 10000,
        "paper_positions": [],
        "paper_trades": [{"pnl": -100, "fee": 1}],
    })
    _write(costs, {"costs": [{"agentId": "openrouter-trader", "remoteApiCost": 25}]})

    result = build_state(agent, bot, costs, stale_after=3600)

    # The old scoreboard returned 9700 by adding realized P&L to cash again.
    assert result["competitors"]["bot"]["gross_equity_usd"] == 9800
    assert result["competitors"]["agent"]["net_equity_usd"] == 10075
    assert result["standings"]["leader"] == "agent"
    assert result["standings"]["valid_for_ranking"] is True


def test_unmarked_open_bot_positions_block_ranking(tmp_path):
    agent = tmp_path / "agent.json"
    bot = tmp_path / "bot.json"
    costs = tmp_path / "costs.json"

    _write(agent, {"starting_capital": 10000, "equity": 10000, "positions": {}, "trades": []})
    _write(bot, {
        "paper_starting_capital": 10000,
        "paper_cash": 9900,
        "paper_positions": [{"product_id": "BTC-USD", "side": "LONG", "qty": 0.01, "entry_price": 10000}],
        "paper_trades": [],
    })

    result = build_state(agent, bot, costs, stale_after=3600)

    assert "bot_open_positions_missing_current_marks" in result["warnings"]
    assert result["standings"]["valid_for_ranking"] is False
    assert result["standings"]["leader"] == "unknown"
