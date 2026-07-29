#!/usr/bin/env python3
"""Build the authoritative bot-versus-agent competition snapshot.

The snapshot is deliberately conservative:
- Bot equity follows EventTraderV4's canonical paper-equity contract:
  cash plus marked unrealized P&L. Realized P&L is never added twice.
- Agent operating/model costs are deducted from its gross equity.
- Missing marks, stale files, unequal starting capital, and malformed ledgers
  are surfaced as warnings and can invalidate the ranking.
- Output is written atomically to ``data/competition_state.json``.
"""
from __future__ import annotations

import argparse
import json
import math
import os
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

REPO = Path(__file__).resolve().parent.parent
DEFAULT_AGENT_LEDGER = REPO / "data" / "hermes_agent_ledger.json"
DEFAULT_BOT_STATE = REPO / "data" / "paper_trader_v4_state.json"
DEFAULT_COST_LEDGER = REPO / "data" / "agent_cost_ledger.json"
DEFAULT_OUT = REPO / "data" / "competition_state.json"
DEFAULT_STALE_AFTER_SECONDS = 900.0


def _num(value: Any, default: float | None = 0.0) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return default
    return number if math.isfinite(number) else default


def _load_json(path: Path) -> tuple[dict[str, Any], list[str]]:
    if not path.exists():
        return {}, [f"missing:{path.name}"]
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        return {}, [f"invalid:{path.name}:{type(exc).__name__}"]
    if not isinstance(payload, dict):
        return {}, [f"invalid:{path.name}:expected_object"]
    return payload, []


def _age_seconds(path: Path, now: float) -> float | None:
    try:
        return max(0.0, now - path.stat().st_mtime)
    except OSError:
        return None


def _drawdown_pct(curve: Iterable[Any], current: float, recorded_peak: Any = None) -> float:
    values = [_num(value, None) for value in curve]
    clean = [value for value in values if value is not None]
    peak = max(clean, default=_num(recorded_peak, current) or current)
    trough_drawdown = 0.0
    running_peak = clean[0] if clean else peak
    for value in clean:
        running_peak = max(running_peak, value)
        if running_peak > 0:
            trough_drawdown = max(trough_drawdown, (running_peak - value) / running_peak * 100.0)
    if peak > 0:
        trough_drawdown = max(trough_drawdown, (peak - current) / peak * 100.0)
    return round(max(0.0, trough_drawdown), 4)


def _round_trip_stats(trades: Iterable[Any], pnl_keys: tuple[str, ...]) -> dict[str, Any]:
    pnls: list[float] = []
    fees = 0.0
    for trade in trades:
        if not isinstance(trade, dict):
            continue
        fees += _num(trade.get("commission"), 0.0) or _num(trade.get("fee"), 0.0) or 0.0
        pnl = None
        for key in pnl_keys:
            if key in trade:
                pnl = _num(trade.get(key), None)
                break
        if pnl is not None:
            pnls.append(pnl)
    wins = [pnl for pnl in pnls if pnl > 0]
    losses = [pnl for pnl in pnls if pnl < 0]
    gross_profit = sum(wins)
    gross_loss = abs(sum(losses))
    return {
        "round_trips": len(pnls),
        "wins": len(wins),
        "losses": len(losses),
        "breakeven": len(pnls) - len(wins) - len(losses),
        "win_rate": round(len(wins) / len(pnls), 6) if pnls else None,
        "profit_factor": round(gross_profit / gross_loss, 6) if gross_loss > 0 else (None if not wins else float("inf")),
        "avg_win_usd": round(gross_profit / len(wins), 6) if wins else None,
        "avg_loss_usd": round(gross_loss / len(losses), 6) if losses else None,
        "fees_from_trades_usd": round(fees, 6),
    }


def _position_unrealized(position: dict[str, Any]) -> tuple[float, bool]:
    entry = _num(position.get("entry_price"), None)
    qty = _num(position.get("qty", position.get("base")), None)
    if entry is None or qty is None:
        return 0.0, False
    mark = _num(
        position.get(
            "mark_price",
            position.get("current_price", position.get("last_price", entry)),
        ),
        entry,
    )
    leverage = max(_num(position.get("leverage"), 1.0) or 1.0, 1.0)
    side = str(position.get("side", "LONG")).upper()
    raw = (mark - entry) * qty
    if side in {"SHORT", "SELL"}:
        raw = -raw
    funding = _num(position.get("cum_funding"), 0.0) or 0.0
    used_real_mark = any(key in position for key in ("mark_price", "current_price", "last_price"))
    return raw - funding, used_real_mark


def _agent_cost(cost_payload: dict[str, Any], agent_ledger: dict[str, Any]) -> float:
    explicit = sum(
        _num(agent_ledger.get(key), 0.0) or 0.0
        for key in ("api_cost_usd", "model_cost_usd", "openrouter_cost_usd")
    )
    trade_cost = sum(
        _num(trade.get("api_cost_usd"), 0.0) or 0.0
        for trade in agent_ledger.get("trades", [])
        if isinstance(trade, dict)
    )
    rows = cost_payload.get("costs", cost_payload.get("agentCostLedger", []))
    ledger_cost = 0.0
    if isinstance(rows, list):
        for row in rows:
            if not isinstance(row, dict):
                continue
            agent_id = str(row.get("agentId", row.get("agent_id", ""))).lower()
            if agent_id and not any(token in agent_id for token in ("hermes", "trader", "openrouter")):
                continue
            ledger_cost += (_num(row.get("remoteApiCost", row.get("remote_api_cost")), 0.0) or 0.0)
            ledger_cost += (_num(row.get("localComputeCost", row.get("local_compute_cost")), 0.0) or 0.0)
    summary = cost_payload.get("summary", {}) if isinstance(cost_payload.get("summary"), dict) else {}
    summary_cost = _num(summary.get("spentTodayUsd"), 0.0) or 0.0
    return round(ledger_cost or summary_cost or explicit or trade_cost, 6)


def load_agent(path: Path, cost_payload: dict[str, Any], now: float, stale_after: float) -> tuple[dict[str, Any], list[str]]:
    data, warnings = _load_json(path)
    if not data:
        return {"side": "agent", "label": "Agent", "status": "unknown"}, warnings
    starting = _num(data.get("starting_capital"), 10000.0) or 10000.0
    equity = _num(data.get("equity"), None)
    if equity is None:
        warnings.append("agent_equity_missing")
        equity = _num(data.get("cash"), starting) or starting
    realized = _num(data.get("realized_pnl"), 0.0) or 0.0
    costs = _agent_cost(cost_payload, data)
    trades = data.get("trades", []) if isinstance(data.get("trades"), list) else []
    stats = _round_trip_stats(trades, ("realized_pnl", "pnl"))
    age = _age_seconds(path, now)
    fresh = age is not None and age <= stale_after
    if not fresh:
        warnings.append("agent_ledger_stale")
    gross_pnl = equity - starting
    net_equity = equity - costs
    return {
        "side": "agent",
        "label": str(data.get("label") or "OpenRouter Agent"),
        "status": "ok" if fresh else "stale",
        "source": str(path),
        "age_seconds": round(age, 3) if age is not None else None,
        "starting_capital_usd": round(starting, 6),
        "gross_equity_usd": round(equity, 6),
        "operating_cost_usd": costs,
        "net_equity_usd": round(net_equity, 6),
        "gross_pnl_usd": round(gross_pnl, 6),
        "net_pnl_usd": round(net_equity - starting, 6),
        "gross_return_pct": round(gross_pnl / starting * 100.0, 6) if starting else None,
        "net_return_pct": round((net_equity - starting) / starting * 100.0, 6) if starting else None,
        "realized_pnl_usd": round(realized, 6),
        "max_drawdown_pct": _drawdown_pct(data.get("equity_curve", []), equity, data.get("peak_equity")),
        "open_positions": sum(
            1
            for position in (data.get("positions") or {}).values()
            if isinstance(position, dict) and abs(_num(position.get("base", position.get("exposure")), 0.0) or 0.0) > 1e-12
        ) if isinstance(data.get("positions"), dict) else 0,
        "trade_events": len(trades),
        **stats,
    }, warnings


def load_bot(path: Path, now: float, stale_after: float) -> tuple[dict[str, Any], list[str]]:
    data, warnings = _load_json(path)
    if not data:
        return {"side": "bot", "label": "Bot", "status": "unknown"}, warnings
    starting = _num(data.get("paper_starting_capital"), 10000.0) or 10000.0
    cash = _num(data.get("paper_cash"), starting) or starting
    positions = data.get("paper_positions", [])
    if not isinstance(positions, list):
        positions = []
        warnings.append("bot_positions_invalid")
    unrealized = 0.0
    marked = 0
    for position in positions:
        if not isinstance(position, dict):
            continue
        pnl, used_mark = _position_unrealized(position)
        unrealized += pnl
        marked += int(used_mark)
    if positions and marked < len(positions):
        warnings.append("bot_open_positions_missing_current_marks")
    equity = cash + unrealized
    realized = _num(data.get("paper_realized_pnl"), 0.0) or 0.0
    fees = _num(data.get("paper_fees_paid"), 0.0) or 0.0
    trades = data.get("paper_trades", data.get("trades", []))
    if not isinstance(trades, list):
        trades = []
    stats = _round_trip_stats(trades, ("pnl", "realized_pnl"))
    age = _age_seconds(path, now)
    fresh = age is not None and age <= stale_after
    if not fresh:
        warnings.append("bot_state_stale")
    gross_pnl = equity - starting
    return {
        "side": "bot",
        "label": str(data.get("label") or "EventTraderV4 Bot"),
        "status": "ok" if fresh else "stale",
        "source": str(path),
        "age_seconds": round(age, 3) if age is not None else None,
        "starting_capital_usd": round(starting, 6),
        "cash_usd": round(cash, 6),
        "unrealized_pnl_usd": round(unrealized, 6),
        "gross_equity_usd": round(equity, 6),
        "operating_cost_usd": 0.0,
        "net_equity_usd": round(equity, 6),
        "gross_pnl_usd": round(gross_pnl, 6),
        "net_pnl_usd": round(gross_pnl, 6),
        "gross_return_pct": round(gross_pnl / starting * 100.0, 6) if starting else None,
        "net_return_pct": round(gross_pnl / starting * 100.0, 6) if starting else None,
        "realized_pnl_usd": round(realized, 6),
        "fees_paid_usd": round(fees, 6),
        "max_drawdown_pct": _drawdown_pct(
            data.get("paper_equity_curve", data.get("equity_curve", [])),
            equity,
            data.get("paper_peak_equity"),
        ),
        "open_positions": len(positions),
        "positions_with_live_marks": marked,
        "trade_events": len(trades),
        **stats,
    }, warnings


def rank(agent: dict[str, Any], bot: dict[str, Any], warnings: list[str]) -> dict[str, Any]:
    comparable = (
        agent.get("status") == "ok"
        and bot.get("status") == "ok"
        and agent.get("net_equity_usd") is not None
        and bot.get("net_equity_usd") is not None
    )
    start_delta = abs((_num(agent.get("starting_capital_usd"), 0.0) or 0.0) - (_num(bot.get("starting_capital_usd"), 0.0) or 0.0))
    if start_delta > 0.01:
        comparable = False
        warnings.append("starting_capital_mismatch")
    if "bot_open_positions_missing_current_marks" in warnings:
        comparable = False
    agent_equity = _num(agent.get("net_equity_usd"), 0.0) or 0.0
    bot_equity = _num(bot.get("net_equity_usd"), 0.0) or 0.0
    delta = agent_equity - bot_equity
    leader = "agent" if delta > 0 else "bot" if delta < 0 else "tie"
    agent_cost = _num(agent.get("operating_cost_usd"), 0.0) or 0.0
    agent_gross_pnl = _num(agent.get("gross_pnl_usd"), 0.0) or 0.0
    coverage = None if agent_cost <= 0 else agent_gross_pnl / agent_cost
    return {
        "valid_for_ranking": comparable,
        "leader": leader if comparable else "unknown",
        "edge_usd": round(abs(delta), 6) if comparable else None,
        "agent_minus_bot_usd": round(delta, 6) if comparable else None,
        "agent_cost_coverage_ratio": round(coverage, 6) if coverage is not None else None,
        "agent_break_even_gap_usd": round(max(0.0, agent_cost - agent_gross_pnl), 6),
        "agent_alpha_after_cost_pct_points": (
            round((_num(agent.get("net_return_pct"), 0.0) or 0.0) - (_num(bot.get("net_return_pct"), 0.0) or 0.0), 6)
            if comparable else None
        ),
        "ranking_basis": "net_equity_after_agent_operating_costs",
    }


def build_state(
    agent_path: Path = DEFAULT_AGENT_LEDGER,
    bot_path: Path = DEFAULT_BOT_STATE,
    cost_path: Path = DEFAULT_COST_LEDGER,
    *,
    now: float | None = None,
    stale_after: float = DEFAULT_STALE_AFTER_SECONDS,
) -> dict[str, Any]:
    now = time.time() if now is None else now
    cost_payload, cost_warnings = _load_json(cost_path)
    warnings = [warning for warning in cost_warnings if not warning.startswith("missing:")]
    agent, agent_warnings = load_agent(agent_path, cost_payload, now, stale_after)
    bot, bot_warnings = load_bot(bot_path, now, stale_after)
    warnings.extend(agent_warnings)
    warnings.extend(bot_warnings)
    standings = rank(agent, bot, warnings)
    return {
        "schema_version": 2,
        "generated_at": datetime.fromtimestamp(now, tz=timezone.utc).isoformat(),
        "generated_epoch": now,
        "status": "ok" if standings["valid_for_ranking"] else "degraded",
        "competitors": {"agent": agent, "bot": bot},
        "standings": standings,
        "warnings": sorted(set(warnings)),
        "contracts": {
            "bot_equity": "paper_cash + marked_unrealized_pnl",
            "agent_score": "gross_equity - attributable_model_and_compute_cost",
            "leader": "higher_net_equity_after_costs",
        },
    }


def _atomic_write(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temp_name = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=path.parent)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, indent=2, sort_keys=True)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temp_name, path)
    finally:
        try:
            os.unlink(temp_name)
        except FileNotFoundError:
            pass


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--agent-ledger", type=Path, default=DEFAULT_AGENT_LEDGER)
    parser.add_argument("--bot-state", type=Path, default=DEFAULT_BOT_STATE)
    parser.add_argument("--cost-ledger", type=Path, default=DEFAULT_COST_LEDGER)
    parser.add_argument("--out", type=Path, default=DEFAULT_OUT)
    parser.add_argument("--stale-after", type=float, default=DEFAULT_STALE_AFTER_SECONDS)
    parser.add_argument("--print-json", action="store_true")
    args = parser.parse_args()

    state = build_state(
        args.agent_ledger,
        args.bot_state,
        args.cost_ledger,
        stale_after=max(1.0, args.stale_after),
    )
    _atomic_write(args.out, state)
    if args.print_json:
        print(json.dumps(state, indent=2, sort_keys=True))
    else:
        standings = state["standings"]
        print(
            f"competition={state['status']} "
            f"leader={standings['leader']} "
            f"edge_usd={standings['edge_usd']} "
            f"agent_cost_coverage={standings['agent_cost_coverage_ratio']} "
            f"out={args.out}"
        )


if __name__ == "__main__":
    main()
