#!/usr/bin/env python3
"""Build the authoritative bot-versus-agent competition snapshot.

A winner is declared only inside an explicit shared competition epoch. The
agent and bot may have different raw lifetime balances when the epoch begins;
each is normalized to the same starting capital and scored only on performance
since the baseline. Agent model/compute costs are also baseline-adjusted.
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
DEFAULT_EPOCH = REPO / "data" / "competition_epoch.json"
DEFAULT_OUT = REPO / "data" / "competition_state.json"
DEFAULT_STALE_AFTER_SECONDS = 900.0
REQUIRED_AGENT_ACCOUNTING_VERSION = 2


def _num(value: Any, default: float | None = 0.0) -> float | None:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return default
    return result if math.isfinite(result) else default


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


def _timestamp(value: Any) -> float | None:
    numeric = _num(value, None)
    if numeric is not None:
        return numeric
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00")).timestamp()
    except ValueError:
        return None


def _drawdown_pct(curve: Iterable[Any], current: float, recorded_peak: Any = None) -> float:
    values = [_num(value, None) for value in curve]
    clean = [value for value in values if value is not None]
    peak = max(clean, default=_num(recorded_peak, current) or current)
    running_peak = clean[0] if clean else peak
    maximum = 0.0
    for value in clean:
        running_peak = max(running_peak, value)
        if running_peak > 0:
            maximum = max(maximum, (running_peak - value) / running_peak * 100.0)
    if peak > 0:
        maximum = max(maximum, (peak - current) / peak * 100.0)
    return round(maximum, 4)


def _post_epoch_trades(trades: Iterable[Any], epoch_started: float) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for trade in trades:
        if not isinstance(trade, dict):
            continue
        ts = _timestamp(trade.get("ts", trade.get("timestamp", trade.get("created_at"))))
        if ts is not None and ts >= epoch_started:
            rows.append(trade)
    return rows


def _round_trip_stats(trades: Iterable[Any], pnl_keys: tuple[str, ...]) -> dict[str, Any]:
    pnls: list[float] = []
    fees = 0.0
    for trade in trades:
        if not isinstance(trade, dict):
            continue
        fees += _num(trade.get("commission"), 0.0) or _num(trade.get("fee"), 0.0) or 0.0
        for key in pnl_keys:
            if key in trade:
                pnl = _num(trade.get(key), None)
                if pnl is not None:
                    pnls.append(pnl)
                break
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
        "profit_factor": (
            round(gross_profit / gross_loss, 6)
            if gross_loss > 0
            else (None if not wins else float("inf"))
        ),
        "avg_win_usd": round(gross_profit / len(wins), 6) if wins else None,
        "avg_loss_usd": round(gross_loss / len(losses), 6) if losses else None,
        "fees_from_trades_usd": round(fees, 6),
    }


def _position_unrealized(position: dict[str, Any]) -> tuple[float, bool]:
    entry = _num(position.get("entry_price"), None)
    quantity = _num(position.get("qty", position.get("quantity", position.get("base"))), None)
    mark = _num(
        position.get("mark_price", position.get("current_price", position.get("last_price"))),
        None,
    )
    if entry is None or quantity is None or mark is None:
        return 0.0, False
    side = str(position.get("side", "LONG")).upper()
    pnl = (mark - entry) * quantity
    if side in {"SHORT", "SELL"}:
        pnl = -pnl
    funding = _num(position.get("cum_funding"), 0.0) or 0.0
    return pnl - funding, True


def load_agent_cost(cost_payload: dict[str, Any], agent_ledger: dict[str, Any]) -> tuple[float | None, str]:
    rows = cost_payload.get("costs", cost_payload.get("agentCostLedger", []))
    if isinstance(rows, list) and rows:
        total = 0.0
        matched = 0
        for row in rows:
            if not isinstance(row, dict):
                continue
            agent_id = str(row.get("agentId", row.get("agent_id", ""))).lower()
            if agent_id and not any(token in agent_id for token in ("agent", "hermes", "trader", "openrouter")):
                continue
            total += _num(row.get("remoteApiCost", row.get("remote_api_cost")), 0.0) or 0.0
            total += _num(row.get("localComputeCost", row.get("local_compute_cost")), 0.0) or 0.0
            matched += 1
        if matched:
            return round(total, 6), "agent_cost_ledger_rows"

    summary = cost_payload.get("summary") if isinstance(cost_payload.get("summary"), dict) else {}
    summary_value = _num(summary.get("spentTodayUsd"), None)
    if summary_value is not None:
        return round(summary_value, 6), "agent_cost_summary"

    explicit_keys = ("api_cost_usd", "model_cost_usd", "openrouter_cost_usd")
    if any(key in agent_ledger for key in explicit_keys):
        total = sum(_num(agent_ledger.get(key), 0.0) or 0.0 for key in explicit_keys)
        return round(total, 6), "agent_ledger_explicit_cost"

    trade_rows = agent_ledger.get("trades", [])
    if any(isinstance(trade, dict) and "api_cost_usd" in trade for trade in trade_rows):
        total = sum(
            _num(trade.get("api_cost_usd"), 0.0) or 0.0
            for trade in trade_rows
            if isinstance(trade, dict)
        )
        return round(total, 6), "agent_trade_attributed_cost"
    return None, "unavailable"


def load_agent_book(path: Path, now: float, stale_after: float) -> tuple[dict[str, Any], list[str]]:
    data, warnings = _load_json(path)
    if not data:
        return {"status": "unknown", "source": str(path)}, warnings
    accounting_version = int(_num(data.get("accounting_version"), 0.0) or 0)
    ranking_eligible = data.get("ranking_eligible") is True
    if accounting_version != REQUIRED_AGENT_ACCOUNTING_VERSION:
        warnings.append("agent_accounting_version_invalid")
    if not ranking_eligible:
        warnings.append("agent_history_not_ranking_eligible")
    equity = _num(data.get("equity"), None)
    if equity is None:
        warnings.append("agent_equity_missing")
    age = _age_seconds(path, now)
    fresh = age is not None and age <= stale_after
    if not fresh:
        warnings.append("agent_ledger_stale")
    positions = data.get("positions") if isinstance(data.get("positions"), dict) else {}
    return {
        "status": "ok" if fresh and equity is not None and accounting_version == 2 and ranking_eligible else "degraded",
        "source": str(path),
        "age_seconds": round(age, 3) if age is not None else None,
        "accounting_version": accounting_version,
        "ranking_eligible": ranking_eligible,
        "history_valid_from": data.get("history_valid_from"),
        "raw_equity_usd": equity,
        "raw_realized_pnl_usd": _num(data.get("realized_pnl"), 0.0) or 0.0,
        "raw_peak_equity_usd": _num(data.get("peak_equity"), equity),
        "raw_equity_curve": data.get("equity_curve", []),
        "positions": positions,
        "trades": data.get("trades", []) if isinstance(data.get("trades"), list) else [],
        "payload": data,
    }, warnings


def load_bot_book(path: Path, now: float, stale_after: float) -> tuple[dict[str, Any], list[str]]:
    data, warnings = _load_json(path)
    if not data:
        return {"status": "unknown", "source": str(path)}, warnings
    cash = _num(data.get("paper_cash"), None)
    if cash is None:
        warnings.append("bot_cash_missing")
    positions = data.get("paper_positions", [])
    if not isinstance(positions, list):
        positions = []
        warnings.append("bot_positions_invalid")
    unrealized = 0.0
    marked = 0
    for position in positions:
        if isinstance(position, dict):
            pnl, has_mark = _position_unrealized(position)
            unrealized += pnl
            marked += int(has_mark)
    if positions and marked < len(positions):
        warnings.append("bot_open_positions_missing_current_marks")
    equity = None if cash is None else cash + unrealized
    age = _age_seconds(path, now)
    fresh = age is not None and age <= stale_after
    if not fresh:
        warnings.append("bot_state_stale")
    return {
        "status": "ok" if fresh and equity is not None and marked == len(positions) else "degraded",
        "source": str(path),
        "age_seconds": round(age, 3) if age is not None else None,
        "raw_equity_usd": equity,
        "raw_cash_usd": cash,
        "raw_unrealized_pnl_usd": unrealized,
        "raw_realized_pnl_usd": _num(data.get("paper_realized_pnl"), 0.0) or 0.0,
        "raw_fees_paid_usd": _num(data.get("paper_fees_paid"), 0.0) or 0.0,
        "raw_peak_equity_usd": _num(data.get("paper_peak_equity"), equity),
        "raw_equity_curve": data.get("paper_equity_curve", data.get("equity_curve", [])),
        "positions": positions,
        "positions_with_marks": marked,
        "trades": data.get("paper_trades", data.get("trades", [])) if isinstance(data.get("paper_trades", data.get("trades", [])), list) else [],
        "payload": data,
    }, warnings


def _load_epoch(path: Path) -> tuple[dict[str, Any], list[str]]:
    epoch, raw_warnings = _load_json(path)
    if not epoch:
        return {}, ["competition_epoch_missing"]
    warnings: list[str] = []
    if int(_num(epoch.get("schema_version"), 0.0) or 0) != 1:
        warnings.append("competition_epoch_schema_invalid")
    if not epoch.get("epoch_id"):
        warnings.append("competition_epoch_id_missing")
    if _timestamp(epoch.get("started_at", epoch.get("started_epoch"))) is None:
        warnings.append("competition_epoch_start_invalid")
    baselines = epoch.get("baselines") if isinstance(epoch.get("baselines"), dict) else {}
    required = ("agent_raw_equity_usd", "bot_raw_equity_usd", "agent_cost_usd")
    if any(_num(baselines.get(key), None) is None for key in required):
        warnings.append("competition_epoch_baseline_invalid")
    if _num(epoch.get("normalized_starting_capital_usd"), None) is None:
        warnings.append("competition_epoch_normalized_capital_invalid")
    return epoch, warnings or raw_warnings


def _normalize_competitors(
    agent_book: dict[str, Any],
    bot_book: dict[str, Any],
    epoch: dict[str, Any],
    current_cost: float | None,
    cost_source: str,
    warnings: list[str],
) -> tuple[dict[str, Any], dict[str, Any]]:
    baselines = epoch.get("baselines", {})
    start = _num(epoch.get("normalized_starting_capital_usd"), 10_000.0) or 10_000.0
    epoch_started = _timestamp(epoch.get("started_at", epoch.get("started_epoch"))) or 0.0
    epoch_history = epoch.get("agent_history_valid_from")
    if epoch_history and agent_book.get("history_valid_from") != epoch_history:
        warnings.append("agent_history_epoch_mismatch")

    if current_cost is None:
        warnings.append("agent_cost_unavailable")
        current_cost = 0.0
    baseline_cost = _num(baselines.get("agent_cost_usd"), 0.0) or 0.0
    if current_cost + 1e-8 < baseline_cost:
        warnings.append("agent_cost_counter_regressed")
    epoch_cost = max(0.0, current_cost - baseline_cost)

    agent_raw = _num(agent_book.get("raw_equity_usd"), None)
    bot_raw = _num(bot_book.get("raw_equity_usd"), None)
    agent_base = _num(baselines.get("agent_raw_equity_usd"), None)
    bot_base = _num(baselines.get("bot_raw_equity_usd"), None)
    agent_delta = None if agent_raw is None or agent_base is None else agent_raw - agent_base
    bot_delta = None if bot_raw is None or bot_base is None else bot_raw - bot_base
    agent_gross = None if agent_delta is None else start + agent_delta
    bot_gross = None if bot_delta is None else start + bot_delta
    agent_net = None if agent_gross is None else agent_gross - epoch_cost

    agent_trades = _post_epoch_trades(agent_book.get("trades", []), epoch_started)
    bot_trades = _post_epoch_trades(bot_book.get("trades", []), epoch_started)
    agent_stats = _round_trip_stats(agent_trades, ("realized_pnl", "pnl"))
    bot_stats = _round_trip_stats(bot_trades, ("pnl", "realized_pnl"))

    agent = {
        "side": "agent",
        "label": str(agent_book.get("payload", {}).get("label") or "OpenRouter Agent"),
        "status": agent_book.get("status", "unknown"),
        "source": agent_book.get("source"),
        "age_seconds": agent_book.get("age_seconds"),
        "accounting_version": agent_book.get("accounting_version"),
        "ranking_eligible": agent_book.get("ranking_eligible") is True,
        "history_valid_from": agent_book.get("history_valid_from"),
        "epoch_id": epoch.get("epoch_id"),
        "starting_capital_usd": round(start, 6),
        "raw_lifetime_equity_usd": None if agent_raw is None else round(agent_raw, 6),
        "epoch_baseline_equity_usd": None if agent_base is None else round(agent_base, 6),
        "gross_equity_usd": None if agent_gross is None else round(agent_gross, 6),
        "operating_cost_usd": round(epoch_cost, 6),
        "cost_source": cost_source,
        "net_equity_usd": None if agent_net is None else round(agent_net, 6),
        "gross_pnl_usd": None if agent_delta is None else round(agent_delta, 6),
        "net_pnl_usd": None if agent_delta is None else round(agent_delta - epoch_cost, 6),
        "gross_return_pct": None if agent_delta is None else round(agent_delta / start * 100.0, 6),
        "net_return_pct": None if agent_delta is None else round((agent_delta - epoch_cost) / start * 100.0, 6),
        "realized_pnl_usd": round(
            (_num(agent_book.get("raw_realized_pnl_usd"), 0.0) or 0.0)
            - (_num(baselines.get("agent_realized_pnl_usd"), 0.0) or 0.0),
            6,
        ),
        "max_drawdown_pct": _drawdown_pct(
            [start + ((_num(value, agent_base) or agent_base) - agent_base) for value in agent_book.get("raw_equity_curve", []) if agent_base is not None],
            agent_gross if agent_gross is not None else start,
        ),
        "open_positions": len(agent_book.get("positions", {})),
        "trade_events": len(agent_trades),
        **agent_stats,
    }
    bot = {
        "side": "bot",
        "label": str(bot_book.get("payload", {}).get("label") or "EventTraderV4 Bot"),
        "status": bot_book.get("status", "unknown"),
        "source": bot_book.get("source"),
        "age_seconds": bot_book.get("age_seconds"),
        "epoch_id": epoch.get("epoch_id"),
        "starting_capital_usd": round(start, 6),
        "raw_lifetime_equity_usd": None if bot_raw is None else round(bot_raw, 6),
        "epoch_baseline_equity_usd": None if bot_base is None else round(bot_base, 6),
        "gross_equity_usd": None if bot_gross is None else round(bot_gross, 6),
        "operating_cost_usd": 0.0,
        "net_equity_usd": None if bot_gross is None else round(bot_gross, 6),
        "gross_pnl_usd": None if bot_delta is None else round(bot_delta, 6),
        "net_pnl_usd": None if bot_delta is None else round(bot_delta, 6),
        "gross_return_pct": None if bot_delta is None else round(bot_delta / start * 100.0, 6),
        "net_return_pct": None if bot_delta is None else round(bot_delta / start * 100.0, 6),
        "realized_pnl_usd": round(
            (_num(bot_book.get("raw_realized_pnl_usd"), 0.0) or 0.0)
            - (_num(baselines.get("bot_realized_pnl_usd"), 0.0) or 0.0),
            6,
        ),
        "unrealized_pnl_usd": round(_num(bot_book.get("raw_unrealized_pnl_usd"), 0.0) or 0.0, 6),
        "fees_paid_usd": round(
            (_num(bot_book.get("raw_fees_paid_usd"), 0.0) or 0.0)
            - (_num(baselines.get("bot_fees_paid_usd"), 0.0) or 0.0),
            6,
        ),
        "max_drawdown_pct": _drawdown_pct(
            [start + ((_num(value, bot_base) or bot_base) - bot_base) for value in bot_book.get("raw_equity_curve", []) if bot_base is not None],
            bot_gross if bot_gross is not None else start,
        ),
        "open_positions": len(bot_book.get("positions", [])),
        "positions_with_live_marks": bot_book.get("positions_with_marks", 0),
        "trade_events": len(bot_trades),
        **bot_stats,
    }
    return agent, bot


def rank(agent: dict[str, Any], bot: dict[str, Any], warnings: list[str], epoch: dict[str, Any]) -> dict[str, Any]:
    fatal_prefixes = (
        "competition_epoch_",
        "agent_accounting_",
        "agent_history_",
        "agent_ledger_",
        "agent_equity_",
        "agent_cost_",
        "bot_state_",
        "bot_cash_",
        "bot_positions_",
        "bot_open_positions_",
        "missing:",
        "invalid:",
    )
    fatal = any(warning.startswith(fatal_prefixes) for warning in warnings)
    comparable = bool(
        not fatal
        and epoch.get("epoch_id")
        and agent.get("status") == "ok"
        and bot.get("status") == "ok"
        and agent.get("accounting_version") == REQUIRED_AGENT_ACCOUNTING_VERSION
        and agent.get("ranking_eligible") is True
        and agent.get("epoch_id") == bot.get("epoch_id") == epoch.get("epoch_id")
        and agent.get("net_equity_usd") is not None
        and bot.get("net_equity_usd") is not None
    )
    agent_equity = _num(agent.get("net_equity_usd"), 0.0) or 0.0
    bot_equity = _num(bot.get("net_equity_usd"), 0.0) or 0.0
    delta = agent_equity - bot_equity
    cost = _num(agent.get("operating_cost_usd"), 0.0) or 0.0
    gross_pnl = _num(agent.get("gross_pnl_usd"), 0.0) or 0.0
    coverage = gross_pnl / cost if cost > 0 else None
    return {
        "valid_for_ranking": comparable,
        "leader": "unknown" if not comparable else "agent" if delta > 0 else "bot" if delta < 0 else "tie",
        "edge_usd": round(abs(delta), 6) if comparable else None,
        "agent_minus_bot_usd": round(delta, 6) if comparable else None,
        "agent_cost_coverage_ratio": round(coverage, 6) if coverage is not None else None,
        "agent_break_even_gap_usd": round(max(0.0, cost - gross_pnl), 6),
        "agent_alpha_after_cost_pct_points": (
            round((_num(agent.get("net_return_pct"), 0.0) or 0.0) - (_num(bot.get("net_return_pct"), 0.0) or 0.0), 6)
            if comparable else None
        ),
        "ranking_basis": "shared_epoch_net_equity_after_agent_operating_costs",
        "epoch_id": epoch.get("epoch_id"),
        "required_agent_accounting_version": REQUIRED_AGENT_ACCOUNTING_VERSION,
    }


def build_state(
    agent_path: Path = DEFAULT_AGENT_LEDGER,
    bot_path: Path = DEFAULT_BOT_STATE,
    cost_path: Path = DEFAULT_COST_LEDGER,
    *,
    epoch_path: Path = DEFAULT_EPOCH,
    now: float | None = None,
    stale_after: float = DEFAULT_STALE_AFTER_SECONDS,
) -> dict[str, Any]:
    now = time.time() if now is None else now
    epoch, epoch_warnings = _load_epoch(epoch_path)
    cost_payload, cost_warnings = _load_json(cost_path)
    agent_book, agent_warnings = load_agent_book(agent_path, now, stale_after)
    bot_book, bot_warnings = load_bot_book(bot_path, now, stale_after)
    current_cost, cost_source = load_agent_cost(cost_payload, agent_book.get("payload", {}))
    warnings = [
        *epoch_warnings,
        *[warning for warning in cost_warnings if not warning.startswith("missing:")],
        *agent_warnings,
        *bot_warnings,
    ]
    agent, bot = _normalize_competitors(
        agent_book, bot_book, epoch, current_cost, cost_source, warnings
    )
    standings = rank(agent, bot, warnings, epoch)
    return {
        "schema_version": 3,
        "generated_at": datetime.fromtimestamp(now, tz=timezone.utc).isoformat(),
        "generated_epoch": now,
        "status": "ok" if standings["valid_for_ranking"] else "degraded",
        "epoch": {
            "epoch_id": epoch.get("epoch_id"),
            "started_at": epoch.get("started_at"),
            "normalized_starting_capital_usd": epoch.get("normalized_starting_capital_usd"),
            "source": str(epoch_path),
        },
        "competitors": {"agent": agent, "bot": bot},
        "standings": standings,
        "warnings": sorted(set(warnings)),
        "contracts": {
            "bot_raw_equity": "paper_cash + marked_unrealized_pnl",
            "epoch_normalization": "common_start + current_raw_equity - epoch_raw_equity_baseline",
            "agent_score": "normalized_gross_equity - post_epoch_attributable_model_and_compute_cost",
            "agent_accounting": "v2_margin_notional_quantity_leverage_once",
            "leader": "higher_shared_epoch_net_equity_after_costs",
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
    parser.add_argument("--epoch", type=Path, default=DEFAULT_EPOCH)
    parser.add_argument("--out", type=Path, default=DEFAULT_OUT)
    parser.add_argument("--stale-after", type=float, default=DEFAULT_STALE_AFTER_SECONDS)
    parser.add_argument("--print-json", action="store_true")
    args = parser.parse_args()
    state = build_state(
        args.agent_ledger,
        args.bot_state,
        args.cost_ledger,
        epoch_path=args.epoch,
        stale_after=max(1.0, args.stale_after),
    )
    _atomic_write(args.out, state)
    if args.print_json:
        print(json.dumps(state, indent=2, sort_keys=True))
    else:
        standings = state["standings"]
        print(
            f"competition={state['status']} epoch={standings['epoch_id']} "
            f"leader={standings['leader']} edge_usd={standings['edge_usd']} "
            f"agent_cost_coverage={standings['agent_cost_coverage_ratio']} out={args.out}"
        )


if __name__ == "__main__":
    main()
