from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
from typing import Optional

import yaml

from .agent import LLMConfig, LocalLLMClient, ResearchAgent
from .backtest import run_backtest_from_prices
from .data import fetch_ohlcv
from .report import write_html_report
from .schemas import BacktestConfig
from .tools import ToolRuntime


def _load_yaml(path: Optional[str]) -> dict:
    if not path:
        return {}
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def _build_backtest_config(args: argparse.Namespace, file_cfg: dict) -> BacktestConfig:
    cfg = dict(file_cfg.get("backtest", {}))
    for key in [
        "ticker", "start", "end", "interval", "rsi_period", "buy_rsi_cross", "take_profit_pct",
        "stop_loss_pct", "initial_cash", "position_size_pct", "same_bar_policy", "output_html",
    ]:
        value = getattr(args, key, None)
        if value is not None:
            cfg[key] = value
    return BacktestConfig(**cfg)


def cmd_backtest(args: argparse.Namespace) -> int:
    file_cfg = _load_yaml(args.config)
    cfg = _build_backtest_config(args, file_cfg)
    prices = fetch_ohlcv(cfg.ticker, cfg.start, cfg.end, cfg.interval)
    data, trades, summary = run_backtest_from_prices(prices, cfg)
    report_path = write_html_report(data, trades, summary, cfg, cfg.output_html)

    print(json.dumps({
        "summary": summary.to_dict(),
        "report_html": str(Path(report_path).resolve()),
        "num_rows": len(data),
        "num_trades": len(trades),
    }, indent=2))
    return 0


def cmd_agent(args: argparse.Namespace) -> int:
    file_cfg = _load_yaml(args.config)
    llm_cfg_dict = dict(file_cfg.get("llm", {}))
    llm_cfg = LLMConfig(
        base_url=args.base_url or llm_cfg_dict.get("base_url") or os.getenv("LTA_LLM_BASE_URL", "http://127.0.0.1:8080/v1"),
        model=args.model or llm_cfg_dict.get("model") or os.getenv("LTA_LLM_MODEL", "local-model"),
        temperature=float(args.temperature if args.temperature is not None else llm_cfg_dict.get("temperature", 0.1)),
        max_tokens=int(args.max_tokens if args.max_tokens is not None else llm_cfg_dict.get("max_tokens", 2048)),
    )
    runtime = ToolRuntime(args.workspace)
    agent = ResearchAgent(LocalLLMClient(llm_cfg), runtime, max_steps=args.max_steps)
    result = agent.run(args.task)
    print(json.dumps(result, indent=2, default=str))
    return 0 if result["status"] == "finished" else 2


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Local trading research agent and RSI backtester")
    sub = parser.add_subparsers(dest="command", required=True)

    backtest = sub.add_parser("backtest", help="Run deterministic RSI backtest and write HTML report")
    backtest.add_argument("--config")
    backtest.add_argument("--ticker", default=None)
    backtest.add_argument("--start", default=None)
    backtest.add_argument("--end", default=None)
    backtest.add_argument("--interval", default=None)
    backtest.add_argument("--rsi-period", type=int, dest="rsi_period")
    backtest.add_argument("--buy-rsi-cross", type=float, dest="buy_rsi_cross")
    backtest.add_argument("--take-profit-pct", type=float, dest="take_profit_pct")
    backtest.add_argument("--stop-loss-pct", type=float, dest="stop_loss_pct")
    backtest.add_argument("--initial-cash", type=float, dest="initial_cash")
    backtest.add_argument("--position-size-pct", type=float, dest="position_size_pct")
    backtest.add_argument("--same-bar-policy", choices=["stop_first", "take_profit_first", "close"], dest="same_bar_policy")
    backtest.add_argument("--output-html", dest="output_html")
    backtest.set_defaults(func=cmd_backtest)

    agent = sub.add_parser("agent", help="Run local LLM agent loop against llama.cpp-compatible endpoint")
    agent.add_argument("--config")
    agent.add_argument("--base-url")
    agent.add_argument("--model")
    agent.add_argument("--temperature", type=float)
    agent.add_argument("--max-tokens", type=int)
    agent.add_argument("--workspace", default=os.getenv("LTA_WORKSPACE", "workspace"))
    agent.add_argument("--max-steps", type=int, default=12)
    agent.add_argument("task", help="Natural-language task for the local model")
    agent.set_defaults(func=cmd_agent)

    return parser


def main(argv: Optional[list[str]] = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    return args.func(args)
