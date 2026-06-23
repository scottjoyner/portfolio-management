from __future__ import annotations

import json
import subprocess
from pathlib import Path
from typing import Any

from .backtest import run_backtest_from_prices
from .data import fetch_ohlcv
from .report import write_html_report
from .safety import SafetyError, ensure_safe_shell, resolve_workspace_path
from .schemas import BacktestConfig


class ToolRuntime:
    def __init__(self, workspace: str | Path = "workspace") -> None:
        self.workspace = Path(workspace).resolve()
        self.workspace.mkdir(parents=True, exist_ok=True)
        (self.workspace / "reports").mkdir(exist_ok=True)

    def run_shell(self, command: str, timeout: int = 20) -> dict[str, Any]:
        ensure_safe_shell(command)
        result = subprocess.run(
            command,
            shell=True,
            text=True,
            capture_output=True,
            timeout=timeout,
            cwd=self.workspace,
        )
        return {
            "returncode": result.returncode,
            "stdout": result.stdout[-6000:],
            "stderr": result.stderr[-6000:],
        }

    def write_file(self, path: str, content: str) -> dict[str, Any]:
        target = resolve_workspace_path(self.workspace, path)
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(content, encoding="utf-8")
        return {"path": str(target), "bytes": target.stat().st_size}

    def read_file(self, path: str, max_chars: int = 8000) -> dict[str, Any]:
        target = resolve_workspace_path(self.workspace, path)
        text = target.read_text(encoding="utf-8")
        return {"path": str(target), "content": text[:max_chars], "truncated": len(text) > max_chars}

    def run_backtest(self, **kwargs) -> dict[str, Any]:
        cfg = BacktestConfig(**kwargs)
        output_path = resolve_workspace_path(self.workspace, cfg.output_html)
        cfg = BacktestConfig(**{**cfg.to_dict(), "output_html": str(output_path)})
        prices = fetch_ohlcv(cfg.ticker, cfg.start, cfg.end, cfg.interval)
        data, trades, summary = run_backtest_from_prices(prices, cfg)
        report_path = write_html_report(data, trades, summary, cfg, output_path)
        trades_csv = report_path.with_suffix(".trades.csv")
        data_csv = report_path.with_suffix(".data.csv")
        if trades:
            import pandas as pd
            pd.DataFrame([t.to_dict() for t in trades]).to_csv(trades_csv, index=False)
        else:
            trades_csv.write_text("ticker,entry_time,exit_time,entry_price,exit_price,shares,pnl,return_pct,exit_reason,bars_held\n", encoding="utf-8")
        data.to_csv(data_csv)
        return {
            "summary": summary.to_dict(),
            "num_rows": int(len(data)),
            "num_trades": int(len(trades)),
            "report_html": str(report_path),
            "trades_csv": str(trades_csv),
            "data_csv": str(data_csv),
        }

    def dispatch(self, action: str, args: dict[str, Any]) -> dict[str, Any]:
        try:
            if action == "run_shell":
                return self.run_shell(**args)
            if action == "write_file":
                return self.write_file(**args)
            if action == "read_file":
                return self.read_file(**args)
            if action == "run_backtest":
                return self.run_backtest(**args)
            raise SafetyError(f"Unknown tool action: {action}")
        except Exception as exc:
            return {"error": type(exc).__name__, "message": str(exc)}


TOOL_SPEC = {
    "actions": {
        "run_shell": {
            "description": "Run safe environment-audit commands only. No network, no deletion, no credentials, no trading APIs.",
            "args": {"command": "string", "timeout": "int optional"},
        },
        "run_backtest": {
            "description": "Fetch yfinance OHLCV data, run RSI cross strategy, generate HTML report plus CSV artifacts.",
            "args": BacktestConfig().to_dict(),
        },
        "write_file": {"description": "Write a workspace-local file.", "args": {"path": "string", "content": "string"}},
        "read_file": {"description": "Read a workspace-local file.", "args": {"path": "string", "max_chars": "int optional"}},
        "finish": {"description": "Return final answer to the user.", "args": {"message": "string"}},
    }
}


def tool_spec_json() -> str:
    return json.dumps(TOOL_SPEC, indent=2)
