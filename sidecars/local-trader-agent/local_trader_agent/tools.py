from __future__ import annotations

import json
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .backtest import run_backtest_from_prices
from .data import fetch_ohlcv
from .report import write_html_report
from .safety import SafetyError, parse_safe_shell, resolve_workspace_path
from .schemas import BacktestConfig


class ToolRuntime:
    def __init__(self, workspace: str | Path = "workspace") -> None:
        self.workspace = Path(workspace).resolve()
        self.workspace.mkdir(parents=True, exist_ok=True)
        (self.workspace / "reports").mkdir(exist_ok=True)
        run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        self.run_dir = self.workspace / "runs" / run_id
        self.run_dir.mkdir(parents=True, exist_ok=True)
        self.tool_log = self.run_dir / "tool_calls.jsonl"

    def _log(self, action: str, args: dict[str, Any], result: dict[str, Any]) -> None:
        record = {"ts": datetime.now(timezone.utc).isoformat(), "action": action, "args": args, "result": result}
        with self.tool_log.open("a", encoding="utf-8") as f:
            f.write(json.dumps(record, default=str) + "\n")

    def run_shell(self, command: str, timeout: int = 20) -> dict[str, Any]:
        argv = parse_safe_shell(command)
        result = subprocess.run(argv, shell=False, text=True, capture_output=True, timeout=timeout, cwd=self.workspace)
        return {"argv": argv, "returncode": result.returncode, "stdout": result.stdout[-6000:], "stderr": result.stderr[-6000:]}

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
        cache_dir = resolve_workspace_path(self.workspace, cfg.data_cache_dir)
        cfg = BacktestConfig(**{**cfg.to_dict(), "output_html": str(output_path), "data_cache_dir": str(cache_dir)})
        prices = fetch_ohlcv(
            cfg.ticker,
            cfg.start,
            cfg.end,
            cfg.interval,
            auto_adjust=cfg.price_adjustment == "auto_adjusted",
            cache=cfg.data_cache,
            refresh_cache=cfg.refresh_cache,
            cache_dir=cfg.data_cache_dir,
        )
        data, trades, summary = run_backtest_from_prices(prices, cfg)
        report_path = write_html_report(data, trades, summary, cfg, output_path)
        trades_csv = report_path.with_suffix(".trades.csv")
        data_csv = report_path.with_suffix(".data.csv")
        if trades:
            import pandas as pd
            pd.DataFrame([t.to_dict() for t in trades]).to_csv(trades_csv, index=False)
        else:
            trades_csv.write_text("ticker,entry_time,exit_time,entry_price,exit_price,shares,pnl,return_pct,exit_reason,bars_held,entry_fee,exit_fee,total_fees,mae_pct,mfe_pct\n", encoding="utf-8")
        data.to_csv(data_csv)
        manifest = {
            "created_at": datetime.now(timezone.utc).isoformat(),
            "config": cfg.to_dict(),
            "summary": summary.to_dict(),
            "report_html": str(report_path),
            "trades_csv": str(trades_csv),
            "data_csv": str(data_csv),
            "run_dir": str(self.run_dir),
        }
        manifest_path = report_path.with_suffix(".manifest.json")
        manifest_path.write_text(json.dumps(manifest, indent=2, default=str), encoding="utf-8")
        return {**manifest, "manifest_json": str(manifest_path), "num_rows": int(len(data)), "num_trades": int(len(trades))}

    def dispatch(self, action: str, args: dict[str, Any]) -> dict[str, Any]:
        try:
            if action == "run_shell":
                result = self.run_shell(**args)
            elif action == "write_file":
                result = self.write_file(**args)
            elif action == "read_file":
                result = self.read_file(**args)
            elif action == "run_backtest":
                result = self.run_backtest(**args)
            else:
                raise SafetyError(f"Unknown tool action: {action}")
        except Exception as exc:
            result = {"error": type(exc).__name__, "message": str(exc)}
        self._log(action, args, result)
        return result


TOOL_SPEC = {
    "actions": {
        "run_shell": {"description": "Run safe environment-audit commands only. No shell expansion, no network, no deletion, no credentials, no trading APIs.", "args": {"command": "string", "timeout": "int optional"}},
        "run_backtest": {"description": "Fetch/cache yfinance OHLCV data, run RSI cross strategy, generate offline HTML report plus CSV and manifest artifacts.", "args": BacktestConfig().to_dict()},
        "write_file": {"description": "Write a workspace-local file.", "args": {"path": "string", "content": "string"}},
        "read_file": {"description": "Read a workspace-local file.", "args": {"path": "string", "max_chars": "int optional"}},
        "finish": {"description": "Return final answer to the user.", "args": {"message": "string"}},
    }
}


def tool_spec_json() -> str:
    return json.dumps(TOOL_SPEC, indent=2)
