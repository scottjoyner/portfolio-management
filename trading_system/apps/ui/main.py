from __future__ import annotations

import html
import json
import os
import urllib.error
import urllib.request
from typing import Any, Dict, Iterable, List

from fastapi import FastAPI
from fastapi.responses import HTMLResponse

app = FastAPI(title="Trading Network Dashboard")

API_BASE_URL = os.getenv("TRADING_API_URL", "http://trading-api:8000").rstrip("/")


def fetch_json(path: str) -> Dict[str, Any]:
    url = f"{API_BASE_URL}{path}"
    try:
        with urllib.request.urlopen(url, timeout=10) as response:
            return json.loads(response.read().decode("utf-8"))
    except (urllib.error.URLError, TimeoutError, json.JSONDecodeError) as exc:
        return {"error": str(exc), "url": url}


def esc(value: Any) -> str:
    return html.escape(str(value))


def badge(value: Any, truthy_label: str = "OK", falsey_label: str = "WARN") -> str:
    ok = bool(value)
    label = truthy_label if ok else falsey_label
    klass = "good" if ok else "warn"
    return f'<span class="badge {klass}">{esc(label)}</span>'


def render_kv(items: Dict[str, Any]) -> str:
    rows = []
    for key, value in items.items():
        rows.append(f"<tr><th>{esc(key)}</th><td>{esc(value)}</td></tr>")
    return "".join(rows)


def render_balances(accounts: Iterable[Dict[str, Any]], limit: int = 20) -> str:
    rows = []
    for account in list(accounts)[:limit]:
        rows.append(
            "<tr>"
            f"<td>{esc(account.get('currency', ''))}</td>"
            f"<td>{esc(account.get('available', ''))}</td>"
            f"<td>{esc(account.get('hold', '0'))}</td>"
            f"<td>{esc(account.get('name', ''))}</td>"
            "</tr>"
        )
    if not rows:
        rows.append('<tr><td colspan="4">No balances available</td></tr>')
    return "".join(rows)


def render_strategies(strategies: Iterable[Dict[str, Any]]) -> str:
    rows = []
    for strategy in strategies:
        rows.append(
            "<tr>"
            f"<td>{esc(strategy.get('name', strategy.get('strategy_id', '')))}</td>"
            f"<td>{esc(strategy.get('category', ''))}</td>"
            f"<td>{esc(strategy.get('status', 'unknown'))}</td>"
            f"<td>{esc(strategy.get('mode', 'paper'))}</td>"
            "</tr>"
        )
    if not rows:
        rows.append('<tr><td colspan="4">No strategies available</td></tr>')
    return "".join(rows)


def render_events(events: Iterable[Dict[str, Any]]) -> str:
    rows = []
    for event in events:
        rows.append(
            "<tr>"
            f"<td>{esc(event.get('timestamp', ''))}</td>"
            f"<td>{esc(event.get('source', ''))}</td>"
            f"<td>{esc(event.get('event_type', ''))}</td>"
            f"<td><code>{esc(json.dumps(event.get('payload', {}), sort_keys=True))}</code></td>"
            "</tr>"
        )
    if not rows:
        rows.append('<tr><td colspan="4">No events recorded</td></tr>')
    return "".join(rows)


@app.get("/health")
def health() -> dict:
    return {"status": "ok", "service": "trading-ui"}


@app.get("/", response_class=HTMLResponse)
def dashboard() -> HTMLResponse:
    runtime = fetch_json("/runtime/status")
    coinbase = fetch_json("/coinbase/status")
    balances = fetch_json("/coinbase/balances")
    strategies = fetch_json("/strategies/catalog")
    events = fetch_json("/events?limit=20")

    runtime_summary = {
        "mode": runtime.get("mode", "unknown"),
        "live_trading_enabled": runtime.get("live_trading_enabled", False),
        "coinbase_connected": runtime.get("coinbase_connected", False),
        "worker_status": runtime.get("worker_status", "unknown"),
        "event_log_status": runtime.get("event_log_status", "unknown"),
        "timestamp": runtime.get("timestamp", "unknown"),
    }
    coinbase_summary = {
        "connected": coinbase.get("connected", False),
        "environment": coinbase.get("environment", "unknown"),
        "account_count": coinbase.get("account_count", 0),
        "error": coinbase.get("error"),
    }

    account_rows = render_balances(balances.get("accounts", []))
    strategy_rows = render_strategies(strategies.get("strategies", []))
    event_rows = render_events(events.get("events", []))

    content = f"""
<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <meta http-equiv="refresh" content="15">
  <title>Trading Network Dashboard</title>
  <style>
    body {{ margin: 0; font-family: system-ui, -apple-system, Segoe UI, sans-serif; background: #0f172a; color: #e2e8f0; }}
    header {{ padding: 24px 32px; background: #111827; border-bottom: 1px solid #334155; }}
    h1 {{ margin: 0; font-size: 28px; }}
    h2 {{ margin-top: 0; font-size: 20px; color: #93c5fd; }}
    main {{ padding: 24px 32px; display: grid; gap: 20px; }}
    section {{ background: #111827; border: 1px solid #334155; border-radius: 12px; padding: 18px; box-shadow: 0 10px 30px rgba(0,0,0,.25); }}
    .grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(360px, 1fr)); gap: 20px; }}
    table {{ width: 100%; border-collapse: collapse; }}
    th, td {{ padding: 8px 10px; border-bottom: 1px solid #334155; text-align: left; vertical-align: top; }}
    th {{ color: #cbd5e1; font-weight: 600; }}
    code {{ white-space: pre-wrap; color: #bae6fd; }}
    .badge {{ display: inline-block; padding: 3px 8px; border-radius: 999px; font-size: 12px; font-weight: 700; }}
    .good {{ background: #064e3b; color: #a7f3d0; }}
    .warn {{ background: #713f12; color: #fde68a; }}
    .danger {{ background: #7f1d1d; color: #fecaca; }}
    .muted {{ color: #94a3b8; }}
  </style>
</head>
<body>
  <header>
    <h1>Trading Network Dashboard</h1>
    <div class="muted">Read-only operator UI. Auto-refreshes every 15 seconds. Live execution controls are intentionally absent.</div>
  </header>
  <main>
    <div class="grid">
      <section>
        <h2>Runtime Status {badge(runtime_summary.get('coinbase_connected'), 'CONNECTED', 'DISCONNECTED')}</h2>
        <table>{render_kv(runtime_summary)}</table>
      </section>
      <section>
        <h2>Coinbase Account {badge(coinbase_summary.get('connected'), 'CONNECTED', 'DISCONNECTED')}</h2>
        <table>{render_kv(coinbase_summary)}</table>
      </section>
    </div>
    <section>
      <h2>Balances</h2>
      <table><thead><tr><th>Asset</th><th>Available</th><th>Hold</th><th>Wallet</th></tr></thead><tbody>{account_rows}</tbody></table>
    </section>
    <section>
      <h2>Strategy Catalog</h2>
      <table><thead><tr><th>Name</th><th>Category</th><th>Status</th><th>Mode</th></tr></thead><tbody>{strategy_rows}</tbody></table>
    </section>
    <section>
      <h2>Recent Events</h2>
      <table><thead><tr><th>Timestamp</th><th>Source</th><th>Type</th><th>Payload</th></tr></thead><tbody>{event_rows}</tbody></table>
    </section>
  </main>
</body>
</html>
"""
    return HTMLResponse(content)
