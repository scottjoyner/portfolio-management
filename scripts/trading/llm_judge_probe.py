#!/usr/bin/env python3
from __future__ import annotations

import argparse
import concurrent.futures as cf
import json
import os
import re
import sys
from dataclasses import dataclass
from typing import Any, Dict, List, Optional
from urllib import request

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
sys.path.insert(0, REPO_ROOT)


@dataclass
class JudgeEndpoint:
    name: str
    base_url: str
    model: str


def _json_loads_maybe(text: str) -> Dict[str, Any]:
    text = text.strip()
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        m = re.search(r"\{.*\}", text, flags=re.DOTALL)
        if not m:
            raise
        return json.loads(m.group(0))


def _fetch_status(status_url: str) -> Dict[str, Any]:
    with request.urlopen(status_url, timeout=10) as resp:
        return json.loads(resp.read().decode("utf-8"))


def _compact_status(status: Dict[str, Any]) -> Dict[str, Any]:
    paper = status.get("paper") or {}
    return {
        "mode": status.get("mode"),
        "status": status.get("status"),
        "health_ok": status.get("health_ok"),
        "alerts": status.get("alerts", []),
        "tick_count": status.get("tick_count"),
        "ws_connected": status.get("ws_connected"),
        "paper": {
            "equity": paper.get("equity"),
            "cash": paper.get("cash"),
            "drawdown": paper.get("drawdown"),
            "win_rate": paper.get("win_rate"),
            "trades": paper.get("trades"),
            "positions": paper.get("positions"),
        },
        "scans": {
            "minute": status.get("last_minute_scan"),
            "fast": status.get("last_scan"),
        },
        "guardrails": {
            "paper_product_cooldown_s": status.get("paper_product_cooldown_s"),
            "live_max_order_usd": status.get("execution_guards", {}).get("live_max_order_usd"),
            "live_min_cash_reserve_usd": status.get("execution_guards", {}).get("live_min_cash_reserve_usd"),
        },
    }


def _build_json_prompt(status: Dict[str, Any]) -> List[Dict[str, str]]:
    compact = _compact_status(status)
    system = (
        "You are an advisory-only live trading risk reviewer. "
        "Return strict JSON with keys vote, confidence, reason, risks, action_items. "
        "Vote must be one of continue, warn, stop, flatten_only. "
        "Do not suggest trades or invent status."
    )
    user = json.dumps({"status": compact, "instruction": "Review for execution safety and give a conservative vote."})
    return [{"role": "system", "content": system}, {"role": "user", "content": user}]


def _build_verdict_prompt(status: Dict[str, Any]) -> List[Dict[str, str]]:
    paper = _compact_status(status).get("paper", {})
    brief = (
        f"health_ok={status.get('health_ok')} alerts={len(status.get('alerts', []))} "
        f"drawdown={paper.get('drawdown')} win_rate={paper.get('win_rate')} "
        f"trades={paper.get('trades')} positions={paper.get('positions')}"
    )
    system = (
        "You are an advisory-only live trading risk reviewer. "
        "Reply in one line only: vote=<continue|warn|stop|flatten_only> confidence=<0-1> reason=<short>. "
        "No JSON, no extra text."
    )
    user = f"Status: {brief}."
    return [{"role": "system", "content": system}, {"role": "user", "content": user}]


def _parse_verdict_text(text: str) -> Dict[str, Any]:
    vote = "warn"
    confidence = 0.5
    reason = text.strip()
    for part in re.split(r"\s+", text.strip()):
        if part.startswith("vote="):
            vote = part.split("=", 1)[1].strip().lower()
        elif part.startswith("confidence="):
            try:
                confidence = float(part.split("=", 1)[1].strip())
            except ValueError:
                pass
        elif part.startswith("reason="):
            reason = part.split("=", 1)[1].strip()
    if vote not in {"continue", "warn", "stop", "flatten_only"}:
        vote = "warn"
    return {
        "vote": vote,
        "confidence": confidence,
        "reason": reason,
        "risks": [],
        "action_items": [],
    }


def _call_judge(judge: JudgeEndpoint, status: Dict[str, Any], timeout_s: int, max_tokens: int) -> Dict[str, Any]:
    is_fast = "vibe" in judge.name.lower()
    payload = {
        "model": judge.model,
        "messages": _build_verdict_prompt(status),
        "temperature": 0.1,
        "max_tokens": max_tokens,
        "stream": False,
    }
    url = judge.base_url.rstrip("/") + "/chat/completions"
    req = request.Request(url, data=json.dumps(payload).encode("utf-8"), headers={"Content-Type": "application/json"})
    with request.urlopen(req, timeout=timeout_s) as resp:
        data = json.loads(resp.read().decode("utf-8"))
    message = data["choices"][0]["message"]
    content = (message.get("content") or "").strip()
    reasoning = (message.get("reasoning_content") or "").strip()
    blob = content or reasoning
    parsed = _parse_verdict_text(blob) if is_fast else _json_loads_maybe(blob)
    return {
        "judge": judge.name,
        "endpoint": judge.base_url,
        "model": judge.model,
        "raw": parsed,
        "usage": data.get("usage", {}),
    }


def _parse_judge(raw: str) -> JudgeEndpoint:
    # format: name=http://host:port/v1:model
    name, rest = raw.split("=", 1)
    base_url, model = rest.rsplit(":", 1)
    return JudgeEndpoint(name=name.strip(), base_url=base_url.strip(), model=model.strip())


def main() -> int:
    ap = argparse.ArgumentParser(description="Probe one or more local OpenAI-compatible judge endpoints")
    ap.add_argument("--status-url", default="http://localhost:9090/health")
    ap.add_argument("--judge", action="append", default=[], help="name=http://host:port/v1:model")
    ap.add_argument("--max-tokens", type=int, default=1200)
    ap.add_argument("--timeout-seconds", type=int, default=60)
    args = ap.parse_args()

    judges = [_parse_judge(j) for j in args.judge] if args.judge else [
        _parse_judge("orinth=http://127.0.0.1:1234/v1:orinth-1.0-35b"),
        _parse_judge("vibethinker=http://deathstar-xps-8920.tailcb8954.ts.net:1234/v1:vibethinker-3b"),
    ]
    status = _fetch_status(args.status_url)

    results: List[Dict[str, Any]] = []
    with cf.ThreadPoolExecutor(max_workers=len(judges) or 1) as pool:
        futs = [pool.submit(_call_judge, j, status, args.timeout_seconds, args.max_tokens) for j in judges]
        for fut in cf.as_completed(futs):
            try:
                results.append(fut.result())
            except Exception as exc:
                results.append({"error": str(exc)})

    print(json.dumps({"status": status, "judges": results}, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
