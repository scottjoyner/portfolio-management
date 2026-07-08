#!/usr/bin/env python3
from __future__ import annotations

import argparse
import concurrent.futures as cf
import json
import os
import re
import sys
import time
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Any, Dict, List
from urllib import request

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT))

_STARTUP_TS: float = time.time()

# Watchdog state (module-level, persisted across cycles)
_last_flatten_ts: float = 0.0
_consecutive_llm_failures: int = 0
_circuit_breaker_until: float = 0.0


@dataclass
class JudgeConfig:
    name: str
    base_url: str
    model: str = ""
    max_tokens: int = 80
    timeout_s: int = 45
    role: str = "primary"
    resolved_model: str = ""


def _fetch_status(status_url: str) -> Dict[str, Any]:
    with request.urlopen(status_url, timeout=10) as resp:
        return json.loads(resp.read().decode("utf-8"))


def _compact_status(status: Dict[str, Any]) -> Dict[str, Any]:
    paper = status.get("paper") or {}
    pulses = status.get("pulses") or {}
    return {
        "mode": status.get("mode"),
        "status": status.get("status"),
        "health_ok": status.get("health_ok"),
        "alerts": status.get("alerts", []),
        "tick_count": status.get("tick_count"),
        "ws_connected": status.get("ws_connected"),
        "last_ticker_age_s": max(0.0, time.time() - float(status.get("last_ticker_ts") or 0.0)) if status.get("last_ticker_ts") else None,
        "last_scan_age_s": max(0.0, time.time() - float(status.get("last_scan_ts") or 0.0)) if status.get("last_scan_ts") else None,
        "last_minute_scan_age_s": max(0.0, time.time() - float(status.get("last_minute_scan_ts") or 0.0)) if status.get("last_minute_scan_ts") else None,
        "paper": {
            "equity": paper.get("equity"),
            "cash": paper.get("cash"),
            "drawdown": paper.get("drawdown"),
            "win_rate": paper.get("win_rate"),
            "trades": paper.get("trades"),
            "positions": paper.get("positions"),
        },
        "pulses": {
            "hot_count": pulses.get("hot_count", 0),
            "total": pulses.get("total", 0),
        },
        "last_minute_scan": status.get("last_minute_scan"),
        "last_scan": status.get("last_scan"),
        "guardrails": {
            "paper_product_cooldown_s": status.get("paper_product_cooldown_s"),
            "live_max_order_usd": status.get("execution_guards", {}).get("live_max_order_usd"),
            "live_min_cash_reserve_usd": status.get("execution_guards", {}).get("live_min_cash_reserve_usd"),
            "live_max_open_positions": status.get("execution_guards", {}).get("live_max_open_positions"),
        },
    }


def _status_brief(status: Dict[str, Any]) -> str:
    compact = _compact_status(status)
    paper = compact.get("paper", {})
    scans = compact.get("last_scan") or {}
    minute = compact.get("last_minute_scan") or {}
    pulses = compact.get("pulses", {})
    return (
        f"health_ok={compact.get('health_ok')} alerts={len(compact.get('alerts', []))} "
        f"last_ticker_age_s={compact.get('last_ticker_age_s')} last_scan_age_s={compact.get('last_scan_age_s')} "
        f"drawdown={paper.get('drawdown')} win_rate={paper.get('win_rate')} trades={paper.get('trades')} positions={paper.get('positions')} "
        f"pulses_hot={pulses.get('hot_count')} pulses_total={pulses.get('total')} "
        f"top_scan={str(scans.get('top_buy', ''))[:120]} top_minute={str(minute.get('top_buy', ''))[:120]}"
    )


def _build_prompt(task: str, status: Dict[str, Any], cfg: JudgeConfig, primary: Dict[str, Any] | None = None) -> List[Dict[str, str]]:
    brief = _status_brief(status)
    pulses = status.get("pulses") or {}
    pulse_detail = pulses.get("hot", {})
    pulse_lines = []
    for key, info in pulse_detail.items():
        parts = key.split(":", 2)
        pid = parts[0] if len(parts) > 0 else key
        strat = parts[1] if len(parts) > 1 else "?"
        pulse_lines.append(f"  {pid} [{strat}]: {info.get('pulses', 0)} pulses dir={info.get('dir')} conf={info.get('conf')}")
    pulse_block = "\n".join(pulse_lines) if pulse_lines else "  (none)"
    if cfg.role == "confirm":
        system = (
            "You are an advisory-only confirmation reviewer for live trading risk. "
            "Validate the primary judge conservatively. "
            "Return exactly one line and nothing else. "
            "If you disagree, choose the safer vote. "
            "Use only this format: vote=<continue|warn|stop|flatten_only> confidence=<0-1> reason=<short>."
        )
        primary_line = ""
        if primary:
            primary_line = (
                f"Primary vote: {primary.get('vote')} confidence={primary.get('confidence')} reason={primary.get('reason')}\n"
            )
        user = f"Task: {task}\n{primary_line}Status: {brief}\nRepeat signal pulses (product [strategy]: pulses dir=conf):\n{pulse_block}\nOutput only the confirmation verdict line."
    else:
        system = (
            "You are an advisory-only primary live trading risk reviewer. "
            "Choose the best next coarse course of action. "
            "Pay attention to repeat signal pulses: products with 3+ rapid BUY/SELL signals "
            "may indicate noise or stale data. Frequent flip-flopping suggests a position should flatten. "
            "Return exactly one line and nothing else. "
            "Use only this format: vote=<continue|warn|stop|flatten_only> confidence=<0-1> reason=<short>."
        )
        user = f"Task: {task}\nStatus: {brief}\nRepeat signal pulses (product [strategy]: pulses dir=conf):\n{pulse_block}\nOutput only the primary verdict line."
    return [{"role": "system", "content": system}, {"role": "user", "content": user}]


def _default_judges() -> List[JudgeConfig]:
    return [
        JudgeConfig(
            name="vibethinker",
            base_url=os.getenv("LLM_API_BASE_VIBE", "http://deathstar-xps-8920.tailcb8954.ts.net:1234/v1"),
            model=os.getenv("LLM_VIBE_MODEL", "vibethinker-3b"),
            max_tokens=int(os.getenv("LLM_VIBE_MAX_TOKENS", "80")),
            timeout_s=int(os.getenv("LLM_VIBE_TIMEOUT_S", "45")),
            role="primary",
        ),
    ]


def _parse_judge(raw: str) -> JudgeConfig:
    m = re.match(r"^(?P<name>[^=]+)=(?P<base>.+):(?P<model>[^:]+)(?::(?P<timeout>\d+)(?::(?P<max_tokens>\d+))?)?$", raw.strip())
    if not m:
        raise ValueError(f"invalid judge spec: {raw!r}")
    return JudgeConfig(
        name=m.group("name").strip(),
        base_url=m.group("base").strip(),
        model=m.group("model").strip(),
        timeout_s=int(m.group("timeout")) if m.group("timeout") else 45,
        max_tokens=int(m.group("max_tokens")) if m.group("max_tokens") else 80,
    )


def _resolve_model(cfg: JudgeConfig) -> str:
    if cfg.model:
        return cfg.model
    if cfg.resolved_model:
        return cfg.resolved_model
    url = cfg.base_url.rstrip("/") + "/models"
    req = request.Request(url, headers={"Accept": "application/json"})
    with request.urlopen(req, timeout=min(20, cfg.timeout_s)) as resp:
        data = json.loads(resp.read().decode("utf-8"))
    candidates = data.get("data") if isinstance(data, dict) else None
    if isinstance(candidates, list) and candidates:
        first = candidates[0]
        if isinstance(first, dict):
            model = first.get("id") or first.get("model") or ""
        else:
            model = str(first)
    else:
        model = ""
        if isinstance(data, dict):
            model = data.get("id") or data.get("model") or ""
    if not model:
        raise RuntimeError(f"could not resolve model for {cfg.name} at {cfg.base_url}")
    cfg.resolved_model = str(model)
    return cfg.resolved_model


def _merge_verdicts(primary: Dict[str, Any], confirm: Dict[str, Any] | None) -> Dict[str, Any]:
    primary_result = primary["result"]
    merged = dict(primary_result)
    merged["primary_vote"] = primary_result.get("vote")
    merged["primary_confidence"] = primary_result.get("confidence")
    merged["primary_reason"] = primary_result.get("reason")
    merged["confirmation"] = None

    if confirm is None:
        merged["vote"] = primary_result.get("vote", "warn")
        merged["reason"] = f"{primary_result.get('reason')} unconfirmed"
        merged["confidence"] = max(0.1, float(primary_result.get("confidence", 0.5)) - 0.1)
        return merged

    confirm_result = confirm["result"]
    merged["confirmation"] = {
        "vote": confirm_result.get("vote"),
        "confidence": confirm_result.get("confidence"),
        "reason": confirm_result.get("reason"),
        "judge": confirm.get("judge"),
    }

    primary_vote = primary_result.get("vote", "warn")
    confirm_vote = confirm_result.get("vote", "warn")
    primary_conf = float(primary_result.get("confidence", 0.5))
    confirm_conf = float(confirm_result.get("confidence", 0.5))

    if primary_vote == confirm_vote:
        merged["vote"] = primary_vote
        merged["confidence"] = min(0.99, (primary_conf + confirm_conf) / 2.0 + 0.05)
        merged["reason"] = primary_result.get("reason")
    else:
        order = {"continue": 0, "warn": 1, "stop": 2, "flatten_only": 3}
        safer_vote = primary_vote if order.get(primary_vote, 1) >= order.get(confirm_vote, 1) else confirm_vote
        merged["vote"] = safer_vote
        merged["confidence"] = max(0.1, min(primary_conf, confirm_conf) - 0.1)
        merged["reason"] = f"{primary_result.get('reason')} conf={confirm_result.get('reason')}"
    return merged


def _is_emergency(primary_result: Dict[str, Any], confirm_result: Dict[str, Any] | None) -> bool:
    if primary_result.get("vote") in {"stop", "flatten_only"}:
        return True
    if confirm_result is None:
        return False
    confirm_vote = confirm_result.get("vote")
    if confirm_vote in {"stop", "flatten_only"}:
        return True
    if primary_result.get("vote") != confirm_vote:
        severe = {primary_result.get("vote"), confirm_vote}
        return severe <= {"warn", "stop", "flatten_only"} and "continue" not in severe
    return False


def _parse_verdict_text(text: str) -> Dict[str, Any]:
    vote = "warn"
    confidence = 0.5
    reason = text.strip()
    vm = re.search(r"vote\s*=\s*(\S+)", text, re.IGNORECASE)
    if vm:
        v = vm.group(1).strip().lower().rstrip(",.!;")
        if v in {"continue", "warn", "stop", "flatten_only"}:
            vote = v
    cm = re.search(r"confidence\s*=\s*(\d+(?:\.\d+)?)", text, re.IGNORECASE)
    if cm:
        try:
            confidence = float(cm.group(1))
        except ValueError:
            pass
    confidence = max(0.0, min(1.0, confidence))
    rm = re.search(r"reason\s*=\s*(.+)", text, re.IGNORECASE)
    if rm:
        reason = rm.group(1).strip().rstrip(",.!;")
    return {
        "vote": vote,
        "confidence": confidence,
        "reason": reason,
        "risks": [],
        "action_items": [],
    }


def _fallback_verdict(task: str, status: Dict[str, Any]) -> Dict[str, Any]:
    paper = (status.get("paper") or {})
    alerts = status.get("alerts") or []
    health_ok = bool(status.get("health_ok"))
    drawdown = float(paper.get("drawdown") or 0.0)
    last_ticker_age = float(status.get("last_ticker_ts") and max(0.0, time.time() - float(status.get("last_ticker_ts"))) or 0.0)
    last_scan_age = float(status.get("last_scan_ts") and max(0.0, time.time() - float(status.get("last_scan_ts"))) or 0.0)
    positions = int(paper.get("positions") or 0)

    if not health_ok or alerts:
        return {"vote": "stop", "confidence": 0.95, "reason": "health_alert", "risks": alerts, "action_items": []}
    if drawdown >= 0.05:
        return {"vote": "flatten_only", "confidence": 0.9, "reason": "drawdown", "risks": [f"drawdown={drawdown:.4f}"], "action_items": []}
    if last_ticker_age >= 120 or last_scan_age >= 600:
        return {"vote": "warn", "confidence": 0.85, "reason": "stale_data", "risks": [f"ticker_age_s={last_ticker_age:.0f}", f"scan_age_s={last_scan_age:.0f}"], "action_items": []}
    if "position" in task.lower() and positions >= 10:
        return {"vote": "warn", "confidence": 0.75, "reason": "position_load", "risks": [f"positions={positions}"], "action_items": []}
    return {"vote": "continue", "confidence": 0.9, "reason": "healthy", "risks": [], "action_items": []}


def _call_judge(cfg: JudgeConfig, task: str, status: Dict[str, Any], primary: Dict[str, Any] | None = None) -> Dict[str, Any]:
    model = _resolve_model(cfg)
    payload = {
        "model": model,
        "messages": _build_prompt(task, status, cfg, primary),
        "temperature": 0.0,
        "max_tokens": cfg.max_tokens,
        "stream": False,
    }
    url = cfg.base_url.rstrip("/") + "/chat/completions"
    req = request.Request(url, data=json.dumps(payload).encode("utf-8"), headers={"Content-Type": "application/json"})
    with request.urlopen(req, timeout=cfg.timeout_s) as resp:
        data = json.loads(resp.read().decode("utf-8"))
    msg = data["choices"][0]["message"]
    text = (msg.get("content") or msg.get("reasoning_content") or "").strip()
    if not text:
        raise RuntimeError("empty judge response")
    parsed = _parse_verdict_text(text)
    if not re.search(r"(?m)^\s*vote=", text):
        parsed = _fallback_verdict(task, status)
    return {
        "judge": cfg.name,
        "task": task,
        "endpoint": cfg.base_url,
        "model": model,
        "result": parsed,
        "raw": text,
        "usage": data.get("usage", {}),
    }


def _task_schedule() -> List[Dict[str, Any]]:
    return [
        {"name": "fast_watchdog", "interval_s": 60, "task": "Check health alerts, feed freshness, scan lag, repeat signal pulses, and whether the trader should continue or warn."},
        {"name": "position_hygiene", "interval_s": 300, "task": "Review open positions for stuck brackets, oversized exposure, bad exits, and recent repeat signals that may indicate needing to flatten."},
        {"name": "pulse_review", "interval_s": 300, "task": "Review the repeat signal pulse summary. Products with 3+ rapid same-direction signals or flip-flopping BUY/SELL within minutes indicate noise or stale data — flatten those positions."},
        {"name": "trade_review", "interval_s": 300, "task": "Review current scan candidates and repeat signal patterns to decide whether the set is safe enough to keep watching."},
        {"name": "scoreboard", "interval_s": 1800, "task": "Score judge quality over the recent window and note false alarms or misses."},
        {"name": "hourly_digest", "interval_s": 3600, "task": "Summarize the last hour of trading health, the biggest changes, and any repeat signal patterns observed."},
    ]


def _update_scoreboard(score: Dict[str, Any], rec: Dict[str, Any]) -> Dict[str, Any]:
    r = rec["result"]
    vote = r["vote"]
    score["cycles"] = int(score.get("cycles", 0)) + 1
    score.setdefault("votes", {})
    score["votes"][vote] = int(score["votes"].get(vote, 0)) + 1
    score["confidence_sum"] = float(score.get("confidence_sum", 0.0)) + float(r.get("confidence", 0.0))
    score["last_vote"] = vote
    score["last_reason"] = r.get("reason")
    score["last_task"] = rec.get("task")
    return score


def _write_json(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True, default=str), encoding="utf-8")


def _append_jsonl(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(payload, sort_keys=True, default=str) + "\n")


def _run_once(judges: List[JudgeConfig], status_url: str, history_path: Path, scoreboard_path: Path, startup_grace_s: float = 120.0) -> Dict[str, Any]:
    global _last_flatten_ts, _consecutive_llm_failures, _circuit_breaker_until

    now = time.time()

    # Circuit breaker: if too many consecutive LLM failures, cool down
    is_circuit_open = _circuit_breaker_until > now
    if is_circuit_open:
        print(f"CIRCUIT BREAKER: cooling down until {_circuit_breaker_until:.0f} ({_circuit_breaker_until - now:.0f}s remaining)", flush=True)

    try:
        status = _fetch_status(status_url)
    except Exception as exc:
        status = {"error": str(exc), "health_ok": False, "mode": "unknown", "status": "unreachable"}
        age = now - _STARTUP_TS
        if age < startup_grace_s:
            print(f"WATCHDOG startup grace ({age:.0f}s < {startup_grace_s:.0f}s): health unreachable, skipping", flush=True)
            return {"status": status, "results": [], "scoreboard": {"startup_grace": True}}
    results = []
    scoreboard_root = json.loads(scoreboard_path.read_text(encoding="utf-8")) if scoreboard_path.exists() else {}
    scoreboards = scoreboard_root.get("judges", {}) if isinstance(scoreboard_root, dict) else {}
    primary_cfg = judges[0] if judges else None
    confirmers = [cfg for cfg in judges[1:] if cfg.role != "emergency"] if len(judges) > 1 else []
    emergency_cfgs = [cfg for cfg in judges if cfg.role == "emergency"]

    llm_call_failed = False
    for task_info in _task_schedule():
        primary_rec = None
        if primary_cfg and not is_circuit_open:
            try:
                primary_rec = _call_judge(primary_cfg, task_info["task"], status)
            except Exception as exc:
                llm_call_failed = True
                primary_rec = {
                    "judge": primary_cfg.name,
                    "task": task_info["task"],
                    "endpoint": primary_cfg.base_url,
                    "model": primary_cfg.model or primary_cfg.resolved_model or "",
                    "result": _fallback_verdict(task_info["task"], status),
                    "raw": f"fallback: {exc}",
                    "usage": {},
                }
        if primary_rec is not None:
            primary_rec["ts"] = now
            scoreboard = scoreboards.get(primary_cfg.name, {}) if isinstance(scoreboards, dict) else {}
            scoreboard = _update_scoreboard(scoreboard, primary_rec)
            if scoreboard.get("cycles", 0) > 0:
                scoreboard["avg_confidence"] = scoreboard.get("confidence_sum", 0.0) / max(1, scoreboard["cycles"])
            scoreboard["updated_at"] = now
            scoreboard["judge"] = asdict(primary_cfg)
            scoreboards[primary_cfg.name] = scoreboard

        confirm_rec = None
        if primary_rec is not None and not is_circuit_open:
            confirm_success_count = 0
            for cfg in confirmers:
                try:
                    confirm_rec = _call_judge(cfg, task_info["task"], status, primary=primary_rec.get("result"))
                    confirm_success_count += 1
                except Exception:
                    continue
                confirm_rec["ts"] = now
                scoreboard = scoreboards.get(cfg.name, {}) if isinstance(scoreboards, dict) else {}
                scoreboard = _update_scoreboard(scoreboard, confirm_rec)
                if scoreboard.get("cycles", 0) > 0:
                    scoreboard["avg_confidence"] = scoreboard.get("confidence_sum", 0.0) / max(1, scoreboard["cycles"])
                scoreboard["updated_at"] = now
                scoreboard["judge"] = asdict(cfg)
                scoreboards[cfg.name] = scoreboard
            # Confirmer minimum guard: if confirmers configured but none responded, reduce confidence
            if confirmers and confirm_success_count == 0:
                if primary_rec is not None:
                    pconf = float(primary_rec["result"].get("confidence", 0.5))
                    primary_rec["result"]["confidence"] = max(0.1, pconf - 0.15)
                    primary_rec["result"]["reason"] = f"{primary_rec['result'].get('reason', '')} [no_confirmers]"

        emergency_rec = None
        if primary_rec is not None and _is_emergency(primary_rec["result"], confirm_rec["result"] if confirm_rec else None):
            for cfg in emergency_cfgs:
                try:
                    emergency_rec = _call_judge(cfg, task_info["task"], status, primary=primary_rec.get("result"))
                except Exception:
                    continue
                emergency_rec["ts"] = now
                scoreboard = scoreboards.get(cfg.name, {}) if isinstance(scoreboards, dict) else {}
                scoreboard = _update_scoreboard(scoreboard, emergency_rec)
                if scoreboard.get("cycles", 0) > 0:
                    scoreboard["avg_confidence"] = scoreboard.get("confidence_sum", 0.0) / max(1, scoreboard["cycles"])
                scoreboard["updated_at"] = now
                scoreboard["judge"] = asdict(cfg)
                scoreboards[cfg.name] = scoreboard
                break

        if primary_rec is not None:
            merged = {
                "task": task_info["task"],
                "ts": now,
                "judge": primary_cfg.name,
                "endpoint": primary_cfg.base_url,
                "model": primary_cfg.model,
                "primary": primary_rec,
                "confirmation": confirm_rec,
                "emergency": emergency_rec,
                "result": _merge_verdicts(primary_rec, confirm_rec),
            }
            if emergency_rec is not None:
                merged["result"] = {
                    **merged["result"],
                    "vote": emergency_rec["result"].get("vote"),
                    "confidence": min(0.99, float(emergency_rec["result"].get("confidence", 0.5)) + 0.05),
                    "reason": f"{merged['result'].get('reason')} emergency={emergency_rec['result'].get('reason')}",
                    "emergency_vote": emergency_rec["result"].get("vote"),
                    "emergency_reason": emergency_rec["result"].get("reason"),
                }
            results.append(merged)
            _append_jsonl(history_path, merged)

            # Debounce guard: only flatten once per DEBOUNCE_S interval
            DEBOUNCE_S = 120
            merged_vote = merged.get("result", {}).get("vote", "")
            if merged_vote in ("stop", "flatten_only") and (now - _last_flatten_ts) >= DEBOUNCE_S:
                _last_flatten_ts = now
                flatten_url = status_url.rstrip("/health").rstrip("/") + "/flatten"
                flatten_token = os.getenv("APPROVAL_TOKEN", "")
                try:
                    req = request.Request(flatten_url)
                    if flatten_token:
                        req.add_header("Authorization", f"Bearer {flatten_token}")
                    with request.urlopen(req, timeout=10) as resp:
                        flatten_result = json.loads(resp.read().decode("utf-8"))
                    print(
                        f"OVERSEER FLATTEN: vote={merged_vote} "
                        f"reason={merged.get('result', {}).get('reason', '')} "
                        f"closed={flatten_result.get('closed', 0)}",
                        flush=True,
                    )
                except Exception as exc:
                    print(f"OVERSEER FLATTEN failed: {exc}", flush=True)
            elif merged_vote in ("stop", "flatten_only"):
                print(f"OVERSEER FLATTEN skipped: debounce active ({now - _last_flatten_ts:.0f}s < {DEBOUNCE_S}s)", flush=True)

    # Circuit breaker: update consecutive failure counter
    if llm_call_failed:
        _consecutive_llm_failures += 1
    else:
        _consecutive_llm_failures = max(0, _consecutive_llm_failures - 1)

    if _consecutive_llm_failures >= 3 and not is_circuit_open:
        _circuit_breaker_until = now + 300.0
        print(f"CIRCUIT BREAKER OPEN: {_consecutive_llm_failures} consecutive failures, cooling down 300s", flush=True)
    elif _consecutive_llm_failures == 0:
        _circuit_breaker_until = 0.0

    scoreboard_root = {"updated_at": now, "judges": scoreboards}
    _write_json(scoreboard_path, scoreboard_root)
    return {"status": status, "results": results, "scoreboard": scoreboard_root}


def main() -> int:
    ap = argparse.ArgumentParser(description="Vibethinker watchdog daemon for live trading oversight")
    ap.add_argument("--status-url", default="http://localhost:9090/health")
    ap.add_argument("--judge", action="append", default=[], help="name=base_url:model[:timeout_s[:max_tokens]]")
    ap.add_argument("--history-path", default="data/llm_watchdog_history.jsonl")
    ap.add_argument("--scoreboard-path", default="data/llm_watchdog_scoreboard.json")
    ap.add_argument("--interval-s", type=int, default=60)
    ap.add_argument("--once", action="store_true")
    ap.add_argument("--cycles", type=int, default=0, help="Optional fixed number of cycles before exit")
    args = ap.parse_args()

    judges = [_parse_judge(j) for j in args.judge] if args.judge else _default_judges()
    history_path = Path(args.history_path)
    scoreboard_path = Path(args.scoreboard_path)

    def run_cycle() -> Dict[str, Any]:
        return _run_once(judges, args.status_url, history_path, scoreboard_path)

    if args.once or args.cycles == 1:
        print(json.dumps(run_cycle(), indent=2, sort_keys=True))
        return 0

    remaining = args.cycles if args.cycles > 1 else None
    while True:
        payload = run_cycle()
        print(json.dumps(payload["scoreboard"], indent=2, sort_keys=True))
        if remaining is not None:
            remaining -= 1
            if remaining <= 0:
                break
        time.sleep(max(1, args.interval_s))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
