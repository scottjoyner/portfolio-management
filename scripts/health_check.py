#!/usr/bin/env python3
"""Production health gate for the operator-run Python trading services.

Exit code is the contract, because that is what cron, systemd timers, and
external monitors can actually act on:

  0  healthy, or deliberately halted by the operator
  1  degraded -- something the operator needs to look at
  2  the check itself could not run (misconfigured, no supervisor state)

Why this exists: while trader-v4 had been blocked by a corruption sentinel for
weeks, every monitoring surface reported green. `systemctl is-active` returned
"active" because the supervisor was alive. `run_production.py status` exited 0.
The dashboard's /health said "healthy" and did not even mention the trading
process. Nothing alerted.

Checks, in order of severity:

  supervisor      run_production.py status exits non-zero (any child down/blocked)
  readiness       /ready returns 503 (system cannot trade)
  liveness        /health returns non-200 (dashboard process is down)
  freshness       supervisor state and daemon heartbeat are recent
  kill switch     engaged by an operator is reported, not treated as a fault

Usage:
  python3 scripts/health_check.py                 # human-readable + exit code
  python3 scripts/health_check.py --json          # machine-readable
  python3 scripts/health_check.py --quiet         # only speak when degraded

Exit 0 for an operator-initiated halt, so engaging the kill switch does not page
anyone: that is the system working as designed. `--alert-on-halt` overrides that.
"""
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
SUPERVISOR_STATE = REPO_ROOT / "logs" / "supervisor_state.json"
DAEMON_HEARTBEAT = REPO_ROOT / "data" / ".daemon_heartbeat"
DASHBOARD_PORT = int(os.environ.get("DASHBOARD_PORT", "8002"))

# A supervisor that has not republished state in this long is itself a fault,
# regardless of what the last state it wrote said.
STATE_STALE_SEC = 300
HEARTBEAT_STALE_SEC = 300
PROBE_TIMEOUT_SEC = 10


class Finding:
    def __init__(self, severity: str, check: str, detail: str):
        self.severity = severity  # "degraded" | "halted" | "error"
        self.check = check
        self.detail = detail

    def as_dict(self) -> dict:
        return {"severity": self.severity, "check": self.check, "detail": self.detail}


def _probe(url: str) -> tuple[int | None, str]:
    try:
        with urllib.request.urlopen(url, timeout=PROBE_TIMEOUT_SEC) as response:
            return response.status, response.read(4096).decode("utf-8", "replace")
    except urllib.error.HTTPError as error:
        return error.code, error.read(4096).decode("utf-8", "replace")
    except Exception as error:
        return None, f"{type(error).__name__}: {error}"


def check_supervisor() -> tuple[list[Finding], dict]:
    """`run_production.py status` is the source of truth for child liveness."""
    findings: list[Finding] = []
    result = subprocess.run(
        [sys.executable, str(REPO_ROOT / "run_production.py"), "status"],
        capture_output=True, text=True, timeout=60, cwd=str(REPO_ROOT),
    )
    output = (result.stdout + result.stderr).strip()

    if result.returncode != 0:
        blocked = [line.strip() for line in output.splitlines() if "BLOCKED" in line]
        if blocked:
            findings.append(Finding(
                "degraded", "supervisor",
                "child blocked by a safety gate: " + "; ".join(blocked),
            ))
        findings.append(Finding(
            "degraded", "supervisor",
            f"status exited {result.returncode}: {output.splitlines()[-1] if output else 'no output'}",
        ))
    return findings, {"exit_code": result.returncode, "output": output}


def check_state_freshness() -> list[Finding]:
    findings = []
    if not SUPERVISOR_STATE.exists():
        findings.append(Finding(
            "degraded", "supervisor_state_freshness",
            f"{SUPERVISOR_STATE.relative_to(REPO_ROOT)} missing; run `run_production.py status`",
        ))
        return findings
    try:
        ts = float(SUPERVISOR_STATE.read_text().strip().splitlines()[-1].split(":")[-1].strip(" ,"))
    except Exception:
        try:
            ts = float(json.loads(SUPERVISOR_STATE.read_text()).get("ts", 0))
        except Exception:
            findings.append(Finding("degraded", "supervisor_state_freshness",
                                    "supervisor state is unreadable"))
            return findings
    age = time.time() - ts
    if age > STATE_STALE_SEC:
        findings.append(Finding(
            "degraded", "supervisor_state_freshness",
            f"supervisor state is {age:.0f}s old (> {STATE_STALE_SEC}s); status is not being republished",
        ))
    return findings


def check_daemon_heartbeat() -> list[Finding]:
    if not DAEMON_HEARTBEAT.exists():
        return [Finding("degraded", "daemon_heartbeat", "market data daemon heartbeat missing")]
    try:
        age = time.time() - float(DAEMON_HEARTBEAT.read_text().strip())
    except Exception:
        return [Finding("degraded", "daemon_heartbeat", "heartbeat unreadable")]
    if age > HEARTBEAT_STALE_SEC:
        return [Finding("degraded", "daemon_heartbeat",
                        f"heartbeat {age:.0f}s old (> {HEARTBEAT_STALE_SEC}s)")]
    return []


def check_readiness() -> list[Finding]:
    """503 from /ready means the system is up but cannot trade."""
    status, body = _probe(f"http://127.0.0.1:{DASHBOARD_PORT}/ready")
    if status is None:
        # Dashboard unreachable: report once, not twice (liveness also fails).
        return [Finding("degraded", "readiness", f"dashboard unreachable: {body}")]
    if status == 200:
        return []
    try:
        reason = json.loads(body).get("reason")
    except Exception:
        reason = body[:200]
    return [Finding("degraded", "readiness", f"/ready returned {status}: {reason}")]


def check_liveness() -> list[Finding]:
    """Non-200 on /health means the dashboard process itself is gone."""
    status, body = _probe(f"http://127.0.0.1:{DASHBOARD_PORT}/health")
    if status is None:
        return [Finding("degraded", "liveness", f"dashboard unreachable: {body}")]
    if status != 200:
        return [Finding("degraded", "liveness", f"/health returned {status}")]
    return []


def check_kill_switch() -> list[Finding]:
    """An engaged kill switch is an operator decision, so report it as `halted`."""
    try:
        sys.path.insert(0, str(REPO_ROOT))
        from coinbase.src.config import is_kill_switch_active
        active = bool(is_kill_switch_active())
    except Exception as error:
        return [Finding("degraded", "kill_switch", f"could not resolve state: {error}")]
    if active:
        return [Finding("halted", "kill_switch", "kill switch engaged; trading halted by operator")]
    return []


def run(alert_on_halt: bool = False) -> dict:
    findings: list[Finding] = []
    supervisor, supervisor_detail = check_supervisor()
    findings.extend(supervisor)
    findings.extend(check_state_freshness())
    findings.extend(check_daemon_heartbeat())
    findings.extend(check_readiness())
    findings.extend(check_liveness())
    findings.extend(check_kill_switch())

    hard = [f for f in findings if f.severity in ("degraded", "error")]
    halted = [f for f in findings if f.severity == "halted"]

    if hard:
        code = 1
    elif halted and alert_on_halt:
        code = 1
    else:
        code = 0

    return {
        "healthy": code == 0,
        "exit_code": code,
        "timestamp": time.time(),
        "findings": [f.as_dict() for f in findings],
        "degraded": [f.as_dict() for f in hard],
        "halted": [f.as_dict() for f in halted],
        "supervisor": supervisor_detail,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Production health gate for the Python trading services.")
    parser.add_argument("--json", action="store_true")
    parser.add_argument("--quiet", action="store_true", help="only print when degraded")
    parser.add_argument("--alert-on-halt", action="store_true",
                        help="treat an operator kill-switch halt as a failure")
    args = parser.parse_args()

    report = run(alert_on_halt=args.alert_on_halt)

    if args.json:
        print(json.dumps(report, indent=2, default=str))
    elif report["healthy"]:
        if not args.quiet:
            print(f"healthy: no findings ({time.strftime('%Y-%m-%d %H:%M:%S')})")
    else:
        print(f"DEGRADED ({time.strftime('%Y-%m-%d %H:%M:%S')})")
        for finding in report["findings"]:
            marker = "HALTED" if finding["severity"] == "halted" else "FAIL"
            print(f"  [{marker}] {finding['check']}: {finding['detail']}")
        if report["halted"]:
            print("  note: kill-switch halts are operator intent; pass --alert-on-halt to fail on them")

    return report["exit_code"]


if __name__ == "__main__":
    raise SystemExit(main())