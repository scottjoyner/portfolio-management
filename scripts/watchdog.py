#!/usr/bin/env python3
"""
External reliability watchdog for the portfolio-management production stack.

The in-process supervisor inside `run_production.py` already auto-restarts
children that die AND (as of the health-healing update) children that are alive
but unhealthy. This watchdog is the *outer* layer:

  * Detects SUPERVISOR death (via `logs/supervisor_heartbeat`) and relaunches
    `run_production.py start` so the whole stack self-heals even if the
    supervisor itself crashes.
  * Independently probes every child endpoint and alerts (stderr / optional
    webhook) when a child is unhealthy, as a second opinion to the supervisor.
  * Writes its own heartbeat (`data/.watchdog_heartbeat`) so *it* can be
    monitored by cron/systemd.

Usage:
    python3 scripts/watchdog.py [--once] [--interval 15] [--restart-supervisor]
                               [--alert-cmd "cmd {}"]

In `--once` mode it performs a single check and exits non-zero if any critical
child (trader-v4, dashboard) is down — handy for cron/Nagios.
"""
import argparse
import json
import os
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
LOGDIR = ROOT / "logs"
DATA = ROOT / "data"

SUPERVISOR_HEARTBEAT = LOGDIR / "supervisor_heartbeat"
WATCHDOG_HEARTBEAT = DATA / ".watchdog_heartbeat"

# (name, kind, target) — kind is "http" or "heartbeat" or "pidfile"
CHILDREN = [
    ("trader-v4", "http", "http://127.0.0.1:9090/health"),
    ("dashboard", "http", "http://127.0.0.1:8002/health"),
    ("daemon", "heartbeat", str(DATA / ".daemon_heartbeat")),
    ("llm-watchdog", "pidfile", str(LOGDIR / "llm-watchdog.pid")),
]

SUPERVISOR_STALE_S = 90.0
HEARTBEAT_STALE_S = 180.0
HTTP_TIMEOUT_S = 5.0
CRITICAL = {"trader-v4", "dashboard"}


def _http_ok(url: str) -> bool:
    # Liveness probe: an HTTP 200 means the process is up and serving. A
    # "degraded" status is still alive (informational, not a restart trigger).
    try:
        req = urllib_request.Request(url, headers={"User-Agent": "watchdog/1.0"})
        with urllib_urlopen(req, timeout=HTTP_TIMEOUT_S) as resp:
            return resp.status == 200
    except Exception:
        return False


def _heartbeat_ok(path: str, stale: float = HEARTBEAT_STALE_S) -> bool:
    p = Path(path)
    if not p.exists():
        return False
    try:
        return (time.time() - float(p.read_text().strip())) <= stale
    except (ValueError, OSError):
        return False


def _pidfile_ok(path: str) -> bool:
    p = Path(path)
    if not p.exists():
        return False
    try:
        pid = int(p.read_text().strip())
        os.kill(pid, 0)
        return True
    except (ValueError, OSError):
        return False


def child_ok(name: str, kind: str, target: str) -> bool:
    if kind == "http":
        return _http_ok(target)
    if kind == "heartbeat":
        return _heartbeat_ok(target)
    if kind == "pidfile":
        return _pidfile_ok(target)
    return True


def supervisor_alive() -> bool:
    if not SUPERVISOR_HEARTBEAT.exists():
        return False
    try:
        return (time.time() - float(SUPERVISOR_HEARTBEAT.read_text().strip())) <= SUPERVISOR_STALE_S
    except (ValueError, OSError):
        return False


def restart_supervisor() -> bool:
    # Stop any orphaned children first, then relaunch the supervisor.
    try:
        subprocess.run(
            [sys.executable, str(ROOT / "run_production.py"), "restart"],
            check=False, timeout=60,
        )
        return True
    except Exception as e:  # pragma: no cover
        sys.stderr.write(f"watchdog: supervisor restart failed: {e}\n")
        return False


def _import_urllib():
    global urllib_request, urllib_urlopen
    import urllib.request as urllib_request
    import urllib.request as urllib_urlopen


def check_all() -> tuple[list[str], list[str]]:
    """Return (unhealthy_critical, unhealthy_noncritical)."""
    down_crit, down_other = [], []
    for name, kind, target in CHILDREN:
        ok = child_ok(name, kind, target)
        if not ok:
            (down_crit if name in CRITICAL else down_other).append(name)
    if not supervisor_alive():
        down_crit.append("supervisor")
    return down_crit, down_other


def main() -> int:
    _import_urllib()
    ap = argparse.ArgumentParser()
    ap.add_argument("--once", action="store_true", help="single check, exit non-zero if critical down")
    ap.add_argument("--interval", type=float, default=15.0)
    ap.add_argument("--restart-supervisor", action="store_true",
                    help="if supervisor is dead, relaunch run_production.py")
    ap.add_argument("--alert-cmd", default="", help="command to run with {} replaced by alert text")
    args = ap.parse_args()

    if args.once:
        crit, other = check_all()
        if crit:
            msg = "WATCHDOG CRITICAL: " + ", ".join(crit)
            sys.stderr.write(msg + "\n")
            if args.alert_cmd:
                os.system(args.alert_cmd.replace("{}", msg))
            return 2
        return 0

    sys.stdout.write("watchdog started (interval %.0fs)\n" % args.interval)
    sys.stdout.flush()
    while True:
        try:
            WATCHDOG_HEARTBEAT.write_text(f"{time.time():.3f}\n")
        except OSError:
            pass
        crit, other = check_all()
        if crit:
            msg = "WATCHDOG CRITICAL: " + ", ".join(crit)
            sys.stderr.write(msg + "\n")
            sys.stderr.flush()
            if args.alert_cmd:
                os.system(args.alert_cmd.replace("{}", msg))
            if args.restart_supervisor and "supervisor" in crit:
                sys.stdout.write("watchdog: supervisor dead, relaunching...\n")
                sys.stdout.flush()
                if restart_supervisor():
                    sys.stdout.write("watchdog: supervisor relaunched\n")
                    sys.stdout.flush()
        elif other:
            sys.stdout.write("watchdog: degraded: " + ", ".join(other) + "\n")
            sys.stdout.flush()
        time.sleep(args.interval)


if __name__ == "__main__":
    sys.exit(main())
