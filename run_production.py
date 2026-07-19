#!/usr/bin/env python3
"""
Production supervisor for the unified market system.

Launches and monitors:
  1. unified_market_daemon.py — scans Coinbase WS + prediction markets + arbitrage
  2. dashboard_server.py     — serves the web UI on port 8002
  3. trader-v4               — EventTraderV4 paper trading with all-Rust strategies,
                                cross-asset regime, config.py pydantic fallback
  4. llm-watchdog             — multi-model LLM risk oversight daemon

 The supervisor normally forks to background on `start`, writes per-process PID files,
 and auto-restarts crashed children. When launched under systemd (or via `run`), it stays
 in the foreground so `Type=simple` units work correctly. Traps SIGINT/SIGTERM for graceful shutdown.

Hardening applied in trader-v4 (all internal):
  - Cross-asset regime gates long entries & scales sizing
  - config.py falls back to @dataclass when pydantic is absent
  - Per-product cooldown (1800s) prevents re-entry chatter
  - Maker/taker split (50% maker) reduces simulated fee impact
  - Min price change filter (0.05%) avoids noise evaluations
  - Minute-level scans (60s) + batch scans (300s) + full scans (3600s)
  - Hot-ticker prioritisation for minute scans

Usage:
    python3 run_production.py {start|run|stop|restart|status}
"""

import os
import signal
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent
LOGDIR = ROOT / "logs"
PYTHON = sys.executable

os.makedirs(LOGDIR, exist_ok=True)
os.makedirs(ROOT / "data", exist_ok=True)

PROCESSES: dict[str, dict] = {
    "daemon": {
        "script": "trading_system/apps/worker/unified_market_daemon.py",
        "args": [
            "--products", "BTC-USD,ETH-USD,SOL-USD",
            "--scan-interval", "90",
            "--pm-timeout", "35",
            "--arb-timeout", "25",
        ],
        "pidfile": LOGDIR / "daemon.pid",
        "logfile": LOGDIR / "daemon.log",
        "proc": None,
    },
    "dashboard": {
        "script": "trading_system/ui/dashboard_server.py",
        "args": ["--port", "8002"],
        "pidfile": LOGDIR / "dashboard.pid",
        "logfile": LOGDIR / "dashboard.log",
        "proc": None,
    },
    "trader-v4": {
        "script": "coinbase/src/run_trader_v4.py",
        "args": [
            "--mode", "paper",
            "--health-port", "9090",
            "--log-file", "logs/trader-v4.log",
            "--enable-shorts",
            "--enable-leverage",
            "--max-leverage", "2.0",
            "--minute-scan-interval", "60",
            "--minute-scan-top", "150",
            "--minute-scan-min-top", "10",
            "--minute-scan-max-top", "80",
            "--minute-scan-use-hotset",
            "--minute-scan-hotset-size", "150",
            "--scan-interval", "300",
            "--scan-top", "50",
            "--scan-min-vol", "1000",
            "--full-scan-interval", "300",
            "--paper-product-cooldown-seconds", "300",
            "--paper-maker-pct", "0.80",
            "--min-change", "0.05",
        ],
        "pidfile": LOGDIR / "trader-v4.pid",
        "logfile": LOGDIR / "trader-v4.log",
        "proc": None,
    },
    "llm-watchdog": {
        "script": "scripts/trading/llm_watchdog_daemon.py",
        "args": [
            "--status-url", "http://localhost:9090/health",
        ],
        "pidfile": LOGDIR / "llm-watchdog.pid",
        "logfile": LOGDIR / "llm-watchdog.log",
        "proc": None,
    },
}

_shutdown_requested = False

# Crash-loop backoff tracking
_restart_counts: dict[str, int] = {}
_last_restart_ts: dict[str, float] = {}
_START_BACKOFF_S: float = 5.0
_MAX_BACKOFF_S: float = 300.0
_HEALTHY_UPTIME_S: float = 60.0

# Health-based self-healing: a child that is alive but unhealthy (serving
# errors, hung, or stale) is restarted after this many consecutive failed probes.
_HEALTH_FAIL_LIMIT: int = 3
_HEALTH_PROBE_TIMEOUT_S: float = 5.0
# Stale heartbeat (daemon/llm-watchdog write a heartbeat file) older than this
# counts as unhealthy.
_HEARTBEAT_STALE_S: float = 180.0

import json as _json
import urllib.request as _urllib_req
import urllib.error as _urllib_err

# Per-child health probe. Returns True if the child is considered healthy.
# HTTP children are probed via their health/summary endpoint; process-only
# children fall back to "alive == healthy".
_HEALTH_PROBES: dict[str, dict] = {
    "trader-v4": {"url": "http://127.0.0.1:9090/health", "status_key": "status",
                  "ok_values": ("running", "healthy")},
    "dashboard": {"url": "http://127.0.0.1:8002/health", "status_key": "status",
                  "ok_values": ("healthy", "ok", "running")},
    "daemon": {"heartbeat": "data/.daemon_heartbeat"},
    "llm-watchdog": {"heartbeat": "data/.llm_watchdog_heartbeat"},
}

# Consecutive failed-health counts, reset on a passing probe.
_health_fail_counts: dict[str, int] = {}


def _probe_http(url: str, status_key: str, ok_values) -> bool:
    try:
        req = _urllib_req.Request(url, headers={"User-Agent": "supervisor-probe/1.0"})
        with _urllib_req.urlopen(req, timeout=_HEALTH_PROBE_TIMEOUT_S) as resp:
            if resp.status != 200:
                return False
            body = _json.loads(resp.read().decode("utf-8", "replace"))
        val = body.get(status_key)
        return val in ok_values if ok_values else val is not None
    except (_urllib_err.URLError, ValueError, OSError, TimeoutError):
        return False


def _probe_heartbeat(path: str) -> bool:
    p = ROOT / path
    if not p.exists():
        return False
    try:
        age = time.time() - float(p.read_text().strip())
        return age <= _HEARTBEAT_STALE_S
    except (ValueError, OSError):
        return False


def health_ok(name: str) -> bool:
    """Return True if the named child passes its health probe (or has no probe)."""
    probe = _HEALTH_PROBES.get(name)
    if not probe:
        return True
    if "url" in probe:
        return _probe_http(probe["url"], probe.get("status_key", "status"),
                           probe.get("ok_values"))
    if "heartbeat" in probe:
        return _probe_heartbeat(probe["heartbeat"])
    return True


def _signal_handler(signum: int, _frame) -> None:
    global _shutdown_requested
    _shutdown_requested = True
    sys.stdout.write(f"\nSignal {signum} received, shutting down...\n")
    sys.stdout.flush()


def _running_under_systemd() -> bool:
    return bool(os.environ.get("INVOCATION_ID") or os.environ.get("NOTIFY_SOCKET") or os.environ.get("JOURNAL_STREAM"))


def _pidfile_path() -> Path:
    return LOGDIR / "supervisor.pid"


def _write_supervisor_pid(pidfile: Path) -> None:
    pidfile.write_text(str(os.getpid()))


def _remove_supervisor_pid(pidfile: Path) -> None:
    try:
        pidfile.unlink(missing_ok=True)
    except OSError:
        pass


def _backoff_delay(name: str, now: float) -> float:
    """Exponential backoff: 5s, 10s, 20s, 40s, ... max 300s."""
    n = _restart_counts.get(name, 0)
    if n == 0:
        return 0.0
    delay = min(_START_BACKOFF_S * (2 ** (n - 1)), _MAX_BACKOFF_S)
    return delay


def _start(name: str) -> subprocess.Popen:
    cfg = PROCESSES[name]
    now = time.time()

    # Crash-loop backoff: if restarted too recently, wait
    delay = _backoff_delay(name, now)
    if delay > 0:
        last_ts = _last_restart_ts.get(name, 0.0)
        elapsed = now - last_ts
        if elapsed < delay:
            wait = delay - elapsed
            sys.stdout.write(f"  {name}: crash-loop backoff, waiting {wait:.0f}s (restart #{_restart_counts.get(name, 0)})\n")
            sys.stdout.flush()
            for _ in range(int(wait)):
                if _shutdown_requested:
                    break
                time.sleep(1)
            if _shutdown_requested:
                return cfg.get("proc")

    fh = open(cfg["logfile"], "a")
    fh.write(f"\n--- Started at {time.strftime('%Y-%m-%dT%H:%M:%S')} ---\n")
    fh.flush()
    proc = subprocess.Popen(
        [PYTHON, str(ROOT / cfg["script"])] + cfg["args"],
        stdout=fh,
        stderr=subprocess.STDOUT,
        cwd=str(ROOT),
    )
    cfg["proc"] = proc
    cfg["pidfile"].write_text(str(proc.pid))
    _restart_counts[name] = _restart_counts.get(name, 0) + 1
    _last_restart_ts[name] = now
    sys.stdout.write(f"  {name}: PID {proc.pid} (restart #{_restart_counts[name]})\n")
    sys.stdout.flush()
    return proc


def _stop(name: str, timeout: float = 10) -> bool:
    cfg = PROCESSES.get(name, {})
    proc = cfg.get("proc")
    if not proc:
        return True
    try:
        proc.terminate()
        proc.wait(timeout=timeout)
    except subprocess.TimeoutExpired:
        sys.stdout.write(f"  {name}: force killing...\n")
        sys.stdout.flush()
        proc.kill()
        proc.wait(timeout=5)
    except Exception as e:
        sys.stdout.write(f"  {name}: stop error: {e}\n")
        sys.stdout.flush()
    cfg["proc"] = None
    sys.stdout.write(f"  {name}: stopped\n")
    sys.stdout.flush()
    return True


def _shutdown_all() -> None:
    for name in reversed(list(PROCESSES.keys())):
        _stop(name)
    sys.stdout.write("All processes stopped.\n")
    sys.stdout.flush()


def supervise() -> None:
    signal.signal(signal.SIGTERM, _signal_handler)
    signal.signal(signal.SIGINT, _signal_handler)

    sys.stdout.write("Starting production system...\n")
    sys.stdout.flush()
    for name in PROCESSES:
        _start(name)
    sys.stdout.write("Monitoring (Ctrl+C to stop)...\n")
    sys.stdout.flush()

    while not _shutdown_requested:
        now = time.time()
        # Advertise supervisor liveness so an external watchdog can detect death.
        try:
            (LOGDIR / "supervisor_heartbeat").write_text(f"{now:.3f}\n")
        except OSError:
            pass
        for name in PROCESSES:
            cfg = PROCESSES[name]
            proc = cfg.get("proc")
            # Reset restart counter if process has been healthy for HEALTHY_UPTIME_S
            last_ts = _last_restart_ts.get(name, 0.0)
            if last_ts > 0 and (now - last_ts) >= _HEALTHY_UPTIME_S:
                if _restart_counts.get(name, 0) > 0:
                    sys.stdout.write(f"  {name}: healthy for {_HEALTHY_UPTIME_S:.0f}s, resetting restart counter\n")
                    sys.stdout.flush()
                _restart_counts[name] = 0

            if proc and proc.poll() is not None:
                ret = proc.poll()
                sys.stdout.write(f"  {name} exited code {ret}, restarting...\n")
                sys.stdout.flush()
                _start(name)
                _health_fail_counts[name] = 0
                continue

            # Health-based self-healing: process is alive but may be unhealthy.
            if proc and name in _HEALTH_PROBES:
                if health_ok(name):
                    if _health_fail_counts.get(name, 0) != 0:
                        sys.stdout.write(f"  {name}: health recovered\n")
                        sys.stdout.flush()
                    _health_fail_counts[name] = 0
                else:
                    _health_fail_counts[name] = _health_fail_counts.get(name, 0) + 1
                    fails = _health_fail_counts[name]
                    sys.stdout.write(f"  {name}: health probe failed ({fails}/{_HEALTH_FAIL_LIMIT})\n")
                    sys.stdout.flush()
                    if fails >= _HEALTH_FAIL_LIMIT:
                        sys.stdout.write(f"  {name}: UNHEALTHY for {fails} probes, restarting...\n")
                        sys.stdout.flush()
                        _stop(name)
                        _start(name)
                        _health_fail_counts[name] = 0

        # Sleep with 1s granularity so SIGTERM is responsive
        for _ in range(5):
            if _shutdown_requested:
                break
            time.sleep(1)

    _shutdown_all()


def run_foreground() -> None:
    pidfile = _pidfile_path()
    _write_supervisor_pid(pidfile)
    try:
        supervise()
    finally:
        _remove_supervisor_pid(pidfile)


def status() -> None:
    sup_pidfile = _pidfile_path()
    if sup_pidfile.exists():
        try:
            pid = int(sup_pidfile.read_text().strip())
            os.kill(pid, 0)
            print(f"Supervisor: RUNNING (PID {pid})")
        except (OSError, ValueError):
            print("Supervisor: STOPPED")
    else:
        print("Supervisor: STOPPED")

    for name in PROCESSES:
        cfg = PROCESSES[name]
        pidfile = cfg["pidfile"]
        if pidfile.exists():
            pid = pidfile.read_text().strip()
            try:
                pid = int(pid)
                os.kill(pid, 0)
                print(f"  {name}: RUNNING (PID {pid})")
            except (OSError, ValueError):
                print(f"  {name}: STALE PID ({pid})")
        else:
            print(f"  {name}: STOPPED")

    hb = ROOT / "data" / ".daemon_heartbeat"
    if hb.exists():
        try:
            age = time.time() - float(hb.read_text().strip())
            print(f"  Heartbeat: {age:.0f}s ago")
        except (ValueError, OSError):
            pass


def stop() -> None:
    sup_pidfile = _pidfile_path()
    pid = None
    if sup_pidfile.exists():
        try:
            pid = int(sup_pidfile.read_text().strip())
        except ValueError:
            pass

    target_pids = []
    if pid:
        target_pids.append(pid)
        os.kill(pid, signal.SIGTERM)
        print(f"Sent SIGTERM to supervisor (PID {pid})")

    for name in reversed(list(PROCESSES.keys())):
        cfg = PROCESSES[name]
        pidfile = cfg["pidfile"]
        if pidfile.exists():
            try:
                p = int(pidfile.read_text().strip())
                target_pids.append(p)
                os.kill(p, signal.SIGTERM)
                print(f"Sent SIGTERM to {name} (PID {p})")
            except (OSError, ValueError):
                pass

    # Wait for processes to die
    for p in target_pids[:]:
        for _ in range(10):
            try:
                os.kill(p, 0)
                time.sleep(1)
            except OSError:
                break
        try:
            os.kill(p, 0)
            os.kill(p, signal.SIGKILL)
            print(f"Force killed PID {p}")
        except OSError:
            pass

    for name in PROCESSES:
        cfg = PROCESSES[name]
        try:
            cfg["pidfile"].unlink(missing_ok=True)
        except OSError:
            pass
    _remove_supervisor_pid(sup_pidfile)
    print("Stopped.")


def main() -> None:
    cmd = sys.argv[1] if len(sys.argv) > 1 else "start"

    if cmd == "start":
        pidfile = _pidfile_path()
        if pidfile.exists():
            try:
                raw = pidfile.read_text().strip()
                if not raw:
                    # Empty/corrupt PID file (e.g. left behind by a crash) must be
                    # treated as "not running" — not as a fatal int('') ValueError.
                    pidfile.unlink(missing_ok=True)
                else:
                    pid = int(raw)
                    os.kill(pid, 0)
                    print(f"Supervisor already running (PID {pid}). Use 'restart' first.")
                    sys.exit(1)
            except (OSError, ValueError):
                # OSError: stale PID (no such process) -> safe to start.
                # ValueError: non-integer content -> treat as not running.
                pass

        os.chdir(str(ROOT))
        run_foreground()
    elif cmd == "run":
        os.chdir(str(ROOT))
        run_foreground()
    elif cmd == "stop":
        stop()
    elif cmd == "restart":
        stop()
        time.sleep(3)
        main()
    elif cmd == "status":
        status()
    else:
        print(f"Usage: {sys.argv[0]} {{start|run|stop|restart|status}}")
        sys.exit(1)


if __name__ == "__main__":
    main()
