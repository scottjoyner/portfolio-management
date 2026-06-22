#!/usr/bin/env python3
"""
Production supervisor for the unified market system.

Launches and monitors:
  1. unified_market_daemon.py — scans Coinbase WS + prediction markets + arbitrage
  2. dashboard_server.py     — serves the web UI on port 8002

Automatically restarts crashed processes. Writes PID files for external
monitoring. Traps SIGINT/SIGTERM for graceful shutdown.

Usage:
    python3 run_production.py {start|stop|restart|status}
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
}

_shutdown_requested = False


def _signal_handler(signum: int, _frame) -> None:
    global _shutdown_requested
    _shutdown_requested = True
    sys.stdout.write(f"\nSignal {signum} received, shutting down...\n")
    sys.stdout.flush()


def _start(name: str) -> subprocess.Popen:
    cfg = PROCESSES[name]
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
    sys.stdout.write(f"  {name}: PID {proc.pid}\n")
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
        for name in PROCESSES:
            proc = PROCESSES[name]["proc"]
            if proc and proc.poll() is not None:
                ret = proc.poll()
                sys.stdout.write(f"  {name} exited code {ret}, restarting...\n")
                sys.stdout.flush()
                _start(name)

        # Sleep with 1s granularity so SIGTERM is responsive
        for _ in range(5):
            if _shutdown_requested:
                break
            time.sleep(1)

    _shutdown_all()


def status() -> None:
    sup_pidfile = LOGDIR / "supervisor.pid"
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
    sup_pidfile = LOGDIR / "supervisor.pid"
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
    try:
        sup_pidfile.unlink(missing_ok=True)
    except OSError:
        pass
    print("Stopped.")


def main() -> None:
    cmd = sys.argv[1] if len(sys.argv) > 1 else "start"

    if cmd == "start":
        pidfile = LOGDIR / "supervisor.pid"
        if pidfile.exists():
            try:
                pid = int(pidfile.read_text().strip())
                os.kill(pid, 0)
                print(f"Supervisor already running (PID {pid}). Use 'restart' first.")
                sys.exit(1)
            except OSError:
                pass

        pid = os.fork()
        if pid == 0:
            os.setsid()
            os.chdir("/")
            os.umask(0)
            sys.stdin.close()
            pidfile.write_text(str(os.getpid()))
            supervise()
        else:
            print(f"Supervisor started (PID {pid})")
            sys.stdout.flush()
            time.sleep(6)
            status()
            sys.exit(0)
    elif cmd == "stop":
        stop()
    elif cmd == "restart":
        stop()
        time.sleep(3)
        main()
    elif cmd == "status":
        status()
    else:
        print(f"Usage: {sys.argv[0]} {{start|stop|restart|status}}")
        sys.exit(1)


if __name__ == "__main__":
    main()
