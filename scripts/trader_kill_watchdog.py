#!/usr/bin/env python3
"""
Out-of-band kill-switch watchdog for the paper/live trader.

WHY THIS EXISTS
---------------
The in-process kill switch (config.is_kill_switch_active) is only checked INSIDE
the running bot's loop. If the bot hangs, deadlocks, or crash-loops, nothing
external enforces the halt. This watchdog is the OUT-OF-BAND control: it runs as
a separate process (cron/systemd timer) and force-kills the trader when the
kill-switch file is present, even if the bot is unresponsive.

It is SAFE to run continuously: it only acts when data/trading_kill_switch exists.
When the file is absent it does nothing (no kill, no signal).

USAGE
-----
    python3 scripts/trader_kill_watchdog.py

Run via cron every minute:
    * * * * * cd /home/scott/git/portfolio-management && .venv/bin/python3 scripts/trader_kill_watchdog.py >> logs/kill_watchdog.log 2>&1

EXIT CODES
    0  = no action needed (kill switch off, or bot not running)
    2  = KILLED the trader (kill switch was engaged)
"""
import os
import signal
import sys
import time

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
KILL_PATH = os.path.join(REPO, "data", "trading_kill_switch")
TRADER_CMDS = ["run_trader_v4.py", "coinbase/src/run_trader_v4.py"]


def _find_trader_pids():
    pids = []
    try:
        import psutil
    except Exception:
        return _find_trader_pids_proc()
    for p in psutil.process_iter(["pid", "cmdline"]):
        try:
            cmd = " ".join(p.info.get("cmdline") or [])
            if any(c in cmd for c in TRADER_CMDS) and "python" in cmd:
                pids.append(p.pid)
        except Exception:
            continue
    return pids


def _find_trader_pids_proc():
    pids = []
    for pid in os.listdir("/proc"):
        if not pid.isdigit():
            continue
        try:
            with open(f"/proc/{pid}/cmdline", "rb") as f:
                cmd = f.read().decode("utf-8", "replace").replace("\0", " ")
            if any(c in cmd for c in TRADER_CMDS) and "python" in cmd:
                pids.append(int(pid))
        except Exception:
            continue
    return pids


def main():
    if not os.path.exists(KILL_PATH):
        # Kill switch not engaged — stand down.
        return 0

    print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] KILL SWITCH ENGAGED ({KILL_PATH}) — halting trader", flush=True)
    pids = _find_trader_pids()
    if not pids:
        print("  no trader process found — nothing to kill", flush=True)
        return 0

    for pid in pids:
        try:
            os.kill(pid, signal.SIGTERM)
            print(f"  SIGTERM sent to pid {pid}", flush=True)
        except ProcessLookupError:
            pass
        except Exception as e:
            print(f"  failed to kill {pid}: {e}", flush=True)
    # Give it a moment, then SIGKILL any survivors.
    time.sleep(5)
    for pid in pids:
        try:
            os.kill(pid, signal.SIGKILL)
            print(f"  SIGKILL sent to pid {pid}", flush=True)
        except ProcessLookupError:
            pass
        except Exception:
            pass
    return 2


if __name__ == "__main__":
    sys.exit(main())
