#!/usr/bin/env python3
"""
Trader self-heal / auto-relaunch watchdog.

WHY THIS EXISTS
---------------
The v4 trader (coinbase/src/run_trader_v4.py) is launched manually and has NO
auto-restart. If the machine loses power and reboots (even with auto-login), or
if the process crashes, the trader stays DEAD and any LIVE positions sit
orphaned on the exchange — unmanaged, no stop, no exit. This script self-heals:
run it from cron @reboot and every few minutes; if the trader process is missing
it relaunches it with the hardened posture.

It is intentionally SEPARATE from the kill-switch watchdog
(trader_kill_watchdog.py): that one STOPS the trader on demand; this one STARTS
it when absent. They never fight (kill-switch takes precedence: if the kill file
exists, this script will NOT relaunch).

USAGE (cron):
    @reboot /home/scott/git/portfolio-management/scripts/trader_autostart.py
    */5 * * * * /home/scott/git/portfolio-management/scripts/trader_autostart.py
"""
import os
import subprocess
import sys
import time

REPO = "/home/scott/git/portfolio-management"
LOG = os.path.join(REPO, "logs", "trader_autostart.log")
# Module path of the running trader process (ps matches this).
PROC_MATCH = "coinbase/src/run_trader_v4.py"
# Kill-switch file — if present, do NOT relaunch (operator wants it stopped).
KILL_FILE = os.path.join(REPO, "data", "trading_kill_switch")
# Launch command. Start in PAPER by default for safety; flip --mode live only
# after the API key is scoped and trivial-capital proof is done.
LAUNCH = [
    sys.executable, "coinbase/src/run_trader_v4.py",
    "--mode", "paper",
    "--log-file", "logs/run_trader_v4_paper.log",
]


def _log(msg: str) -> None:
    line = f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] {msg}\n"
    try:
        os.makedirs(os.path.dirname(LOG), exist_ok=True)
        with open(LOG, "a") as f:
            f.write(line)
    except OSError:
        pass
    print(line, end="")


def _trader_running() -> bool:
    try:
        out = subprocess.run(
            ["pgrep", "-af", PROC_MATCH],
            capture_output=True, text=True, timeout=10,
        ).stdout
    except Exception:
        return False
    # Exclude our own process and the autostart script itself.
    for line in out.splitlines():
        if "trader_autostart" in line:
            continue
        if PROC_MATCH in line:
            return True
    return False


def main() -> int:
    if os.path.exists(KILL_FILE):
        _log("Kill-switch file present — NOT relaunching trader.")
        return 0
    if _trader_running():
        return 0  # already up, nothing to do
    _log("Trader NOT running — relaunching...")
    try:
        subprocess.Popen(
            LAUNCH,
            cwd=REPO,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            start_new_session=True,  # detach so it survives the cron session
        )
        _log("Relaunch issued.")
    except Exception as e:
        _log(f"Relaunch FAILED: {e}")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
