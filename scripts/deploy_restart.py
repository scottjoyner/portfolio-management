#!/usr/bin/env python3
"""Guarded restart of the operator-run Python trading stack, with rollback.

`sudo systemctl restart portfolio-trader.service` is all-or-nothing. If the new
revision does not come up healthy, you are left with whatever state that
revision produced, and discovering the cause by hand is slow.

This does the restart in the order that preserves a way back:

  1. refuse to deploy a dirty tree unless --allow-dirty
  2. record the current revision
  3. restart the unit
  4. wait for every supervised child to reach RUNNING, or a child to BLOCK
  5. on timeout or failure, restart the previous revision and report

A child that ends up BLOCKED is a safety gate doing its job, so it is treated as
a failed deploy and rolled back -- but it is called out separately from a crash,
because the operator has to clear the gate before the system can trade at all.

This does not clear gates, kill switches, or corrupt-state sentinels. Those are
operator decisions and are never touched here.

Usage:
  python3 scripts/deploy_restart.py                 # restart and verify
  python3 scripts/deploy_restart.py --dry-run       # report what would happen
  python3 scripts/deploy_restart.py --timeout 300
"""
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
STATE_FILE = REPO_ROOT / "logs" / "supervisor_state.json"
UNIT = "portfolio-trader.service"


def _run(cmd: list[str], timeout: int = 120) -> subprocess.CompletedProcess:
    return subprocess.run(cmd, capture_output=True, text=True, timeout=timeout, cwd=str(REPO_ROOT))


def current_revision() -> str:
    result = _run(["git", "rev-parse", "HEAD"])
    return result.stdout.strip() if result.returncode == 0 else "unknown"


def dirty_files() -> list[str]:
    result = _run(["git", "status", "--porcelain"])
    return [line for line in result.stdout.splitlines() if line.strip()]


def read_children() -> list[dict]:
    try:
        return json.loads(STATE_FILE.read_text()).get("children", [])
    except Exception:
        return []


def snapshot() -> dict:
    """Force a fresh supervisor status so we never read a stale state file."""
    result = _run([sys.executable, str(REPO_ROOT / "run_production.py"), "status"], timeout=120)
    payload = {
        "children": read_children(),
        "healthy": result.returncode == 0,
        "output": (result.stdout + result.stderr).strip(),
    }
    return payload


def wait_healthy(timeout_s: int, poll_s: float = 5.0) -> tuple[bool, dict]:
    deadline = time.time() + timeout_s
    last = {"children": [], "healthy": False, "output": "no observation yet"}
    while time.time() < deadline:
        last = snapshot()
        if last["healthy"]:
            return True, last
        # A safety block will not clear on its own; stop waiting.
        if any(c.get("state") == "BLOCKED" for c in last["children"]):
            return False, last
        time.sleep(poll_s)
    return False, last


def restart_unit() -> tuple[bool, str]:
    result = _run(["sudo", "-n", "systemctl", "restart", UNIT], timeout=180)
    if result.returncode != 0:
        return False, (result.stdout + result.stderr).strip()
    return True, "restarted"


def checkout(revision: str) -> tuple[bool, str]:
    result = _run(["git", "checkout", "--detach", revision], timeout=120)
    if result.returncode != 0:
        return False, (result.stdout + result.stderr).strip()
    return True, f"checked out {revision}"


def describe(state: dict) -> None:
    for child in state["children"]:
        line = f"  {child['name']}: {child['state']}"
        if child.get("detail"):
            line += f" -- {child['detail']}"
        print(line)
    if state["children"]:
        print("  " + state["output"].splitlines()[-1])


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--timeout", type=int, default=180)
    parser.add_argument("--allow-dirty", action="store_true",
                        help="deploy with uncommitted changes (recorded as a dirty revision)")
    args = parser.parse_args()

    if os.geteuid() == 0:
        print("refusing to run as root: sudo inside sudo, and a wrong-root deploy is hard to undo",
              file=sys.stderr)
        return 2

    revision = current_revision()
    dirty = dirty_files()

    print(f"current revision: {revision[:12]}{' (dirty)' if dirty else ''}")
    if dirty:
        print(f"  {len(dirty)} uncommitted change(s)")
        if not args.allow_dirty:
            print("\nrefusing to deploy a dirty tree.")
            print("Commit first, or pass --allow-dirty if you understand that a rollback")
            print("to HEAD would not restore what is on disk right now.")
            return 2

    before = snapshot()
    print("\nBEFORE:")
    describe(before)

    if args.dry_run:
        print(f"\ndry run: would `sudo systemctl restart {UNIT}` and wait up to "
              f"{args.timeout}s for all children to reach RUNNING, rolling back to "
              f"{revision[:12]} if they do not.")
        return 0

    ok, message = restart_unit()
    if not ok:
        print(f"\nrestart failed: {message}", file=sys.stderr)
        return 1
    print(f"\n{message}; waiting for children (timeout {args.timeout}s)...")

    healthy, after = wait_healthy(args.timeout)
    print("\nAFTER:")
    describe(after)

    if healthy:
        print(f"\nOK: all supervised children are RUNNING at {revision[:12]}")
        return 0

    blocked = [c["name"] for c in after["children"] if c.get("state") == "BLOCKED"]
    if blocked:
        print(f"\nDEPLOY FAILED: {', '.join(blocked)} blocked by a safety gate.",
              file=sys.stderr)
        print("This is the system refusing to trade on bad state, not a crash.", file=sys.stderr)
        print("Nothing was cleared automatically. Inspect the gate before trading again.",
              file=sys.stderr)
    else:
        print("\nDEPLOY FAILED: not all children reached RUNNING.", file=sys.stderr)

    print(f"\nrolling back to {revision[:12]}", file=sys.stderr)
    ok, message = checkout(revision)
    print(f"  {message}", file=sys.stderr)
    ok, message = restart_unit()
    print(f"  {message}", file=sys.stderr)

    rolled, state = wait_healthy(60)
    if rolled:
        print("rollback restored a healthy stack", file=sys.stderr)
    else:
        print("WARNING: the stack is still not healthy after rollback. Manual intervention needed.",
              file=sys.stderr)
        describe(state)
    return 1


if __name__ == "__main__":
    raise SystemExit(main())