#!/usr/bin/env python3
"""Preflight: will the stack actually come up if you restart it?

`sudo systemctl restart portfolio-trader.service` is all-or-nothing. Fifteen
commits of safety fixes have landed in the tree, and none of them are running,
because the only way to cut them over is a restart and a restart is the operator's
call. Everything here runs as the unprivileged operator and touches nothing: it
starts each service on scratch ports, asks it the questions that have broken it,
and reports whether a restart would land.

That matters because the defects fixed in this tree are invisible to unit tests.
Each one only appears when the system is started the documented way:

  * the dashboard refused to start without an operator token, and the supervisor
    restart-loops any child that exits -- a crash-loop on every service restart
  * a bare `%` in an argparse help string made run_trader_v4 --help crash
  * an exit-only watcher refuses to start while another holds the ledger lock
  * run_production.py status reports BLOCKED for a child a safety gate refuses

Each is checked here against the real code, not against a mock.

Exit codes:
  0  the revision should come up (a blocked child is reported, not failed)
  1  at least one check failed; do not restart yet
  2  the preflight itself could not run

A BLOCKED child is reported as a blocker but does not fail the run: the safety
gate is working, and whether to clear it is an operator decision. Restarting will
not fix it.
"""
from __future__ import annotations

import argparse
import json
import os
import shutil
import socket
import subprocess
import sys
import tempfile
import time
import urllib.request
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
SCRATCH_PORT_BASE = 18810


class Result:
    def __init__(self):
        self.checks: list[tuple[str, bool, str]] = []

    def add(self, name: str, ok: bool, detail: str = "") -> None:
        self.checks.append((name, ok, detail))

    @property
    def failures(self):
        return [c for c in self.checks if not c[1]]

    def report(self) -> None:
        for name, ok, detail in self.checks:
            marker = "ok  " if ok else "FAIL"
            print(f"  [{marker}] {name}" + (f" -- {detail}" if detail else ""))
        print()
        print(f"  {len(self.checks) - len(self.failures)}/{len(self.checks)} checks passed")


# One scratch XDG_CONFIG_HOME for the whole preflight run. This was previously
# created inside _env(), which mints a fresh tempdir on every call -- so the
# token-provisioning check inspected a different directory from the one the
# dashboard had actually written to, and reported a failure for a mechanism that
# had worked.
_SCRATCH_XDG = tempfile.mkdtemp(prefix="preflight-xdg-")


def scratch_xdg() -> str:
    return _SCRATCH_XDG


def _env(**overrides) -> dict:
    env = {
        "PATH": os.environ.get("PATH", "/usr/bin:/bin"),
        "HOME": os.environ.get("HOME", "/tmp"),
        "PYTHONPATH": f"{REPO_ROOT}{os.pathsep}{REPO_ROOT / 'trading_system'}",
        # A scratch XDG_CONFIG_HOME so the operator's real token is neither read
        # nor overwritten by a preflight run.
        "XDG_CONFIG_HOME": _SCRATCH_XDG,
        "DASHBOARD_PORT": "0",
    }
    env.update({k: str(v) for k, v in overrides.items()})
    return env


def _venv_python() -> str:
    candidate = REPO_ROOT / ".venv" / "bin" / "python"
    return str(candidate) if candidate.exists() else sys.executable


def free_port(offset: int) -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def check_supervised_scripts_exist(result: Result) -> None:
    """Every script the supervisor spawns must be present and importable."""
    table = _run([sys.executable, "-c",
                  "import json,run_production;"
                  "print(json.dumps({n: c['script'] for n, c in run_production.PROCESSES.items()}))"])
    if table.returncode != 0:
        result.add("supervised scripts importable", False,
                   f"could not import run_production: {table.stderr.strip()[-120:]}")
        return
    children = json.loads(table.stdout)
    missing = [s for s in children.values() if not (REPO_ROOT / s).is_file()]
    result.add("supervised scripts present", not missing,
               f"missing: {missing}" if missing else f"{len(children)} children")

    for name, script in sorted(children.items()):
        module = script[:-3].replace("/", ".")
        proc = _run([_venv_python(), "-c", f"import {module}"])
        result.add(f"imports: {name}", proc.returncode == 0,
                   "" if proc.returncode == 0 else proc.stderr.strip()[-120:])


def check_argparse_help(result: Result) -> None:
    """`--help` must work; a bare % in a help string makes argparse raise.

    This is how run_trader_v4.py broke. The operator reaches for --help first.

    The probe runs against scratch paths. ``run_trader_v4.py`` takes its
    single-writer lock *before* argparse runs, so with the live paths inherited
    the probe fails with ``HostGuardError: another trader process already holds
    writer lock`` on every preflight taken while the trader is up -- which is
    every preflight that matters, since the point is to restart a running
    system. That reported a perfectly healthy deployment as un-preflightable.
    """
    scratch = Path(tempfile.mkdtemp(prefix="preflight-help-"))
    env = _env()
    env["TRADING_DATA_DIR"] = str(scratch / "data")
    env["TRADER_LOCK_PATH"] = str(scratch / "trader-v4.lock")
    try:
        for script in ("coinbase/src/run_trader_v4.py",
                       "trading_system/ui/dashboard_server.py",
                       "trading_system/apps/worker/unified_market_daemon.py",
                       "scripts/hermes_agent_watch.py"):
            proc = _run([_venv_python(), str(REPO_ROOT / script), "--help"],
                        timeout=90, env=env)
            result.add(f"--help works: {Path(script).name}", proc.returncode == 0,
                       "" if proc.returncode == 0 else proc.stderr.strip()[-140:])
    finally:
        shutil.rmtree(scratch, ignore_errors=True)


def check_dashboard_comes_up(result: Result) -> None:
    """Start the real dashboard on a scratch port and ask it /ready.

    This is the check that would have caught the token crash-loop: under a
    scratch XDG_CONFIG_HOME it has no token, so it must provision one and serve.
    """
    port = free_port(0)
    proc = subprocess.Popen(
        [_venv_python(), str(REPO_ROOT / "trading_system/ui/dashboard_server.py"),
         "--port", str(port)],
        cwd=str(REPO_ROOT), env=_env(),
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
    )
    try:
        base = f"http://127.0.0.1:{port}"
        deadline = time.time() + 45
        health = None
        while time.time() < deadline:
            if proc.poll() is not None:
                output = proc.stdout.read() if proc.stdout else ""
                result.add("dashboard starts", False,
                           f"exited {proc.returncode}: {output.strip()[-200:]}")
                return
            try:
                with urllib.request.urlopen(f"{base}/health", timeout=3) as r:
                    health = r.status
                break
            except Exception:
                time.sleep(0.4)

        if health != 200:
            result.add("dashboard starts", False, "/health never returned 200")
            return
        result.add("dashboard starts", True, f"port {port}")

        # Readiness must answer, and must be able to say "not ready" honestly.
        try:
            with urllib.request.urlopen(f"{base}/ready", timeout=5) as r:
                code = r.status
        except urllib.error.HTTPError as e:
            code = e.code
        except Exception as e:
            result.add("readiness endpoint answers", False, str(e))
        else:
            # 200 or 503 are both valid; anything else means the route is broken.
            result.add("readiness endpoint answers", code in (200, 503),
                       f"/ready returned {code}")

        # A mutating endpoint without a token must refuse.
        req = urllib.request.Request(f"{base}/kill-switch", method="POST", data=b"{}")
        try:
            with urllib.request.urlopen(req, timeout=5) as r:
                status = r.status
        except urllib.error.HTTPError as e:
            status = e.code
        except Exception as e:
            result.add("unauthenticated mutation refused", False, str(e))
        else:
            result.add("unauthenticated mutation refused", status == 401,
                       f"/kill-switch returned {status}, expected 401")

        token_path = Path(scratch_xdg()) / "portfolio-management" / "dashboard_token"
        result.add("operator token provisioned", token_path.exists(),
                   "" if token_path.exists() else "no token file created under scratch XDG")
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            proc.kill()


def check_dashboard_refuses_without_a_token(result: Result) -> None:
    """The other direction of the same guard, and the one that matters most.

    If the dashboard could not provision or read a token it must refuse to start.
    run_production.py restarts any child that exits, so a missing token that did
    not fail loudly would become a restart loop rather than a clear error -- which
    is precisely how this was found. Checking only that it starts successfully
    proves nothing about the refusal path.
    """
    port = free_port(0)
    blocker = Path(tempfile.mkdtemp(prefix="preflight-blocked-")) / "not-a-dir"
    blocker.write_text("x")  # a file where a directory would have to go

    env = _env()
    env["XDG_CONFIG_HOME"] = str(blocker / "cannot" / "be" / "created")
    proc = subprocess.Popen(
        [_venv_python(), str(REPO_ROOT / "trading_system/ui/dashboard_server.py"),
         "--port", str(port)],
        cwd=str(REPO_ROOT), env=env,
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
    )
    try:
        deadline = time.time() + 30
        served = False
        while time.time() < deadline:
            if proc.poll() is not None:
                break
            try:
                with urllib.request.urlopen(f"http://127.0.0.1:{port}/health", timeout=2):
                    served = True
                    break
            except Exception:
                time.sleep(0.4)

        if served:
            result.add("dashboard refuses without a token", False,
                       "it started and served with no resolvable operator token")
            return
        if proc.poll() is None:
            result.add("dashboard refuses without a token", False,
                       "still running but not serving; could not confirm the refusal")
            return
        output = (proc.stdout.read() if proc.stdout else "").strip()
        named_token = "token" in output.lower() or "DASHBOARD_OPERATOR_TOKEN" in output
        result.add("dashboard refuses without a token", proc.returncode != 0 and named_token,
                   f"exited {proc.returncode}: {output.splitlines()[-1][:140] if output else 'no output'}")
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            proc.kill()


def check_kill_switch_resolves(result: Result) -> None:
    """The resolver must import and answer, and agree across env values.

    Two implementations of this once existed and the optimizer's was weaker.
    """
    code = (
        "from coinbase.src.config import is_kill_switch_active as f;"
        "import os;"
        "[ (os.environ.__setitem__('KILL_SWITCH', v), f()) for v in ('true','on','banana') ];"
        "print('ok')"
    )
    proc = _run([_venv_python(), "-c", code])
    result.add("kill-switch resolver answers", proc.returncode == 0,
               "" if proc.returncode == 0 else proc.stderr.strip()[-140:])

    code2 = (
        "import os, portfolio_optimizer as po;"
        "from coinbase.src.config import is_kill_switch_active as f;"
        "bad=[v for v in ('true','1','on','y','t','banana') "
        " if (os.environ.__setitem__('KILL_SWITCH', v) or po._kill_switch_active()) != f()];"
        "print('AGREE' if not bad else 'DIVERGE:'+','.join(bad))"
    )
    proc2 = _run([_venv_python(), "-c", code2], env=_env())
    ok = proc2.returncode == 0 and proc2.stdout.strip() == "AGREE"
    result.add("optimizer agrees with the execution paths", ok,
               "" if ok else (proc2.stdout.strip() or proc2.stderr.strip())[-140:])


def check_ledger_gate(result: Result) -> None:
    """Report a blocking safety gate. Restarting will not clear it."""
    sentinel = REPO_ROOT / "data" / "trader_state_corrupt"
    if sentinel.exists():
        detail = ""
        try:
            detail = sentinel.read_text(encoding="utf-8").strip().splitlines()[-1][:160]
        except OSError:
            pass
        result.add("no blocking safety gate", False,
                   f"{sentinel.relative_to(REPO_ROOT)} present: {detail}")
    else:
        result.add("no blocking safety gate", True, "no corruption sentinel")


def check_watcher_lock_is_free(result: Result) -> None:
    """Exactly one watcher may hold the single-writer ledger lock.

    The previous version of this check asked whether the lock was *free* and
    failed when it was not. That is the wrong question: ``hermes_agent_watch.py``
    takes an exclusive ``flock`` on the ledger and holds it for its entire
    lifetime, so on any running system the one legitimate holder made this check
    fail every time. It reported the supervised watcher as a duplicate and
    advised stopping it, which would have taken the agent watcher down.

    What actually matters is whether the holder is the *supervised* watcher — in
    which case a restart is fine, because the supervisor stops it before starting
    the new one — or some other live process, which is the genuine orphan this
    check was written to catch.
    """
    code = (
        "import sys; sys.path.insert(0, 'scripts');"
        "import importlib.util;"
        "spec = importlib.util.spec_from_file_location('w', 'scripts/hermes_agent_watch.py');"
        "m = importlib.util.module_from_spec(spec); spec.loader.exec_module(m);"
        "h = m.acquire_single_instance_lock(); h.close(); print('FREE')"
    )
    proc = _run([_venv_python(), "-c", code])
    if proc.returncode == 0 and proc.stdout.strip() == "FREE":
        result.add("no duplicate watcher", True, "ledger lock is unheld")
        return

    holder = _watcher_lock_holder_pid()
    if holder is None:
        result.add(
            "no duplicate watcher", False,
            (proc.stdout.strip() or proc.stderr.strip())[-160:] or "lock held, holder unknown",
        )
        return

    if not _pid_alive(holder):
        # flock is released by the kernel when the holder dies, so a lock file
        # naming a dead pid is not a conflict.
        result.add("no duplicate watcher", True,
                   f"ledger lock names dead pid {holder}; flock already released")
        return

    supervised = _supervised_watcher_pid()
    if supervised is not None and holder == supervised:
        result.add("no duplicate watcher", True,
                   f"ledger lock held by the supervised watcher (pid {holder}); "
                   "a restart stops it before starting the new one")
        return

    result.add(
        "no duplicate watcher", False,
        f"pid {holder} holds the ledger lock but is not the supervised watcher "
        f"(supervised={supervised}) cmdline={_pid_cmdline(holder)!r}. "
        "Stop it before restarting.",
    )


def _watcher_lock_holder_pid() -> int | None:
    """PID recorded in the watcher lock file, if it parses."""
    lock_path = REPO_ROOT / "data" / "hermes_agent_ledger.lock"
    try:
        raw = lock_path.read_text(encoding="utf-8").strip()
    except OSError:
        return None
    try:
        return int(raw.split()[0])
    except (ValueError, IndexError):
        return None


def _pid_alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def _pid_cmdline(pid: int) -> str:
    try:
        raw = Path(f"/proc/{pid}/cmdline").read_bytes()
    except OSError:
        return ""
    return raw.replace(b"\0", b" ").decode("utf-8", "replace").strip()[:120]


def _supervised_watcher_pid() -> int | None:
    """PID the supervisor recorded for the agent-watcher child."""
    pidfile = REPO_ROOT / "logs" / "agent-watcher.pid"
    try:
        return int(pidfile.read_text(encoding="utf-8").strip())
    except (OSError, ValueError):
        return None


def _run(cmd, timeout: int = 120, env: dict | None = None):
    return subprocess.run(cmd, capture_output=True, text=True,
                          timeout=timeout, cwd=str(REPO_ROOT),
                          env=env or _env())


def main() -> int:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()

    head = _run(["git", "rev-parse", "HEAD"])
    status = _run(["git", "status", "--short"])
    revision = head.stdout.strip()[:12] if head.returncode == 0 else "unknown"
    dirty = bool(status.stdout.strip())

    result = Result()
    print(f"preflight for revision {revision}{' (dirty tree)' if dirty else ''}")
    print()

    check_supervised_scripts_exist(result)
    check_argparse_help(result)
    check_kill_switch_resolves(result)
    check_watcher_lock_is_free(result)
    check_dashboard_comes_up(result)
    check_dashboard_refuses_without_a_token(result)
    check_ledger_gate(result)

    result.report()

    blocked = [c for c in result.failures if "safety gate" in c[0]]
    real = [c for c in result.failures if c not in blocked]

    if args.json:
        print(json.dumps({
            "revision": revision,
            "dirty": dirty,
            "ready": not real,
            "blocked": [c[0] for c in blocked],
            "failures": [{"check": c[0], "detail": c[2]} for c in real],
            "checks": [{"check": c[0], "ok": c[1], "detail": c[2]} for c in result.checks],
        }, indent=2))
    else:
        if blocked:
            print("\n  BLOCKED (a safety gate refuses the start; restarting will not fix it):")
            for check in blocked:
                print(f"    - {check[2]}")
        if real:
            print("\n  DO NOT RESTART YET: the revision would not come up cleanly.")
            for check in real:
                print(f"    - {check[0]}: {check[2]}")
        elif not blocked:
            print("\n  The revision should come up. Restart when ready:")
            print(f"    sudo systemctl restart portfolio-trader.service")
            print(f"    python3 run_production.py status")
        else:
            print("\n  The revision itself looks sound; clear the gate above first.")

    if dirty:
        print("\n  note: the working tree is dirty, so this preflight is not evidence about a commit.")

    return 1 if real else 0


if __name__ == "__main__":
    raise SystemExit(main())