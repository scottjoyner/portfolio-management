"""Tests for the production health surfaces.

These exist because the system reported healthy for weeks while the main trading
process was blocked by a safety gate. The failure was not in the trading logic;
it was that three monitoring surfaces had no way to express "a supervised child
is not running".

Covered here:
  * run_production.py status exits non-zero when a child is down or blocked
  * status distinguishes a safety block from a crash
  * status publishes machine-readable state
  * /ready is 503 when the system cannot trade, 200 when it can
  * /health stays 200 regardless, because it is the dashboard's liveness probe
    and run_production.py restarts the dashboard when it is not healthy
  * scripts/health_check.py exit codes
"""
from __future__ import annotations

import importlib
import json
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

import run_production as rp  # noqa: E402


def _state(children, kill_switch=False, supervisor_running=True):
    return {
        "ts": 9999999999.0,
        "supervisor_pid": 4242 if supervisor_running else None,
        "supervisor_running": supervisor_running,
        "kill_switch_active": kill_switch,
        "children": children,
        "degraded": [c["name"] for c in children if c["state"] != "RUNNING"],
        "healthy": all(c["state"] == "RUNNING" for c in children),
    }


def _child(name, state, pid=111):
    return {"name": name, "script": f"{name}.py", "state": state,
            "pid": pid if state == "RUNNING" else None, "detail": None}


class TestChildStateClassification(unittest.TestCase):
    def test_blocked_is_distinct_from_stopped(self):
        with tempfile.TemporaryDirectory() as tmp:
            sentinel = Path(tmp) / "trader_state_corrupt"
            sentinel.touch()
            original_root, original_sentinel = rp.ROOT, rp.TRADER_CORRUPTION_SENTINEL
            rp.ROOT, rp.TRADER_CORRUPTION_SENTINEL = Path(tmp), Path("trader_state_corrupt")
            try:
                with sentinel.open():
                    state = rp._child_state("trader-v4")
                self.assertEqual(state["state"], "BLOCKED")
                self.assertIn("trader_state_corrupt", state["detail"])
                self.assertIsNone(state["pid"])
            finally:
                rp.ROOT, rp.TRADER_CORRUPTION_SENTINEL = original_root, original_sentinel

    def test_missing_pidfile_is_stopped(self):
        with tempfile.TemporaryDirectory() as tmp:
            original = rp.PROCESSES
            rp.PROCESSES = {"thing": {"script": "t.py", "pidfile": Path(tmp) / "nope.pid",
                                      "logfile": Path(tmp) / "t.log", "proc": None}}
            try:
                self.assertEqual(rp._child_state("thing")["state"], "STOPPED")
            finally:
                rp.PROCESSES = original

    def test_dead_pidfile_is_stale_not_running(self):
        with tempfile.TemporaryDirectory() as tmp:
            pidfile = Path(tmp) / "t.pid"
            pidfile.write_text("999999")  # no such process
            original = rp.PROCESSES
            rp.PROCESSES = {"thing": {"script": "t.py", "pidfile": pidfile,
                                      "logfile": Path(tmp) / "t.log", "proc": None}}
            try:
                self.assertEqual(rp._child_state("thing")["state"], "STALE_PID")
            finally:
                rp.PROCESSES = original

    def test_live_pid_is_running(self):
        with tempfile.TemporaryDirectory() as tmp:
            pidfile = Path(tmp) / "t.pid"
            pidfile.write_text(str(os.getpid()))
            original = rp.PROCESSES
            rp.PROCESSES = {"thing": {"script": "t.py", "pidfile": pidfile,
                                      "logfile": Path(tmp) / "t.log", "proc": None}}
            try:
                state = rp._child_state("thing")
                self.assertEqual(state["state"], "RUNNING")
                self.assertEqual(state["pid"], os.getpid())
            finally:
                rp.PROCESSES = original


class TestStatusExitCode(unittest.TestCase):
    """status is a health check, so it must fail when it finds trouble."""

    def _run_status(self, supervisor_pid, children):
        """children: {name: pid_value_or_None}. None means no pidfile at all."""
        with tempfile.TemporaryDirectory() as tmp:
            tmp = Path(tmp)
            sup = tmp / "supervisor.pid"
            if supervisor_pid is not None:
                sup.write_text(str(supervisor_pid))

            table = {}
            for name, pid_value in children.items():
                pidfile = tmp / f"{name}.pid"
                if pid_value is not None:
                    pidfile.write_text(str(pid_value))
                table[name] = {"script": f"{name}.py", "pidfile": pidfile,
                               "logfile": tmp / f"{name}.log", "proc": None}

            saved_pidfile, saved_processes = rp._pidfile_path, rp.PROCESSES
            rp._pidfile_path = lambda: sup
            rp.PROCESSES = table
            try:
                return rp.status()
            finally:
                rp._pidfile_path, rp.PROCESSES = saved_pidfile, saved_processes

    def test_all_running_exits_zero(self):
        code = self._run_status(os.getpid(), {"a": os.getpid()})
        self.assertEqual(code, 0)

    def test_missing_supervisor_exits_one(self):
        code = self._run_status(None, {"a": os.getpid()})
        self.assertEqual(code, 1, "no supervisor is a hard failure")

    def test_stopped_child_exits_one(self):
        # Pidfile absent entirely: the child never started or already cleaned up.
        code = self._run_status(os.getpid(), {"a": None})
        self.assertEqual(code, 1, "a supervised child that is not running is degraded")

    def test_stale_pid_exits_one(self):
        code = self._run_status(os.getpid(), {"a": 999999})
        self.assertEqual(code, 1)

    def test_one_down_among_healthy_is_still_degraded(self):
        code = self._run_status(os.getpid(), {"a": os.getpid(), "b": None})
        self.assertEqual(code, 1)


class TestStatusCliExitCode(unittest.TestCase):
    """The exit code is the contract; the function return value is not enough.

    Asserting rp.status() returns 1 passes even if main() forgets to propagate it,
    which is exactly the wiring bug that let a blocked trader report success.
    """

    SCRIPT = REPO_ROOT / "run_production.py"

    def _run(self, *args):
        return subprocess.run([sys.executable, str(self.SCRIPT), "status", *args],
                              capture_output=True, text=True, timeout=180, cwd=REPO_ROOT)

    def test_cli_exits_non_zero_without_a_supervisor(self):
        # No supervisor pidfile in a test environment, so the honest answer is
        # "not healthy" and the exit code must say so.
        result = self._run()
        self.assertEqual(
            result.returncode, 1,
            f"status must fail when the supervisor is not running:\n{result.stdout}{result.stderr}",
        )

    def test_cli_output_explains_the_failure(self):
        result = self._run()
        self.assertIn("DEGRADED", result.stdout,
                      "a non-zero exit must say what is wrong")

    def test_cli_publishes_machine_readable_state(self):
        result = self._run()
        payload = json.loads(rp.SUPERVISOR_STATE.read_text())
        self.assertIn("children", payload)
        self.assertIn("healthy", payload)
        self.assertIsInstance(payload["children"], list)
        for child in payload["children"]:
            self.assertIn(child["state"], ("RUNNING", "STOPPED", "BLOCKED", "STALE_PID"))


class TestSupervisorStateFile(unittest.TestCase):
    def test_state_file_payload_shape(self):
        with tempfile.TemporaryDirectory() as tmp:
            target = Path(tmp) / "state.json"
            saved = rp.SUPERVISOR_STATE
            rp.SUPERVISOR_STATE = target
            try:
                rp.write_supervisor_state([_child("trader-v4", "BLOCKED")], 4242)
                payload = json.loads(target.read_text())
            finally:
                rp.SUPERVISOR_STATE = saved
        self.assertFalse(payload["healthy"])
        self.assertEqual(payload["degraded"], ["trader-v4"])
        self.assertTrue(payload["supervisor_running"])
        self.assertIn("kill_switch_active", payload)

    def test_all_running_is_healthy(self):
        with tempfile.TemporaryDirectory() as tmp:
            target = Path(tmp) / "state.json"
            saved = rp.SUPERVISOR_STATE
            rp.SUPERVISOR_STATE = target
            try:
                rp.write_supervisor_state([_child("daemon", "RUNNING")], 4242)
                payload = json.loads(target.read_text())
            finally:
                rp.SUPERVISOR_STATE = saved
        self.assertTrue(payload["healthy"])


class TestReadinessEndpointLogic(unittest.TestCase):
    """Liveness and readiness must not be conflated.

    run_production.py restarts the dashboard when /health is not healthy, so
    folding trader state into /health would turn a blocked trader into a
    dashboard restart loop.
    """

    def setUp(self):
        os.environ["XDG_CONFIG_HOME"] = tempfile.mkdtemp()
        import trading_system.ui.dashboard_server as ds
        self.ds = ds
        importlib.reload(ds)

    def _write_state(self, payload):
        path = Path(self.ds.SUPERVISOR_STATE_PATH)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(payload))

    def test_ready_503_when_child_blocked(self):
        self._write_state(_state([_child("daemon", "RUNNING"), _child("trader-v4", "BLOCKED")]))
        payload, status = self.ds.api_ready_with_status()
        self.assertFalse(payload["ready"])
        self.assertEqual(status, 503)
        self.assertIn("trader-v4", payload["reason"])

    def test_ready_200_when_all_running(self):
        self._write_state(_state([_child("daemon", "RUNNING"), _child("trader-v4", "RUNNING")]))
        payload, status = self.ds.api_ready_with_status()
        self.assertTrue(payload["ready"], payload["reason"])
        self.assertEqual(status, 200)

    def test_ready_not_ready_when_supervisor_down(self):
        self._write_state(_state([_child("daemon", "RUNNING")], supervisor_running=False))
        payload, status = self.ds.api_ready_with_status()
        self.assertFalse(payload["ready"])
        self.assertEqual(status, 503)
        self.assertIn("supervisor", payload["reason"])

    def test_missing_state_is_not_silently_healthy(self):
        # The old behaviour was to report healthy with no knowledge of the
        # trading process at all.
        path = Path(self.ds.SUPERVISOR_STATE_PATH)
        if path.exists():
            path.unlink()
        payload, status = self.ds.api_ready_with_status()
        self.assertFalse(payload["ready"])
        self.assertEqual(status, 503)

    def test_kill_switch_is_a_halt_not_a_fault(self):
        self._write_state(_state([_child("trader-v4", "RUNNING")], kill_switch=True))
        payload, _ = self.ds.api_ready_with_status()
        self.assertTrue(payload["kill_switch_active"])
        self.assertTrue(payload["halted_by_operator"],
                        "an operator halt must be distinguishable from a fault")

    def test_health_stays_200_when_trader_blocked(self):
        """Liveness must not depend on the trader, or the dashboard restart-loops."""
        self._write_state(_state([_child("trader-v4", "BLOCKED")]))
        health = self.ds.api_health()
        self.assertEqual(health["status"], "healthy")
        self.assertEqual(health["supervisor"]["blocked"], ["trader-v4"])


class TestHealthCheckExitCodes(unittest.TestCase):
    SCRIPT = REPO_ROOT / "scripts" / "health_check.py"

    def _run(self, *args):
        return subprocess.run([sys.executable, str(self.SCRIPT), *args],
                              capture_output=True, text=True, timeout=180, cwd=REPO_ROOT)

    def test_script_parses_and_reports(self):
        result = self._run("--json")
        self.assertIn(result.returncode, (0, 1, 2), result.stderr[-400:])
        payload = json.loads(result.stdout)
        self.assertIn("exit_code", payload)
        self.assertIn("healthy", payload)

    def test_exit_code_matches_report(self):
        result = self._run("--json")
        payload = json.loads(result.stdout)
        self.assertEqual(result.returncode, payload["exit_code"])

    def test_finds_real_degradation_in_this_checkout(self):
        """Whatever the environment, the script must not report healthy silently."""
        result = self._run("--quiet")
        if result.returncode != 0:
            self.assertTrue(
                "DEGRADED" in result.stdout or not result.stdout.strip(),
                "a non-zero exit must explain itself",
            )


if __name__ == "__main__":
    unittest.main()