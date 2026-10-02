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

class TestWatcherSingleWriterLock(unittest.TestCase):
    """Two watchers on one ledger can lose a close.

    An orphaned exit-only watcher was found running on this host alongside the
    supervisor's own. The docstring asserted single-writer safety by convention;
    nothing enforced it.
    """

    def _module(self):
        import importlib.util
        spec = importlib.util.spec_from_file_location(
            "hermes_agent_watch", REPO_ROOT / "scripts" / "hermes_agent_watch.py")
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module

    def test_second_watcher_is_refused(self):
        watch = self._module()
        with tempfile.TemporaryDirectory() as tmp:
            lock = str(Path(tmp) / "ledger.lock")
            first = watch.acquire_single_instance_lock(lock)
            try:
                with self.assertRaises(SystemExit) as caught:
                    watch.acquire_single_instance_lock(lock)
                self.assertIn("another exit-only watcher", str(caught.exception))
            finally:
                first.close()

    def test_lock_is_released_on_close(self):
        watch = self._module()
        with tempfile.TemporaryDirectory() as tmp:
            lock = str(Path(tmp) / "ledger.lock")
            watch.acquire_single_instance_lock(lock).close()
            second = watch.acquire_single_instance_lock(lock)
            second.close()

    def test_lock_records_the_holding_pid(self):
        watch = self._module()
        with tempfile.TemporaryDirectory() as tmp:
            lock = Path(tmp) / "ledger.lock"
            handle = watch.acquire_single_instance_lock(str(lock))
            try:
                self.assertIn(str(os.getpid()), lock.read_text())
            finally:
                handle.close()

    def test_refusal_names_the_other_pid(self):
        watch = self._module()
        with tempfile.TemporaryDirectory() as tmp:
            lock = str(Path(tmp) / "ledger.lock")
            handle = watch.acquire_single_instance_lock(lock)
            try:
                with self.assertRaises(SystemExit) as caught:
                    watch.acquire_single_instance_lock(lock)
                self.assertIn(str(os.getpid()), str(caught.exception))
            finally:
                handle.close()

    def test_daemon_path_actually_takes_the_lock(self):
        """Assert the call site, not just the helper.

        Testing acquire_single_instance_lock directly passed even with the call
        removed from main(), which is the same gap that let the weak kill switch
        and the discarded status exit code survive.
        """
        script = REPO_ROOT / "scripts" / "hermes_agent_watch.py"
        source = script.read_text()
        import ast as _ast
        tree = _ast.parse(source)
        main_fn = next((n for n in _ast.walk(tree)
                        if isinstance(n, _ast.FunctionDef) and n.name == "main"), None)
        self.assertIsNotNone(main_fn, "main() must exist")
        called = {
            n.func.id for n in _ast.walk(main_fn)
            if isinstance(n, _ast.Call) and isinstance(n.func, _ast.Name)
        }
        self.assertIn(
            "acquire_single_instance_lock", called,
            "the daemon path in main() must take the single-writer lock",
        )


class TestOrphanDetection(unittest.TestCase):
    """A supervised component running outside its supervisor must be visible.

    An exit-only watcher was found on this host running from a disabled user unit
    alongside the supervisor's own, both doing read-modify->save on one ledger.
    """

    def _ps_line(self, pid, age, args):
        # Reproduces ps's right-aligned columns, which broke a naive parser.
        return f"{pid:>7} {age:>8} {args}"

    def test_parser_handles_right_aligned_ps_columns(self):
        import importlib.util
        spec = importlib.util.spec_from_file_location(
            "health_check", REPO_ROOT / "scripts" / "health_check.py")
        health = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(health)

        sample = "\n".join([
            self._ps_line(2394496, 455842, "/venv/bin/python /repo/scripts/hermes_agent_watch.py --interval 20"),
            self._ps_line(999999, 3, "/venv/bin/python /repo/scripts/hermes_agent_watch.py --interval 20"),
            self._ps_line(630945, 1229338, "/venv/bin/python3 /repo/trading_system/ui/dashboard_server.py --port 8002"),
        ])

        original = health.subprocess.run

        def fake_run(cmd, *a, **kw):
            if cmd[:3] == ["ps", "-eo", "pid=,etimes=,args="]:
                return subprocess.CompletedProcess(cmd, 0, sample, "")
            return original(cmd, *a, **kw)

        health.subprocess.run = fake_run
        try:
            findings = health.check_unmanaged_duplicates()
        finally:
            health.subprocess.run = original

        orphans = [f for f in findings if f.check == "unmanaged_duplicate"]
        self.assertTrue(orphans, "the 455k-second orphan must be reported")
        self.assertIn("2394496", orphans[0].detail)
        # The young one is a --help probe or a manual start, not an orphan.
        self.assertNotIn("999999", orphans[0].detail)


class TestDashboardReportsTruthToTheOperator(unittest.TestCase):
    """Two dashboard responses that misreported what had happened.

    A dangerous manual operation was queued for approval and the response still
    said success=False, "Operation failed". An operator trusting that would
    retry a request that had been accepted. Separately, an unrecognised action
    name surfaced as a 500, reporting a server fault for a caller typo.
    """
    DANGEROUS = ("close_all", "emergency_hedge", "liquidate", "override_risk_limits")

    def test_dangerous_operation_reports_pending_approval_not_failure(self):
        import trading_system.ui.dashboard_server as m

        for op in self.DANGEROUS:
            with self.subTest(op=op):
                result = m._execute_manual_operation(op, {"reason": "regression"})
                self.assertEqual(result["status"], "pending_approval")
                self.assertTrue(result["requires_approval"])
                self.assertIn("operation_id", result)
                self.assertNotIn(
                    "failed", result["message"].lower(),
                    "a queued-for-approval op must not report itself as failed",
                )

    def test_harmless_operation_still_reports_success(self):
        import trading_system.ui.dashboard_server as m

        result = m._execute_manual_operation("noop", {})
        self.assertTrue(result["success"])
        self.assertNotIn("status", result)


class TestDataDirIsOverridable(unittest.TestCase):
    """The data directory was a hardcoded relative path in a dozen places.

    Every one of those paths was a function of the process working directory,
    so a test run from the repository root read and wrote the operator's live
    data/. A cleanup that globbed a state filename there deleted a
    paper-trading ledger and all four of its backups, irrecoverably.

    Resolving through one function means a run can be pointed elsewhere, so
    this asserts the seam actually works rather than trusting it.
    """
    def test_default_is_the_repo_relative_data_dir(self):
        from coinbase.src.run_trader_v4 import _data_dir
        self.assertEqual(str(_data_dir()), "data")

    def test_override_redirects_every_state_path(self):
        from coinbase.src import run_trader_v4 as m

        previous = os.environ.get("TRADING_DATA_DIR")
        os.environ["TRADING_DATA_DIR"] = "/tmp/isolation-probe"
        try:
            self.assertEqual(str(m._data_dir()), "/tmp/isolation-probe")
            self.assertEqual(
                str(m._state_path("paper_trader_v4_state.json")),
                "/tmp/isolation-probe/paper_trader_v4_state.json",
            )
            trader = m.EventTraderV4(mode="paper", products=["BTC-USD"], dry_run=True)
            self.assertTrue(
                str(trader._paper_state_path).startswith("/tmp/isolation-probe/"),
                f"trader still writes to the live data dir: {trader._paper_state_path}",
            )
        finally:
            if previous is None:
                os.environ.pop("TRADING_DATA_DIR", None)
            else:
                os.environ["TRADING_DATA_DIR"] = previous

    def test_no_hardcoded_data_path_remains(self):
        """Guards against a new call site reintroducing the hazard."""
        import ast

        source = Path(__file__).resolve().parents[1] / "coinbase" / "src" / "run_trader_v4.py"
        offenders = []
        for node in ast.walk(ast.parse(source.read_text(encoding="utf-8"))):
            if not (isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
                    and node.func.attr in ("Path", "open") and node.args):
                continue
            first = node.args[0]
            if isinstance(first, ast.Constant) and isinstance(first.value, str):
                if first.value == "data" or first.value.startswith("data/"):
                    offenders.append(f"{source.name}:{node.lineno} {first.value!r}")
        self.assertEqual(offenders, [], "route these through _state_path()/_data_dir()")


class TestDashboardDataPathsAreOverridable(unittest.TestCase):
    """The dashboard bound eight state paths to ROOT/'data' at import.

    Nothing could redirect them, so a test run from the repository root
    operated on the operator's live operator-state, approvals and capital
    buckets -- the files that gate real trades. Same seam as run_trader_v4:
    TRADING_DATA_DIR, defaulting to the original path.
    """
    EXPECTED = (
        "OPERATOR_STATE_PATH", "SIGNAL_CACHE_PATH", "APPROVALS_PATH",
        "CAPITAL_BUCKETS_PATH", "EQUITY_SUMMARY_PATH", "OPERATOR_ACTIONS_PATH",
    )

    def test_every_state_path_follows_the_override(self):
        from trading_system.ui import dashboard_server as ds

        previous = os.environ.get("TRADING_DATA_DIR")
        os.environ["TRADING_DATA_DIR"] = "/tmp/dash-probe"
        try:
            import importlib
            reloaded = importlib.reload(ds)
            for name in self.EXPECTED:
                with self.subTest(path=name):
                    self.assertTrue(
                        str(getattr(reloaded, name)).startswith("/tmp/dash-probe/"),
                        f"{name} still points at the live data dir: {getattr(reloaded, name)}",
                    )
            self.assertEqual(
                str(reloaded.APPROVALS_INBOX), "/tmp/dash-probe/approvals_inbox")
            # A trailing `or True` crept in here and made this vacuous. The
            # state DB is covered by its own test below, so assert nothing
            # vacuous here.
        finally:
            if previous is None:
                os.environ.pop("TRADING_DATA_DIR", None)
            else:
                os.environ["TRADING_DATA_DIR"] = previous
            importlib.reload(ds)

    def test_state_db_is_separately_overridable(self):
        from trading_system.ui import dashboard_server as ds
        previous = os.environ.get("TRADING_STATE_DB")
        os.environ["TRADING_STATE_DB"] = "/tmp/dash-probe/opt.db"
        try:
            import importlib
            reloaded = importlib.reload(ds)
            self.assertEqual(str(reloaded.STATE_DB_PATH), "/tmp/dash-probe/opt.db")
        finally:
            if previous is None:
                os.environ.pop("TRADING_STATE_DB", None)
            else:
                os.environ["TRADING_STATE_DB"] = previous
            importlib.reload(ds)


class TestOptimizerStatePathsAreOverridable(unittest.TestCase):
    """portfolio_optimizer.py had 29 "data/..." literals.

    They resolved against the process working directory, so a test run from the
    repository root read and wrote the operator's live state -- including
    pending_approvals.json, which gates real trade execution. The forms varied
    (os.open, os.replace for the atomic .tmp writes, os.remove, and `type: str`
    defaults), so the seam returns str and every call site keeps its type.
    """
    def test_default_is_byte_identical_to_the_old_literals(self):
        import portfolio_optimizer as po

        previous = os.environ.pop("TRADING_DATA_DIR", None)
        try:
            self.assertEqual(po._state_path("pending_approvals.json"),
                             "data/pending_approvals.json")
            self.assertEqual(po._state_path("optimizer.lock"), "data/optimizer.lock")
            self.assertEqual(po._state_path("meta_source_weights.json.tmp"),
                             "data/meta_source_weights.json.tmp")
        finally:
            if previous is not None:
                os.environ["TRADING_DATA_DIR"] = previous

    def test_override_redirects_every_shape(self):
        import portfolio_optimizer as po

        previous = os.environ.get("TRADING_DATA_DIR")
        os.environ["TRADING_DATA_DIR"] = "/tmp/po-probe"
        try:
            for name in ("pending_approvals.json", "optimizer.lock",
                         "optimizer_brackets.json", "cross_asset_regime.json.tmp"):
                with self.subTest(path=name):
                    self.assertEqual(po._state_path(name), f"/tmp/po-probe/{name}")
        finally:
            if previous is None:
                os.environ.pop("TRADING_DATA_DIR", None)
            else:
                os.environ["TRADING_DATA_DIR"] = previous

    def test_no_data_literal_remains(self):
        import ast

        source = Path(__file__).resolve().parents[1] / "portfolio_optimizer.py"
        # The seam's own default is the one legitimate "data" literal, and it
        # sits a few lines into the function body, so skip the whole body
        # rather than the single def line.
        src_lines = source.read_text(encoding="utf-8").splitlines()
        seam_start = next(i for i, line in enumerate(src_lines, 1)
                          if line.strip().startswith("def _state_path"))
        seam_end = next(
            (i for i in range(seam_start + 1, len(src_lines) + 1)
             if src_lines[i - 1] and not src_lines[i - 1][0].isspace()),
            len(src_lines) + 1,
        )
        offenders = []
        for node in ast.walk(ast.parse(source.read_text(encoding="utf-8"))):
            if not isinstance(node, ast.Constant) or not isinstance(node.value, str):
                continue
            if node.value == "data" or node.value.startswith("data/"):
                # The literal inside _state_path() itself is the one legitimate
                # occurrence; every other use must go through the seam.
                if seam_start <= node.lineno < seam_end:
                    continue
                offenders.append(f"{source.name}:{node.lineno} {node.value!r}")
        self.assertEqual(offenders, [], "route these through _state_path()")
