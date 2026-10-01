"""The supervisor's children must start cleanly.

`run_production.py` is what systemd runs. It spawns each child with a fixed
argument list and restarts any child that exits. That combination makes an
argument the script rejects, or a startup precondition the supervised
environment cannot satisfy, into a crash-loop -- not a visible error.

Two real examples this prevents:

  * `dashboard_server.py` refused to start without DASHBOARD_OPERATOR_TOKEN.
    `run_production.py` does not inherit the operator's shell environment, so
    the dashboard died on every service restart even though starting it by hand
    worked.
  * `run_trader_v4.py --help` raised ValueError: unsupported format character
    '/', because argparse expands `help=` text with %-formatting and one string
    contained a bare `25%/50%/75%/90%`.

Neither is a functional bug in the trading logic. Both mean the documented way to
run the system does not work.
"""
from __future__ import annotations

import ast
import os
import subprocess
import sys
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
SUPERVISOR = REPO_ROOT / "run_production.py"

# Entry points the docs tell operators to run. `--help` must work on all of them.
OPERATOR_ENTRY_POINTS = (
    "trading_system/apps/worker/unified_market_daemon.py",
    "trading_system/ui/dashboard_server.py",
    "coinbase/src/run_trader_v4.py",
    "coinbase/src/run_trader_v2.py",
    "scripts/hermes_agent_watch.py",
    "approval_server.py",
    "portfolio_optimizer.py",
)

MIN_HELP_TIMEOUT_S = 90


def supervised_children() -> dict[str, dict]:
    """Extract {name: {script, args}} from the PROCESSES table.

    Parsed rather than imported because importing run_production.py creates log
    directories and pidfile paths as a side effect.
    """
    tree = ast.parse(SUPERVISOR.read_text(encoding="utf-8"))
    table = None
    for node in ast.walk(tree):
        targets = []
        if isinstance(node, ast.Assign):
            targets = list(node.targets)
        elif isinstance(node, ast.AnnAssign):
            targets = [node.target]
        if any(isinstance(t, ast.Name) and t.id == "PROCESSES" for t in targets):
            table = node.value
            break
    if table is None:
        raise AssertionError("run_production.py no longer defines PROCESSES")

    children: dict[str, dict] = {}
    for key, value in zip(table.keys, table.values):
        entry = {}
        for field_key, field_value in zip(value.keys, value.values):
            if not isinstance(field_key, ast.Constant):
                continue
            if field_key.value == "script":
                entry["script"] = ast.literal_eval(field_value)
            elif field_key.value == "args":
                entry["args"] = [ast.literal_eval(e) for e in field_value.elts]
        if "script" in entry:
            children[ast.literal_eval(key)] = entry
    return children


def _clean_env() -> dict:
    """An environment resembling the systemd unit's, not the operator's shell.

    The unit sets only HOME and PATH, so nothing an operator exported in their
    shell -- including secrets -- is present. Anything a supervised child needs
    at startup must be provisionable without it.
    """
    env = {"PATH": "/usr/local/bin:/usr/bin:/bin", "HOME": os.environ.get("HOME", "/tmp")}
    env["PYTHONPATH"] = f"{REPO_ROOT}{os.pathsep}{REPO_ROOT / 'trading_system'}"
    return env


class TestSupervisedChildrenAreReal(unittest.TestCase):
    def test_processes_table_is_not_empty(self):
        self.assertTrue(supervised_children(), "no supervised children were parsed")

    def test_every_supervised_script_exists(self):
        for name, entry in supervised_children().items():
            with self.subTest(child=name):
                script = REPO_ROOT / entry["script"]
                self.assertTrue(
                    script.is_file(),
                    f"{name} is configured to run {entry['script']}, which does not exist; "
                    "the supervisor would restart-loop on exit 2",
                )


class TestArgparseConfigurationIsSound(unittest.TestCase):
    """argparse expands `help=` with %-formatting, so a bare % crashes it."""

    def test_help_works_for_every_operator_entry_point(self):
        for script in OPERATOR_ENTRY_POINTS:
            with self.subTest(script=script):
                result = subprocess.run(
                    [sys.executable, str(REPO_ROOT / script), "--help"],
                    capture_output=True, text=True, timeout=MIN_HELP_TIMEOUT_S,
                    cwd=REPO_ROOT, env=_clean_env(),
                )
                self.assertEqual(
                    result.returncode, 0,
                    f"{script} --help failed: {result.stderr.strip()[-500:]}",
                )
                self.assertTrue(result.stdout.strip(), f"{script} --help printed nothing")

    def test_no_help_string_contains_a_bare_percent(self):
        """Catches the bug statically, so it cannot reappear unnoticed."""
        offenders = []
        for script in OPERATOR_ENTRY_POINTS:
            tree = ast.parse((REPO_ROOT / script).read_text(encoding="utf-8"))
            for node in ast.walk(tree):
                if not isinstance(node, ast.Call):
                    continue
                func = node.func
                if not (isinstance(func, ast.Attribute) and func.attr == "add_argument"):
                    continue
                for keyword in node.keywords:
                    if keyword.arg != "help" or not isinstance(keyword.value, ast.Constant):
                        continue
                    text = keyword.value.value
                    if not isinstance(text, str):
                        continue
                    index = 0
                    while index < len(text):
                        if text[index] == "%":
                            if text[index:index + 2] == "%%":
                                index += 2
                                continue
                            if text[index:index + 2] == "%(":
                                index += 2
                                continue
                            offenders.append(f"{script}: {text!r}")
                            break
                        index += 1
        self.assertEqual(offenders, [], "bare % in a help= string breaks argparse expansion")


class TestSupervisedStartupPreconditions(unittest.TestCase):
    def test_dashboard_starts_without_an_operator_shell_environment(self):
        """The crash-loop the auto-provisioned token exists to prevent.

        Asserted on the resolution helper rather than by binding a port, so it is
        deterministic and does not need a free port in CI.
        """
        script = str(REPO_ROOT / "trading_system" / "ui" / "dashboard_server.py")
        code = (
            "import sys; sys.path[:0]=[%r,%r];"
            "import trading_system.ui.dashboard_server as ds;"
            "import os; os.environ.pop('DASHBOARD_OPERATOR_TOKEN', None);"
            "token=ds.operator_token();"
            "assert len(token)>=ds.MIN_OPERATOR_TOKEN_LEN, 'token too short: %%r' %% len(token);"
            "assert ds.operator_token()==token, 'token not stable across calls';"
            "print('ok')"
        ) % (str(REPO_ROOT), str(REPO_ROOT / "trading_system"))
        result = subprocess.run(
            [sys.executable, "-c", code],
            capture_output=True, text=True, timeout=MIN_HELP_TIMEOUT_S,
            cwd=REPO_ROOT, env=_clean_env(),
        )
        self.assertEqual(result.returncode, 0, result.stderr.strip()[-500:])
        self.assertIn("ok", result.stdout)


if __name__ == "__main__":
    unittest.main()