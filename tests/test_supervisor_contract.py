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
import tempfile
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

    Two extra variables are added purely for test isolation. Both entry points
    below are spawned as subprocesses, which no in-process monkeypatching can
    reach, so without these they operate on the live deployment:

    * ``TRADING_DATA_DIR`` — otherwise the child resolves the repo's real
      ``data/`` and can read or overwrite operator state.
    * ``TRADER_LOCK_PATH`` — ``run_trader_v4.py`` takes a single-writer lock
      before argparse runs, so ``--help`` fails with
      ``HostGuardError: another trader process already holds writer lock`` any
      time a trader is actually running. That is correct production behaviour
      and a broken test, not a broken entry point.

    These are test-only overrides and are deliberately not described as unit
    variables; the docstring above still describes the real unit.
    """
    env = {"PATH": "/usr/local/bin:/usr/bin:/bin", "HOME": os.environ.get("HOME", "/tmp")}
    env["PYTHONPATH"] = f"{REPO_ROOT}{os.pathsep}{REPO_ROOT / 'trading_system'}"
    scratch = Path(tempfile.mkdtemp(prefix="supervisor-contract-"))
    env["TRADING_DATA_DIR"] = str(scratch / "data")
    env["TRADER_LOCK_PATH"] = str(scratch / "trader-v4.lock")
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

class TestTraderTestSuiteCannotDeleteOperatorData(unittest.TestCase):
    """tests/coverage/coinbase/test_run_trader_v4.py destroyed real state.

    Its setUp and tearDown globbed `data/<name>.json*` for anything ending in
    state.json. In a live checkout that matches every sibling sharing the prefix:
    .bak, .bak2, .bak3, .pre-repair.*, .reconcile-audit.*. Running the suite
    deleted a 98KB paper-trading ledger and all four of its backups, with no way
    to recover them. The files were gitignored and nothing else referenced them.

    Deleting a user's trading history is not an acceptable side effect of a unit
    test, so the deletion set is now an explicit list and this pins it.
    """
    SUITE = REPO_ROOT / "tests" / "coverage" / "coinbase" / "test_run_trader_v4.py"

    def _paths(self):
        import importlib.util

        spec = importlib.util.spec_from_file_location("v4suite", self.SUITE)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module, module._state_paths_to_clear(REPO_ROOT)

    def test_deletion_set_never_contains_a_backup(self):
        module, paths = self._paths()
        forbidden = (".bak", "pre-repair", "reconcile-audit", ".tmp.")
        offenders = [p.name for p in paths if any(t in p.name for t in forbidden)]
        self.assertEqual(
            offenders, [],
            "the suite must not be able to delete backups or audit files",
        )

    def test_deletion_set_is_explicit_and_contained(self):
        module, paths = self._paths()
        for path in paths:
            with self.subTest(path=path.name):
                self.assertEqual(
                    path.parent.resolve(), (REPO_ROOT / "data").resolve(),
                    "the suite may only delete inside the repository's data dir",
                )
                self.assertFalse(
                    path.is_dir(), "it must never delete a directory"
                )

    def test_no_glob_in_the_suite_can_match_a_backup(self):
        """Checked against the AST, not the text.

        A text search also matches the prose that documents this defect, which
        makes the check report the explanation as the bug.
        """
        import ast

        tree = ast.parse(self.SUITE.read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call) or not isinstance(node.func, ast.Attribute):
                continue
            if node.func.attr != "glob" or not node.args:
                continue
            argument = node.args[0]
            pattern = argument.value if isinstance(argument, ast.Constant) else None
            with self.subTest(line=node.lineno, pattern=pattern):
                self.assertIsNotNone(pattern, "glob argument must be a literal so it can be audited")
                if pattern and pattern.endswith("state.json*"):
                    self.fail(
                        f"line {node.lineno}: glob({pattern!r}) also matches .bak, .bak2, .bak3, "
                        ".pre-repair.* and .reconcile-audit.* -- this is what destroyed the ledger"
                    )


class TestNoCoverageSuiteDeletesByWildcard(unittest.TestCase):
    """The trader-ledger loss was not confined to one file.

    test_run_trader_v4_extra.py carried its own copy of the same
    `glob("paper_trader_v4_state.json*")` cleanup. Fixing only the file the
    incident was noticed in would have left the identical bug one file away, so
    scan the whole coverage tree instead of trusting that one file stays fixed.
    """
    COVERAGE = REPO_ROOT / "tests" / "coverage"

    def test_no_coverage_suite_globs_a_wildcard_over_state_files(self):
        import ast

        offenders = []
        for path in sorted(self.COVERAGE.rglob("*.py")):
            try:
                tree = ast.parse(path.read_text(encoding="utf-8"))
            except (OSError, SyntaxError):
                continue
            for node in ast.walk(tree):
                if not isinstance(node, ast.Call) or not isinstance(node.func, ast.Attribute):
                    continue
                if node.func.attr != "glob" or not node.args:
                    continue
                argument = node.args[0]
                pattern = argument.value if isinstance(argument, ast.Constant) else None
                if pattern and pattern.endswith("state.json*"):
                    offenders.append(f"{path.relative_to(REPO_ROOT)}:{node.lineno} {pattern}")
        self.assertEqual(
            offenders, [],
            "a glob ending in state.json* also deletes .bak/.bak2/.bak3/"
            ".pre-repair-*/.reconcile-audit-*",
        )

    def test_no_coverage_suite_unlinks_anything_it_found_by_globbing(self):
        """Catch the dangerous shape directly: glob, then unlink the result.

        Checking unlink() arguments statically turns out to be the wrong tool.
        os.unlink(fd.name) on a NamedTemporaryFile(delete=False) is safe, and
        rejecting it would be a false positive that trains people to ignore the
        check. What actually destroyed the ledger was the *combination* -- a name
        bound from glob(), then unlink() called on it -- and that is checkable
        per function, so check that and nothing broader.
        """
        import ast

        offenders = []
        for path in sorted(self.COVERAGE.rglob("*.py")):
            try:
                tree = ast.parse(path.read_text(encoding="utf-8"))
            except (OSError, SyntaxError):
                continue
            for func in [n for n in ast.walk(tree) if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef))]:
                globbed = set()
                for node in ast.walk(func):
                    if (isinstance(node, ast.Assign)
                            and isinstance(node.value, ast.Call)
                            and isinstance(node.value.func, ast.Attribute)
                            and node.value.func.attr == "glob"):
                        for target in node.targets:
                            if isinstance(target, ast.Name):
                                globbed.add(target.id)
                    if (isinstance(node, ast.Call)
                            and isinstance(node.func, ast.Attribute)
                            and node.func.attr == "unlink"
                            and node.args
                            and isinstance(node.args[0], ast.Name)
                            and node.args[0].id in globbed):
                        offenders.append(
                            f"{path.relative_to(REPO_ROOT)}:{node.lineno} "
                            f"unlinks {node.args[0].id!r}, which came from glob()"
                        )
        self.assertEqual(offenders, [])
