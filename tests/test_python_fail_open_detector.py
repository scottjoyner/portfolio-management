"""Self-tests for the fail-open detector.

These exist because the detector itself was wrong twice in ways no test caught:
it excluded all 872 files (the repo lives under a directory literally named
``data``, which was in the exclusion set) and one of its rules could not fire.
Both made it report a clean tree while scanning nothing.

So the tests assert the detector finds real bugs, does not invent bugs, and
actually visits the repository.
"""
from __future__ import annotations

import importlib.util
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
DETECTOR = REPO_ROOT / "scripts" / "detect_python_fail_open.py"


def _load_detector():
    spec = importlib.util.spec_from_file_location("detect_python_fail_open", DETECTOR)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


d = _load_detector()


def _kinds(source: str) -> set[str]:
    path = Path(tempfile.mkdtemp()) / "probe.py"
    path.write_text(source)
    return {f["kind"] for f in d.analyse_file(path)}


class TestDetectorFindsRealBugs(unittest.TestCase):
    """Every rule must be reachable, or it is decoration."""

    def test_verifier_short_circuit(self):
        self.assertIn(
            "verifier_short_circuits_to_permit",
            _kinds("def verify_totp(code):\n    return True\n    if not code:\n        return False\n"),
        )

    def test_missing_input_permits(self):
        self.assertIn(
            "missing_input_returns_permissive",
            _kinds(
                "def check_balance(a):\n"
                "    b = db.get(a)\n"
                "    if b is None:\n"
                '        return True, "ok"\n'
                '    return False, "no"\n'
            ),
        )

    def test_except_branch_permits(self):
        self.assertIn(
            "except_returns_permissive",
            _kinds(
                "def is_risk_ok(p):\n"
                "    try:\n"
                "        v = vol(p)\n"
                "    except Exception:\n"
                "        return True\n"
                "    return v < 5\n"
            ),
        )

    def test_state_machine_falls_through_to_permit(self):
        self.assertIn(
            "state_machine_falls_through_to_permit",
            _kinds(
                "def can_trade(self):\n"
                '    if self.state == "OPEN":\n'
                "        return False\n"
                '    if self.state == "HALT":\n'
                "        return False\n"
                "    return True\n"
            ),
        )

    def test_permissive_lookup_default(self):
        self.assertIn(
            "permissive_lookup_default",
            _kinds(
                "def check_validations(r):\n"
                '    return r.get("security_scan_passed", True)\n'
            ),
        )


class TestDetectorDoesNotInventBugs(unittest.TestCase):
    """A detector that cries wolf gets muted, which is worse than none."""

    def test_ordinary_return_false_guards_are_clean(self):
        self.assertEqual(
            _kinds(
                "def check_order(p, s, sz, px):\n"
                "    l = limits.get(p)\n"
                "    if l is None:\n"
                '        return False, "no limit"\n'
                "    if not l.allows_side(s):\n"
                '        return False, "side"\n'
                '    return True, ""\n'
            ),
            set(),
        )

    def test_success_return_true_is_not_a_short_circuit(self):
        # A checker's final `return True` is the success path.
        self.assertEqual(
            _kinds("def verify_x(a, b):\n    if not a:\n        return False\n    return True\n"),
            set(),
        )

    def test_returncode_zero_is_not_missing_input(self):
        # `if r.returncode == 0: return True` is success, not absence.
        self.assertEqual(
            _kinds(
                "def verify_auth():\n"
                "    r = run(cmd)\n"
                "    if r.returncode == 0:\n"
                "        return True\n"
                "    return False\n"
            ),
            set(),
        )

    def test_inverted_polarity_true_is_fail_closed(self):
        # is_kill_switch_active -> True means halted, so returning True on an
        # unreadable path is the safe answer, not a fail-open.
        self.assertEqual(
            _kinds(
                "def is_kill_switch_active():\n"
                "    try:\n"
                "        return path.exists()\n"
                "    except OSError:\n"
                "        return True\n"
            ),
            set(),
        )

    def test_correctly_refusing_unknown_state_is_clean(self):
        self.assertEqual(
            _kinds(
                "def can_execute(self):\n"
                '    if self._state == "CLOSED":\n'
                "        return True\n"
                '    if self._state == "OPEN":\n'
                "        return False\n"
                "    return False\n"
            ),
            set(),
        )


class TestDetectorActuallyScansTheRepo(unittest.TestCase):
    """Guards against the silent no-op scan.

    The detector once excluded every file because the exclusion set contained
    "data" and this checkout is at /media/scott/data/git/portfolio-management.
    It reported "0 findings" for weeks of scanning nothing.
    """

    def test_scope_is_relative_not_absolute(self):
        target = REPO_ROOT / "trading_system" / "risk" / "limits" / "service.py"
        self.assertTrue(target.exists())
        self.assertTrue(
            d._in_scope(target),
            "a file inside an analysed tree must be in scope even though an "
            "ancestor directory is named 'data'",
        )

    def test_out_of_tree_files_are_rejected(self):
        self.assertFalse(d._in_scope(Path("/tmp/elsewhere/trading_system/x.py")))

    def test_a_real_file_yields_real_findings_when_mutated(self):
        # End-to-end: mutate a real repo file, confirm analyse_file reports it.
        target = REPO_ROOT / "trading_system" / "safety" / "bulkeproof_safety_system.py"
        original = target.read_text()
        mutated = original.replace(
            "        logging.error(\n"
            '            "Circuit breaker in unrecognised state %r; refusing execution", self._state\n'
            "        )\n"
            "        return False",
            "        return True",
        )
        if mutated == original:
            self.skipTest("fix no longer present in the expected shape; update this test")
        try:
            target.write_text(mutated)
            kinds = {f["kind"] for f in d.analyse_file(target)}
        finally:
            target.write_text(original)
        self.assertIn(
            "state_machine_falls_through_to_permit",
            kinds,
            "reintroducing the known circuit-breaker fail-open must be detected",
        )

    def test_repository_is_clean_after_the_fixes(self):
        self.assertEqual(
            d.scan(),
            [],
            "no known fail-open shapes remain; if this fails, either a fix was "
            "reverted or a new one was introduced",
        )


class TestDetectorCli(unittest.TestCase):
    def test_exit_zero_on_clean_tree(self):
        result = subprocess.run(
            [sys.executable, str(DETECTOR)], capture_output=True, text=True, cwd=REPO_ROOT
        )
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)

    def test_exit_one_on_new_finding_and_baseline_absorbs_it(self):
        probe = REPO_ROOT / "trading_system" / "_fail_open_probe.py"
        baseline = d.BASELINE_PATH
        existed = baseline.exists()
        saved = baseline.read_text() if existed else None
        probe.write_text("def verify_probe(x):\n    return True\n    return False\n")
        try:
            failing = subprocess.run(
                [sys.executable, str(DETECTOR)], capture_output=True, text=True, cwd=REPO_ROOT
            )
            self.assertEqual(failing.returncode, 1, "a new finding must fail the gate")

            baselined = subprocess.run(
                [sys.executable, str(DETECTOR), "--baseline"],
                capture_output=True, text=True, cwd=REPO_ROOT,
            )
            self.assertEqual(baselined.returncode, 0)

            passing = subprocess.run(
                [sys.executable, str(DETECTOR)], capture_output=True, text=True, cwd=REPO_ROOT
            )
            self.assertEqual(passing.returncode, 0, "a baselined finding must not fail the gate")
        finally:
            probe.unlink(missing_ok=True)
            if existed:
                baseline.write_text(saved)
            elif baseline.exists():
                baseline.unlink()


if __name__ == "__main__":
    unittest.main()
