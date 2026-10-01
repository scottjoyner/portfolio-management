"""The systemd units are the production launch mechanism. Test them.

Nothing else checks them. A unit whose ExecStart names a moved or renamed script
is a stack that does not start; a unit that duplicates a component the supervisor
already manages produces two processes doing the same job, outside the
supervisor's accounting. Both have happened here:

  * deploy/portfolio-agent-watcher.service invited installing a second exit-only
    watcher. An orphan from exactly that unit was found running alongside the
    supervisor's own for five days, both doing read-modify->save on one ledger.
  * The dashboard refused to start without an operator token, and
    run_production.py restarts any child that exits -- a crash-loop on every
    service restart, invisible to unit tests because it only appears when the
    documented way to run the system is used.

Unit tests exercise the scripts. These exercise the descriptors that launch them.
"""
from __future__ import annotations

import os
import re
import shutil
import subprocess
import sys
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
DEPLOY = REPO_ROOT / "deploy"

# Units that are meant to be installable.
LIVE_UNITS = (
    "portfolio-trader.service",
    "portfolio-health-check.service",
    "portfolio-health-check.timer",
)

# Units kept in the tree only to explain why they must not be installed.
RETIRED_UNITS = ("portfolio-agent-watcher.service",)


def _unit_path(name: str) -> Path:
    return DEPLOY / name


def _directives(text: str) -> dict[str, list[str]]:
    out: dict[str, list[str]] = {}
    for line in text.splitlines():
        line = line.strip()
        if not line or line.startswith("#") or line.startswith("[") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        out.setdefault(key.strip(), []).append(value.strip())
    return out


class TestUnitFilesParse(unittest.TestCase):
    def test_all_units_exist(self):
        for name in LIVE_UNITS + RETIRED_UNITS:
            with self.subTest(unit=name):
                self.assertTrue(_unit_path(name).is_file(), f"{name} is missing from deploy/")

    def test_live_units_verify_with_systemd_analyze(self):
        if not shutil.which("systemd-analyze"):
            self.skipTest("systemd-analyze unavailable")
        for name in LIVE_UNITS:
            with self.subTest(unit=name):
                result = subprocess.run(
                    ["systemd-analyze", "verify", str(_unit_path(name))],
                    capture_output=True, text=True, timeout=120,
                )
                # `verify` returns non-zero for warnings too, so assert on the
                # message rather than the code.
                output = (result.stdout + result.stderr)
                self.assertNotIn("Failed to parse", output, output[:400])
                self.assertNotIn("Unknown key", output, output[:400])

    def test_retired_unit_would_fail_loudly_if_installed(self):
        """A retired unit must not silently succeed if it is already installed."""
        text = _unit_path("portfolio-agent-watcher.service").read_text()
        self.assertIn("DO NOT INSTALL", text)
        # If someone installs it, systemd runs ExecStart, which must explain
        # itself rather than starting a second watcher.
        self.assertIn("retired", text.lower())
        self.assertNotIn(
            "hermes_agent_watch.py --interval",
            "\n".join(
                line for line in text.splitlines()
                if line.strip().startswith("ExecStart=")
            ),
            "the retired unit must not execute the watcher",
        )


def _execstart_targets(name: str) -> list[str]:
    """Script paths named by a unit's ExecStart lines."""
    found = []
    for value in _directives(_unit_path(name).read_text()).get("ExecStart", []):
        for token in value.split():
            if token.endswith(".py") or token.endswith(".sh"):
                found.append(token)
    return found


class TestUnitLaunchTargetsExist(unittest.TestCase):
    """An ExecStart naming a moved script means the stack never starts."""

    def test_every_execstart_script_exists(self):
        for name in LIVE_UNITS:
            if name.endswith(".timer"):
                continue  # a timer schedules, it does not launch
            targets = _execstart_targets(name)
            self.assertTrue(targets, f"{name} has no ExecStart script to check")
            for target in targets:
                candidate = Path(target)
                if not candidate.is_absolute():
                    continue
                # Units pin an absolute install path; resolve it through the repo
                # so the check is meaningful on a checkout elsewhere.
                resolved = _resolve_install_path(target)
                with self.subTest(unit=name, target=target):
                    self.assertTrue(
                        resolved.is_file(),
                        f"{name} ExecStart names {target}, which does not exist "
                        f"(resolved to {resolved})",
                    )

    def test_working_directory_is_the_checkout(self):
        for name in LIVE_UNITS:
            for value in _directives(_unit_path(name).read_text()).get("WorkingDirectory", []):
                with self.subTest(unit=name, wd=value):
                    self.assertTrue(
                        _resolve_install_path(value).is_dir(),
                        f"{name} WorkingDirectory {value} does not resolve to a directory",
                    )

    def test_pythonpath_entries_exist(self):
        for name in LIVE_UNITS:
            text = _unit_path(name).read_text()
            for match in re.finditer(r"^Environment=PYTHONPATH=(.*)$", text, re.M):
                for entry in match.group(1).split(":"):
                    with self.subTest(unit=name, entry=entry):
                        self.assertTrue(
                            _resolve_install_path(entry).is_dir(),
                            f"{name} puts a nonexistent path on PYTHONPATH: {entry}",
                        )

    def test_supervisor_unit_launches_run_production(self):
        text = _unit_path("portfolio-trader.service").read_text()
        self.assertIn(
            "run_production.py start",
            "\n".join(_directives(text)["ExecStart"]),
            "the trading unit must launch the supervisor, not the children directly",
        )


class TestNoDuplicateSupervision(unittest.TestCase):
    """The guard that would have prevented the orphan watcher.

    run_production.py manages four children. Any unit that starts one of those
    scripts itself is a second supervisor for the same component.
    """

    def _supervised_scripts(self) -> set[str]:
        import json
        sys_path = str(REPO_ROOT)
        result = subprocess.run(
            [sys.executable, "-c",
             "import json,run_production;"
             "print(json.dumps({n: c['script'] for n, c in run_production.PROCESSES.items()}))"],
            capture_output=True, text=True, timeout=120, cwd=str(REPO_ROOT),
            env={**os.environ, "PYTHONPATH": sys_path},
        )
        self.assertEqual(result.returncode, 0, result.stderr[-300:])
        return set(json.loads(result.stdout).values())

    def test_no_live_unit_starts_a_supervised_child(self):
        supervised = self._supervised_scripts()
        for name in LIVE_UNITS:
            for target in _execstart_targets(name):
                for script in supervised:
                    if script.endswith(target.split("/")[-1]):
                        self.fail(
                            f"{name} starts {target}, which run_production.py already "
                            "supervises. Installing both starts two copies: a second "
                            "process doing the same job, invisible to the supervisor's "
                            "pidfile accounting, so it is never restarted or stopped."
                        )

    def test_retired_unit_named_a_supervised_child(self):
        self.assertTrue(
            any(s.endswith("hermes_agent_watch.py") for s in self._supervised_scripts()),
            "if the watcher is no longer supervised, this test's premise changed: "
            "update the retirement notice accordingly",
        )


class TestHealthCheckUnitMatchesTheRealCheck(unittest.TestCase):
    def test_unit_runs_the_health_check(self):
        text = _unit_path("portfolio-health-check.service").read_text()
        self.assertIn("scripts/health_check.py", text)

    def test_unit_does_not_depend_on_the_stack_it_monitors(self):
        """It must still run when the trading stack is what is broken."""
        directives = _directives(_unit_path("portfolio-health-check.service").read_text())
        for key in ("Requires", "BindsTo", "PartOf"):
            self.assertNotIn(
                key, directives,
                f"{key}= would stop the health check from running when the trading "
                "stack fails -- which is exactly when it must run",
            )


class TestRetiredScriptsAreMarked(unittest.TestCase):
    """A script that no longer monitors this deployment must say so."""

    def test_health_monitor_sh_is_marked_retired(self):
        path = DEPLOY / "health_monitor.sh"
        if not path.is_file():
            self.skipTest("health_monitor.sh not present")
        head = "\n".join(path.read_text().splitlines()[:20])
        self.assertIn("RETIRED", head.upper())
        self.assertIn("health_check.py", head,
                      "a retired monitor must point at what replaced it")


def _resolve_install_path(value: str) -> Path:
    """Map a pinned absolute install path back onto this checkout.

    The units hardcode /home/<user>/<repo>; on a checkout elsewhere that path does
    not exist and every assertion would be vacuous or falsely failing. This checks
    the trailing path components against the checkout instead.
    """
    candidate = Path(value)
    parts = candidate.parts
    repo_name = REPO_ROOT.name
    if repo_name in parts:
        index = len(parts) - 1 - list(reversed(parts)).index(repo_name)
        return REPO_ROOT.joinpath(*parts[index + 1:]) if index + 1 < len(parts) else REPO_ROOT
    return candidate


if __name__ == "__main__":
    unittest.main()