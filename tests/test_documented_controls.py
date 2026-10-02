"""Documented controls must actually exist and be wired.

Three controls in this repository were documented as active and were not:

  * AGENTS.md: "Per-module line + branch coverage gate (default 90%) enforced by
    scripts/coverage_gate.py". The script exists, but nothing in
    .github/workflows/ci.yml runs it, it has no baseline mode, and its input
    scripts/coverage/python_coverage.json is a committed artifact from July that
    nothing regenerates in CI.
  * deploy/README_DEPLOYMENT.md presented health_monitor.sh as the
    post-deployment health verification script. It inspects a Docker container
    that this deployment does not run.
  * deploy/portfolio-agent-watcher.service was shipped as an installable unit
    while portfolio-trader.service's own comment said the watcher is a managed
    child of run_production.py. Following the documented file produced a second,
    orphaned watcher.

A control that is documented but not enforced is worse than an absent one: it
buys the belief that something is being checked. This pins the specific claims
that were false and asserts every script path AGENTS.md names actually resolves,
so documentation cannot drift away from the tree unnoticed.
"""
from __future__ import annotations

import re
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
AGENTS = REPO_ROOT / "AGENTS.md"
CI = REPO_ROOT / ".github" / "workflows" / "ci.yml"

# Controls enforced by a CI job. Each must be referenced somewhere in ci.yml --
# not necessarily in the pytest argument list, since a gate can be its own step.
CI_ENFORCED = {
    "scripts/detect_python_fail_open.py": "fail-open detector",
    "tests/test_python_fail_closed.py": "fail-closed regression tests",
    "tests/test_python_fail_open_detector.py": "detector self-tests",
    "tests/test_supervisor_contract.py": "supervised startup contract",
    "tests/test_production_health.py": "supervisor health/readiness tests",
    "tests/test_systemd_units.py": "systemd unit contract tests",
    "tests/test_optimizer_trade_decision.py": "trade-decision characterisation",
    "tests/test_documented_controls.py": "this file",
}

# Controls enforced on the host rather than in CI. The health gate runs from a
# systemd timer; putting it in CI would only assert that the host is healthy at
# build time, which says nothing about the host it eventually runs on.
HOST_ENFORCED = {
    "scripts/health_check.py": "production health gate (systemd timer)",
}

# Claims that were false and are corrected here rather than left standing.
# Matched case-insensitively and as a pattern, not a literal: an earlier version
# used a case-sensitive substring and a mutation that wrote "Enforced by ..." was
# reported as MISSED, which is the wrong answer rather than a weak test.
CORRECTED_CLAIMS = (
    (re.compile(r"enforced by\s+`?scripts/coverage_gate\.py`?", re.I),
     "coverage_gate.py is not run by any CI job, has no baseline mode, and reads a "
     "committed coverage artifact that nothing regenerates; describing it as enforced "
     "buys a false belief"),
)


class TestEveryNamedScriptExists(unittest.TestCase):
    def test_scripts_referenced_in_agents_md_resolve(self):
        text = AGENTS.read_text(encoding="utf-8")
        # Paths like `scripts/foo.py`, `coinbase/src/bar.py`, `run_production.py`.
        referenced = set()
        for match in re.finditer(r"`([A-Za-z0-9_./-]+\.(?:py|mjs|sh|service|timer|yml))`", text):
            referenced.add(match.group(1))

        # AGENTS.md writes some paths relative to a parent named in the same table
        # cell: `trading_system/` for `core/portfolio_manager.py`,
        # `coinbase/src/` for `graph/sync_coingecko_universe.py`. Those resolve
        # against a documented base, not the repo root.
        bases = ("", "trading_system/", "coinbase/src/", "scripts/", "deploy/")

        missing = []
        for rel in sorted(referenced):
            if "/" not in rel:
                # Bare filenames can legitimately live in several places.
                continue
            if not any((REPO_ROOT / base / rel).exists() for base in bases):
                missing.append(rel)
        self.assertEqual(
            missing, [],
            f"AGENTS.md references files that do not exist: {missing}",
        )

    def test_claimed_enforced_controls_exist(self):
        for rel, description in {**CI_ENFORCED, **HOST_ENFORCED}.items():
            with self.subTest(control=rel):
                self.assertTrue(
                    (REPO_ROOT / rel).is_file(),
                    f"{description} is named as enforced but {rel} does not exist",
                )


class TestClaimedGatesAreActuallyWired(unittest.TestCase):
    def test_ci_file_exists(self):
        self.assertTrue(CI.is_file(), ".github/workflows/ci.yml is missing")

    def test_ci_enforced_controls_are_referenced_by_ci(self):
        ci_text = CI.read_text(encoding="utf-8")
        not_wired = [
            f"{rel} ({description})"
            for rel, description in CI_ENFORCED.items()
            if rel not in ci_text
        ]
        self.assertEqual(
            not_wired, [],
            f"documented as enforced but absent from ci.yml: {not_wired}",
        )

    def test_host_enforced_controls_are_referenced_by_a_deployed_unit(self):
        """The health gate is enforced by systemd on the host, not by CI.

        Asserting it against a systemd unit rather than ci.yml keeps the two kinds
        of enforcement from being conflated, which is how a control ends up
        documented as CI-enforced while only ever running on one machine.
        """
        unit = (REPO_ROOT / "deploy" / "portfolio-health-check.service").read_text(encoding="utf-8")
        timer = (REPO_ROOT / "deploy" / "portfolio-health-check.timer").read_text(encoding="utf-8")
        for rel, description in HOST_ENFORCED.items():
            with self.subTest(control=rel):
                self.assertIn(rel, unit, f"{description} is not run by any deployed unit")
        self.assertIn("portfolio-health-check.service", timer)

    def test_agents_md_does_not_claim_coverage_gate_is_enforced(self):
        text = AGENTS.read_text(encoding="utf-8")
        for pattern, why in CORRECTED_CLAIMS:
            with self.subTest(pattern=pattern.pattern):
                self.assertIsNone(
                    pattern.search(text),
                    f"AGENTS.md must not claim this: {why}",
                )

    def test_coverage_gate_is_either_wired_or_documented_as_local_only(self):
        """The honest position must be visible, whichever way it is resolved.

        Either ci.yml runs coverage_gate.py, or the documentation says plainly
        that it is a local tool. What is not acceptable is silence.
        """
        ci_text = CI.read_text(encoding="utf-8")
        agents_text = AGENTS.read_text(encoding="utf-8")
        wired = "coverage_gate.py" in ci_text
        admits_local = "not run in CI" in agents_text or "local tool" in agents_text
        self.assertTrue(
            wired or admits_local,
            "coverage_gate.py is neither wired into CI nor documented as a local "
            "tool; one of those must be true so the reader is not misled",
        )


class TestRetiredUnitStaysRetired(unittest.TestCase):
    def test_agent_watcher_unit_is_still_marked_do_not_install(self):
        path = REPO_ROOT / "deploy" / "portfolio-agent-watcher.service"
        if not path.is_file():
            self.skipTest("unit removed entirely, which is also acceptable")
        text = path.read_text(encoding="utf-8")
        self.assertIn(
            "DO NOT INSTALL", text,
            "this unit must not regress into an installable second supervisor for "
            "the agent watcher",
        )

    def test_health_monitor_sh_still_declares_it_does_not_cover_the_systemd_stack(self):
        path = REPO_ROOT / "deploy" / "health_monitor.sh"
        if not path.is_file():
            self.skipTest("script removed entirely, which is also acceptable")
        head = "\n".join(path.read_text(encoding="utf-8").splitlines()[:20])
        self.assertIn(
            "RETIRED", head.upper(),
            "this script inspects a container this deployment does not run; it must "
            "keep saying so at the top or operators will trust it",
        )


class TestHealthCheckIsWiredToATimer(unittest.TestCase):
    def test_timer_and_service_reference_the_real_check(self):
        service = REPO_ROOT / "deploy" / "portfolio-health-check.service"
        timer = REPO_ROOT / "deploy" / "portfolio-health-check.timer"
        self.assertTrue(service.is_file())
        self.assertTrue(timer.is_file())
        self.assertIn(
            "scripts/health_check.py", service.read_text(encoding="utf-8"),
            "the unit must run the check that actually exists",
        )

    def test_health_check_unit_does_not_require_the_stack_it_monitors(self):
        text = (REPO_ROOT / "deploy" / "portfolio-health-check.service").read_text(encoding="utf-8")
        for line in text.splitlines():
            stripped = line.strip()
            if stripped.startswith("#") or "=" not in stripped:
                continue
            self.assertFalse(
                stripped.startswith(("Requires=", "BindsTo=", "PartOf=")),
                "the health check must still run when the trading stack is broken",
            )


if __name__ == "__main__":
    unittest.main()