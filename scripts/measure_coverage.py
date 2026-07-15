#!/usr/bin/env python3
"""Measure coverage for the 16 in-scope modules against the >=90% line / >=90% branch gate.

Why two measurement modes:
  * Real packages (trading_system.*, alembic) are measured from the repo root
    so the repo .coveragerc (source = .) applies; we then pick the target file
    out of the JSON report.
  * The tests.* helpers live under a `tests` namespace. The repo .coveragerc
    omits */tests/*, so they are measured from a NEUTRAL cwd (/tmp) with an
    explicit --source and the absolute test path.
"""
import json
import os
import subprocess
import sys
import tempfile

ROOT = "/home/scott/git/portfolio-management"
COV = os.path.join(ROOT, ".venv", "bin", "coverage")
GATE = 90.0
TMP = tempfile.mkdtemp()

# (label, source_for_report_glob, cwd, test_path, optional explicit --source)
CASES = [
    # real packages -> repo root, .coveragerc, filter by file
    ("trading_system/unified_execution/models.py", "trading_system/unified_execution/models.py", ROOT,
     "tests/coverage/unified_execution/test_models.py", None),
    ("trading_system/unified_execution/interfaces.py", "trading_system/unified_execution/interfaces.py", ROOT,
     "tests/coverage/unified_execution/test_interfaces.py", None),
    ("trading_system/unified_execution/adapters/mock.py", "trading_system/unified_execution/adapters/mock.py", ROOT,
     "tests/coverage/unified_execution/test_mock_adapter.py", None),
    ("trading_system/unified_execution/adapters/coinbase.py", "trading_system/unified_execution/adapters/coinbase.py", ROOT,
     "tests/coverage/unified_execution/test_coinbase_adapter.py", None),
    ("trading_system/database/queries/accounts.py", "trading_system/database/queries/accounts.py", ROOT,
     "tests/coverage/database/test_accounts.py", None),
    ("trading_system/database/queries/positions.py", "trading_system/database/queries/positions.py", ROOT,
     "tests/coverage/database/test_positions.py", None),
    ("trading_system/database/queries/trades.py", "trading_system/database/queries/trades.py", ROOT,
     "tests/coverage/database/test_trades.py", None),
    ("trading_system/database/queries/auto_approval_rules.py", "trading_system/database/queries/auto_approval_rules.py", ROOT,
     "tests/coverage/database/test_auto_approval_rules.py", None),
    ("trading_system/alembic/env.py", "trading_system/alembic/env.py", ROOT,
     "tests/coverage/alembic/test_env.py", None),
    ("trading_system/alembic/versions/0001_initial.py", "trading_system/alembic/versions/0001_initial.py", ROOT,
     "tests/coverage/alembic/test_versions.py", None),
    ("trading_system/alembic/versions/0002_onchain_runtime.py", "trading_system/alembic/versions/0002_onchain_runtime.py", ROOT,
     "tests/coverage/alembic/test_versions.py", None),
    ("trading_system/ui/dashboard_server.py", "trading_system/ui/dashboard_server.py", ROOT,
     "tests/coverage/ui/", None),
    # tests.* helpers -> /tmp with explicit --source
    ("tests/coverage/event_markets/em_helpers.py", "tests/coverage/event_markets/em_helpers.py", "/tmp",
     f"{ROOT}/tests/coverage/event_markets/test_em_helpers.py",
     "tests.coverage.event_markets.em_helpers"),
    ("tests/coverage/strategies/strat_helpers.py", "tests/coverage/strategies/strat_helpers.py", "/tmp",
     f"{ROOT}/tests/coverage/strategies/test_strat_helpers.py",
     "tests.coverage.strategies.strat_helpers"),
    ("tests/coverage/graph_alpha_bot/conftest.py", "tests/coverage/graph_alpha_bot/conftest.py", "/tmp",
     f"{ROOT}/tests/coverage/graph_alpha_bot/test_conftest_coverage.py",
     "tests.coverage.graph_alpha_bot.conftest"),
]


def measure(label, report_glob, cwd, test_path, explicit_source):
    df = os.path.join(TMP, label.replace("/", "_").replace(".", "_") + ".coverage")
    cmd = [COV, "run", "--branch", f"--data-file={df}", "-m", "pytest", test_path, "-q"]
    if explicit_source:
        cmd.insert(3, f"--source={explicit_source}")
    subprocess.run(cmd, cwd=cwd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    jf = df + ".json"
    subprocess.run([COV, "json", "-o", jf, f"--data-file={df}"],
                   stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    if not os.path.exists(jf):
        return None, None
    data = json.load(open(jf))
    # pick the file whose path ends with the report glob
    cand = [v for k, v in data["files"].items() if k.endswith(report_glob)]
    if not cand:
        return None, None
    info = cand[0]["summary"]
    line = info["percent_covered"]
    branches = info.get("num_branches", 0)
    br_missing = info.get("missing_branches", 0)
    branch = 100.0 if branches == 0 else (branches - br_missing) / branches * 100.0
    return line, branch


def main():
    results = []
    for label, glob, cwd, test, src in CASES:
        line, branch = measure(label, glob, cwd, test, src)
        if line is None:
            results.append((label, False, None, None))
        else:
            ok = line >= GATE and branch >= GATE
            results.append((label, ok, line, branch))
    print("==================== COVERAGE GATE (>=%.0f%% line & branch) ====================" % GATE)
    npass = nfail = 0
    for label, ok, line, branch in results:
        if ok is None or line is None or branch is None:
            print(f"FAIL  {label}  (no data collected)")
            nfail += 1
        elif ok:
            print(f"PASS  {label}  line={line:.1f}% branch={branch:.1f}%")
            npass += 1
        else:
            print(f"FAIL  {label}  line={line:.1f}% branch={branch:.1f}% (gate={GATE:.0f})")
            nfail += 1
    print("==================== SUMMARY ====================")
    print(f"PASS={npass} FAIL={nfail}")
    return 1 if nfail else 0


if __name__ == "__main__":
    sys.exit(main())
