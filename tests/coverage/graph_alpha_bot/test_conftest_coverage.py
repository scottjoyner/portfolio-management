"""Tests exercising the graph_alpha_bot coverage conftest (sys.path setup)."""
import importlib
import os
import subprocess
import sys

ROOT = "/home/scott/git/portfolio-management"
CONF_MOD = "tests.coverage.graph_alpha_bot.conftest"
CONF_FILE = os.path.join(ROOT, "tests", "coverage", "graph_alpha_bot", "conftest.py")


def _import_conftest():
    if ROOT not in sys.path:
        sys.path.insert(0, ROOT)
    return importlib.import_module(CONF_MOD)


def test_conftest_is_idempotent():
    # Importing twice must not duplicate paths.
    mod = _import_conftest()
    mod2 = _import_conftest()
    assert mod is mod2


def test_conftest_inserts_expected_paths():
    # Run the conftest standalone (no pytest sys.path manipulation) and verify
    # it inserts ROOT plus the two graph-alpha-bot dirs in the right order.
    script = (
        "import sys, runpy\n"
        f"sys.path.insert(0, {ROOT!r})\n"
        f"runpy.run_path({CONF_FILE!r}, run_name='__main__')\n"
        "ga = " + repr(os.path.join(ROOT, "graph-alpha-bot")) + "\n"
        "ga_strat = " + repr(os.path.join(ROOT, "graph-alpha-bot", "app", "strategies")) + "\n"
        "assert ga in sys.path, 'graph-alpha-bot not added'\n"
        "assert ga_strat in sys.path, 'graph-alpha-bot/app/strategies not added'\n"
        "# Both graph-alpha-bot dirs must take priority over ROOT.\n"
        "assert sys.path.index(ga_strat) < sys.path.index(" + repr(ROOT) + ")\n"
        "assert sys.path.index(ga) < sys.path.index(" + repr(ROOT) + ")\n"
        "print('OK')\n"
    )
    res = subprocess.run(
        [sys.executable, "-c", script],
        capture_output=True, text=True,
    )
    assert res.returncode == 0, res.stderr
    assert "OK" in res.stdout
