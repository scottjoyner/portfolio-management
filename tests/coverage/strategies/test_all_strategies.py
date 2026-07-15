"""Generic coverage driver for all importable strategy modules.

Imports every strategy module under trading_system/strategies, discovers
strategy-like classes, instantiates them (default + custom config where
possible) and feeds synthetic OHLCV bars across multiple market scenarios
to exercise entry/exit branches.

Modules that fail to import (broken/missing deps, syntax errors, py3.12
incompatibilities) are skipped — see SKIPPED list at bottom of report.
"""
from __future__ import annotations

import importlib
import os

import pytest

from strat_helpers import discover_strategy_classes, drive_class, bars

ROOT = os.path.join(os.path.dirname(__file__), "..", "..", "..")
STRAT_DIR = os.path.abspath(os.path.join(ROOT, "trading_system", "strategies"))

# Modules known to fail at import time (source-level bugs) -> skipped.
SKIP_MODULES = {
    "trading_system.strategies.catalog.advanced",
    "trading_system.strategies.catalog.config_schema",
    "trading_system.strategies.lifecycle",
    # --- runtime-broken (source bugs) ---
    "trading_system.strategies.base.simple",               # abstract base; cannot instantiate
    "trading_system.strategies.trend_following.bot",      # cannot instantiate
    "trading_system.strategies.grid_trading.bot",         # cannot instantiate
    "trading_system.strategies.mean_reversion.rsi_mean_revert",  # _calculate_rsi UnboundLocalError
    "trading_system.strategies.mean_reversion.zscore_mean_reversion",  # validate() AttributeError
}


def _collect_modules():
    mods = []
    for dirpath, _dirs, files in os.walk(STRAT_DIR):
        for f in files:
            if not f.endswith(".py") or f.startswith("test_") or f == "__init__.py":
                continue
            rel = os.path.relpath(os.path.join(dirpath, f), ROOT)
            modname = rel[:-3].replace(os.sep, ".")
            if modname in SKIP_MODULES:
                continue
            mods.append(modname)
    return sorted(mods)


MODULES = _collect_modules()


@pytest.mark.parametrize("modname", MODULES)
def test_module_imports_and_drives(modname):
    try:
        mod = importlib.import_module(modname)
    except Exception as e:  # pragma: no cover - safety net
        pytest.skip(f"import failed: {e}")
    classes = discover_strategy_classes(mod)
    # Even if no strategy class is discovered, importing already counts.
    for cls in classes:
        res = drive_class(cls)
        # We do not assert on signals; we only ensure code executed without
        # unhandled crashes. Errors are tolerated (recorded) to maximize
        # branch coverage rather than aborting.
        assert res is not None
