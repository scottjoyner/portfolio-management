"""Data-directory isolation for the coinbase coverage suites.

Production modules resolve state through ``_data_dir()``, which honours
``TRADING_DATA_DIR`` and otherwise falls back to the CWD-relative ``data/``.
Without redirection, any suite here that constructs a trader writes real state
into the repository's ``data/`` -- ``paper_trader_v4_state.json``,
``core_holdings.json``, ``strategy_perf.db`` and ``.bak*`` siblings -- and the
next suite in the same run loads them at construction.

That is not hypothetical. Measured on this directory:

- 31 failures across 11 files, every one of which passed in isolation.
- 248 skipped, because ``test_run_trader_v4.py``'s live-data guard found a
  ``paper_trader_v4_state.json`` left behind by a *different* suite and refused
  to run at all -- the pollution disabled the guard it depended on.

Redirecting here fixes the whole directory at one point and means no test here
can reach operator state.

This is scoped to ``coinbase/`` on purpose. An earlier version put the fixture in
``tests/coverage/conftest.py`` and applied it tree-wide, which broke six tests in
``optimizer/`` and ``trading_system_api/`` that assert on repo-relative ``data/``
paths. Those suites were not the ones with the problem, so they should not have
been changed; if they want the same isolation, it belongs in their own conftest
alongside fixing their hardcoded paths.
"""

from __future__ import annotations

import os
import shutil
import tempfile

import pytest


@pytest.fixture(autouse=True)
def _isolate_trading_data_dir():
    """Point each test in this directory at a private, empty data directory."""
    prev = os.environ.get("TRADING_DATA_DIR")
    tmp = tempfile.mkdtemp(prefix="covtest-coinbase-data-")
    os.environ["TRADING_DATA_DIR"] = tmp
    try:
        yield tmp
    finally:
        if prev is None:
            os.environ.pop("TRADING_DATA_DIR", None)
        else:
            os.environ["TRADING_DATA_DIR"] = prev
        shutil.rmtree(tmp, ignore_errors=True)
