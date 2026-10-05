"""One resolver for every path under the operator's ``data/`` directory.

Why this module exists
----------------------
``tests/test_production_health.py::TestDataDirIsOverridable`` documents the
intent: *"The data directory was a hardcoded relative path in a dozen places...
a cleanup that globbed a state filename there deleted a paper-trading ledger and
all four of its backups, irrecoverably."* Resolving through a single function
was supposed to fix that.

It did not, because the fix was applied only to ``run_trader_v4`` and never to
the rest of the tree. As of 2026-10-04 these call sites still hardcoded
``data/...``, so pointing ``TRADING_DATA_DIR`` at a temp directory did not move
them:

* ``coinbase/src/live_performance.py``    ``data/live_performance.json``
* ``coinbase/src/capital_buckets.py``     ``data/capital_buckets.json``
* ``coinbase/src/strategy_registry.py``   ``data/strategy_perf.db``
* ``coinbase/src/config.py``              ``data/trading_kill_switch``
* ``coinbase/src/orchestrator.py``        ``data/pending_approvals.json``
* ``coinbase/src/run_trader_v4.py``       ``data/bot_killed_strategies.json``,
                                          ``data/unified_expectancy.json``
* ``approval_server.py``                  ``data/pending_approvals.json``
* ``run_production.py``                   ``data/trader_state_corrupt``

Measured consequence, running the test suite with ``TRADING_DATA_DIR`` set:
``live_performance.json`` was written 332 times, ``capital_buckets.json`` 105
times, and one suite *deleted* the live ``data/trading_kill_switch`` — which on a
live deployment is the file that stops trading.

Contract
--------
:func:`data_dir` returns the override when ``TRADING_DATA_DIR`` is set, and
otherwise the relative ``data`` path — deliberately relative, because callers,
tests, and the running services all compare against ``"data"`` and changing it to
an absolute path would silently relocate a deployment's state.

:func:`resolve` rewrites a hardcoded ``data/...`` default through that override
while leaving every other path untouched, so it is safe to apply to a literal
that may or may not be a data path.
"""

from __future__ import annotations

import os
from pathlib import Path

__all__ = [
    "DATA_DIR_NAME",
    "data_dir",
    "state_path",
    "resolve",
    "absolute",
    "is_data_path",
]

#: The conventional name of the operator data directory, relative to the repo.
DATA_DIR_NAME = "data"


def data_dir() -> Path:
    """Directory holding operator state.

    Honours ``TRADING_DATA_DIR`` so a run can be pointed at scratch storage.
    With no override this is the relative ``data`` path, which is what the
    deployed services expect.
    """
    override = os.environ.get("TRADING_DATA_DIR")
    if override:
        return Path(override)
    return Path(DATA_DIR_NAME)


def state_path(*parts: str) -> Path:
    """A path inside :func:`data_dir`."""
    return data_dir().joinpath(*parts)


def is_data_path(value: str | os.PathLike[str]) -> bool:
    """True when *value* is the literal ``data`` directory or something under it."""
    try:
        parts = Path(value).parts
    except TypeError:
        return False
    return bool(parts) and parts[0] == DATA_DIR_NAME


def resolve(value: str | os.PathLike[str]) -> Path:
    """Return *value* with any leading ``data/`` segment replaced by :func:`data_dir`.

    Lets a module keep a readable default::

        KILL_PATH = paths.resolve("data/trading_kill_switch")

    instead of a literal that no test can redirect.
    """
    candidate = Path(value)
    if not is_data_path(candidate):
        return candidate
    return data_dir().joinpath(*candidate.parts[1:])


def absolute(value: str | os.PathLike[str], base: str | os.PathLike[str]) -> Path:
    """:func:`resolve` anchored to *base* when the result is still relative.

    Use this in anything invoked from an arbitrary working directory — cron
    entries, chat-triggered scripts — where a bare relative ``data/...`` would
    resolve against whatever directory the scheduler happened to use.

    It preserves both properties that matter: an explicit ``TRADING_DATA_DIR``
    override is honoured as given, and the *default* stays absolute exactly as
    it was before, rather than silently becoming CWD-dependent. Getting this
    wrong in either direction is a real fault: too relative and a cron job
    writes to the wrong tree, too absolute and a test cannot redirect it.
    """
    resolved = resolve(value)
    if resolved.is_absolute():
        return resolved
    return Path(base) / resolved