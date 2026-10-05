"""Fail-fast guard: tests must never write into the repository's live ``data/``.

Why this exists
---------------
``tests/coverage/coinbase/conftest.py`` redirects ``TRADING_DATA_DIR`` to a
per-test temp dir, and that is necessary but not sufficient. It only holds while
a fixture is active, and several code paths in the trader outlive it:

* background ``socketserver`` handler threads started by ``TestHealthServer``
  keep running after their test finishes (the root ``conftest.py`` forces every
  thread to be a daemon so the process can exit), and
* a leaked thread calling ``_paper_close_position`` reaches
  ``_save_paper_state`` with ``TRADING_DATA_DIR`` unset, so ``_data_dir()``
  falls back to the CWD-relative ``data/``.

Measured on 2026-10-04, before this guard: running the test suite wrote
``live_performance.json`` 332 times, ``capital_buckets.json`` 105 times, and
``paper_trader_v4_state.json`` 56 times, plus its ``.bak``/``.bak2``/``.bak3``
rotation and ``.tmp``. That destroyed a live paper-trading ledger and the
trader's live-performance records. Restoring the ledger was only possible
because a ``.bak3`` sibling happened to survive.

This module makes that class of damage *loud* instead of silent. It patches the
filesystem entry points a write can go through and raises when the target is
inside the repository's ``data/`` directory.

Scope and escape hatch
----------------------
Read-only access is never blocked, and neither is anything outside ``data/``.
A test that genuinely must write there can set
``PYTEST_ALLOW_LIVE_DATA_WRITES=1``, which downgrades the raise to a warning.
Do not reach for that to silence a real leak; redirect the path instead.
"""

from __future__ import annotations

import builtins
import os
import pathlib
import warnings
from typing import Any

__all__ = ["install", "uninstall", "live_data_dir", "ALLOW_ENV"]

ALLOW_ENV = "PYTEST_ALLOW_LIVE_DATA_WRITES"

# Written by the live system, read-only for tests, and safe to leave alone even
# though they sit in data/. Anything not listed here raises.
_NEVER_BLOCKED_NAMES = frozenset({".gitkeep"})

_ORIGINALS: dict[str, Any] = {}
_INSTALLED = False


def live_data_dir() -> pathlib.Path:
    """The repository's live data directory."""
    return (pathlib.Path(__file__).resolve().parent.parent / "data").resolve()


def _is_blocked(path: Any) -> bool:
    try:
        candidate = pathlib.Path(path)
    except TypeError:
        return False
    try:
        resolved = candidate.resolve()
    except (OSError, ValueError):
        return False
    data = live_data_dir()
    if resolved == data:
        return True
    # Must be a *descendant* test, not a direct-parent test: operator state is
    # not only data/x.json but also data/state_backups/x.json and
    # data/feed_cache/*.parquet.
    if data not in resolved.parents:
        return False
    return resolved.name not in _NEVER_BLOCKED_NAMES


def _check(path: Any, how: str) -> None:
    if not _is_blocked(path):
        return
    message = (
        f"test wrote into the live data directory via {how}: {path}\n"
        f"Live operator state lives in {live_data_dir()}. Point the code under "
        f"test at a tmp_path (or set TRADING_DATA_DIR) instead of writing there."
    )
    if os.environ.get(ALLOW_ENV) == "1":
        warnings.warn(message, RuntimeWarning, stacklevel=3)
        return
    raise RuntimeError(message)


def install() -> None:
    """Patch the filesystem write entry points. Idempotent."""
    global _INSTALLED
    if _INSTALLED:
        return

    _ORIGINALS["Path.write_text"] = pathlib.Path.write_text
    _ORIGINALS["Path.write_bytes"] = pathlib.Path.write_bytes
    _ORIGINALS["Path.open"] = pathlib.Path.open
    _ORIGINALS["Path.replace"] = pathlib.Path.replace
    _ORIGINALS["Path.unlink"] = pathlib.Path.unlink
    _ORIGINALS["Path.touch"] = pathlib.Path.touch
    _ORIGINALS["os.replace"] = os.replace
    _ORIGINALS["os.rename"] = os.rename
    _ORIGINALS["os.remove"] = os.remove
    _ORIGINALS["os.unlink"] = os.unlink
    _ORIGINALS["builtins.open"] = builtins.open

    def _method_guard(key: str, how: str):
        original = _ORIGINALS[key]

        def guarded(self, *args, **kwargs):
            _check(self, how)
            return original(self, *args, **kwargs)

        guarded.__name__ = original.__name__
        return guarded

    def _open_method_guard(self, mode="r", *args, **kwargs):
        # Path.open defaults to mode "r"; only a mutating mode is a write.
        # Without this, every read of a live state file would fail the suite.
        if isinstance(mode, str) and any(ch in mode for ch in "wxa+"):
            _check(self, "Path.open")
        return _ORIGINALS["Path.open"](self, mode, *args, **kwargs)

    def _replace_guard(self, target, *args, **kwargs):
        _check(self, "Path.replace (source)")
        _check(target, "Path.replace (target)")
        return _ORIGINALS["Path.replace"](self, target, *args, **kwargs)

    def _os_guard(key: str, how: str, index: int):
        original = _ORIGINALS[key]

        def guarded(*args, **kwargs):
            if len(args) > index:
                _check(args[index], how)
            return original(*args, **kwargs)

        guarded.__name__ = original.__name__
        return guarded

    def _open_guard(file, *args, **kwargs):
        mode = args[0] if args else kwargs.get("mode", "r")
        if isinstance(mode, str) and any(ch in mode for ch in "wxa+"):
            _check(file, "open()")
        return _ORIGINALS["builtins.open"](file, *args, **kwargs)

    pathlib.Path.write_text = _method_guard("Path.write_text", "Path.write_text")
    pathlib.Path.write_bytes = _method_guard("Path.write_bytes", "Path.write_bytes")
    pathlib.Path.open = _open_method_guard
    pathlib.Path.unlink = _method_guard("Path.unlink", "Path.unlink")
    pathlib.Path.touch = _method_guard("Path.touch", "Path.touch")
    pathlib.Path.replace = _replace_guard
    os.replace = _os_guard("os.replace", "os.replace", 1)
    os.rename = _os_guard("os.rename", "os.rename", 1)
    os.remove = _os_guard("os.remove", "os.remove", 0)
    os.unlink = _os_guard("os.unlink", "os.unlink", 0)
    builtins.open = _open_guard
    _INSTALLED = True


def uninstall() -> None:
    """Restore the real filesystem functions."""
    global _INSTALLED
    if not _INSTALLED:
        return
    pathlib.Path.write_text = _ORIGINALS["Path.write_text"]
    pathlib.Path.write_bytes = _ORIGINALS["Path.write_bytes"]
    pathlib.Path.open = _ORIGINALS["Path.open"]
    pathlib.Path.unlink = _ORIGINALS["Path.unlink"]
    pathlib.Path.touch = _ORIGINALS["Path.touch"]
    pathlib.Path.replace = _ORIGINALS["Path.replace"]
    os.replace = _ORIGINALS["os.replace"]
    os.rename = _ORIGINALS["os.rename"]
    os.remove = _ORIGINALS["os.remove"]
    os.unlink = _ORIGINALS["os.unlink"]
    builtins.open = _ORIGINALS["builtins.open"]
    _INSTALLED = False