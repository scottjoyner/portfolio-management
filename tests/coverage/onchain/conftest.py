"""Import wiring for the onchain coverage suite.

Why this file exists
--------------------
``trading_system/onchain/`` imports its siblings by *bare top-level name* rather
than through the ``trading_system`` package — 86 ``from onchain...`` statements,
plus one each of ``from risk.engine`` and ``from execution.hybrid``. That only
resolves when ``trading_system/`` itself is on ``sys.path``.

So every test under this directory used to fail collection with
``ModuleNotFoundError: No module named 'onchain'`` — 86 modules, 100% of the
suite. The only way to run it was to prefix the command with
``PYTHONPATH=trading_system``, which is not what AGENTS.md documents:

    for d in tests/coverage/*/; do
      .venv/bin/python -m coverage run --append --source=. -m pytest "$d"
    done

That mismatch is why the documented workflow reported a clean run while this
directory could not be collected at all: the claim was never checked for this
module. With the aliases below, ``pytest tests/coverage/onchain/`` works as
documented (455 tests, no environment prefix).

Why these four names and not a blanket sys.path entry
-----------------------------------------------------
``risk`` and ``execution`` are also the names of sibling test directories
(``tests/coverage/risk/``, ``tests/coverage/execution/``). Aliasing them in the
shared ``tests/coverage/conftest.py`` would change what every other suite sees,
which is exactly the collateral that forced the coinbase data-dir fixture to be
scoped rather than tree-wide. Scoping the alias to the suite that needs it keeps
that blast radius at zero.

``core`` is aliased here too because ``strategies/treasury/profit_sweep.py``
imports ``from core.models.domain``. The shared conftest already aliases ``core``
for the same reason; repeating it here means this directory also works when run
on its own, without depending on import order.
"""

from __future__ import annotations

import importlib
import os
import sys
import types

# tests/coverage/onchain/conftest.py -> repo root is four levels up.
REPO = os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)), *[os.pardir] * 3))

_ALIASES = {
    "onchain": "trading_system.onchain",
    "risk": "trading_system.risk",
    "execution": "trading_system.execution",
    "core": "trading_system.core",
}


def _alias(top: str, real: str) -> None:
    """Expose the real ``trading_system.*`` package under the bare name *top*."""
    real_dir = os.path.join(REPO, *real.split("."))
    if not os.path.isdir(real_dir):
        return
    placeholder = types.ModuleType(top)
    placeholder.__path__ = [real_dir]
    sys.modules[top] = placeholder
    try:
        imported = importlib.import_module(real)
    except Exception:
        # Leave the placeholder: it still makes submodules importable by path,
        # which is strictly better than the bare ModuleNotFoundError.
        return
    sys.modules[top] = imported
    sys.modules[real] = imported


for _top, _real in _ALIASES.items():
    _alias(_top, _real)