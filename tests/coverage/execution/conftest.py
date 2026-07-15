"""Local conftest: alias top-level absolute imports used by the target
execution modules to their real ``trading_system.*`` packages.

The source modules import via top-level names (``execution.*``,
``market_data.*``, ``onchain.*``) which are not importable as standalone
packages. Aliasing them to the real ``trading_system.*`` packages lets the
target modules execute and be measured without modifying source business
logic.
"""

from __future__ import annotations

import importlib
import os
import sys
import types

_REPO = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def _alias(top: str, real: str) -> None:
    real_dir = os.path.join(_REPO, *real.split("."))
    placeholder = types.ModuleType(top)
    placeholder.__path__ = [real_dir]
    sys.modules[top] = placeholder
    try:
        imported = importlib.import_module(real)
    except Exception:
        return
    sys.modules[top] = imported
    sys.modules[real] = imported


_alias("execution", "trading_system.execution")
_alias("market_data", "trading_system.market_data")
_alias("onchain", "trading_system.onchain")
