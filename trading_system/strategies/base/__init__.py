"""
Base Strategy Classes and Interfaces
"""
from trading_system.strategies.base.interfaces import (
    Strategy,
    StrategyConfig,
    StrategyMetadata,
    StrategySignal,
)
from trading_system.strategies.base.simple import (
    BaseSignalStrategy,
    SimpleSignalModel,
)

# ---------------------------------------------------------------------------
# Backward-compatibility shims.
#
# A sibling module ``base.py`` (shadowed by this package) historically defined
# the reusable strategy building blocks used by several strategy modules
# (``BaseStrategy``, ``OHLCVBar`` and the ``compute_*`` helpers).  Because the
# package takes import precedence over the module of the same name, those names
# would otherwise be unreachable.  Load the legacy module by file location and
# re-export the building blocks so downstream imports keep working.
import importlib.util as _ilu
import os as _os
import sys as _sys

_legacy_path = _os.path.join(
    _os.path.dirname(_os.path.dirname(_os.path.abspath(__file__))), "base.py"
)
_spec = _ilu.spec_from_file_location(__name__ + "._legacy", _legacy_path)
_legacy = _ilu.module_from_spec(_spec)
_sys.modules[_spec.name] = _legacy
_spec.loader.exec_module(_legacy)

BaseStrategy = _legacy.BaseStrategy
OHLCVBar = _legacy.OHLCVBar
compute_sma = _legacy.compute_sma
compute_ema = _legacy.compute_ema
compute_z_score = _legacy.compute_z_score

__all__ = [
    "BaseSignalStrategy",
    "SimpleSignalModel",
    "Strategy",
    "StrategyConfig",
    "StrategyMetadata",
    "StrategySignal",
    "BaseStrategy",
    "OHLCVBar",
    "compute_sma",
    "compute_ema",
    "compute_z_score",
]
