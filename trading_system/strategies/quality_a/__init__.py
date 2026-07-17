"""Quality-A strategy package: 8 novel, diversifying BaseSignalStrategy strategies.

Each module exposes a single strategy class plus a factory instance.
"""
from __future__ import annotations

from trading_system.strategies.quality_a.fisher_transform import (
    FisherTransformStochStrategy,
)
from trading_system.strategies.quality_a.chaikin_vol import (
    ChaikinVolatilityBreakoutStrategy,
)
from trading_system.strategies.quality_a.williams_r import WilliamsPctRStrategy
from trading_system.strategies.quality_a.updown_volume import (
    UpDownVolumeRatioStrategy,
)
from trading_system.strategies.quality_a.rvi import RelativeVigorIndexStrategy
from trading_system.strategies.quality_a.tema import TripleEmaTrendStrategy
from trading_system.strategies.quality_a.cci_short import CciShortReversalStrategy
from trading_system.strategies.quality_a.session_or import (
    SessionOpeningRangeBreakoutStrategy,
)

__all__ = [
    "FisherTransformStochStrategy",
    "ChaikinVolatilityBreakoutStrategy",
    "WilliamsPctRStrategy",
    "UpDownVolumeRatioStrategy",
    "RelativeVigorIndexStrategy",
    "TripleEmaTrendStrategy",
    "CciShortReversalStrategy",
    "SessionOpeningRangeBreakoutStrategy",
]

QUALITY_A_STRATEGIES = [
    FisherTransformStochStrategy(),
    ChaikinVolatilityBreakoutStrategy(),
    WilliamsPctRStrategy(),
    UpDownVolumeRatioStrategy(),
    RelativeVigorIndexStrategy(),
    TripleEmaTrendStrategy(),
    CciShortReversalStrategy(),
    SessionOpeningRangeBreakoutStrategy(),
]
