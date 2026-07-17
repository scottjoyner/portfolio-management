from trading_system.strategies.quality_b.kaufman_efficiency_crossover import KaufmanEfficiencyCrossover
from trading_system.strategies.quality_b.elder_power_trend import ElderPowerTrend
from trading_system.strategies.quality_b.chande_kroll_stop_trend import ChandeKrollStopTrend
from trading_system.strategies.quality_b.roc_decel_momentum import RocDecelMomentum
from trading_system.strategies.quality_b.relative_volatility_index import RelativeVolatilityIndex
from trading_system.strategies.quality_b.envelope_reversion import EnvelopeReversion
from trading_system.strategies.quality_b.aroon_trend_composite import AroonTrendComposite
from trading_system.strategies.quality_b.narrow_range_breakout import NarrowRangeBreakout

CLASSES = [
    KaufmanEfficiencyCrossover,
    ElderPowerTrend,
    ChandeKrollStopTrend,
    RocDecelMomentum,
    RelativeVolatilityIndex,
    EnvelopeReversion,
    AroonTrendComposite,
    NarrowRangeBreakout,
]

__all__ = [c.__name__ for c in CLASSES]

ALL_QUALITY_B = [c() for c in CLASSES]
