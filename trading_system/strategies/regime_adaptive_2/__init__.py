"""Regime-adaptive novelty strategies (batch 5b).

Pure-Python, deterministic signal strategies exploring genuinely new angles
not covered by the existing registry:

  * VolRegimeKalmanSwitchStrategy   -- hidden 2-state volatility model, trades
                                       the inferred regime.
  * KatzFractalBreakoutStrategy     -- Katz fractal-dimension breakout on price.
  * HurstAdaptiveLookbackStrategy   -- persistence-driven EMA lookback selection.
"""
from trading_system.strategies.regime_adaptive_2.vol_kalman_switch import (
    VolRegimeKalmanSwitchStrategy,
)
from trading_system.strategies.regime_adaptive_2.fractal_breakout import (
    KatzFractalBreakoutStrategy,
)
from trading_system.strategies.regime_adaptive_2.hurst_adaptive_lookback import (
    HurstAdaptiveLookbackStrategy,
)

__all__ = [
    "VolRegimeKalmanSwitchStrategy",
    "KatzFractalBreakoutStrategy",
    "HurstAdaptiveLookbackStrategy",
]
