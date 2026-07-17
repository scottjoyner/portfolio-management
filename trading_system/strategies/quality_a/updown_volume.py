"""Intrabar intensity / up-down volume ratio (UVOL/DVOL proxy).

Approximates accumulation/distribution by splitting volume into up-volume
and down-volume using the prior close vs current close within each bar
(no per-trade data available). A rolling ratio above 1 = accumulation
(bullish); below 1 = distribution (bearish). We emit a continuous signal
scaled by how far the ratio deviates from 1, in the direction of the ratio.
"""
from __future__ import annotations

import math
from time import monotonic

from strategies.base import BaseSignalStrategy, StrategyConfig, StrategyMetadata
from trading_system.strategies.base.interfaces import StrategySignal


class UpDownVolumeRatioStrategy(BaseSignalStrategy):
    """Accumulation/distribution from up/down volume split proxy."""

    def __init__(self, window: int = 20) -> None:
        super().__init__(
            metadata=StrategyMetadata(
                strategy_id="UpDownVolumeRatioStrategy",
                strategy_type="volume",
                status="implemented",
                live_supported=False,
                replay_supported=True,
                backtest_supported=True,
                products=["BTC-USD"],
                data_requirements=["product_id", "closes", "volumes"],
                risk_mode_hint="NORMAL",
                capital_bucket="ACTIVE_TRADING",
            ),
            config=StrategyConfig(threshold=0.05, cooldown_seconds=0.0, warmup_period=window + 2),
        )
        self.window = window

    def generate_signal(self, market_state: dict) -> StrategySignal | None:
        disabled, _ = self.is_disabled(market_state)
        if disabled or self.in_cooldown():
            return None
        closes = market_state.get("closes") or []
        volumes = market_state.get("volumes") or []
        n = len(closes)
        if n < self.config.warmup_period + 1 or len(volumes) < n:
            return None

        win = min(self.window, n - 1)
        up_vol = dn_vol = 0.0
        for j in range(n - win, n):
            if closes[j] >= closes[j - 1]:
                up_vol += volumes[j]
            else:
                dn_vol += volumes[j]
        if dn_vol <= 0 and up_vol <= 0:
            return None
        ratio = up_vol / dn_vol if dn_vol > 0 else 5.0
        if up_vol <= 0:
            ratio = 0.0
        if abs(ratio - 1.0) <= 0.05:
            return None
        score = math.tanh((ratio - 1.0) * 1.5)
        if abs(score) <= self.config.threshold:
            return None
        self._last_emit_ts = monotonic()
        return StrategySignal(
            strategy_id=self.strategy_id,
            product_id=str(market_state.get("product_id", "BTC-USD")),
            score=max(-1.0, min(1.0, score)),
            reason=f"uvol={up_vol:.1f} dvol={dn_vol:.1f} ratio={ratio:.2f}",
            confidence=min(1.0, abs(score)),
            warmup_passed=bool(market_state.get("warmup_complete", True)),
            tags=[self.metadata_model.strategy_type, self.metadata_model.status],
            features={"ratio": round(ratio, 3)},
        )
