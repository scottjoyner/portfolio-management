from __future__ import annotations

import math

from strategies.base import BaseSignalStrategy, StrategyConfig, StrategyMetadata
from trading_system.strategies.base.interfaces import StrategySignal


class VolTargetOverlayStrategy(BaseSignalStrategy):
    """Volatility-targeting overlay: scale directional exposure by (target - actual) vol.

    When realized vol is BELOW target, increase directional exposure (bullish bias
    scaled by calm, gated by a fast MA trend). When vol is ABOVE target, reduce or
    flatten (bearish/neutral bias). Emits a signed score proportional to the sign of
    (target_vol - actual_vol), gated by trend direction from a fast MA.
    """

    def __init__(self, window: int = 40, fast_ma: int = 8, target_vol: float = 0.02) -> None:
        super().__init__(
            metadata=StrategyMetadata(
                strategy_id="VolTargetOverlay",
                strategy_type="risk",
                data_requirements=["product_id", "closes", "volumes", "warmup_complete"],
                risk_mode_hint="NORMAL",
                capital_bucket="ACTIVE_TRADING",
            ),
            config=StrategyConfig(threshold=0.1, cooldown_seconds=30, warmup_period=window),
        )
        self.window = window
        self.fast_ma = fast_ma
        self.target_vol = target_vol

    def generate_signal(self, market_state: dict) -> StrategySignal | None:
        disabled, _ = self.is_disabled(market_state)
        if disabled:
            return None
        if self.in_cooldown():
            return None

        closes = market_state.get("closes") or []
        if len(closes) < self.window:
            return None

        win = closes[-self.window:]
        log_rets = []
        for i in range(1, len(win)):
            p0, p1 = win[i - 1], win[i]
            if p0 > 0:
                log_rets.append(math.log(p1 / p0))
        if len(log_rets) < 2:
            return None

        mean = sum(log_rets) / len(log_rets)
        var = sum((r - mean) ** 2 for r in log_rets) / (len(log_rets) - 1)
        actual_vol = math.sqrt(var)

        # fast MA trend gate
        fm = closes[-self.fast_ma:]
        fast_ma_val = sum(fm) / len(fm)
        trend = 1.0 if closes[-1] >= fast_ma_val else -1.0

        # signed vol gap: calm -> positive exposure, elevated -> negative/flat
        vol_gap = (self.target_vol - actual_vol)
        if actual_vol > 0:
            norm_gap = max(-1.0, min(1.0, vol_gap / (actual_vol + 1e-9)))
        else:
            norm_gap = 0.0

        score = norm_gap * trend
        if abs(score) <= self.config.threshold:
            return None

        self._last_emit_ts = __import__("time").monotonic()
        return StrategySignal(
            strategy_id=self.strategy_id,
            product_id=str(market_state.get("product_id", "BTC-USD")),
            score=score,
            reason=f"vol_gap={vol_gap:.5f} trend={trend:+.0f} actual_vol={actual_vol:.5f}",
            confidence=min(1.0, abs(score)),
            warmup_passed=bool(market_state.get("warmup_complete", True)),
            tags=[self.metadata_model.strategy_type, self.metadata_model.status],
            features={
                "actual_vol": round(actual_vol, 6),
                "target_vol": round(self.target_vol, 6),
                "trend": trend,
            },
        )
