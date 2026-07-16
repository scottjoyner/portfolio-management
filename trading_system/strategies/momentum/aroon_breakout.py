from __future__ import annotations

from strategies.base import BaseSignalStrategy, StrategyConfig, StrategyMetadata


def _aroon(highs: list[float], lows: list[float], period: int = 25) -> tuple[float | None, float | None]:
    if len(highs) < period:
        return None, None
    window_highs = highs[-period:]
    window_lows = lows[-period:]
    idx_high = window_highs.index(max(window_highs))
    idx_low = window_lows.index(min(window_lows))
    aroon_up = 100.0 * idx_high / (period - 1)
    aroon_down = 100.0 * idx_low / (period - 1)
    return aroon_up, aroon_down


class AroonBreakoutMomentumStrategy(BaseSignalStrategy):
    def __init__(self) -> None:
        super().__init__(
            metadata=StrategyMetadata(
                strategy_id="AroonBreakoutMomentumStrategy",
                strategy_type="momentum",
                live_supported=True,
                data_requirements=["product_id", "highs", "lows", "warmup_complete"],
                risk_mode_hint="NORMAL",
                capital_bucket="ACTIVE_TRADING",
            ),
            config=StrategyConfig(threshold=0.4, cooldown_seconds=30, warmup_period=30),
        )

    def generate_signal(self, market_state: dict) -> "object":
        from trading_system.strategies.base.interfaces import StrategySignal
        import time

        disabled, _ = self.is_disabled(market_state)
        if disabled:
            return None
        if self.in_cooldown():
            return None

        highs = market_state.get("highs") or []
        lows = market_state.get("lows") or []
        if len(highs) < 25:
            return None

        aroon_up, aroon_down = _aroon(highs, lows, period=25)
        if aroon_up is None or aroon_down is None:
            return None

        if aroon_up > 70 and aroon_up > aroon_down:
            score = 0.5 + 0.5 * (aroon_up - aroon_down) / 100.0
        elif aroon_down > 70 and aroon_down > aroon_up:
            score = -(0.5 + 0.5 * (aroon_down - aroon_up) / 100.0)
        else:
            return None

        if score <= self.config.threshold:
            return None

        self._last_emit_ts = time.monotonic()
        return StrategySignal(
            strategy_id=self.strategy_id,
            product_id=str(market_state.get("product_id", "BTC-USD")),
            score=score,
            reason=f"AroonUp={aroon_up:.1f} AroonDown={aroon_down:.1f} (breakout momentum)",
            confidence=min(1.0, abs(score)),
            warmup_passed=bool(market_state.get("warmup_complete", True)),
            tags=[self.metadata_model.strategy_type, self.metadata_model.status],
            features={
                "aroon_up": round(aroon_up, 2),
                "aroon_down": round(aroon_down, 2),
            },
        )
