"""
Donchian Channel Mean Reversion Strategy (BaseSignalStrategy implementation).

Mean reversion against a Donchian channel: when price reverts toward the channel
midpoint after tagging (or approaching) an extreme boundary, emit a signal. A
pierce of the lower bound is treated as oversold (BUY); a pierce of the upper
bound as overbought (SELL). Signal strength scales with how far price deviated
past the boundary, normalized by channel width.
"""
from __future__ import annotations

from time import monotonic

from strategies.base import BaseSignalStrategy, StrategyConfig, StrategyMetadata, StrategySignal


class DonchianMeanReversionStrategy(BaseSignalStrategy):
    def __init__(self, period: int = 20) -> None:
        self._period = period
        super().__init__(
            metadata=StrategyMetadata(
                strategy_id="DonchianMeanReversionStrategy",
                strategy_type="mean_reversion",
                live_supported=True,
                data_requirements=["product_id", "ohlc_history", "close"],
                risk_mode_hint="CONSERVATIVE",
                capital_bucket="ACTIVE_TRADING",
            ),
            config=StrategyConfig(threshold=0.05, cooldown_seconds=30, warmup_period=period),
        )

    def generate_signal(self, market_state: dict) -> StrategySignal | None:
        disabled, _ = self.is_disabled(market_state)
        if disabled:
            return None
        if self.in_cooldown():
            return None

        ohlc = market_state.get("ohlc_history")
        if not ohlc or len(ohlc) < self._period:
            return None

        window = ohlc[-self._period:]
        highs = [float(b.get("high", 0.0)) for b in window]
        lows = [float(b.get("low", 0.0)) for b in window]
        upper = max(highs)
        lower = min(lows)
        width = upper - lower
        if width <= 0:
            return None

        price = float(market_state.get("close", 0.0))
        if price <= 0:
            return None

        score = 0.0
        reason = ""
        if price <= lower:
            score = min(1.0, (lower - price) / width + 0.1)
            reason = f"price {price:.2f} at/below Donchian low {lower:.2f} (oversold)"
        elif price >= upper:
            score = min(1.0, (price - upper) / width + 0.1)
            reason = f"price {price:.2f} at/above Donchian high {upper:.2f} (overbought)"

        if score <= self.config.threshold:
            return None

        self._last_emit_ts = monotonic()
        return StrategySignal(
            strategy_id=self.strategy_id,
            product_id=str(market_state.get("product_id", "BTC-USD")),
            score=score,
            reason=reason,
            confidence=min(1.0, max(0.0, score)),
            warmup_passed=True,
            tags=[self.metadata_model.strategy_type, self.metadata_model.status],
            features={"donchian_high": upper, "donchian_low": lower, "width": width, "price": price},
        )

    def explain_trade(self, signal: StrategySignal) -> str:
        return (
            f"{self.strategy_id} {signal.product_id}: {signal.reason} "
            f"(score={signal.score:.3f}, confidence={signal.confidence:.2f})"
        )
