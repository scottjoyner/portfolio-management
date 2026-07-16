"""
Bollinger-Band Mean Reversion Strategy (BaseSignalStrategy implementation).

Mean reversion: price touches the lower band (oversold) -> BUY signal;
price touches the upper band (overbought) -> SELL signal. Signal magnitude
scales with how far price pierces the band.
"""
from __future__ import annotations

import math
from time import monotonic

from strategies.base import BaseSignalStrategy, StrategyConfig, StrategyMetadata, StrategySignal


class BollingerBandReversionStrategy(BaseSignalStrategy):
    def __init__(self, period: int = 20, num_std: float = 2.0) -> None:
        self._period = period
        self._num_std = num_std
        super().__init__(
            metadata=StrategyMetadata(
                strategy_id="BollingerBandReversionStrategy",
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

        closes = [float(b.get("close", 0.0)) for b in ohlc]
        if len(closes) < self._period:
            return None

        window = closes[-self._period:]
        mean = sum(window) / len(window)
        var = sum((p - mean) ** 2 for p in window) / len(window)
        std = math.sqrt(var)
        if std <= 0:
            return None

        price = float(market_state.get("close", closes[-1]))
        upper = mean + self._num_std * std
        lower = mean - self._num_std * std

        score = 0.0
        reason = ""
        if price < lower:
            score = min(1.0, (lower - price) / std)
            reason = f"price {price:.2f} below lower band {lower:.2f} (oversold)"
        elif price > upper:
            score = min(1.0, (price - upper) / std)
            reason = f"price {price:.2f} above upper band {upper:.2f} (overbought)"

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
            features={"lower_band": lower, "upper_band": upper, "mean": mean, "price": price},
        )

    def explain_trade(self, signal: StrategySignal) -> str:
        return (
            f"{self.strategy_id} {signal.product_id}: {signal.reason} "
            f"(score={signal.score:.3f}, confidence={signal.confidence:.2f})"
        )
