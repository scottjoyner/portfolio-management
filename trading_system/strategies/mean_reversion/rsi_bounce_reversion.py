"""
RSI Oversold/Overbought Bounce Strategy (BaseSignalStrategy implementation).

Mean reversion: when rolling RSI(period) drops below the oversold threshold the
asset is statistically cheap -> BUY; when RSI rises above the overbought
threshold the asset is statistically expensive -> SELL. Signal strength scales
with the distance past the threshold.
"""
from __future__ import annotations

from time import monotonic

from strategies.base import BaseSignalStrategy, StrategyConfig, StrategyMetadata, StrategySignal


class RsiBounceReversionStrategy(BaseSignalStrategy):
    def __init__(
        self,
        period: int = 14,
        oversold: float = 30.0,
        overbought: float = 70.0,
    ) -> None:
        self._period = period
        self._oversold = oversold
        self._overbought = overbought
        super().__init__(
            metadata=StrategyMetadata(
                strategy_id="RsiBounceReversionStrategy",
                strategy_type="mean_reversion",
                live_supported=True,
                data_requirements=["product_id", "ohlc_history", "close"],
                risk_mode_hint="CONSERVATIVE",
                capital_bucket="ACTIVE_TRADING",
            ),
            config=StrategyConfig(threshold=0.05, cooldown_seconds=30, warmup_period=period + 1),
        )

    def _rolling_rsi(self, closes: list[float]) -> float | None:
        if len(closes) < self._period + 1:
            return None
        window = closes[-(self._period + 1):]
        gains = 0.0
        losses = 0.0
        for i in range(1, len(window)):
            change = window[i] - window[i - 1]
            if change >= 0:
                gains += change
            else:
                losses -= change
        avg_gain = gains / self._period
        avg_loss = losses / self._period
        if avg_loss == 0:
            return 100.0
        rs = avg_gain / avg_loss
        return 100.0 - (100.0 / (1.0 + rs))

    def generate_signal(self, market_state: dict) -> StrategySignal | None:
        disabled, _ = self.is_disabled(market_state)
        if disabled:
            return None
        if self.in_cooldown():
            return None

        ohlc = market_state.get("ohlc_history")
        if not ohlc:
            return None
        closes = [float(b.get("close", 0.0)) for b in ohlc]
        rsi = self._rolling_rsi(closes)
        if rsi is None:
            return None

        score = 0.0
        reason = ""
        if rsi < self._oversold:
            score = min(1.0, (self._oversold - rsi) / self._oversold)
            reason = f"RSI {rsi:.1f} below oversold {self._oversold:.0f} (buy the dip)"
        elif rsi > self._overbought:
            score = min(1.0, (rsi - self._overbought) / (100.0 - self._overbought))
            reason = f"RSI {rsi:.1f} above overbought {self._overbought:.0f} (fade the rip)"

        if score <= self.config.threshold:
            return None

        price = float(market_state.get("close", closes[-1]))
        self._last_emit_ts = monotonic()
        return StrategySignal(
            strategy_id=self.strategy_id,
            product_id=str(market_state.get("product_id", "BTC-USD")),
            score=score,
            reason=reason,
            confidence=min(1.0, max(0.0, score)),
            warmup_passed=True,
            tags=[self.metadata_model.strategy_type, self.metadata_model.status],
            features={"rsi": rsi, "oversold": self._oversold, "overbought": self._overbought},
        )

    def explain_trade(self, signal: StrategySignal) -> str:
        return (
            f"{self.strategy_id} {signal.product_id}: {signal.reason} "
            f"(score={signal.score:.3f}, confidence={signal.confidence:.2f})"
        )
