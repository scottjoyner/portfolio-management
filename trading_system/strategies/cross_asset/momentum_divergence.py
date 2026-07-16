"""
Cross-asset momentum divergence (e.g. BTC vs DXY / equity index).

Primary outperforms its peer when the primary's recent return exceeds the peer's
recent return by a meaningful margin. Signal requires confirmation: the primary
lead must persist over `confirm_bars` and the absolute lead must exceed
`min_divergence`. Positive divergence (primary leads up) => BUY; negative
divergence (primary lags / peer leads up) => SELL. Callers MUST supply
`peer_closes` in market_state.
"""
from __future__ import annotations

from time import monotonic

from strategies.base import BaseSignalStrategy, StrategyConfig, StrategyMetadata, StrategySignal


class CrossAssetMomentumDivergenceStrategy(BaseSignalStrategy):
    def __init__(
        self,
        lookback: int = 20,
        confirm_bars: int = 3,
        min_divergence: float = 0.02,
    ) -> None:
        self._lookback = lookback
        self._confirm_bars = confirm_bars
        self._min_divergence = min_divergence
        super().__init__(
            metadata=StrategyMetadata(
                strategy_id="CrossAssetMomentumDivergenceStrategy",
                strategy_type="cross_asset",
                live_supported=False,
                replay_supported=True,
                backtest_supported=True,
                data_requirements=["product_id", "closes", "peer_closes"],
                risk_mode_hint="NORMAL",
                capital_bucket="ACTIVE_TRADING",
            ),
            config=StrategyConfig(threshold=0.05, cooldown_seconds=120, warmup_period=lookback),
        )

    @staticmethod
    def _return(series: list[float], n: int) -> float:
        n = min(n, len(series) - 1)
        if n < 1:
            return 0.0
        a, b = series[-1 - n], series[-1]
        if a == 0:
            return 0.0
        return b / a - 1.0

    def generate_signal(self, market_state: dict) -> StrategySignal | None:
        disabled, _ = self.is_disabled(market_state)
        if disabled:
            return None
        if self.in_cooldown():
            return None

        closes = market_state.get("closes")
        peer = market_state.get("peer_closes")
        if not closes or not peer:
            return None
        if len(closes) < self._lookback + 1 or len(peer) < self._lookback + 1:
            return None

        primary_ret = self._return(closes, self._lookback)
        peer_ret = self._return(peer, self._lookback)
        divergence = primary_ret - peer_ret
        if abs(divergence) < self._min_divergence:
            return None

        # Confirmation: lead must hold over the trailing confirm_bars window.
        confirmed = True
        for k in range(1, self._confirm_bars + 1):
            if self._return(closes, k) - self._return(peer, k) < 0:
                confirmed = False
                break
        if not confirmed:
            return None

        score = min(1.0, abs(divergence) / (self._min_divergence * 3.0))
        product_id = str(market_state.get("product_id", "BTC-USD"))
        side = "long" if divergence > 0 else "short"
        reason = (
            f"primary {side} vs peer (div={divergence:+.4f}, "
            f"primary_ret={primary_ret:+.4f}, peer_ret={peer_ret:+.4f})"
        )

        self._last_emit_ts = monotonic()
        return StrategySignal(
            strategy_id=self.strategy_id,
            product_id=product_id,
            score=score,
            reason=reason,
            confidence=min(1.0, max(0.0, score)),
            warmup_passed=True,
            tags=[self.metadata_model.strategy_type, self.metadata_model.status],
            features={
                "divergence": divergence,
                "primary_return": primary_ret,
                "peer_return": peer_ret,
                "min_divergence": self._min_divergence,
            },
        )
