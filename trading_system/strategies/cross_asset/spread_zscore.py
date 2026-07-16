"""
BTC-ETH spread z-score reversion (cross-asset stat-arb).

Computes a rolling spread between the primary asset (`closes`) and a correlated
peer (`peer_closes`), then z-scores that spread. Extreme positive spread => the
primary is rich vs its peer => SELL; extreme negative spread => primary is cheap
=> BUY. Callers MUST supply `peer_closes` in market_state.
"""
from __future__ import annotations

from time import monotonic

from strategies.base import BaseSignalStrategy, StrategyConfig, StrategyMetadata, StrategySignal


class SpreadZScoreReversionStrategy(BaseSignalStrategy):
    def __init__(
        self,
        window: int = 60,
        entry_z: float = 2.0,
        exit_z: float = 0.5,
    ) -> None:
        self._window = window
        self._entry_z = entry_z
        self._exit_z = exit_z
        super().__init__(
            metadata=StrategyMetadata(
                strategy_id="SpreadZScoreReversionStrategy",
                strategy_type="stat_arb",
                live_supported=False,
                replay_supported=True,
                backtest_supported=True,
                data_requirements=["product_id", "closes", "peer_closes"],
                risk_mode_hint="NORMAL",
                capital_bucket="ACTIVE_TRADING",
            ),
            config=StrategyConfig(threshold=0.05, cooldown_seconds=60, warmup_period=window),
        )

    @staticmethod
    def _mean(xs: list[float]) -> float:
        return sum(xs) / len(xs) if xs else 0.0

    @staticmethod
    def _std(xs: list[float], mean: float) -> float:
        if len(xs) < 2:
            return 0.0
        var = sum((x - mean) ** 2 for x in xs) / (len(xs) - 1)
        return var ** 0.5

    def _spread(self, closes: list[float], peer: list[float]) -> list[float] | None:
        n = min(len(closes), len(peer))
        if n < self._window:
            return None
        c = closes[-n:]
        p = peer[-n:]
        return [c[i] / p[i] for i in range(n)]

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

        spread = self._spread(closes, peer)
        if spread is None:
            return None

        window_spread = spread[-self._window:]
        mean = self._mean(window_spread)
        std = self._std(window_spread, mean)
        if std == 0.0:
            return None

        z = (spread[-1] - mean) / std
        product_id = str(market_state.get("product_id", "BTC-USD"))

        if abs(z) < self._entry_z:
            return None

        score = min(1.0, abs(z) / (self._entry_z * 2.0))
        reason = ""
        if z > 0:
            reason = f"primary rich vs peer (z={z:.2f}>0): short spread"
        else:
            reason = f"primary cheap vs peer (z={z:.2f}<0): long spread"

        self._last_emit_ts = monotonic()
        return StrategySignal(
            strategy_id=self.strategy_id,
            product_id=product_id,
            score=score,
            reason=reason,
            confidence=min(1.0, max(0.0, score)),
            warmup_passed=True,
            tags=[self.metadata_model.strategy_type, self.metadata_model.status],
            features={"spread_z": z, "spread_mean": mean, "spread_std": std, "entry_z": self._entry_z},
        )
