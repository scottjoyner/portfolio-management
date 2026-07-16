"""
Beta-adjusted cointegration reversion (OLS-free).

Estimates a hedge ratio (beta) between the primary (`closes`) and a peer
(`peer_closes`) via a simple rolling regression of primary on peer, then forms a
residual series (primary - beta*peer). Trades the deviation of the residual from
its rolling mean when the rolling correlation confirms the series are co-moving.
Positive residual => primary rich => SELL; negative => primary cheap => BUY.
Callers MUST supply `peer_closes` in market_state.
"""
from __future__ import annotations

from time import monotonic

from strategies.base import BaseSignalStrategy, StrategyConfig, StrategyMetadata, StrategySignal


class BetaAdjustedCointegrationReversionStrategy(BaseSignalStrategy):
    def __init__(
        self,
        window: int = 60,
        entry_z: float = 2.0,
        min_corr: float = 0.5,
    ) -> None:
        self._window = window
        self._entry_z = entry_z
        self._min_corr = min_corr
        super().__init__(
            metadata=StrategyMetadata(
                strategy_id="BetaAdjustedCointegrationReversionStrategy",
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

    def _beta_corr(self, y: list[float], x: list[float]) -> tuple[float, float] | None:
        n = min(len(y), len(x))
        if n < self._window:
            return None
        yw = y[-n:]
        xw = x[-n:]
        ym = self._mean(yw)
        xm = self._mean(xw)
        sxx = sum((xi - xm) ** 2 for xi in xw)
        sxy = sum((yi - ym) * (xi - xm) for yi, xi in zip(yw, xw))
        if sxx == 0:
            return None
        beta = sxy / sxx
        syy = sum((yi - ym) ** 2 for yi in yw)
        if syy == 0:
            return None
        corr = sxy / (sxx ** 0.5 * syy ** 0.5)
        return beta, corr

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

        bc = self._beta_corr(closes, peer)
        if bc is None:
            return None
        beta, corr = bc
        if abs(corr) < self._min_corr:
            return None

        n = min(len(closes), len(peer))
        yw = closes[-n:]
        xw = peer[-n:]
        ym = self._mean(yw)
        residual = yw[-1] - beta * xw[-1]
        residuals = [yw[i] - beta * xw[i] for i in range(n)]
        rmean = self._mean(residuals)
        rstd = self._std(residuals, rmean)
        if rstd == 0.0:
            return None

        z = (residual - rmean) / rstd
        if abs(z) < self._entry_z:
            return None

        score = min(1.0, abs(z) / (self._entry_z * 2.0))
        product_id = str(market_state.get("product_id", "BTC-USD"))
        side = "short" if z > 0 else "long"
        reason = (
            f"beta-adjusted residual {side} (z={z:.2f}, beta={beta:.3f}, corr={corr:.2f})"
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
            features={"residual_z": z, "beta": beta, "corr": corr, "entry_z": self._entry_z},
        )
