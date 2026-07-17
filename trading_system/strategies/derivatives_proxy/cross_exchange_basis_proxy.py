"""
Cross-exchange basis proxy strategy (derivatives proxy).

There is NO perp/spot exchange feed here, so we synthesize a "perp fair value"
from a funding-lite proxy: a drift-adjusted price using recent log-return drift
and a volume-weighted displacement. The basis is the gap between spot `closes`
and this synthetic perp fair. We trade basis divergence as mean-reversion:

* Synthetic perp fair = last close * (1 + drift * vol_weight), where drift is the
  recent average log-return and vol_weight discounts the fair when volume is thin.
* basis = (spot_last - perp_fair) / perp_fair.
* A large positive basis -> spot rich vs synthetic perp -> fade (SELL spot / short).
* A large negative basis -> spot cheap -> fade (BUY / long).

Fallback: when `volumes` is absent we fall back to the (high-low) range as the
volatility/displacement proxy (per `highs`/`lows`), still producing a basis gauge.

Pure Python, deterministic. Returns None gracefully when required fields are
absent so it never crashes in the replay/backtest harness.
"""
from __future__ import annotations

from math import log
from time import monotonic

from strategies.base import BaseSignalStrategy, StrategyConfig, StrategyMetadata, StrategySignal


class CrossExchangeBasisProxyStrategy(BaseSignalStrategy):
    def __init__(
        self,
        lookback: int = 20,
        basis_threshold: float = 0.004,
        vol_discount: float = 0.5,
    ) -> None:
        self._lookback = lookback
        self._basis_threshold = basis_threshold
        self._vol_discount = vol_discount
        super().__init__(
            metadata=StrategyMetadata(
                strategy_id="CrossExchangeBasisProxyStrategy",
                strategy_type="derivatives_proxy",
                live_supported=False,
                replay_supported=True,
                backtest_supported=True,
                data_requirements=["product_id", "closes", "warmup_complete"],
                risk_mode_hint="NORMAL",
                capital_bucket="ACTIVE_TRADING",
            ),
            config=StrategyConfig(threshold=0.1, cooldown_seconds=60, warmup_period=lookback),
        )

    @staticmethod
    def _mean(xs: list[float]) -> float:
        return sum(xs) / len(xs) if xs else 0.0

    def generate_signal(self, market_state: dict) -> StrategySignal | None:
        disabled, _ = self.is_disabled(market_state)
        if disabled:
            return None
        if self.in_cooldown():
            return None

        closes = market_state.get("closes")
        if not closes or len(closes) < self._lookback + 1:
            return None

        window = closes[-(self._lookback + 1):]
        last = window[-1]
        if last <= 0:
            return None

        # Drift from recent log-returns.
        log_rets = [log(window[i] / window[i - 1]) for i in range(1, len(window))]
        drift = self._mean(log_rets)

        # Volume weight: thin volume -> discount the synthetic fair (more uncertain).
        vol_weight = 1.0
        volumes = market_state.get("volumes")
        if volumes and len(volumes) >= self._lookback:
            v_window = volumes[-self._lookback:]
            avg_vol = self._mean(v_window)
            if avg_vol > 0:
                recent_vol = self._mean(v_window[-min(5, len(v_window)):])
                vol_weight = 1.0 - self._vol_discount * (1.0 - min(1.0, recent_vol / avg_vol))
        else:
            # Fallback: use (high-low) range as displacement proxy.
            highs = market_state.get("highs")
            lows = market_state.get("lows")
            if highs and lows and len(highs) >= self._lookback and len(lows) >= self._lookback:
                h = highs[-1]
                l = lows[-1]
                rng = (h - l) / last if last > 0 else 0.0
                vol_weight = 1.0 - self._vol_discount * min(1.0, rng * 10.0)
            else:
                vol_weight = 1.0 - self._vol_discount * 0.5

        perp_fair = last * (1.0 + drift * vol_weight)
        if perp_fair <= 0:
            return None

        basis = (last - perp_fair) / perp_fair
        if abs(basis) < self._basis_threshold:
            return None

        # Positive basis (spot rich) -> fade short; negative -> fade long.
        raw = -1.0 if basis > 0 else 1.0
        score = raw * min(1.0, abs(basis) / (self._basis_threshold * 4.0))
        if abs(score) < self.config.threshold:
            return None

        product_id = str(market_state.get("product_id", "BTC-USD"))
        self._last_emit_ts = monotonic()
        return StrategySignal(
            strategy_id=self.strategy_id,
            product_id=product_id,
            score=score,
            reason=f"basis={basis:.4f} drift={drift:.5f} vol_weight={vol_weight:.3f}",
            confidence=min(1.0, abs(score)),
            warmup_passed=bool(market_state.get("warmup_complete", True)),
            tags=[self.metadata_model.strategy_type, self.metadata_model.status],
            features={
                "basis": basis,
                "perp_fair": perp_fair,
                "drift": drift,
            },
        )
