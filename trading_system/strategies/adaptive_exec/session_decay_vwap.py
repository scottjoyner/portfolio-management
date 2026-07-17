from __future__ import annotations

from time import monotonic

from strategies.base import BaseSignalStrategy, StrategyConfig, StrategyMetadata
from trading_system.strategies.base.interfaces import StrategySignal


class SessionDecayVwapStrategy(BaseSignalStrategy):
    """Liquidity-weighted VWAP deviation with intra-session volume decay.

    Novel angle: a plain VWAP weights every bar's volume equally through the
    session. Real intraday liquidity is tilted -- the open and close carry the
    bulk of executable volume while the mid-session lulls are thin. This
    strategy builds a *decayed* VWAP where each historical bar's volume weight
    is tilted by an exponential session-position kernel (recent bars near the
    "close" of the rolling session count more), then trades mean reversion of
    price to that liquidity-weighted fair value.

    Deviation is normalised by the liquidity-weighted dispersion so the signed
    score is comparable across regimes. Price rich to the decayed VWAP -> SELL
    (negative score); price cheap -> BUY (positive score).
    """

    def __init__(self, window: int = 48, decay: float = 0.06, band: float = 0.004) -> None:
        super().__init__(
            metadata=StrategyMetadata(
                strategy_id="SessionDecayVWAPDeviation",
                strategy_type="adaptive_exec",
                live_supported=True,
                data_requirements=[
                    "product_id",
                    "closes",
                    "highs",
                    "lows",
                    "volumes",
                    "warmup_complete",
                ],
                risk_mode_hint="NORMAL",
                capital_bucket="ACTIVE_TRADING",
            ),
            config=StrategyConfig(threshold=0.15, cooldown_seconds=20, warmup_period=window),
        )
        self.window = int(window)
        self.decay = float(decay)
        self.band = float(band)

    def generate_signal(self, market_state: dict) -> StrategySignal | None:
        disabled, _ = self.is_disabled(market_state)
        if disabled or self.in_cooldown():
            return None

        closes = market_state.get("closes") or []
        highs = market_state.get("highs") or []
        lows = market_state.get("lows") or []
        volumes = market_state.get("volumes") or []
        n = min(len(closes), len(highs), len(lows), len(volumes))
        if n < self.window:
            return None

        c = closes[-self.window:]
        h = highs[-self.window:]
        low = lows[-self.window:]
        v = volumes[-self.window:]

        # Session-position decay kernel: the most recent bar has weight 1.0,
        # older bars decay geometrically -> tilts VWAP toward live liquidity.
        num = 0.0
        den = 0.0
        typ_prices = []
        weights = []
        for i in range(self.window):
            age = (self.window - 1) - i  # 0 for most recent
            kernel = (1.0 - self.decay) ** age
            typ = (h[i] + low[i] + c[i]) / 3.0
            w = max(0.0, float(v[i])) * kernel
            typ_prices.append(typ)
            weights.append(w)
            num += typ * w
            den += w
        if den <= 0:
            return None

        vwap = num / den
        # Liquidity-weighted dispersion around the decayed VWAP.
        var = 0.0
        for tp, w in zip(typ_prices, weights):
            var += w * (tp - vwap) ** 2
        disp = (var / den) ** 0.5
        if disp <= 0 or vwap <= 0:
            return None

        price = float(c[-1])
        # Relative deviation, band-gated so we ignore micro-noise.
        rel_dev = (price - vwap) / vwap
        if abs(rel_dev) < self.band:
            return None

        # Normalise by liquidity-weighted dispersion (in price units).
        z = (price - vwap) / disp
        # Mean reversion: rich -> SELL (negative), cheap -> BUY (positive).
        score = max(-1.0, min(1.0, -z / 3.0))
        if abs(score) <= self.config.threshold:
            return None

        self._last_emit_ts = monotonic()
        return StrategySignal(
            strategy_id=self.strategy_id,
            product_id=str(market_state.get("product_id", "BTC-USD")),
            score=score,
            reason=f"decayVWAP={vwap:.4f} price={price:.4f} z={z:+.2f} rel_dev={rel_dev:+.4f}",
            confidence=min(1.0, abs(score)),
            warmup_passed=bool(market_state.get("warmup_complete", True)),
            tags=[self.metadata_model.strategy_type, self.metadata_model.status],
            features={
                "decayed_vwap": round(vwap, 6),
                "dispersion": round(disp, 6),
                "z_score": round(z, 4),
                "rel_dev": round(rel_dev, 6),
            },
        )
