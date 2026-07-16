"""
Liquidation-cascade proxy (crypto microstructure).

A liquidation cascade shows up in spot/OHLCV micro-structure as a sharp volume
spike on an aggressive move with a pronounced wick (e.g. a long wick low on a
down-move = stop-run / forced selling). Once the volume spike exhausts (the
immediately following bars show volume fading relative to the spike), we treat
the move as an exhaustion and fade it (mean-reversion).

Mirror logic for up-moves with long wick highs.

Pure Python, deterministic. Returns None gracefully when required proxy fields
are missing. No live open_interest / liquidation feed required; callers MAY
optionally pass `liquidation_volume` to strengthen the detection.
"""
from __future__ import annotations

from time import monotonic

from strategies.base import BaseSignalStrategy, StrategyConfig, StrategyMetadata, StrategySignal


class LiquidationCascadeProxyStrategy(BaseSignalStrategy):
    def __init__(
        self,
        lookback: int = 15,
        volume_spike_mult: float = 2.0,
        wick_fraction: float = 0.5,
        exhaustion_bars: int = 2,
    ) -> None:
        self._lookback = lookback
        self._volume_spike_mult = volume_spike_mult
        self._wick_fraction = wick_fraction
        self._exhaustion_bars = exhaustion_bars
        super().__init__(
            metadata=StrategyMetadata(
                strategy_id="LiqCascadeProxyStrategy",
                strategy_type="crypto_microstructure",
                live_supported=False,
                replay_supported=True,
                backtest_supported=True,
                data_requirements=["product_id", "closes", "highs", "lows", "volumes", "warmup_complete"],
                risk_mode_hint="NORMAL",
                capital_bucket="ACTIVE_TRADING",
            ),
            config=StrategyConfig(threshold=0.1, cooldown_seconds=60, warmup_period=lookback),
        )

    @staticmethod
    def _sma(xs: list[float], n: int) -> float:
        if len(xs) < n:
            return 0.0
        return sum(xs[-n:]) / n

    def generate_signal(self, market_state: dict) -> StrategySignal | None:
        disabled, _ = self.is_disabled(market_state)
        if disabled:
            return None
        if self.in_cooldown():
            return None

        closes = market_state.get("closes")
        highs = market_state.get("highs")
        lows = market_state.get("lows")
        volumes = market_state.get("volumes")
        need = self._lookback + self._exhaustion_bars + 1
        if not (closes and highs and lows and volumes) or len(closes) < need:
            return None

        # Optional explicit liquidation volume strengthens the cascade read.
        liq_vol = market_state.get("liquidation_volume")

        n = len(closes)
        window_vol = volumes[n - (self._lookback + 1): n - self._exhaustion_bars]
        avg_vol = self._sma(window_vol, len(window_vol)) if window_vol else 0.0
        spike_bar = n - (self._exhaustion_bars + 1)
        spike_vol = volumes[spike_bar] if spike_bar >= 0 else 0.0

        if avg_vol <= 0 or spike_vol < avg_vol * self._volume_spike_mult:
            return None

        # exhaustion: volume faded after the spike bar.
        post_vols = volumes[n - self._exhaustion_bars:]
        if post_vols and spike_vol <= max(post_vols):
            return None

        spike_close = closes[spike_bar]
        spike_open = closes[spike_bar - 1] if spike_bar > 0 else spike_close
        spike_high = highs[spike_bar]
        spike_low = lows[spike_bar]
        rng = spike_high - spike_low
        if rng <= 0:
            return None

        lower_wick = (min(spike_close, spike_open) - spike_low) / rng
        upper_wick = (spike_high - max(spike_close, spike_open)) / rng

        product_id = str(market_state.get("product_id", "BTC-USD"))
        score = min(1.0, spike_vol / (avg_vol * self._volume_spike_mult))
        raw = 0.0
        reason = ""
        # Cascade DOWN (close below open, long lower wick) -> exhaustion -> LONG.
        if spike_close < spike_open and lower_wick >= self._wick_fraction:
            raw = 1.0
            reason = f"down cascade exhaustion (lower_wick={lower_wick:.2f}, vol_spike={spike_vol/avg_vol:.2f})"
        # Cascade UP (close above open, long upper wick) -> exhaustion -> SHORT.
        elif spike_close > spike_open and upper_wick >= self._wick_fraction:
            raw = -1.0
            reason = f"up cascade exhaustion (upper_wick={upper_wick:.2f}, vol_spike={spike_vol/avg_vol:.2f})"

        if raw == 0.0:
            return None

        if liq_vol is not None:
            reason += f" liq_vol={liq_vol}"

        self._last_emit_ts = monotonic()
        return StrategySignal(
            strategy_id=self.strategy_id,
            product_id=product_id,
            score=raw * score,
            reason=reason,
            confidence=min(1.0, max(0.0, score)),
            warmup_passed=bool(market_state.get("warmup_complete", True)),
            tags=[self.metadata_model.strategy_type, self.metadata_model.status],
            features={
                "spike_volume": spike_vol,
                "avg_volume": avg_vol,
                "lower_wick": lower_wick,
                "upper_wick": upper_wick,
            },
        )
