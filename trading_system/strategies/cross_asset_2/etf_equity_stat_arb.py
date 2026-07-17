"""
ETF / equity stat-arb proxy strategy (cross-asset-2).

The system has no direct equity-index feed, so we proxy an equity-index proxy
from a list of `peer_closes` (each a synthetic/equity-index proxy series sent in
market_state).  We build a synthetic equity-index proxy as the equal-weight mean
of the supplied peer series, then form the crypto-vs-equity spread and test it
for cointegration-style mean reversion via a rolling z-score of the spread.

When the spread z-score is extreme (rich in one direction), we fade it: if the
crypto leg looks rich vs the equity proxy (spread >> mean) we go short the
crypto; if cheap (spread << mean) we go long.

Pure Python, deterministic, math only. Returns None gracefully when `closes`
or `peer_closes` are absent or insufficient.
"""
from __future__ import annotations

from math import log, sqrt
from time import monotonic

from strategies.base import BaseSignalStrategy, StrategyConfig, StrategyMetadata, StrategySignal


class EtfEquityStatArbProxyStrategy(BaseSignalStrategy):
    def __init__(
        self,
        window: int = 40,
        entry_z: float = 1.8,
        exit_z: float = 0.5,
    ) -> None:
        self._window = window
        self._entry_z = entry_z
        self._exit_z = exit_z
        super().__init__(
            metadata=StrategyMetadata(
                strategy_id="EtfEquityStatArbProxyStrategy",
                strategy_type="cross_asset_2",
                live_supported=False,
                replay_supported=True,
                backtest_supported=True,
                data_requirements=["product_id", "closes", "peer_closes", "warmup_complete"],
                risk_mode_hint="NORMAL",
                capital_bucket="ACTIVE_TRADING",
            ),
            config=StrategyConfig(threshold=0.1, cooldown_seconds=120, warmup_period=window),
        )

    @staticmethod
    def _mean(xs: list[float]) -> float:
        return sum(xs) / len(xs) if xs else 0.0

    @staticmethod
    def _stdev(xs: list[float], mean: float) -> float:
        if len(xs) < 2:
            return 0.0
        var = sum((x - mean) ** 2 for x in xs) / (len(xs) - 1)
        return sqrt(var)

    def generate_signal(self, market_state: dict) -> StrategySignal | None:
        disabled, _ = self.is_disabled(market_state)
        if disabled:
            return None
        if self.in_cooldown():
            return None

        closes = market_state.get("closes")
        peers = market_state.get("peer_closes")
        if not closes or len(closes) < self._window + 1:
            return None
        if not peers or not isinstance(peers, (list, tuple)) or len(peers) == 0:
            return None

        # Align all series to the same length.
        n = min(len(closes), min(len(p) for p in peers) if isinstance(peers, (list, tuple)) else 0)
        if isinstance(peers, dict):
            peer_series = [v for v in peers.values()]
        else:
            peer_series = list(peers)
        if not peer_series:
            return None
        n = min(len(closes), min(len(p) for p in peer_series))
        if n < self._window + 1:
            return None

        c = closes[-n:]
        # Synthetic equity-index proxy = equal-weight mean of peers.
        proxy = [self._mean([p[-n + i] for p in peer_series]) for i in range(n)]
        # Spread: crypto normalized vs equity proxy (log-ratio, scale-free).
        spread = [log(c[i] / proxy[i]) if proxy[i] > 0 and c[i] > 0 else 0.0 for i in range(n)]

        win = spread[-(self._window + 1):]
        mu = self._mean(win)
        sd = self._stdev(win, mu)
        if sd <= 1e-12:
            return None
        z = (win[-1] - mu) / sd

        score = 0.0
        if z >= self._entry_z:
            # Crypto rich vs equity proxy -> fade (short).
            score = -min(1.0, z / (self._entry_z * 2.0))
        elif z <= -self._entry_z:
            # Crypto cheap vs equity proxy -> fade (long).
            score = min(1.0, -z / (self._entry_z * 2.0))

        if abs(score) < self.config.threshold:
            return None

        product_id = str(market_state.get("product_id", "BTC-USD"))
        self._last_emit_ts = monotonic()
        return StrategySignal(
            strategy_id=self.strategy_id,
            product_id=product_id,
            score=score,
            reason=f"equity-proxy spread z={z:.3f} (entry>{self._entry_z}) fade",
            confidence=min(1.0, abs(score)),
            warmup_passed=bool(market_state.get("warmup_complete", True)),
            tags=[self.metadata_model.strategy_type, self.metadata_model.status],
            features={"spread_z": z, "spread_mean": mu, "spread_sd": sd},
        )
