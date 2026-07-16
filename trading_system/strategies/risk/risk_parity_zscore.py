from __future__ import annotations

import math

from strategies.base import BaseSignalStrategy, StrategyConfig, StrategyMetadata
from trading_system.strategies.base.interfaces import StrategySignal


class RiskParityZScoreStrategy(BaseSignalStrategy):
    """Risk-parity rotation proxy via volatility-regime z-score.

    Single-asset proxy: compare the asset's recent realized vol against its own
    trailing mean to build a vol z-score. Low-vol z (calm) -> mild long; high-vol z
    (stressed) -> short/flat. Captures mean-reversion of the vol spread using
    volume-normalized return magnitude as the cross-sectional proxy.
    """

    def __init__(self, window: int = 40, z_window: int = 20) -> None:
        super().__init__(
            metadata=StrategyMetadata(
                strategy_id="RiskParityZScore",
                strategy_type="risk",
                data_requirements=["product_id", "closes", "volumes", "warmup_complete"],
                risk_mode_hint="NORMAL",
                capital_bucket="ACTIVE_TRADING",
            ),
            config=StrategyConfig(threshold=0.2, cooldown_seconds=30, warmup_period=window),
        )
        self.window = window
        self.z_window = z_window

    def generate_signal(self, market_state: dict) -> StrategySignal | None:
        disabled, _ = self.is_disabled(market_state)
        if disabled:
            return None
        if self.in_cooldown():
            return None

        closes = market_state.get("closes") or []
        if len(closes) < self.window:
            return None

        win = closes[-self.window:]
        log_rets = []
        for i in range(1, len(win)):
            p0, p1 = win[i - 1], win[i]
            if p0 > 0:
                log_rets.append(math.log(p1 / p0))
        if len(log_rets) < 2:
            return None

        # long-run vol over full window
        mean_all = sum(log_rets) / len(log_rets)
        var_all = sum((r - mean_all) ** 2 for r in log_rets) / (len(log_rets) - 1)
        long_vol = math.sqrt(var_all) + 1e-9

        # recent vol over z_window and its stdev across rolling sub-windows
        recent = log_rets[-self.z_window:]
        mean_r = sum(recent) / len(recent)
        var_r = sum((r - mean_r) ** 2 for r in recent) / (len(recent) - 1)
        recent_vol = math.sqrt(var_r) + 1e-9

        # distribution of rolling vol to get spread stdev
        sub = max(5, self.z_window // 4)
        vols = []
        for i in range(len(log_rets) - sub + 1):
            chunk = log_rets[i:i + sub]
            m = sum(chunk) / len(chunk)
            v = sum((c - m) ** 2 for c in chunk) / (len(chunk) - 1)
            vols.append(math.sqrt(v))
        if len(vols) < 2:
            return None
        mv = sum(vols) / len(vols)
        sd = math.sqrt(sum((x - mv) ** 2 for x in vols) / (len(vols) - 1)) + 1e-9

        z = (recent_vol - mv) / sd
        # low-vol z -> long (positive), high-vol z -> short (negative)
        score = max(-1.0, min(1.0, -z / 2.0))
        if abs(score) <= self.config.threshold:
            return None

        self._last_emit_ts = __import__("time").monotonic()
        return StrategySignal(
            strategy_id=self.strategy_id,
            product_id=str(market_state.get("product_id", "BTC-USD")),
            score=score,
            reason=f"vol_z={z:.2f} recent_vol={recent_vol:.5f} long_vol={long_vol:.5f}",
            confidence=min(1.0, abs(score)),
            warmup_passed=bool(market_state.get("warmup_complete", True)),
            tags=[self.metadata_model.strategy_type, self.metadata_model.status],
            features={"vol_z": round(z, 4), "recent_vol": round(recent_vol, 6)},
        )
