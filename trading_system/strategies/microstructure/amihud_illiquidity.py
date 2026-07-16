from strategies.base import BaseSignalStrategy, StrategyConfig, StrategyMetadata


class AmihudIlliquidityProxyStrategy(BaseSignalStrategy):
    """Amihud illiquidity proxy: average |return| / volume over a window.

    Rising illiquidity implies a liquidity risk premium. When the rolling
    illiquidity measure becomes extreme (anomalously high), we emit a
    mean-reversion signal: liquidity should normalize, favouring a reversal
    of the recent price drift direction.
    """

    def __init__(self, window: int = 20, extreme_z: float = 2.0) -> None:
        super().__init__(
            metadata=StrategyMetadata(
                strategy_id="AmihudIlliquidityProxyStrategy",
                strategy_type="microstructure",
                data_requirements=[
                    "product_id",
                    "closes",
                    "volumes",
                    "warmup_complete",
                ],
                risk_mode_hint="NORMAL",
                capital_bucket="ACTIVE_TRADING",
            ),
            config=StrategyConfig(threshold=0.0, cooldown_seconds=30.0, warmup_period=window),
        )
        self.window = window
        self.extreme_z = extreme_z

    def generate_signal(self, market_state: dict):
        from trading_system.strategies.base.interfaces import StrategySignal

        disabled, _ = self.is_disabled(market_state)
        if disabled:
            return None
        if self.in_cooldown():
            return None

        missing = self.required_inputs() - set(market_state)
        if missing:
            return None

        closes = list(market_state.get("closes", []))
        volumes = list(market_state.get("volumes", []))
        if len(closes) < self.window + 1 or len(volumes) < self.window + 1:
            return None

        rets = []
        illiq = []
        for i in range(1, len(closes)):
            if volumes[i] <= 0.0 or closes[i - 1] == 0.0:
                continue
            ret = (closes[i] - closes[i - 1]) / closes[i - 1]
            illiq.append(abs(ret) / volumes[i])
            rets.append(ret)

        if len(illiq) < self.window:
            return None

        window_illiq = illiq[-self.window:]
        mean_illiq = sum(window_illiq) / len(window_illiq)
        var = sum((x - mean_illiq) ** 2 for x in window_illiq) / len(window_illiq)
        std_illiq = var ** 0.5
        if std_illiq <= 0.0:
            return None

        last_illiq = window_illiq[-1]
        z = (last_illiq - mean_illiq) / std_illiq

        if z <= self.extreme_z:
            return None

        recent_ret = rets[-1]
        score = -min(1.0, z / (self.extreme_z * 2.0)) * (1.0 if recent_ret >= 0 else -1.0)
        confidence = min(1.0, abs(z) / (self.extreme_z * 2.0))
        self._last_emit_ts = self._now()
        return StrategySignal(
            strategy_id=self.strategy_id,
            product_id=str(market_state.get("product_id", "BTC-USD")),
            score=score,
            reason=f"amihud illiquidity z={z:.2f} extreme (mean-revert vs recent drift)",
            confidence=confidence,
            warmup_passed=bool(market_state.get("warmup_complete", True)),
            tags=[self.metadata_model.strategy_type, self.metadata_model.status],
            features={
                "illiquidity_z": z,
                "mean_illiquidity": mean_illiq,
                "recent_return": recent_ret,
            },
        )

    def _now(self) -> float:
        from time import monotonic

        return monotonic()
