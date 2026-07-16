from __future__ import annotations

import math

from strategies.base import BaseSignalStrategy, StrategyConfig, StrategyMetadata
from trading_system.strategies.base.interfaces import StrategySignal


class EwmaVarBreakoutStrategy(BaseSignalStrategy):
    """EWMA-variance breakout (GARCH-lite) signal in pure Python.

    Computes an EWMA conditional variance vs a long-run variance baseline. A spike in
    conditional variance predicts continued elevated vol -> trade the vol-breakout
    direction using the sign of the return that accompanied the spike.
    """

    def __init__(self, window: int = 40, lambda_: float = 0.94) -> None:
        super().__init__(
            metadata=StrategyMetadata(
                strategy_id="EwmaVarBreakout",
                strategy_type="risk",
                data_requirements=["product_id", "closes", "volumes", "warmup_complete"],
                risk_mode_hint="AGGRESSIVE",
                capital_bucket="ACTIVE_TRADING",
            ),
            config=StrategyConfig(threshold=0.2, cooldown_seconds=30, warmup_period=window),
        )
        self.window = window
        self.lambda_ = lambda_

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
        if len(log_rets) < 3:
            return None

        # long-run (expanding) variance baseline
        long_mean = sum(log_rets) / len(log_rets)
        long_var = sum((r - long_mean) ** 2 for r in log_rets) / (len(log_rets) - 1)

        # EWMA variance
        ewma_var = long_var
        for r in log_rets:
            ewma_var = self.lambda_ * ewma_var + (1.0 - self.lambda_) * (r ** 2)

        ratio = ewma_var / (long_var + 1e-9)
        last_ret = log_rets[-1]
        if ratio > 1.0 and abs(last_ret) > 0:
            # vol spike: trade the direction of the most recent return
            score = max(-1.0, min(1.0, math.copysign(min(1.0, ratio - 1.0), last_ret) * 2.0))
        else:
            score = 0.0

        if abs(score) <= self.config.threshold:
            return None

        self._last_emit_ts = __import__("time").monotonic()
        return StrategySignal(
            strategy_id=self.strategy_id,
            product_id=str(market_state.get("product_id", "BTC-USD")),
            score=score,
            reason=f"ewma_var_ratio={ratio:.3f} last_ret={last_ret:.5f}",
            confidence=min(1.0, abs(score)),
            warmup_passed=bool(market_state.get("warmup_complete", True)),
            tags=[self.metadata_model.strategy_type, self.metadata_model.status],
            features={"ewma_var_ratio": round(ratio, 4), "last_ret": round(last_ret, 6)},
        )
