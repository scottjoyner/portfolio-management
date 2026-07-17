"""Commodity Channel Index (CCI) short-period reversal variant.

Distinct from the existing true_cci (period=20, |CCI|>100). This is a
SHORT-period (10) CCI that trades quick mean-reversions: heavily
oversold (CCI <= -band) -> long bounce; heavily overbought (CCI >= +band)
-> short fade. Magnitude scales with depth into the extreme.
"""
from __future__ import annotations

import math
from time import monotonic

from strategies.base import BaseSignalStrategy, StrategyConfig, StrategyMetadata
from trading_system.strategies.base.interfaces import StrategySignal


class CciShortReversalStrategy(BaseSignalStrategy):
    """Short-period (10) CCI reversal variant."""

    def __init__(self, period: int = 10, band: float = 80.0) -> None:
        super().__init__(
            metadata=StrategyMetadata(
                strategy_id="CciShortReversalStrategy",
                strategy_type="mean_reversion",
                status="implemented",
                live_supported=False,
                replay_supported=True,
                backtest_supported=True,
                products=["BTC-USD"],
                data_requirements=["product_id", "closes", "highs", "lows"],
                risk_mode_hint="NORMAL",
                capital_bucket="ACTIVE_TRADING",
            ),
            config=StrategyConfig(threshold=0.05, cooldown_seconds=0.0, warmup_period=period + 2),
        )
        self.period = period
        self.band = band

    def _cci(self, closes: list[float], highs: list[float], lows: list[float], i: int) -> float | None:
        if i < self.period or i >= len(closes):
            return None
        tp = [(highs[j] + lows[j] + closes[j]) / 3.0 for j in range(i - self.period + 1, i + 1)]
        ma = sum(tp) / len(tp)
        md = sum(abs(t - ma) for t in tp) / len(tp)
        if md <= 1e-12:
            return 0.0
        return (tp[-1] - ma) / (0.015 * md)

    def generate_signal(self, market_state: dict) -> StrategySignal | None:
        disabled, _ = self.is_disabled(market_state)
        if disabled or self.in_cooldown():
            return None
        closes = market_state.get("closes") or []
        highs = market_state.get("highs") or []
        lows = market_state.get("lows") or []
        n = len(closes)
        if n < self.config.warmup_period + 1 or len(highs) < n or len(lows) < n:
            return None

        cci = self._cci(closes, highs, lows, n - 1)
        if cci is None:
            return None

        if cci <= -self.band:
            score = min(1.0, (-self.band - cci) / 100.0 + 0.2)
        elif cci >= self.band:
            score = -min(1.0, (cci - self.band) / 100.0 + 0.2)
        else:
            return None
        if abs(score) <= self.config.threshold:
            return None
        self._last_emit_ts = monotonic()
        return StrategySignal(
            strategy_id=self.strategy_id,
            product_id=str(market_state.get("product_id", "BTC-USD")),
            score=max(-1.0, min(1.0, score)),
            reason=f"cci={cci:.1f}",
            confidence=min(1.0, abs(score)),
            warmup_passed=bool(market_state.get("warmup_complete", True)),
            tags=[self.metadata_model.strategy_type, self.metadata_model.status],
            features={"cci": round(cci, 2)},
        )
