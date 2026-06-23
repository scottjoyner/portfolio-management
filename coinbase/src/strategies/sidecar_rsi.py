from __future__ import annotations

from typing import List, Optional

from ..protocols import Bar, BaseStrategy, BracketSetup, Direction, InstrumentType


class SidecarRSICrossStrategy(BaseStrategy):
    """Coinbase BaseStrategy adapter for the sidecar RSI-cross research logic.

    The strategy emits BracketSetup objects only. Sizing, ranking, gating, and any
    order routing remain the job of the Coinbase orchestrator.
    """

    def __init__(
        self,
        product_id: str = "",
        rsi_period: int = 14,
        buy_rsi_cross: float = 30.0,
        take_profit_pct: float = 0.02,
        stop_loss_pct: float = 0.01,
        min_bars: Optional[int] = None,
        confidence: float = 0.45,
    ):
        self.product_id = product_id
        self.rsi_period = rsi_period
        self.buy_rsi_cross = buy_rsi_cross
        self.take_profit_pct = take_profit_pct
        self.stop_loss_pct = stop_loss_pct
        self.min_bars = min_bars or max(20, rsi_period + 2)
        self.confidence = confidence
        self._name = "sidecar_rsi_cross"

    def name(self) -> str:
        return self._name

    def set_product_id(self, product_id: str):
        self.product_id = product_id

    def on_bar(self, bar: Bar, history: List[Bar]) -> Optional[BracketSetup]:
        bars = history + [bar]
        if len(bars) < self.min_bars:
            return None
        closes = [b.close for b in bars]
        prev_rsi = self._rsi(closes[:-1], self.rsi_period)
        curr_rsi = self._rsi(closes, self.rsi_period)
        if not (prev_rsi <= self.buy_rsi_cross < curr_rsi):
            return None
        entry = bar.close
        stop = entry * (1.0 - self.stop_loss_pct)
        target = entry * (1.0 + self.take_profit_pct)
        risk = abs(entry - stop)
        reward = abs(target - entry)
        if risk <= 0:
            return None
        return BracketSetup(
            direction=Direction.LONG,
            entry_price=entry,
            stop_price=stop,
            target_price=target,
            risk_reward=reward / risk,
            confidence=self.confidence,
            reason=f"sidecar RSI crossed above {self.buy_rsi_cross}: {prev_rsi:.1f}->{curr_rsi:.1f}",
            strategy_name=self._name,
            atr=self._estimate_atr(bars),
            instrument_type=InstrumentType.SPOT,
            leverage=1.0,
            metadata={
                "product_id": self.product_id,
                "rsi_period": self.rsi_period,
                "buy_rsi_cross": self.buy_rsi_cross,
                "take_profit_pct": self.take_profit_pct,
                "stop_loss_pct": self.stop_loss_pct,
                "prev_rsi": round(prev_rsi, 4),
                "curr_rsi": round(curr_rsi, 4),
            },
        )

    @staticmethod
    def _rsi(closes: List[float], period: int) -> float:
        if len(closes) < period + 1:
            return 50.0
        deltas = [closes[i] - closes[i - 1] for i in range(len(closes) - period, len(closes))]
        gains = sum(d for d in deltas if d > 0) / period
        losses = sum(abs(d) for d in deltas if d < 0) / period
        if losses == 0:
            return 100.0
        rs = gains / losses
        return 100.0 - (100.0 / (1.0 + rs))

    @staticmethod
    def _estimate_atr(bars: List[Bar], period: int = 14) -> float:
        if len(bars) < 2:
            return 0.0
        recent = bars[-period:]
        tr_values = []
        for idx, current in enumerate(recent):
            prev_close = recent[idx - 1].close if idx > 0 else current.close
            tr_values.append(max(current.high - current.low, abs(current.high - prev_close), abs(current.low - prev_close)))
        return sum(tr_values) / max(len(tr_values), 1)
