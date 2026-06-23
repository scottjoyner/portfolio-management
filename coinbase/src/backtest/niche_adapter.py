from __future__ import annotations
from typing import List, Optional

from ..protocols import Direction, InstrumentType, Bar, BracketSetup, BaseStrategy
from .coinbase_niche_strategies import (
    OHLCVBar,
    BaseStrategy as NicheBase,
    MultiTimeframeRSIMomentumStrategy,
    BollingerSqueezeBreakoutStrategy,
    RegimeAwareAdaptiveStrategy,
    AnchoredVWAPMeanReversionStrategy,
    LiquidityVacuumReversalStrategy,
    DonchianPullbackContinuationStrategy,
    RSIFailureSwingReversalStrategy,
    VolatilityCompressionBreakoutStrategy,
    ImpulseExhaustionReversalStrategy,
    VolRegimeSwitchStrategy,
    SentimentMomentumCompositeStrategy,
    OnChainRegimeWhaleFlowStrategy,
)


def _bar_to_ohlcv(bar: Bar, history: List[Bar]) -> OHLCVBar:
    closes = [b.close for b in history] + [bar.close]
    volumes = [b.volume for b in history] + [bar.volume]
    return OHLCVBar(
        timestamp=str(bar.timestamp),
        open=bar.open,
        high=bar.high,
        low=bar.low,
        close=bar.close,
        volume=bar.volume,
        close_window=closes,
        volume_window=volumes,
    )


class NicheStrategyWrapper(BaseStrategy):
    def __init__(self, niche: NicheBase):
        self.niche = niche
        self._name = niche.__class__.__name__

    def name(self) -> str:
        return self._name

    def on_bar(self, bar: Bar, history: List[Bar]) -> Optional[BracketSetup]:
        ohlcv_bar = _bar_to_ohlcv(bar, history)
        signal = self.niche.on_bar(ohlcv_bar)
        if signal is None:
            return None

        if isinstance(signal, str):
            action = signal
        elif hasattr(signal, 'action'):
            action = signal.action
        else:
            return None

        closes = [b.close for b in history] + [bar.close]
        atr = self._estimate_atr(closes, [b.high for b in history] + [bar.high],
                                 [b.low for b in history] + [bar.low])

        if action == "BUY":
            direction = Direction.LONG
            stop = bar.close - atr * 2.0
            target = bar.close + atr * 3.0
        elif action == "SELL":
            direction = Direction.SHORT
            stop = bar.close + atr * 2.0
            target = bar.close - atr * 3.0
        else:
            return None

        rr = abs(target - bar.close) / max(abs(bar.close - stop), 1e-9)
        return BracketSetup(
            direction=direction,
            entry_price=bar.close,
            stop_price=stop,
            target_price=target,
            risk_reward=rr,
            confidence=0.5,
            reason=f"niche:{self._name}:{action}",
            strategy_name=self._name.lower(),
            atr=atr,
        )

    @staticmethod
    def _estimate_atr(closes: List[float], highs: List[float],
                      lows: List[float], period: int = 14) -> float:
        if len(closes) < period + 1:
            return 0.0
        tr_vals = []
        for i in range(1, min(period + 1, len(closes))):
            tr = max(
                highs[-i] - lows[-i],
                abs(highs[-i] - closes[-i - 1]),
                abs(lows[-i] - closes[-i - 1]),
            )
            tr_vals.append(tr)
        return sum(tr_vals) / len(tr_vals) if tr_vals else 0.0


ALL_NICHE_STRATEGIES = [
    MultiTimeframeRSIMomentumStrategy,
    BollingerSqueezeBreakoutStrategy,
    RegimeAwareAdaptiveStrategy,
    AnchoredVWAPMeanReversionStrategy,
    LiquidityVacuumReversalStrategy,
    DonchianPullbackContinuationStrategy,
    RSIFailureSwingReversalStrategy,
    VolatilityCompressionBreakoutStrategy,
    ImpulseExhaustionReversalStrategy,
    VolRegimeSwitchStrategy,
    SentimentMomentumCompositeStrategy,
    OnChainRegimeWhaleFlowStrategy,
]


def wrap_all_niche_strategies() -> List[BaseStrategy]:
    result = []
    for cls in ALL_NICHE_STRATEGIES:
        try:
            result.append(NicheStrategyWrapper(cls()))
        except Exception:
            continue
    return result
