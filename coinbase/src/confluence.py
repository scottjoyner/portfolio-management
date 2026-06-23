from __future__ import annotations
from dataclasses import dataclass, field
from typing import List, Dict, Optional, Tuple
from .protocols import Direction, Opportunity, BracketSetup


@dataclass
class TimeframeSignal:
    timeframe: str
    direction: Optional[Direction]
    confidence: float
    reason: str

    @property
    def is_bullish(self) -> bool:
        return self.direction == Direction.LONG

    @property
    def is_bearish(self) -> bool:
        return self.direction == Direction.SHORT

    @property
    def strength(self) -> float:
        return self.confidence * (1.0 if self.direction else 0.0)


@dataclass
class ConfluenceResult:
    product_id: str
    overall_direction: Optional[Direction]
    confidence: float
    agreement_pct: float
    timeframe_signals: List[TimeframeSignal]
    dominant_timeframe: str = ""
    divergence_detected: bool = False


TIMEFRAME_ORDER = ["1m", "5m", "15m", "1h", "4h", "1d"]
TIMEFRAME_WEIGHTS = {"1m": 0.3, "5m": 0.4, "15m": 0.6, "1h": 1.0, "4h": 1.2, "1d": 1.5}


class MultiTimeframeConfluence:
    def __init__(self, min_agreement: float = 0.5):
        self.min_agreement = min_agreement

    def evaluate(self, product_id: str,
                 signals: Dict[str, List[TimeframeSignal]]) -> ConfluenceResult:
        all_signals: List[TimeframeSignal] = []
        for tf_signals in signals.values():
            all_signals.extend(tf_signals)

        if not all_signals:
            return ConfluenceResult(
                product_id=product_id,
                overall_direction=None,
                confidence=0.0,
                agreement_pct=0.0,
                timeframe_signals=[],
            )

        bullish_weight = sum(
            s.strength * TIMEFRAME_WEIGHTS.get(s.timeframe, 1.0)
            for s in all_signals if s.is_bullish
        )
        bearish_weight = sum(
            s.strength * TIMEFRAME_WEIGHTS.get(s.timeframe, 1.0)
            for s in all_signals if s.is_bearish
        )

        total = bullish_weight + bearish_weight
        if total <= 0:
            return ConfluenceResult(
                product_id=product_id, overall_direction=None,
                confidence=0.0, agreement_pct=0.0, timeframe_signals=all_signals,
            )

        if bullish_weight > bearish_weight:
            direction = Direction.LONG
            agreement = bullish_weight / total
        else:
            direction = Direction.SHORT
            agreement = bearish_weight / total

        direction_signals = [s for s in all_signals if s.direction == direction]
        dominant_tf = max(
            set(s.timeframe for s in direction_signals),
            key=lambda tf: sum(
                s.strength for s in direction_signals if s.timeframe == tf
            ),
        ) if direction_signals else ""

        timeframes_with_direction = set(
            s.timeframe for s in all_signals if s.direction == direction
        )
        timeframes_opposite = set(
            s.timeframe for s in all_signals
            if s.direction and s.direction != direction
        )
        divergence = len(timeframes_opposite) > 0 and len(timeframes_with_direction) > 0

        return ConfluenceResult(
            product_id=product_id,
            overall_direction=direction,
            confidence=agreement * (1.0 + len(direction_signals) * 0.05),
            agreement_pct=round(agreement, 3),
            timeframe_signals=all_signals,
            dominant_timeframe=dominant_tf,
            divergence_detected=divergence,
        )

    def boost_opportunity(self, opp: Opportunity,
                          confluence: ConfluenceResult) -> Opportunity:
        if confluence.overall_direction is None or confluence.overall_direction != opp.direction:
            opp.confidence *= 0.5
            opp.score *= 0.5
            opp.meta["confluence"] = "conflict"
        else:
            boost = 1.0 + confluence.agreement_pct * 0.5
            if confluence.divergence_detected:
                boost *= 0.85
            opp.confidence = min(1.0, opp.confidence * boost)
            opp.score = opp.score * boost
            opp.meta["confluence_agreement"] = round(confluence.agreement_pct, 3)
            opp.meta["confluence_dominant_tf"] = confluence.dominant_timeframe
            opp.meta["confluence_divergence"] = confluence.divergence_detected
        return opp

    @staticmethod
    def build_signals(opportunities: List[Opportunity],
                      timeframe: str = "1h") -> List[TimeframeSignal]:
        return [
            TimeframeSignal(
                timeframe=timeframe,
                direction=o.direction,
                confidence=o.confidence,
                reason=o.reason,
            )
            for o in opportunities
        ]


class OrderBookImbalanceStrategy:
    def __init__(self, imbalance_threshold: float = 0.3,
                 depth_levels: int = 10):
        self.threshold = imbalance_threshold
        self.depth = depth_levels
        self._name = "orderbook_imbalance"

    def name(self) -> str:
        return self._name

    def compute_imbalance(self, bids: List[Tuple[float, float]],
                          asks: List[Tuple[float, float]]) -> float:
        bid_vol = sum(s for _, s in bids[:self.depth])
        ask_vol = sum(s for _, s in asks[:self.depth])
        total = bid_vol + ask_vol
        if total <= 0:
            return 0.0
        return (bid_vol - ask_vol) / total

    def evaluate(self, bids: List[Tuple[float, float]],
                 asks: List[Tuple[float, float]],
                 current_price: float, atr: float,
                 confidence_mult: float = 0.5) -> Optional[BracketSetup]:
        imbalance = self.compute_imbalance(bids, asks)
        if abs(imbalance) < self.threshold:
            return None

        direction = Direction.LONG if imbalance > 0 else Direction.SHORT
        conf = min(abs(imbalance) * confidence_mult, 0.8)

        if direction == Direction.LONG:
            entry = current_price
            stop = entry - atr * 1.5
            target = entry + atr * 2.5
        else:
            entry = current_price
            stop = entry + atr * 1.5
            target = entry - atr * 2.5

        rr = abs(target - entry) / max(abs(entry - stop), 1e-9)
        return BracketSetup(
            direction=direction,
            entry_price=entry,
            stop_price=stop,
            target_price=target,
            risk_reward=rr,
            confidence=conf,
            reason=f"Order book imbalance {imbalance:+.2f}",
            strategy_name="orderbook_imbalance",
            atr=atr,
        )
