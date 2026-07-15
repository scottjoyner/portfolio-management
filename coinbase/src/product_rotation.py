from __future__ import annotations
import math
from dataclasses import dataclass, field
from typing import List, Dict, Optional, Tuple

from .protocols import Direction, Bar, BracketSetup, BaseStrategy, Opportunity


@dataclass
class ProductScore:
    product_id: str
    momentum_1d: float = 0.0
    momentum_3d: float = 0.0
    momentum_7d: float = 0.0
    composite: float = 0.0
    volume_ranking: float = 0.0
    volatility_ranking: float = 0.0
    rank: int = 0


class ProductRotator:
    def __init__(self, top_n: int = 3, momentum_window: int = 7,
                 rebalance_cooldown_bars: int = 24):
        self.top_n = top_n
        self.momentum_window = momentum_window
        self.rebalance_cooldown = rebalance_cooldown_bars
        self._price_histories: Dict[str, List[float]] = {}
        self._volume_histories: Dict[str, List[float]] = {}
        self._active_products: List[str] = []
        self._bars_since_rebalance: int = 0
        self._scores: List[ProductScore] = []

    def record_bar(self, product_id: str, close: float, volume: float):
        if product_id not in self._price_histories:
            self._price_histories[product_id] = []
            self._volume_histories[product_id] = []
        self._price_histories[product_id].append(close)
        self._volume_histories[product_id].append(volume)
        if len(self._price_histories[product_id]) > 200:
            self._price_histories[product_id] = self._price_histories[product_id][-200:]
            self._volume_histories[product_id] = self._volume_histories[product_id][-200:]

    def score_all(self) -> List[ProductScore]:
        scores = []
        for pid in self._price_histories:
            prices = self._price_histories[pid]
            if len(prices) < 8:
                continue
            vols = self._volume_histories.get(pid, [])
            m1 = self._return(prices, 1)
            m3 = self._return(prices, 3)
            m7 = self._return(prices, min(7, len(prices) - 1))
            vol = self._volatility(prices[-20:]) if len(prices) >= 20 else 0.02

            rank_mom = 50 + m7 * 500
            rank_mom = max(0, min(100, rank_mom))

            avg_vol = sum(vols[-5:]) / 5 if len(vols) >= 5 else 1.0
            hist_vol = sum(vols[-20:]) / 20 if len(vols) >= 20 else avg_vol
            vol_ratio = avg_vol / max(hist_vol, 0.01)

            composite = (m7 * 0.5 + m3 * 0.3 + m1 * 0.2) * 100 + math.log(max(vol_ratio, 0.1)) * 5

            scores.append(ProductScore(
                product_id=pid,
                momentum_1d=m1, momentum_3d=m3, momentum_7d=m7,
                composite=round(composite, 4),
                volume_ranking=round(vol_ratio, 2),
                volatility_ranking=round(vol * 100, 2),
            ))

        scores.sort(key=lambda s: s.composite, reverse=True)
        for i, s in enumerate(scores):
            s.rank = i + 1
        self._scores = scores
        return scores

    def rebalance(self) -> List[str]:
        self._bars_since_rebalance += 1
        if self._bars_since_rebalance < self.rebalance_cooldown and self._active_products:
            return self._active_products

        scores = self.score_all()
        self._active_products = [s.product_id for s in scores[:self.top_n]]
        self._bars_since_rebalance = 0
        return self._active_products

    @property
    def ranked_products(self) -> List[str]:
        if not self._active_products:
            return self.rebalance()
        return self._active_products

    @staticmethod
    def _return(prices: List[float], days: int) -> float:
        if len(prices) < days + 1:  # pragma: no cover
            log.warning("Insufficient price data for %d-day return: have %d prices", days, len(prices))  # pragma: no cover
            return 0.0
        return (prices[-1] - prices[-days - 1]) / max(prices[-days - 1], 1e-9)

    @staticmethod
    def _volatility(prices: List[float]) -> float:
        if len(prices) < 2:  # pragma: no cover
            log.debug("Insufficient price data for volatility: have %d prices", len(prices))  # pragma: no cover
            return 0.0
        returns = [(prices[i] - prices[i-1]) / max(prices[i-1], 1e-9) for i in range(1, len(prices))]
        if not returns:
            return 0.0
        mean = sum(returns) / len(returns)
        var = sum((r - mean) ** 2 for r in returns) / len(returns)
        return math.sqrt(var)

    def top_opportunity_filter(self, opportunities: List[Opportunity]) -> List[Opportunity]:
        active = set(self.ranked_products)
        return [o for o in opportunities if o.product_id in active]


class MomentumRotationStrategy(BaseStrategy):
    def __init__(self, rotator: Optional[ProductRotator] = None):
        self.rotator = rotator or ProductRotator()
        self._name = "momentum_rotation"

    def name(self) -> str:
        return self._name

    def set_product_id(self, product_id: str):
        self._current_pid = product_id

    def on_bar(self, bar: Bar, history: List[Bar]) -> Optional[BracketSetup]:
        product_id = getattr(self, '_current_pid', None)
        if product_id is None:
            return None

        self.rotator.record_bar(product_id, bar.close, bar.volume)
        scores = self.rotator.score_all()

        pid_scores = [s for s in scores if s.product_id == product_id]
        if not pid_scores:
            return None
        score = pid_scores[0]

        if score.rank > self.rotator.top_n:
            return None

        bars = history + [bar]
        closes = [b.close for b in bars]
        highs = [b.high for b in bars]
        lows = [b.low for b in bars]
        volumes = [b.volume for b in bars]
        atr = self._estimate_atr(closes, highs, lows)
        if atr <= 0:
            return None

        current = closes[-1]

        if score.composite > 0:
            direction = Direction.LONG
            stop = current - atr * 2.0
            target = current + atr * 3.0
            confidence = min(0.65, max(0.3, score.composite / 50))
            reason = (f"ROTATION: {product_id} rank={score.rank} "
                      f"mom_7d={score.momentum_7d:+.2%} score={score.composite:.1f}")
        elif score.composite < -5:
            direction = Direction.SHORT
            stop = current + atr * 2.0
            target = current - atr * 3.0
            confidence = min(0.6, max(0.3, abs(score.composite) / 50))
            reason = (f"ROTATION: short {product_id} rank={score.rank} "
                      f"mom_7d={score.momentum_7d:+.2%} score={score.composite:.1f}")
        else:
            return None

        vol_confirm = len(volumes) >= 10 and volumes[-1] > sum(volumes[-10:-1]) / 9 * 1.2
        if vol_confirm and direction == Direction.LONG:
            confidence = min(0.75, confidence + 0.1)

        rr = abs(target - current) / max(abs(current - stop), 1e-9)
        if rr < 1.2:  # pragma: no cover - rr is fixed at 1.5 (target/stop = 3atr/2atr)
            return None

        return BracketSetup(
            direction=direction, entry_price=current,
            stop_price=round(stop, 4), target_price=round(target, 4),
            risk_reward=round(rr, 2), confidence=round(confidence, 3),
            reason=reason, strategy_name=self._name, atr=atr,
            metadata={
                "rotation_rank": score.rank,
                "rotation_score": round(score.composite, 2),
                "mom_7d": round(score.momentum_7d, 4),
            },
        )

    @staticmethod
    def _estimate_atr(closes: List[float], highs: List[float],
                       lows: List[float], period: int = 14) -> float:
        if len(closes) < period + 1:
            return 0.0
        tr_vals = []
        for i in range(1, min(period + 1, len(closes))):
            tr = max(highs[-i] - lows[-i],
                     abs(highs[-i] - closes[-i-1]),
                     abs(lows[-i] - closes[-i-1]))
            tr_vals.append(tr)
        return sum(tr_vals) / len(tr_vals) if tr_vals else 0.0
