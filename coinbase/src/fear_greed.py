from __future__ import annotations
import math
import time
from dataclasses import dataclass, field
from typing import List, Optional, Tuple

from .protocols import Direction, Bar, BracketSetup, BaseStrategy


@dataclass
class FearGreedSnapshot:
    value: float = 50.0
    classification: str = "neutral"
    volatility_component: float = 50.0
    momentum_component: float = 50.0
    volume_component: float = 50.0
    breadth_component: float = 50.0


CLASSIFICATION_RANGES = [
    (0, 25, "extreme_fear"),
    (25, 40, "fear"),
    (40, 60, "neutral"),
    (60, 75, "greed"),
    (75, 100, "extreme_greed"),
]


class FearGreedIndex:
    def __init__(self, lookback_vol: int = 30, lookback_mom: int = 14):
        self.lookback_vol = lookback_vol
        self.lookback_mom = lookback_mom
        self._cache: FearGreedSnapshot = FearGreedSnapshot()
        self._cache_ts: float = 0.0
        self._cache_ttl: float = 300.0

    def compute(self, closes_dict: dict, volumes_dict: Optional[dict] = None
                ) -> FearGreedSnapshot:
        now = time.time()
        if now - self._cache_ts < self._cache_ttl:
            return self._cache

        vol_scores = []
        mom_scores = []
        vol_vol_scores = []

        for pid, closes in closes_dict.items():
            if len(closes) < 30:
                continue
            vols = (volumes_dict or {}).get(pid, None)

            mom_score = self._momentum_score(closes)
            mom_scores.append(mom_score)

            v_score = self._volatility_score(closes)
            vol_scores.append(v_score)

            vv_score = self._volume_volume_score(closes, vols)
            vol_vol_scores.append(vv_score)

        if not mom_scores:
            return self._cache

        avg_mom = sum(mom_scores) / len(mom_scores)
        avg_vol = sum(vol_scores) / len(vol_scores)
        avg_vv = sum(vol_vol_scores) / len(vol_vol_scores) if vol_vol_scores else 50.0
        avg_breadth = self._breadth_score(closes_dict)

        fg = 0.4 * avg_mom + 0.3 * avg_vol + 0.2 * avg_vv + 0.1 * avg_breadth
        fg = max(0, min(100, fg))

        classification = self._classify(fg)
        self._cache = FearGreedSnapshot(
            value=round(fg, 1),
            classification=classification,
            volatility_component=round(avg_vol, 1),
            momentum_component=round(avg_mom, 1),
            volume_component=round(avg_vv, 1),
            breadth_component=round(avg_breadth, 1),
        )
        self._cache_ts = now
        return self._cache

    def _momentum_score(self, closes: List[float]) -> float:
        if len(closes) < self.lookback_mom + 1:
            return 50.0
        short_ret = (closes[-1] - closes[-5]) / max(closes[-5], 1e-9)
        med_ret = (closes[-1] - closes[-self.lookback_mom]) / max(closes[-self.lookback_mom], 1e-9)
        long_ret = (closes[-1] - closes[-min(len(closes), 50)]) / max(closes[-min(len(closes), 50)], 1e-9)

        composite = short_ret * 0.5 + med_ret * 0.3 + long_ret * 0.2
        score = 50 + composite * 500
        return max(0, min(100, score))

    def _volatility_score(self, closes: List[float]) -> float:
        if len(closes) < self.lookback_vol + 1:
            return 50.0
        returns = [(closes[i] - closes[i-1]) / max(closes[i-1], 1e-9)
                   for i in range(-self.lookback_vol, 0)]
        if not returns:
            return 50.0
        mean_r = sum(returns) / len(returns)
        variance = sum((r - mean_r) ** 2 for r in returns) / len(returns)
        vol = math.sqrt(variance) if variance > 0 else 0.0

        if vol < 0.01:
            return 70.0
        elif vol < 0.02:
            return 60.0
        elif vol < 0.04:
            return 50.0
        elif vol < 0.08:
            return 35.0
        return 20.0

    def _volume_volume_score(self, closes: List[float],
                              volumes: Optional[List[float]]) -> float:
        if not volumes or len(volumes) < 20:
            return 50.0
        recent_avg = sum(volumes[-5:]) / 5
        hist_avg = sum(volumes[-20:-5]) / 15 if len(volumes) >= 20 else recent_avg
        if hist_avg <= 0:
            return 50.0
        ratio = recent_avg / hist_avg

        if len(closes) < 5:
            return 50.0
        price_dir = closes[-1] - closes[-5]
        if price_dir > 0 and ratio > 1.5:
            return 75.0
        elif price_dir < 0 and ratio > 1.5:
            return 25.0
        elif ratio > 1.2:
            return 60.0 if price_dir > 0 else 40.0
        return 50.0

    def _breadth_score(self, closes_dict: dict) -> float:
        n = len(closes_dict)
        if n < 2:
            return 50.0
        up = 0
        for pid, closes in closes_dict.items():
            if len(closes) < 5:
                continue
            if closes[-1] > closes[-5]:
                up += 1
        ratio = up / max(n, 1)
        return 30 + ratio * 40

    @staticmethod
    def _classify(value: float) -> str:
        for lo, hi, label in CLASSIFICATION_RANGES:
            if lo <= value < hi:
                return label
        return "neutral"


class FearGreedSignalAdapter(BaseStrategy):
    def __init__(self, fg_index: Optional[FearGreedIndex] = None,
                 extreme_fear_bias: str = "long",
                 extreme_greed_bias: str = "short",
                 min_confidence: float = 0.35):
        self.fg = fg_index or FearGreedIndex()
        self._name = "fear_greed"
        self._extreme_fear_bias = extreme_fear_bias
        self._extreme_greed_bias = extreme_greed_bias
        self._min_conf = min_confidence
        self._current_fg: Optional[FearGreedSnapshot] = None
        self._price_history: dict = {}
        self._volume_history: dict = {}

    def name(self) -> str:
        return self._name

    def set_product_id(self, product_id: str):
        self._current_pid = product_id

    def on_bar(self, bar: Bar, history: List[Bar]) -> Optional[BracketSetup]:
        product_id = getattr(self, '_current_pid', None)
        if product_id is None:
            return None

        pid = product_id
        if pid not in self._price_history:
            self._price_history[pid] = []
            self._volume_history[pid] = []
        self._price_history[pid].append(bar.close)
        self._volume_history[pid].append(bar.volume)
        if len(self._price_history[pid]) > 100:
            self._price_history[pid] = self._price_history[pid][-100:]
            self._volume_history[pid] = self._volume_history[pid][-100:]

        if len(self._price_history[pid]) < 30:
            return None

        snapshot = self.fg.compute(
            self._price_history,
            self._volume_history if any(self._volume_history.values()) else None,
        )
        self._current_fg = snapshot

        bars = history + [bar]
        closes = [b.close for b in bars]
        highs = [b.high for b in bars]
        lows = [b.low for b in bars]
        atr = self._estimate_atr(closes, highs, lows)
        if atr <= 0:
            return None

        current = closes[-1]

        if snapshot.classification == "extreme_fear":
            direction = Direction.LONG
            conf = max(self._min_conf, min(0.7, (25 - snapshot.value) / 25 * 0.5 + 0.3))
            stop = current - atr * 2.0
            target = current + atr * 3.0
            reason = f"FG: extreme_fear={snapshot.value:.0f} — contrarian long"
        elif snapshot.classification == "fear":
            direction = Direction.LONG
            conf = 0.4
            stop = current - atr * 1.5
            target = current + atr * 2.5
            reason = f"FG: fear={snapshot.value:.0f} — cautious long"
        elif snapshot.classification == "extreme_greed":
            direction = Direction.SHORT
            conf = max(self._min_conf, min(0.7, (snapshot.value - 75) / 25 * 0.5 + 0.3))
            stop = current + atr * 2.0
            target = current - atr * 3.0
            reason = f"FG: extreme_greed={snapshot.value:.0f} — contrarian short"
        elif snapshot.classification == "greed":
            direction = Direction.SHORT
            conf = 0.4
            stop = current + atr * 1.5
            target = current - atr * 2.5
            reason = f"FG: greed={snapshot.value:.0f} — cautious short"
        else:
            return None

        rr = abs(target - current) / max(abs(current - stop), 1e-9)
        if rr < 1.2:
            return None

        return BracketSetup(
            direction=direction, entry_price=current,
            stop_price=round(stop, 4), target_price=round(target, 4),
            risk_reward=round(rr, 2), confidence=round(conf, 3),
            reason=reason, strategy_name=self._name, atr=atr,
            metadata={
                "fear_greed": round(snapshot.value, 1),
                "classification": snapshot.classification,
                "vol_component": round(snapshot.volatility_component, 1),
                "mom_component": round(snapshot.momentum_component, 1),
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
