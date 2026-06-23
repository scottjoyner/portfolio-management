from __future__ import annotations
import math
from dataclasses import dataclass, field
from typing import List, Optional, Tuple, Dict

from .protocols import Direction, Bar, BracketSetup, BaseStrategy


@dataclass
class OrderFlowState:
    bid_volume: List[float] = field(default_factory=list)
    ask_volume: List[float] = field(default_factory=list)
    cvd: List[float] = field(default_factory=list)
    volume_clusters: List[Dict] = field(default_factory=list)
    absorption_signals: int = 0

    @property
    def cvd_direction(self) -> str:
        if len(self.cvd) < 5:
            return "neutral"
        recent = self.cvd[-5:]
        if recent[-1] > recent[0] * 1.02:
            return "rising"
        elif recent[-1] < recent[0] * 0.98:
            return "falling"
        return "neutral"


class SmartMoneyFlowStrategy(BaseStrategy):
    def __init__(self, cvd_lookback: int = 30,
                 absorption_vol_mult: float = 2.5,
                 cluster_price_bins: int = 10,
                 ad_threshold: float = 0.02):
        self.cvd_lookback = cvd_lookback
        self.absorption_vol_mult = absorption_vol_mult
        self.cluster_bins = cluster_price_bins
        self.ad_threshold = ad_threshold
        self._name = "smart_money_flow"
        self._state: Dict[str, OrderFlowState] = {}

    def name(self) -> str:
        return self._name

    def set_product_id(self, product_id: str):
        self._current_pid = product_id

    def on_bar(self, bar: Bar, history: List[Bar]) -> Optional[BracketSetup]:
        product_id = getattr(self, '_current_pid', None)
        if product_id is None:
            return None
        bars = history + [bar]
        if len(bars) < 20:
            return None

        if product_id not in self._state:
            self._state[product_id] = OrderFlowState()
        state = self._state[product_id]

        closes = [b.close for b in bars]
        highs = [b.high for b in bars]
        lows = [b.low for b in bars]
        volumes = [b.volume for b in bars]
        atr = self._estimate_atr(closes, highs, lows)
        if atr <= 0:
            return None

        estimate_bid_ask = self._estimate_bid_ask_volume(bars[-5:])
        bid_vol, ask_vol = estimate_bid_ask_volume

        state.bid_volume.append(bid_vol)
        state.ask_volume.append(ask_vol)
        if len(state.bid_volume) > self.cvd_lookback:
            state.bid_volume.pop(0)
            state.ask_volume.pop(0)

        prev_cvd = state.cvd[-1] if state.cvd else 0.0
        cvd_tick = bid_vol - ask_vol
        state.cvd.append(prev_cvd + cvd_tick)
        if len(state.cvd) > self.cvd_lookback:
            state.cvd.pop(0)

        self._update_volume_clusters(state, bar, volumes)

        setup = self._check_cvd_divergence(bar, state, atr)
        if setup:
            return setup

        setup = self._check_volume_absorption(bar, state, atr, volumes)
        if setup:
            return setup

        setup = self._check_ad_line(bar, closes, volumes, atr)
        if setup:
            return setup

        return None

    def _check_cvd_divergence(self, bar: Bar, state: OrderFlowState,
                               atr: float) -> Optional[BracketSetup]:
        if len(state.cvd) < 20 or len(state.bid_volume) < 20:
            return None

        price_trend = self._trend([b.close for b in []] + [bar.close], 10)
        cvd_trend = self._trend(state.cvd, 10)

        if price_trend > 0.01 and cvd_trend < -0.01:
            direction = Direction.SHORT
            reason = f"SM: CVD bearish divergence (price up, CVD down)"
        elif price_trend < -0.01 and cvd_trend > 0.01:
            direction = Direction.LONG
            reason = f"SM: CVD bullish divergence (price down, CVD up)"
        else:
            return None

        confidence = min(0.65, abs(price_trend - cvd_trend) * 5.0)

        if direction == Direction.LONG:
            stop = bar.close - atr * 2.0
            target = bar.close + atr * 3.0
        else:
            stop = bar.close + atr * 2.0
            target = bar.close - atr * 3.0

        rr = abs(target - bar.close) / max(abs(bar.close - stop), 1e-9)
        if rr < 1.2:
            return None

        return BracketSetup(
            direction=direction, entry_price=bar.close,
            stop_price=stop, target_price=target,
            risk_reward=rr, confidence=round(confidence, 3),
            reason=reason, strategy_name=self._name, atr=atr,
        )

    def _check_volume_absorption(self, bar: Bar, state: OrderFlowState,
                                  atr: float,
                                  volumes: List[float]) -> Optional[BracketSetup]:
        if len(volumes) < 20 or len(state.bid_volume) < 5:
            return None

        avg_vol = sum(volumes[-20:-1]) / 19 if len(volumes) >= 20 else sum(volumes) / len(volumes)
        current_vol = volumes[-1]

        if current_vol < avg_vol * self.absorption_vol_mult:
            return None

        avg_range = atr
        range_pct = (bar.high - bar.low) / max(bar.close, 1e-9)

        if range_pct < avg_range / max(bar.close, 1e-9) * 1.5:
            recent_bid_pct = sum(state.bid_volume[-5:]) / max(
                sum(state.bid_volume[-5:]) + sum(state.ask_volume[-5:]), 1e-9
            )

            if bar.close > bar.open and recent_bid_pct < 0.45:
                direction = Direction.SHORT
                reason = f"SM: absorption (vol={current_vol:.0f}, bid%={recent_bid_pct:.0%})"
            elif bar.close < bar.open and recent_bid_pct > 0.55:
                direction = Direction.LONG
                reason = f"SM: absorption (vol={current_vol:.0f}, bid%={recent_bid_pct:.0%})"
            else:
                return None

            state.absorption_signals += 1

            if direction == Direction.LONG:
                stop = bar.close - atr * 1.5
                target = bar.close + atr * 2.5
            else:
                stop = bar.close + atr * 1.5
                target = bar.close - atr * 2.5

            rr = abs(target - bar.close) / max(abs(bar.close - stop), 1e-9)
            if rr < 1.5:
                return None

            return BracketSetup(
                direction=direction, entry_price=bar.close,
                stop_price=stop, target_price=target,
                risk_reward=rr, confidence=0.5,
                reason=reason, strategy_name=self._name, atr=atr,
            )

        return None

    def _check_ad_line(self, bar: Bar, closes: List[float],
                        volumes: List[float], atr: float) -> Optional[BracketSetup]:
        if len(closes) < 20 or len(volumes) < 20:
            return None

        ad = [0.0]
        for i in range(1, len(closes)):
            hl = closes[i] - closes[i-1]
            if hl > 0:
                mf = volumes[i] * (closes[i] - closes[i-1])
            elif hl < 0:
                mf = -volumes[i] * (closes[i-1] - closes[i])
            else:
                mf = 0.0
            ad.append(ad[-1] + mf)

        ad = ad[1:] if len(ad) > 1 else ad

        price_trend = self._trend(closes[-10:], 10)
        ad_trend = self._trend(ad[-10:], 10) if len(ad) >= 10 else 0.0

        if price_trend > self.ad_threshold and ad_trend < -self.ad_threshold:
            direction = Direction.SHORT
            reason = "SM: A/D divergence (price up, A/D down)"
        elif price_trend < -self.ad_threshold and ad_trend > self.ad_threshold:
            direction = Direction.LONG
            reason = "SM: A/D divergence (price down, A/D up)"
        else:
            return None

        conf = min(0.6, abs(price_trend - ad_trend) * 3.0)
        if direction == Direction.LONG:
            stop = bar.close - atr * 2.0
            target = bar.close + atr * 3.0
        else:
            stop = bar.close + atr * 2.0
            target = bar.close - atr * 3.0

        rr = abs(target - bar.close) / max(abs(bar.close - stop), 1e-9)
        if rr < 1.2:
            return None

        return BracketSetup(
            direction=direction, entry_price=bar.close,
            stop_price=stop, target_price=target,
            risk_reward=rr, confidence=round(conf, 3),
            reason=reason, strategy_name=self._name, atr=atr,
        )

    @staticmethod
    def _estimate_bid_ask_volume(bars: List[Bar]) -> Tuple[float, float]:
        if not bars:
            return (0.0, 0.0)
        bid_vol = 0.0
        ask_vol = 0.0
        for b in bars:
            total = b.volume
            if b.close > b.open:
                ask_vol += total * 0.6
                bid_vol += total * 0.4
            elif b.close < b.open:
                bid_vol += total * 0.6
                ask_vol += total * 0.4
            else:
                bid_vol += total * 0.5
                ask_vol += total * 0.5
        avg_bid = bid_vol / len(bars)
        avg_ask = ask_vol / len(bars)
        return (avg_bid, avg_ask)

    @staticmethod
    def _update_volume_clusters(state: OrderFlowState, bar: Bar,
                                 volumes: List[float]):
        price_bin = int(bar.close / max(bar.high - bar.low, 1e-9)) % 10
        found = False
        for cluster in state.volume_clusters:
            if abs(cluster.get("price", 0) - bar.close) / max(bar.close, 1e-9) < 0.01:
                cluster["volume"] = cluster.get("volume", 0) + bar.volume
                cluster["count"] = cluster.get("count", 0) + 1
                found = True
                break
        if not found:
            state.volume_clusters.append({
                "price": bar.close,
                "volume": bar.volume,
                "count": 1,
            })
        if len(state.volume_clusters) > 50:
            state.volume_clusters = state.volume_clusters[-25:]

    @staticmethod
    def _trend(values: List[float], n: int) -> float:
        if len(values) < n:
            return 0.0
        recent = values[-n:]
        return (recent[-1] - recent[0]) / max(abs(recent[0]), 1e-9)

    @staticmethod
    def _estimate_atr(closes: List[float], highs: List[float],
                       lows: List[float], period: int = 14) -> float:
        if len(closes) < period + 1:
            return 0.0
        tr_vals = []
        for i in range(1, min(period + 1, len(closes))):
            tr = max(highs[-i] - lows[-i],
                     abs(highs[-i] - closes[-i - 1]),
                     abs(lows[-i] - closes[-i - 1]))
            tr_vals.append(tr)
        return sum(tr_vals) / len(tr_vals) if tr_vals else 0.0
