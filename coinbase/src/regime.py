from __future__ import annotations
import math
from enum import Enum
from dataclasses import dataclass, field
from typing import List, Dict, Optional, Tuple


class Regime(Enum):
    STRONG_UPTREND = "strong_uptrend"
    WEAK_UPTREND = "weak_uptrend"
    RANGING = "ranging"
    WEAK_DOWNTREND = "weak_downtrend"
    STRONG_DOWNTREND = "strong_downtrend"
    HIGH_VOLATILITY = "high_volatility"
    LOW_VOLATILITY = "low_volatility"
    UNKNOWN = "unknown"


REGIME_STRATEGY_MAP: Dict[Regime, List[str]] = {
    Regime.STRONG_UPTREND: [
        "ema_cross", "macd", "donchian", "adx", "hma",
        "trix", "psar", "aroon", "force_idx", "vpt",
    ],
    Regime.WEAK_UPTREND: [
        "ema_cross", "macd", "donchian", "adx", "vwap_revert",
        "keltner", "chaikin_mf",
    ],
    Regime.RANGING: [
        "rsi_revert", "boll_break", "zscore_revert", "williams_r",
        "cmo", "scci", "ema_dev", "snr_idx",
    ],
    Regime.WEAK_DOWNTREND: [
        "rsi_revert", "boll_break", "zscore_revert", "williams_r",
        "vwap_revert", "obv_div",
    ],
    Regime.STRONG_DOWNTREND: [
        "psar", "aroon", "adx", "donchian", "range_exp_idx",
        "force_idx", "vpt",
    ],
    Regime.HIGH_VOLATILITY: [
        "boll_break", "keltner", "donchian", "vol_mom",
        "range_exp_idx", "snr_idx",
    ],
    Regime.LOW_VOLATILITY: [
        "rsi_revert", "zscore_revert", "scci", "ema_dev",
        "vwap_revert",
    ],
    Regime.UNKNOWN: [
        "ema_cross", "rsi_revert", "boll_break", "donchian",
    ],
}


@dataclass
class RegimeFeatures:
    regime: Regime = Regime.UNKNOWN
    adx: float = 0.0
    trend_strength: float = 0.0
    volatility: float = 0.0
    volume_trend: float = 0.0
    price_position: float = 0.5
    hurst_exponent: float = 0.5
    serial_correlation: float = 0.0
    skewness: float = 0.0
    kurtosis: float = 3.0

    @property
    def is_trending(self) -> bool:
        return self.adx > 25 and abs(self.trend_strength) > 0.02

    @property
    def is_volatile(self) -> bool:
        return self.volatility > 0.03

    @property
    def is_ranging(self) -> bool:
        return self.adx < 20 and self.volatility < 0.02


class RegimeDetector:
    def __init__(self, adx_period: int = 14, volatility_period: int = 20,
                 lookback: int = 50):
        self.adx_period = adx_period
        self.volatility_period = volatility_period
        self.lookback = lookback

    def detect(self, closes: List[float], highs: Optional[List[float]] = None,
               lows: Optional[List[float]] = None,
               volumes: Optional[List[float]] = None) -> Tuple[Regime, RegimeFeatures]:
        features = self._compute_features(closes, highs, lows, volumes)
        regime = self._classify(features)
        features.regime = regime
        return regime, features

    def _compute_features(self, closes: List[float],
                          highs: Optional[List[float]] = None,
                          lows: Optional[List[float]] = None,
                          volumes: Optional[List[float]] = None) -> RegimeFeatures:
        features = RegimeFeatures()
        if len(closes) < 30:
            return features

        recent = closes[-self.lookback:] if len(closes) >= self.lookback else closes
        returns = [(recent[i] - recent[i - 1]) / max(recent[i - 1], 1e-9)
                   for i in range(1, len(recent))]

        features.volatility = self._compute_volatility(returns)
        features.trend_strength = self._compute_trend_strength(recent)
        features.adx = self._compute_adx(closes, highs, lows) if highs and lows else 25.0
        features.price_position = self._compute_price_position(recent)
        features.skewness = self._compute_skewness(returns)
        features.kurtosis = self._compute_kurtosis(returns)
        features.hurst_exponent = self._hurst_exponent(recent)
        features.serial_correlation = self._serial_correlation(returns)

        if volumes:
            vol_recent = volumes[-self.lookback:] if len(volumes) >= self.lookback else volumes
            vol_returns = [(vol_recent[i] - vol_recent[i - 1]) / max(vol_recent[i - 1], 1e-9)
                           for i in range(1, len(vol_recent))]
            features.volume_trend = sum(vol_returns[-5:]) / 5 if len(vol_returns) >= 5 else 0.0

        return features

    def _classify(self, f: RegimeFeatures) -> Regime:
        if f.is_volatile and f.is_trending and f.trend_strength > 0:
            return Regime.STRONG_UPTREND
        if f.is_volatile and f.is_trending and f.trend_strength < 0:
            return Regime.STRONG_DOWNTREND
        if f.is_trending and f.trend_strength > 0:
            return Regime.WEAK_UPTREND
        if f.is_trending and f.trend_strength < 0:
            return Regime.WEAK_DOWNTREND
        if f.is_volatile:
            if f.hurst_exponent > 0.6 and abs(f.trend_strength) > 0.01:
                return Regime.STRONG_UPTREND if f.trend_strength > 0 else Regime.STRONG_DOWNTREND
            return Regime.HIGH_VOLATILITY
        if f.is_ranging:
            return Regime.RANGING
        if f.volatility < 0.01:
            return Regime.LOW_VOLATILITY
        return Regime.UNKNOWN

    def recommended_strategies(self, regime: Regime) -> List[str]:
        return REGIME_STRATEGY_MAP.get(regime, REGIME_STRATEGY_MAP[Regime.UNKNOWN])

    @staticmethod
    def _compute_volatility(returns: List[float]) -> float:
        if len(returns) < 2:
            return 0.0
        mean = sum(returns) / len(returns)
        variance = sum((r - mean) ** 2 for r in returns) / len(returns)
        return math.sqrt(variance) if variance > 0 else 0.0

    @staticmethod
    def _compute_trend_strength(prices: List[float]) -> float:
        if len(prices) < 20:
            return 0.0
        ma50 = sum(prices[-50:]) / 50 if len(prices) >= 50 else sum(prices) / len(prices)
        ma20 = sum(prices[-20:]) / 20
        return (ma20 - ma50) / max(ma50, 1e-9)

    @staticmethod
    def _compute_price_position(prices: List[float]) -> float:
        if len(prices) < 2:
            return 0.5
        lo, hi = min(prices), max(prices)
        if hi - lo == 0:
            return 0.5
        return (prices[-1] - lo) / (hi - lo)

    def _compute_adx(self, closes: List[float], highs: List[float], lows: List[float]) -> float:
        """Approximate ADX from directional movement and true range."""
        if len(closes) < 2 or not highs or not lows:
            return 25.0

        n = min(len(closes), len(highs), len(lows))
        highs = highs[-n:]
        lows = lows[-n:]
        closes = closes[-n:]

        trs = []
        plus_dm = []
        minus_dm = []
        for i in range(1, n):
            up_move = highs[i] - highs[i - 1]
            down_move = lows[i - 1] - lows[i]
            plus_dm.append(up_move if up_move > down_move and up_move > 0 else 0.0)
            minus_dm.append(down_move if down_move > up_move and down_move > 0 else 0.0)
            tr = max(
                highs[i] - lows[i],
                abs(highs[i] - closes[i - 1]),
                abs(lows[i] - closes[i - 1]),
            )
            trs.append(tr)

        if not trs:
            return 25.0

        period = min(self.adx_period, len(trs))
        tr_sum = sum(trs[-period:])
        if tr_sum <= 0:
            return 25.0

        plus_di = 100.0 * sum(plus_dm[-period:]) / tr_sum
        minus_di = 100.0 * sum(minus_dm[-period:]) / tr_sum
        denom = max(plus_di + minus_di, 1e-9)
        dx = 100.0 * abs(plus_di - minus_di) / denom
        return max(0.0, min(100.0, dx))

    @staticmethod
    def _compute_skewness(returns: List[float]) -> float:
        if len(returns) < 3:
            return 0.0
        n = len(returns)
        mean = sum(returns) / n
        variance = sum((r - mean) ** 2 for r in returns) / n
        if variance <= 0:
            return 0.0
        std = math.sqrt(variance)
        skew = sum((r - mean) ** 3 for r in returns) / (n * std ** 3)
        return skew

    @staticmethod
    def _compute_kurtosis(returns: List[float]) -> float:
        if len(returns) < 4:
            return 3.0
        n = len(returns)
        mean = sum(returns) / n
        variance = sum((r - mean) ** 2 for r in returns) / n
        if variance <= 0:
            return 3.0
        std = math.sqrt(variance)
        kurt = sum((r - mean) ** 4 for r in returns) / (n * std ** 4)
        return kurt

    @staticmethod
    def _hurst_exponent(prices: List[float]) -> float:
        if len(prices) < 100:
            return 0.5
        n = len(prices)
        max_lag = min(n // 4, 50)
        lags = range(2, max_lag)
        tau = []
        for lag in lags:
            diffs = [abs(prices[i] - prices[i - lag]) for i in range(lag, n)]
            tau.append(sum(diffs) / len(diffs))
        if not tau or tau[0] <= 0:
            return 0.5
        log_tau = [math.log(t) for t in tau]
        log_lag = [math.log(l) for l in lags]
        n_pts = len(log_tau)
        mean_x = sum(log_lag) / n_pts
        mean_y = sum(log_tau) / n_pts
        num = sum((log_lag[i] - mean_x) * (log_tau[i] - mean_y) for i in range(n_pts))
        den = sum((log_lag[i] - mean_x) ** 2 for i in range(n_pts))
        if den == 0:
            return 0.5
        h = num / den
        return max(0.0, min(1.0, h))

    @staticmethod
    def _serial_correlation(returns: List[float], lag: int = 1) -> float:
        if len(returns) < lag + 2:
            return 0.0
        x = returns[:-lag]
        y = returns[lag:]
        n = min(len(x), len(y))
        x, y = x[:n], y[:n]
        mean_x = sum(x) / n
        mean_y = sum(y) / n
        num = sum((x[i] - mean_x) * (y[i] - mean_y) for i in range(n))
        den = math.sqrt(sum((x[i] - mean_x) ** 2 for i in range(n)) *
                        sum((y[i] - mean_y) ** 2 for i in range(n)))
        return num / den if den > 0 else 0.0


class AdaptiveStrategySelector:
    def __init__(self, detector: Optional[RegimeDetector] = None,
                 blender: Optional['BayesianSignalBlender'] = None):
        self.detector = detector or RegimeDetector()
        self.blender = blender
        self._current_regime: Regime = Regime.UNKNOWN
        self._regime_history: List[Tuple[Regime, float]] = []
        self._strategy_swap_count: int = 0

    @property
    def current_regime(self) -> Regime:
        return self._current_regime

    def set_regime(self, regime: Regime | str) -> Regime:
        """Set the active regime from an enum or string label."""
        if isinstance(regime, Regime):
            self._current_regime = regime
        else:
            value = str(regime).lower()
            self._current_regime = next(
                (r for r in Regime if r.value == value),
                Regime.UNKNOWN,
            )
        return self._current_regime

    def update(self, closes: List[float], highs: Optional[List[float]] = None,
               lows: Optional[List[float]] = None,
               volumes: Optional[List[float]] = None) -> Regime:
        regime, features = self.detector.detect(closes, highs, lows, volumes)
        if regime != self._current_regime:
            self._strategy_swap_count += 1
        self._current_regime = regime
        self._regime_history.append((regime, features.volatility))
        if len(self._regime_history) > 100:
            self._regime_history = self._regime_history[-100:]
        return regime

    def active_strategies(self) -> List[str]:
        return self.detector.recommended_strategies(self._current_regime)

    def select(self, strategy_name: str) -> Dict[str, object]:
        """Backward-compatible strategy gate used by the live trader."""
        active = set(self.active_strategies())
        return {
            "enabled": strategy_name in active,
            "regime": self._current_regime.value,
            "active_strategies": list(active),
        }

    def filter_opportunities(self, opportunities: List) -> List:
        active = set(self.active_strategies())
        filtered = [o for o in opportunities if o.strategy_name in active]
        if self.blender:
            filtered = self.blender.blend_signals(
                filtered, self._current_regime.value
            )
        return filtered

    def regime_stability(self) -> float:
        if len(self._regime_history) < 10:
            return 1.0
        recent = [r for r, _ in self._regime_history[-10:]]
        return len(set(recent)) / 10.0

    def regime_summary(self) -> Dict:
        return {
            "regime": self._current_regime.value,
            "strategy_swaps": self._strategy_swap_count,
            "stability": round(self.regime_stability(), 3),
            "strategies": self.active_strategies(),
        }
