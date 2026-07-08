"""Multi-timeframe macro analysis — daily/weekly/monthly trends + Bitcoin halving cycle."""

from __future__ import annotations

import logging
import math
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from coinbase.src.rest_feed import fetch_candles_batch_sync, candle_arrays

log = logging.getLogger("multi_tf")

# ── Bitcoin Halving Dates ────────────────────────────────────────────
HALVING_DATES = [
    datetime(2012, 11, 28, tzinfo=timezone.utc),
    datetime(2016, 7, 9, tzinfo=timezone.utc),
    datetime(2020, 5, 11, tzinfo=timezone.utc),
    datetime(2024, 4, 20, tzinfo=timezone.utc),
    # Next halving ~2028-04-14 (approx every 210,000 blocks ~4 years)
]

HALVING_CYCLE_DAYS = 365.25 * 4  # ~1461 days


def _sma(values: List[float], period: int) -> List[float]:
    if len(values) < period:
        return []
    result = []
    for i in range(len(values) - period + 1):
        result.append(sum(values[i:i + period]) / period)
    return result


def _ema(values: List[float], period: int) -> List[float]:
    if len(values) < period:
        return []
    multiplier = 2.0 / (period + 1)
    result = [sum(values[:period]) / period]
    for v in values[period:]:
        result.append((v - result[-1]) * multiplier + result[-1])
    return result


def _atr(highs: List[float], lows: List[float], closes: List[float], period: int = 14) -> List[float]:
    if len(closes) < period + 1:
        return []
    trs = []
    for i in range(1, len(closes)):
        tr = max(highs[i] - lows[i], abs(highs[i] - closes[i - 1]), abs(lows[i] - closes[i - 1]))
        trs.append(tr)
    return _ema(trs, period)


def _adx(closes: List[float], highs: List[float], lows: List[float], period: int = 14) -> float:
    if len(closes) < period * 2:
        return 0.0
    plus_dm = []
    minus_dm = []
    trs = []
    for i in range(1, len(closes)):
        up = highs[i] - highs[i - 1]
        down = lows[i - 1] - lows[i]
        tr = max(highs[i] - lows[i], abs(highs[i] - closes[i - 1]), abs(lows[i] - closes[i - 1]))
        trs.append(tr)
        plus_dm.append(up if up > down and up > 0 else 0.0)
        minus_dm.append(down if down > up and down > 0 else 0.0)
    if len(trs) < period:
        return 0.0
    atr_vals = _ema(trs, period)
    plus_ema = _ema(plus_dm, period)
    minus_ema = _ema(minus_dm, period)
    if not atr_vals or not plus_ema or not minus_ema:
        return 0.0
    pdi = 100.0 * plus_ema[-1] / max(atr_vals[-1], 1e-9)
    ndi = 100.0 * minus_ema[-1] / max(atr_vals[-1], 1e-9)
    dx = 100.0 * abs(pdi - ndi) / max(pdi + ndi, 1e-9)
    return dx


@dataclass
class MacroCyclePhase:
    name: str
    months_since_halving: float
    bias: str  # "bullish" | "bearish" | "neutral"
    risk_multiplier: float  # 0.0-2.0, scale position sizing
    description: str


@dataclass
class TFStats:
    tf_name: str  # "daily" | "weekly" | "monthly"
    sma50: float = 0.0
    sma200: float = 0.0
    ema12: float = 0.0
    ema26: float = 0.0
    adx: float = 0.0
    atr_pct: float = 0.0
    trend: str = ""  # "bull" | "bear" | "neutral"
    trend_strength: float = 0.0  # 0-1
    price_vs_sma200_pct: float = 0.0
    recent_return_pct: float = 0.0


@dataclass
class CompositeMacroSignal:
    bias: str  # "bullish" | "bearish" | "neutral" | "risk_off"
    confidence: float  # 0-1
    risk_multiplier: float  # 0.0-2.0, applied to position sizing
    allows_new_longs: bool
    allows_new_shorts: bool
    cycle_phase: str = ""
    tf_signals: Dict[str, str] = field(default_factory=dict)
    reason: str = ""
    btc_price: float = 0.0

    def to_dict(self) -> Dict[str, Any]:
        return {
            "bias": self.bias,
            "confidence": round(self.confidence, 4),
            "risk_multiplier": round(self.risk_multiplier, 4),
            "allows_new_longs": self.allows_new_longs,
            "allows_new_shorts": self.allows_new_shorts,
            "cycle_phase": self.cycle_phase,
            "tf_signals": self.tf_signals,
            "reason": self.reason,
            "btc_price": round(self.btc_price, 2),
        }


def detect_bitcoin_cycle(btc_price: float = 0.0) -> MacroCyclePhase:
    now = datetime.now(timezone.utc)
    last_halving = None
    for h in reversed(HALVING_DATES):
        if h <= now:
            last_halving = h
            break
    if last_halving is None:
        return MacroCyclePhase(
            name="pre_halving_genesis",
            months_since_halving=0,
            bias="neutral",
            risk_multiplier=1.0,
            description="No halving record — earliest days",
        )
    months = (now.year - last_halving.year) * 12 + (now.month - last_halving.month)
    pct_through = months / 48.0  # 4-year cycle

    if months <= 0:
        return MacroCyclePhase(
            name="halving_day", months_since_halving=0, bias="neutral",
            risk_multiplier=1.0, description="Halving day — uncertainty",
        )
    if months <= 6:
        phase = "accumulation"
        bias = "bullish"
        mult = 1.15
        desc = "Post-halving accumulation — bullish bias"
    elif months <= 18:
        phase = "expansion"
        bias = "bullish"
        mult = 1.25
        desc = "Early expansion — strongest bullish phase"
    elif months <= 30:
        phase = "mania"
        bias = "neutral"
        mult = 1.0
        desc = "Late expansion / mania — high volatility, trend may exhaust"
    elif months <= 42:
        phase = "distribution"
        bias = "bearish"
        mult = 0.7
        desc = "Distribution — bearish bias, reduce risk"
    else:
        phase = "capitulation"
        bias = "bearish"
        mult = 0.5
        desc = "Capitulation / bottom fishing — maximum risk reduction"
    return MacroCyclePhase(
        name=phase, months_since_halving=months, bias=bias,
        risk_multiplier=mult, description=desc,
    )


class MacroTrendAnalyzer:
    """Analyzes higher-timeframe trends to produce a macro bias signal.

    Fetches daily, weekly, and monthly candles for BTC and top assets via
    Coinbase REST API, computes SMA/EMA crossovers + ADX on each TF, and
    combines with the Bitcoin halving cycle for a composite macro outlook.
    """

    def __init__(self, cache_ttl: float = 900.0):
        self._cache_ttl = cache_ttl
        self._last_fetch: Dict[str, Tuple[float, Any]] = {}  # (ts, result)
        self._cached_ast: Optional[str] = None

    def _check_cache(self, key: str) -> Any:
        entry = self._last_fetch.get(key)
        if entry and (time.time() - entry[0]) < self._cache_ttl:
            return entry[1]
        return None

    def _set_cache(self, key: str, value: Any) -> None:
        self._last_fetch[key] = (time.time(), value)

    def fetch_higher_tf(self, product_id: str) -> Tuple[List[float], List[float], List[float], List[float], str]:
        """Fetch daily candles (most granular that's always available on Coinbase REST)."""
        candles = fetch_candles_batch_sync(
            [product_id], granularity=86400, limit=300, max_workers=1
        )
        if not candles or product_id not in candles:
            log.warning("No daily candles for %s", product_id)
            return [], [], [], [], "daily"
        arr = candle_arrays(candles[product_id])
        if arr is None or not arr.get("closes"):
            return [], [], [], [], "daily"
        closes = arr["closes"]
        highs = arr.get("highs", closes)
        lows = arr.get("lows", closes)
        volumes = arr.get("volumes", [1.0] * len(closes))
        return closes, highs, lows, volumes, "daily"

    def compute_tf_stats(self, closes: List[float], highs: List[float], lows: List[float], tf_name: str) -> TFStats:
        if len(closes) < 50:
            return TFStats(tf_name=tf_name)
        stats = TFStats(tf_name=tf_name)
        current = closes[-1]
        sma50_all = _sma(closes, 50)
        stats.sma50 = sma50_all[-1] if sma50_all else 0.0
        if len(closes) >= 200:
            sma200_all = _sma(closes, 200)
            stats.sma200 = sma200_all[-1] if sma200_all else 0.0
        ema12_all = _ema(closes, 12)
        stats.ema12 = ema12_all[-1] if ema12_all else 0.0
        ema26_all = _ema(closes, 26)
        stats.ema26 = ema26_all[-1] if ema26_all else 0.0
        stats.adx = _adx(closes, highs, lows)
        if stats.sma200 > 0:
            stats.price_vs_sma200_pct = ((current - stats.sma200) / stats.sma200) * 100.0
        if len(closes) >= 30:
            stats.recent_return_pct = ((current - closes[-30]) / max(closes[-30], 1e-9)) * 100.0

        # Classify trend
        above_sma200 = current > stats.sma200 if stats.sma200 > 0 else None
        ema_bullish = stats.ema12 > stats.ema26 if stats.ema12 > 0 and stats.ema26 > 0 else None
        if above_sma200 is True and ema_bullish is True:
            stats.trend = "bull"
            stats.trend_strength = min(1.0, stats.adx / 50.0)
        elif above_sma200 is False and ema_bullish is False:
            stats.trend = "bear"
            stats.trend_strength = min(1.0, stats.adx / 50.0)
        else:
            stats.trend = "neutral"
            stats.trend_strength = 0.3

        atr_vals = _atr(highs, lows, closes)
        if atr_vals:
            stats.atr_pct = (atr_vals[-1] / max(current, 1e-9)) * 100.0
        return stats

    def analyze(self, btc_price: float = 0.0) -> CompositeMacroSignal:
        """Full macro analysis — daily BTC trend + Bitcoin halving cycle."""
        cache_key = "macro_tf"
        cached = self._check_cache(cache_key)
        if cached is not None:
            return cached

        btc_product = "BTC-USD"

        closes, highs, lows, volumes, tf_name = self.fetch_higher_tf(btc_product)

        if len(closes) < 30:
            log.warning("Insufficient daily data (%d bars) for BTC macro analysis", len(closes))
            result = CompositeMacroSignal(
                bias="neutral", confidence=0.3, risk_multiplier=1.0,
                allows_new_longs=True, allows_new_shorts=True,
                cycle_phase="unknown", reason="insufficient_data",
                btc_price=btc_price,
            )
            self._set_cache(cache_key, result)
            return result

        stats = self.compute_tf_stats(closes, highs, lows, "daily")

        cycle = detect_bitcoin_cycle(btc_price)
        btc_trend = stats.trend
        btc_adx = stats.adx

        tf_signals = {"daily": btc_trend, "cycle": cycle.bias}

        # Live 24h return vs last daily close (short-term momentum)
        live_return_pct = 0.0
        if btc_price > 0 and closes:
            live_return_pct = ((btc_price / closes[-1]) - 1.0) * 100.0

        # Composite logic
        bullish_factors = 0
        bearish_factors = 0
        total_factors = 0

        # 1. SMA200 position
        total_factors += 1
        if stats.price_vs_sma200_pct > 5:
            bullish_factors += 1
        elif stats.price_vs_sma200_pct < -5:
            bearish_factors += 1

        # 2. EMA crossover
        total_factors += 1
        if stats.ema12 > stats.ema26 and stats.ema12 > 0:
            bullish_factors += 1
        elif stats.ema12 < stats.ema26 and stats.ema12 > 0:
            bearish_factors += 1

        # 3. ADX trend strength (>25 confirms, <20 suggests ranging)
        total_factors += 1
        if btc_adx > 25:
            if btc_trend == "bull":
                bullish_factors += 1
            elif btc_trend == "bear":
                bearish_factors += 1

        # 4. Recent 30-day return
        total_factors += 1
        if stats.recent_return_pct > 10:
            bullish_factors += 1
        elif stats.recent_return_pct < -10:
            bearish_factors += 1

        # 5. Halving cycle phase
        total_factors += 1
        if cycle.bias == "bullish":
            bullish_factors += 1
        elif cycle.bias == "bearish":
            bearish_factors += 1

        # 6. ATR volatility check — high ATR warns of unstable trends
        total_factors += 1
        if stats.atr_pct > 5.0:
            bearish_factors += 1  # high vol is risky for trend trades

        # 7. Live 24h return relative to last daily close (swing momentum)
        total_factors += 1
        if live_return_pct > 2.0:
            bullish_factors += 1
        elif live_return_pct < -2.0:
            bearish_factors += 1

        # Boost: strong live momentum overrides bearish tilt for buys
        live_momentum_positive = live_return_pct > 1.5

        bullish_ratio = bullish_factors / max(total_factors, 1)
        bearish_ratio = bearish_factors / max(total_factors, 1)

        if bullish_ratio >= 0.6:
            bias = "bullish"
            confidence = bullish_ratio
            risk_mult = cycle.risk_multiplier * 1.1
            if live_momentum_positive:
                risk_mult *= 1.1  # extra boost when live momentum confirms
            allows_new_longs = True
            allows_new_shorts = False
            reason_parts = ["bullish multi-tf signal"]
        elif bearish_ratio >= 0.5:
            # Bearish, but live momentum can soften the stance
            if live_momentum_positive and bearish_ratio < 0.7:
                bias = "neutral"
                confidence = 0.5
                risk_mult = cycle.risk_multiplier * 1.0
                allows_new_longs = True
                allows_new_shorts = True
                reason_parts = ["bearish daily but live momentum positive — neutralized"]
            else:
                bias = "bearish"
                confidence = bearish_ratio
                risk_mult = cycle.risk_multiplier * 0.8
                allows_new_longs = False
                allows_new_shorts = True
                reason_parts = ["bearish multi-tf signal"]
        elif bearish_ratio >= 0.3:
            if live_momentum_positive:
                bias = "neutral"
                confidence = 0.5
                risk_mult = cycle.risk_multiplier * 1.0
                allows_new_longs = True
                allows_new_shorts = True
                reason_parts = ["risk-off daily but live momentum positive — neutralized"]
            else:
                bias = "risk_off"
                confidence = bearish_ratio
                risk_mult = 0.6
                allows_new_longs = False
                allows_new_shorts = True
                reason_parts = ["risk-off: mixed signals but bearish tilt"]
        else:
            bias = "neutral"
            confidence = 0.5
            risk_mult = cycle.risk_multiplier
            if live_momentum_positive:
                risk_mult *= 1.1
            allows_new_longs = True
            allows_new_shorts = True
            reason_parts = ["neutral multi-tf signal"]

        reason_parts.append(f"daily={btc_trend}(adx={btc_adx:.0f})")
        reason_parts.append(f"cycle={cycle.name}(mult={risk_mult:.2f})")
        reason_parts.append(f"sma200dev={stats.price_vs_sma200_pct:.1f}%")
        reason_parts.append(f"live24h={live_return_pct:+.1f}%")

        result = CompositeMacroSignal(
            bias=bias,
            confidence=round(confidence, 2),
            risk_multiplier=round(risk_mult, 2),
            allows_new_longs=allows_new_longs,
            allows_new_shorts=allows_new_shorts,
            cycle_phase=cycle.name,
            tf_signals=tf_signals,
            reason=" ".join(reason_parts),
            btc_price=btc_price,
        )

        self._set_cache(cache_key, result)
        log.info("MACRO TF: bias=%s conf=%.2f risk_mult=%.2f longs=%s shorts=%s reason=%s",
                 result.bias, result.confidence, result.risk_multiplier,
                 result.allows_new_longs, result.allows_new_shorts,
                 result.reason)
        return result
