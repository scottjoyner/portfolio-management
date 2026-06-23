from __future__ import annotations
import math
from enum import Enum
from dataclasses import dataclass, field
from typing import List, Dict, Optional, Set, Tuple

from .protocols import Direction, Bar, BracketSetup, BaseStrategy, Opportunity


class StrategyArchetype(Enum):
    TREND_FOLLOW = "trend_follow"
    MOMENTUM = "momentum"
    MEAN_REVERSION = "mean_reversion"
    BREAKOUT = "breakout"
    VOLATILITY = "volatility"
    SCALPING = "scalping"
    GRID = "grid"
    MARKET_MAKING = "market_making"
    PAIRS = "pairs"
    SENTIMENT_DRIVEN = "sentiment_driven"
    ACCUMULATION = "accumulation"
    DCA = "dca"
    FUNDING_CAPTURE = "funding_capture"
    MOMENTUM_ACCELERATION = "momentum_acceleration"


@dataclass
class MarketConditionProfile:
    regime: str = "unknown"
    fear_greed: float = 50.0
    news_sentiment_pulse: float = 0.0
    trend_strength: float = 0.0
    volatility_bps: float = 30.0
    adx: float = 25.0
    hurst: float = 0.5
    serial_correlation: float = 0.0
    volume_trend: float = 0.0
    breaking_news_ratio: float = 0.0
    has_hacks: bool = False
    has_regulation: bool = False

    @property
    def is_extreme_sentiment(self) -> bool:
        return self.fear_greed < 20 or self.fear_greed > 80 or abs(self.news_sentiment_pulse) > 0.4

    @property
    def is_trending_strongly(self) -> bool:
        return self.adx > 30 and abs(self.trend_strength) > 0.03

    @property
    def is_ranging(self) -> bool:
        return self.adx < 20 and self.volatility_bps < 40

    @property
    def is_high_volatility(self) -> bool:
        return self.volatility_bps > 80 or self.hurst < 0.3

    @property
    def is_mean_reverting(self) -> bool:
        return self.hurst < 0.4 and self.serial_correlation < -0.1

    @property
    def is_trending(self) -> bool:
        return self.hurst > 0.6 and self.serial_correlation > 0.1

    @property
    def is_risk_off(self) -> bool:
        return (self.fear_greed < 25 or self.news_sentiment_pulse < -0.3
                or self.has_hacks or self.regime in ("strong_downtrend", "high_volatility"))

    @property
    def is_risk_on(self) -> bool:
        return (self.fear_greed > 60 and self.news_sentiment_pulse > 0.1
                and self.regime in ("strong_uptrend", "weak_uptrend"))


ARCHETYPE_FIT_RULES: Dict[StrategyArchetype, str] = {
    StrategyArchetype.TREND_FOLLOW: "Strong trends with ADX > 25, Hurst > 0.6",
    StrategyArchetype.MOMENTUM: "Positive trend acceleration, rising volume, ADX > 20",
    StrategyArchetype.MEAN_REVERSION: "Ranging/low vol, Hurst < 0.4, serial correlation < -0.1",
    StrategyArchetype.BREAKOUT: "High volatility or compression breaks, ADX rising, volume spike",
    StrategyArchetype.VOLATILITY: "High volatility (bps > 60), wide ATR, regime = high_vol",
    StrategyArchetype.SCALPING: "Low volatility, tight ranges, ADX < 20",
    StrategyArchetype.GRID: "Ranging, low ADX (< 20), moderate volume, neutral sentiment",
    StrategyArchetype.MARKET_MAKING: "Any condition, reduces size in high vol",
    StrategyArchetype.PAIRS: "Stable relative relationships, moderate volatility",
    StrategyArchetype.SENTIMENT_DRIVEN: "Extreme sentiment (FG < 20 or > 80), breaking news",
    StrategyArchetype.ACCUMULATION: "Risk-off sentiment, downtrend exhaustion, low ADX",
    StrategyArchetype.DCA: "Any condition, steady accumulation regardless",
    StrategyArchetype.FUNDING_CAPTURE: "Calm markets, stable funding rates, moderate vol",
    StrategyArchetype.MOMENTUM_ACCELERATION: "Strong momentum with acceleration, ADX > 25",
}


def _fit_trend_follow(m: MarketConditionProfile) -> float:
    score = 0.0
    if m.is_trending_strongly:
        score += 0.6
    elif m.is_trending:
        score += 0.3
    if m.adx > 25:
        score += 0.2
    if m.is_ranging:
        score -= 0.5
    if "downtrend" in m.regime and m.fear_greed < 30:
        score += 0.1
    return max(0.0, min(1.0, score))


def _fit_momentum(m: MarketConditionProfile) -> float:
    score = 0.0
    if m.trend_strength > 0.02 and m.adx > 20:
        score += 0.4
    if m.volume_trend > 0.02:
        score += 0.2
    if m.serial_correlation > 0.15:
        score += 0.15
    if m.is_extreme_sentiment and m.regime in ("strong_uptrend",):
        score += 0.15
    if m.adx < 15:
        score -= 0.3
    return max(0.0, min(1.0, score))


def _fit_mean_reversion(m: MarketConditionProfile) -> float:
    score = 0.0
    if m.is_mean_reverting:
        score += 0.5
    if m.is_ranging:
        score += 0.3
    if m.volatility_bps < 40:
        score += 0.1
    if m.is_trending_strongly:
        score -= 0.4
    return max(0.0, min(1.0, score))


def _fit_breakout(m: MarketConditionProfile) -> float:
    score = 0.0
    if m.volatility_bps > 60 and m.adx > 25:
        score += 0.4
    if m.volume_trend > 0.03:
        score += 0.2
    if m.hurst > 0.65 and m.regime not in ("ranging", "low_volatility"):
        score += 0.2
    if m.regime == "high_volatility":
        score += 0.15
    return max(0.0, min(1.0, score))


def _fit_volatility(m: MarketConditionProfile) -> float:
    score = 0.0
    if m.volatility_bps > 60:
        score += 0.5
    elif m.volatility_bps > 40:
        score += 0.25
    if m.regime == "high_volatility":
        score += 0.3
    if m.hurst < 0.35:
        score += 0.15
    if m.volatility_bps < 20:
        score -= 0.3
    return max(0.0, min(1.0, score))


def _fit_scalping(m: MarketConditionProfile) -> float:
    score = 0.0
    if m.volatility_bps < 35 and m.adx < 20:
        score += 0.5
    if m.is_ranging:
        score += 0.3
    if m.fear_greed < 30 or m.fear_greed > 70:
        score += 0.1
    if m.is_trending_strongly:
        score -= 0.3
    return max(0.0, min(1.0, score))


def _fit_grid(m: MarketConditionProfile) -> float:
    score = 0.0
    if m.is_ranging:
        score += 0.5
    if m.adx < 18:
        score += 0.2
    if 30 <= m.fear_greed <= 70:
        score += 0.15
    if m.volatility_bps < 50:
        score += 0.1
    if m.is_trending_strongly:
        score -= 0.4
    return max(0.0, min(1.0, score))


def _fit_market_making(m: MarketConditionProfile) -> float:
    score = 0.5
    if m.volatility_bps > 80:
        score -= 0.2
    if "strong" in m.regime:
        score -= 0.1
    if m.volatility_bps < 30:
        score += 0.15
    return max(0.0, min(1.0, score))


def _fit_pairs(m: MarketConditionProfile) -> float:
    score = 0.3
    if m.is_mean_reverting:
        score += 0.2
    if m.volatility_bps > 60:
        score -= 0.1
    if m.is_ranging:
        score += 0.2
    if m.serial_correlation < -0.15:
        score += 0.15
    return max(0.0, min(1.0, score))


def _fit_sentiment_driven(m: MarketConditionProfile) -> float:
    score = 0.0
    if m.is_extreme_sentiment:
        score += 0.5
    if m.breaking_news_ratio > 0.3:
        score += 0.2
    if m.has_hacks or m.has_regulation:
        score += 0.15
    if m.news_sentiment_pulse < -0.3 and "downtrend" in m.regime:
        score += 0.1
    return max(0.0, min(1.0, score))


def _fit_accumulation(m: MarketConditionProfile) -> float:
    score = 0.2
    if "downtrend" in m.regime and m.fear_greed < 30:
        score += 0.3
    if m.adx < 20 and m.fear_greed < 35:
        score += 0.2
    if m.news_sentiment_pulse < -0.2:
        score += 0.15
    if m.is_risk_on:
        score -= 0.3
    return max(0.0, min(1.0, score))


def _fit_dca(m: MarketConditionProfile) -> float:
    return 0.6


def _fit_funding_capture(m: MarketConditionProfile) -> float:
    score = 0.3
    if m.volatility_bps < 50:
        score += 0.2
    if m.adx < 20:
        score += 0.15
    if m.is_ranging:
        score += 0.15
    if m.is_trending_strongly:
        score -= 0.2
    return max(0.0, min(1.0, score))


def _fit_momentum_acceleration(m: MarketConditionProfile) -> float:
    score = 0.0
    if m.trend_strength > 0.03 and m.adx > 25:
        score += 0.4
    if m.volume_trend > 0.03:
        score += 0.2
    if m.serial_correlation > 0.2:
        score += 0.2
    if m.regime in ("strong_uptrend", "weak_uptrend"):
        score += 0.15
    return max(0.0, min(1.0, score))


ARCHETYPE_FIT_FUNCTIONS: Dict[StrategyArchetype, callable] = {
    StrategyArchetype.TREND_FOLLOW: _fit_trend_follow,
    StrategyArchetype.MOMENTUM: _fit_momentum,
    StrategyArchetype.MEAN_REVERSION: _fit_mean_reversion,
    StrategyArchetype.BREAKOUT: _fit_breakout,
    StrategyArchetype.VOLATILITY: _fit_volatility,
    StrategyArchetype.SCALPING: _fit_scalping,
    StrategyArchetype.GRID: _fit_grid,
    StrategyArchetype.MARKET_MAKING: _fit_market_making,
    StrategyArchetype.PAIRS: _fit_pairs,
    StrategyArchetype.SENTIMENT_DRIVEN: _fit_sentiment_driven,
    StrategyArchetype.ACCUMULATION: _fit_accumulation,
    StrategyArchetype.DCA: _fit_dca,
    StrategyArchetype.FUNDING_CAPTURE: _fit_funding_capture,
    StrategyArchetype.MOMENTUM_ACCELERATION: _fit_momentum_acceleration,
}


STRATEGY_TO_ARCHETYPE: Dict[str, StrategyArchetype] = {
    # Trend Follow
    "ema_cross": StrategyArchetype.TREND_FOLLOW,
    "macd": StrategyArchetype.TREND_FOLLOW,
    "adx": StrategyArchetype.TREND_FOLLOW,
    "parabolic_sar": StrategyArchetype.TREND_FOLLOW,
    "psar": StrategyArchetype.TREND_FOLLOW,
    "trix": StrategyArchetype.TREND_FOLLOW,
    "aroon": StrategyArchetype.TREND_FOLLOW,
    "hma": StrategyArchetype.TREND_FOLLOW,
    "donchian": StrategyArchetype.TREND_FOLLOW,
    "trend_rsi_pullback": StrategyArchetype.TREND_FOLLOW,
    "trend_rsi_rip": StrategyArchetype.TREND_FOLLOW,
    "MultiTimeframeRSIMomentumStrategy": StrategyArchetype.TREND_FOLLOW,
    # Momentum
    "force_idx": StrategyArchetype.MOMENTUM,
    "vpt": StrategyArchetype.MOMENTUM,
    "price_eff": StrategyArchetype.MOMENTUM,
    "mom_accel": StrategyArchetype.MOMENTUM_ACCELERATION,
    "momentum_acceleration": StrategyArchetype.MOMENTUM_ACCELERATION,
    "vol_mom": StrategyArchetype.MOMENTUM,
    "MomentumAccelerationStrategy": StrategyArchetype.MOMENTUM_ACCELERATION,
    # Mean Reversion
    "rsi_revert": StrategyArchetype.MEAN_REVERSION,
    "zscore_revert": StrategyArchetype.MEAN_REVERSION,
    "cmo": StrategyArchetype.MEAN_REVERSION,
    "williams_r": StrategyArchetype.MEAN_REVERSION,
    "scci": StrategyArchetype.MEAN_REVERSION,
    "ema_dev": StrategyArchetype.MEAN_REVERSION,
    "vwap_revert": StrategyArchetype.MEAN_REVERSION,
    "donchian_breakout": StrategyArchetype.BREAKOUT,
    "donchian_breakdown": StrategyArchetype.BREAKOUT,
    "range_exp_idx": StrategyArchetype.BREAKOUT,
    "AnchoredVWAPMeanReversionStrategy": StrategyArchetype.MEAN_REVERSION,
    "VolatilityCompressionBreakoutStrategy": StrategyArchetype.BREAKOUT,
    "DonchianPullbackContinuationStrategy": StrategyArchetype.BREAKOUT,
    # Volatility
    "boll_break": StrategyArchetype.VOLATILITY,
    "keltner": StrategyArchetype.VOLATILITY,
    "snr_idx": StrategyArchetype.VOLATILITY,
    "volatility_scalper": StrategyArchetype.VOLATILITY,
    "BollingerSqueezeBreakoutStrategy": StrategyArchetype.VOLATILITY,
    "VolRegimeSwitchStrategy": StrategyArchetype.VOLATILITY,
    # Scalping
    "rsi_failure_swing": StrategyArchetype.SCALPING,
    "impulse_exhaustion": StrategyArchetype.SCALPING,
    "RSIFailureSwingReversalStrategy": StrategyArchetype.SCALPING,
    "ImpulseExhaustionReversalStrategy": StrategyArchetype.SCALPING,
    # Grid
    "grid_trading": StrategyArchetype.GRID,
    # Market Making
    "market_making": StrategyArchetype.MARKET_MAKING,
    # Pairs
    "cointegrated_pairs": StrategyArchetype.PAIRS,
    # Sentiment Driven
    "sentiment_driven": StrategyArchetype.SENTIMENT_DRIVEN,
    "fear_greed": StrategyArchetype.SENTIMENT_DRIVEN,
    "news_risk": StrategyArchetype.SENTIMENT_DRIVEN,
    "kalshi": StrategyArchetype.SENTIMENT_DRIVEN,
    "polymarket": StrategyArchetype.SENTIMENT_DRIVEN,
    "SentimentMomentumCompositeStrategy": StrategyArchetype.SENTIMENT_DRIVEN,
    "OnChainRegimeWhaleFlowStrategy": StrategyArchetype.SENTIMENT_DRIVEN,
    # Accumulation
    "accumulation": StrategyArchetype.ACCUMULATION,
    "dca_accumulation": StrategyArchetype.DCA,
    "funding_capture": StrategyArchetype.FUNDING_CAPTURE,
    # Adaptive / Multi-purpose
    "adaptive_mode": StrategyArchetype.TREND_FOLLOW,
    "momentum_rotation": StrategyArchetype.MOMENTUM,
    "order_book_imbalance": StrategyArchetype.SCALPING,
    "LiquidityVacuumReversalStrategy": StrategyArchetype.BREAKOUT,
    "RegimeAwareAdaptiveStrategy": StrategyArchetype.TREND_FOLLOW,
    # Price Action / Novel
    "price_action_sr": StrategyArchetype.BREAKOUT,
    "hmm_regime": StrategyArchetype.TREND_FOLLOW,
    "candlestick_patterns": StrategyArchetype.BREAKOUT,
    "smart_money_flow": StrategyArchetype.SENTIMENT_DRIVEN,
    # Niche
    "chaikin_mf": StrategyArchetype.MOMENTUM,
    "obv_div": StrategyArchetype.MOMENTUM,
}


class MarketConditionStrategySelector:
    def __init__(self, min_fit_threshold: float = 0.2,
                 top_archetypes: int = 3):
        self.min_fit = min_fit_threshold
        self.top_archetypes = top_archetypes
        self._last_profile: Optional[MarketConditionProfile] = None
        self._last_archetype_scores: Dict[StrategyArchetype, float] = {}
        self._last_strategy_weights: Dict[str, float] = {}

    def evaluate(self, profile: MarketConditionProfile) -> Dict[str, float]:
        self._last_profile = profile

        archetype_scores: Dict[StrategyArchetype, float] = {}
        for archetype, fit_fn in ARCHETYPE_FIT_FUNCTIONS.items():
            archetype_scores[archetype] = fit_fn(profile)
        self._last_archetype_scores = archetype_scores

        strategy_weights: Dict[str, float] = {}
        for strat_name, archetype in STRATEGY_TO_ARCHETYPE.items():
            base_fit = archetype_scores.get(archetype, 0.0)
            if base_fit < self.min_fit:
                strategy_weights[strat_name] = 0.0
            else:
                strategy_weights[strat_name] = round(base_fit, 3)
        self._last_strategy_weights = strategy_weights

        return strategy_weights

    def weight(self, strategy_name: str) -> float:
        return self._last_strategy_weights.get(strategy_name, self.min_fit)

    def is_enabled(self, strategy_name: str) -> bool:
        return self.weight(strategy_name) >= self.min_fit

    def top_archetype_fits(self) -> List[Tuple[StrategyArchetype, float]]:
        fits = sorted(self._last_archetype_scores.items(),
                      key=lambda x: x[1], reverse=True)
        return fits[:self.top_archetypes]

    def top_strategies(self, limit: int = 10) -> List[Tuple[str, float]]:
        enabled = [(n, w) for n, w in self._last_strategy_weights.items()
                   if w >= self.min_fit]
        return sorted(enabled, key=lambda x: x[1], reverse=True)[:limit]

    def market_summary(self) -> str:
        if not self._last_profile:
            return "no market data"
        p = self._last_profile
        parts = [f"regime={p.regime}", f"fg={p.fear_greed:.0f}",
                 f"sent={p.news_sentiment_pulse:+.2f}",
                 f"adx={p.adx:.0f}", f"hurst={p.hurst:.2f}",
                 f"vol={p.volatility_bps:.0f}bps"]
        if p.is_risk_off:
            parts.append("RISK_OFF")
        if p.is_risk_on:
            parts.append("RISK_ON")
        top = self.top_archetype_fits()
        if top:
            parts.append("best=" + ",".join(f"{a.value}({s:.2f})" for a, s in top))
        return " | ".join(parts)

    def filter_opportunities(self, opportunities: List[Opportunity]) -> List[Opportunity]:
        filtered = []
        for opp in opportunities:
            w = self.weight(opp.strategy_name)
            if w >= self.min_fit:
                opp.confidence = min(0.95, opp.confidence * (0.5 + w * 0.5))
                opp.meta["market_fit_weight"] = w
                opp.meta["market_regime"] = self._last_profile.regime if self._last_profile else "unknown"
                reason_tag = f"[fit={w:.2f}]"
                if reason_tag not in opp.reason:
                    opp.reason += f" {reason_tag}"
                filtered.append(opp)
        return filtered

    def summary(self) -> Dict:
        return {
            "profile": {
                "regime": self._last_profile.regime if self._last_profile else None,
                "fear_greed": self._last_profile.fear_greed if self._last_profile else None,
                "is_risk_off": self._last_profile.is_risk_off if self._last_profile else False,
                "is_risk_on": self._last_profile.is_risk_on if self._last_profile else False,
                "is_trending_strongly": self._last_profile.is_trending_strongly if self._last_profile else False,
            } if self._last_profile else {},
            "top_archetypes": [
                {"archetype": a.value, "fit": round(s, 3)}
                for a, s in self.top_archetype_fits()
            ],
            "top_strategies": [
                {"name": n, "weight": round(w, 3)}
                for n, w in self.top_strategies(10)
            ],
            "strategies_enabled": sum(
                1 for w in self._last_strategy_weights.values() if w >= self.min_fit
            ),
            "strategies_total": len(self._last_strategy_weights),
        }
