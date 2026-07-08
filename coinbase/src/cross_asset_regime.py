from __future__ import annotations

import logging
import statistics
import threading
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional

from .resilience import SourceCircuitBreaker, retry_call

log = logging.getLogger(__name__)

_SYMBOLS = {
    "btc": "BTC-USD",
    "spy": "SPY",
    "qqq": "QQQ",
    "vix": "^VIX",
    "dxy": "DX-Y.NYB",
    "tnx": "^TNX",
}


@dataclass
class CrossAssetRegimeState:
    regime: str = "mixed"
    trend_bias: str = "neutral"
    hedge_bias: str = "off"
    allowed_actions: List[str] = field(default_factory=lambda: ["LONG", "FLAT"])
    risk_multiplier: float = 0.75
    confidence: float = 0.0
    allows_new_longs: bool = True
    components: Dict[str, float] = field(default_factory=dict)
    reason: str = "insufficient data"
    updated_at: float = 0.0
    btc_price: float = 0.0

    def to_dict(self) -> Dict[str, Any]:
        return {
            "regime": self.regime,
            "trend_bias": self.trend_bias,
            "hedge_bias": self.hedge_bias,
            "allowed_actions": list(self.allowed_actions),
            "risk_multiplier": round(self.risk_multiplier, 3),
            "confidence": round(self.confidence, 3),
            "allows_new_longs": self.allows_new_longs,
            "components": dict(self.components),
            "reason": self.reason,
            "updated_at": self.updated_at,
            "btc_price": round(self.btc_price, 2),
        }


class CrossAssetRegimeEngine:
    """Cross-asset regime layer for BTC + broad equity weakness.

    Computes a coarse regime that can be used to suppress longs during
    market-wide declines and eventually route to short/hedge-capable
    adapters.
    """

    def __init__(self, cache_ttl_s: float = 300.0, lookback: int = 90):
        self.cache_ttl_s = cache_ttl_s
        self.lookback = lookback
        self._lock = threading.Lock()
        self._breaker = SourceCircuitBreaker("cross_asset:yfinance", failure_threshold=2, reset_timeout_s=300.0)
        self._cached = CrossAssetRegimeState()
        self._last_refresh = 0.0
        self._btc_price: float = 0.0
        self._btc_closes: List[float] = []
        self._btc_volumes: List[float] = []

    def update_btc_snapshot(
        self,
        price: float,
        closes: Optional[List[float]] = None,
        volumes: Optional[List[float]] = None,
    ) -> None:
        with self._lock:
            if price > 0:
                self._btc_price = float(price)
            if closes:
                self._btc_closes = [float(x) for x in closes[-self.lookback :] if isinstance(x, (int, float))]
            if volumes:
                self._btc_volumes = [float(x) for x in volumes[-self.lookback :] if isinstance(x, (int, float))]

    def last_daily_close(self) -> float:
        """Return the most recent daily close for BTC (or 0 if unavailable)."""
        with self._lock:
            if self._btc_closes:
                return self._btc_closes[-1]
            return 0.0

    def get_state(self, refresh: bool = False) -> CrossAssetRegimeState:
        with self._lock:
            now = time.time()
            if not refresh and self._cached.updated_at and (now - self._cached.updated_at) < self.cache_ttl_s:
                return self._cached
            state = self._compute_state(now)
            self._cached = state
            self._last_refresh = now
            return state

    def refresh(self) -> CrossAssetRegimeState:
        return self.get_state(refresh=True)

    def snapshot(self) -> Dict[str, Any]:
        return self.get_state().to_dict()

    def _compute_state(self, now: float) -> CrossAssetRegimeState:
        if not self._breaker.allow():
            cached = self._cached
            if cached.updated_at:
                return cached
            return CrossAssetRegimeState(updated_at=now, reason="breaker_open")

        try:
            btc_score, btc_reason = self._btc_trend_score()
            spy_score, spy_reason = self._yfinance_trend_score(_SYMBOLS["spy"])
            qqq_score, qqq_reason = self._yfinance_trend_score(_SYMBOLS["qqq"])
            vix_score, vix_reason = self._vix_risk_score()
            dxy_score, dxy_reason = self._yfinance_delta_score(_SYMBOLS["dxy"], higher_is_risk_off=True)
            tnx_score, tnx_reason = self._yfinance_delta_score(_SYMBOLS["tnx"], higher_is_risk_off=True)

            components = {
                "btc": btc_score,
                "spy": spy_score,
                "qqq": qqq_score,
                "vix": vix_score,
                "dxy": dxy_score,
                "tnx": tnx_score,
            }
            risk_score = (
                (-btc_score * 0.35)
                + (-spy_score * 0.20)
                + (-qqq_score * 0.20)
                + (vix_score * 0.15)
                + (dxy_score * 0.05)
                + (tnx_score * 0.05)
            )
            btc_trend = btc_score
            equity_trend = (spy_score + qqq_score) / 2.0

            # Live BTC momentum override — if BTC is up >2% from last daily close,
            # cap the regime to at most "mixed" so the UI reflects current price action
            live_override = False
            if self._btc_price > 0 and self._btc_closes:
                live_chg = (self._btc_price - self._btc_closes[-1]) / self._btc_closes[-1]
                if live_chg > 0.02:
                    live_override = True

            regime = "mixed"
            trend_bias = "neutral"
            hedge_bias = "off"
            allows_new_longs = True
            risk_multiplier = 0.75
            confidence = min(1.0, abs(risk_score) / 1.5)

            if live_override:
                # Live BTC momentum overrides stale daily regime for the UI
                regime = "mixed"
                trend_bias = "bullish"
                hedge_bias = "off"
                allows_new_longs = True
                risk_multiplier = 0.85
            elif risk_score >= 0.85 and btc_trend < -0.01 and equity_trend < -0.01:
                regime = "crash"
                trend_bias = "bearish"
                hedge_bias = "on"
                allows_new_longs = False
                risk_multiplier = 0.15
            elif risk_score >= 0.45:
                regime = "risk_off"
                trend_bias = "bearish"
                hedge_bias = "on"
                allows_new_longs = False
                risk_multiplier = 0.35
            elif risk_score <= -0.35 and btc_trend > 0.01 and equity_trend > 0.005:
                regime = "risk_on"
                trend_bias = "bullish"
                hedge_bias = "off"
                allows_new_longs = True
                risk_multiplier = 1.0
            elif btc_trend < 0 and vix_score < 0.15 and equity_trend > -0.005:
                regime = "rebound"
                trend_bias = "bullish"
                hedge_bias = "off"
                allows_new_longs = True
                risk_multiplier = 0.6
            else:
                regime = "mixed"
                trend_bias = "neutral" if abs(btc_trend) < 0.01 else ("bearish" if btc_trend < 0 else "bullish")
                hedge_bias = "on" if risk_score > 0.25 else "off"
                allows_new_longs = risk_score < 0.35
                risk_multiplier = 0.55 if risk_score > 0.25 else 0.75

            reason = "; ".join([
                f"btc={btc_reason}",
                f"spy={spy_reason}",
                f"qqq={qqq_reason}",
                f"vix={vix_reason}",
                f"dxy={dxy_reason}",
                f"tnx={tnx_reason}",
            ])
            if live_override:
                btc_price_val = self._btc_price
                last_close_val = self._btc_closes[-1] if self._btc_closes else 0
                live_pct = ((btc_price_val - last_close_val) / last_close_val) * 100 if last_close_val > 0 else 0
                reason += f"; live_override(btc+{live_pct:.1f}%)"
            self._breaker.on_success()
            return CrossAssetRegimeState(
                regime=regime,
                trend_bias=trend_bias,
                hedge_bias=hedge_bias,
                allowed_actions=["LONG", "FLAT"] if allows_new_longs else ["HEDGE", "FLAT"],
                risk_multiplier=risk_multiplier,
                confidence=confidence,
                allows_new_longs=allows_new_longs,
                components=components,
                reason=reason,
                updated_at=now,
                btc_price=self._btc_price,
            )
        except Exception as e:
            self._breaker.on_failure(e)
            log.debug("Cross-asset regime compute failed: %s", e)
            return self._cached if self._cached.updated_at else CrossAssetRegimeState(updated_at=now, reason=str(e))

    def _btc_trend_score(self) -> tuple[float, str]:
        closes = self._btc_closes
        if len(closes) < 20:
            try:
                from coinbase.src.yahoo_chart import fetch_closes
                closes = retry_call(
                    lambda: fetch_closes(_SYMBOLS["btc"], period="2mo", interval="1d"),
                    attempts=2,
                    base_delay=0.4,
                    max_delay=2.0,
                )
            except Exception as e:
                return 0.0, f"btc_unavailable:{e.__class__.__name__}"
        score, reason = self._trend_score_from_closes(closes, label="btc")
        if self._btc_price > 0 and closes:
            last = closes[-1]
            live_chg = (self._btc_price - last) / last
            if live_chg < -0.02:
                adj = max(-0.3, live_chg * 5.0)
                score += adj
                reason += f"; live_under_last({adj:+.2f})"
            elif live_chg > 0.02:
                adj = min(0.3, live_chg * 5.0)
                score += adj
                reason += f"; live_over_last({adj:+.2f})"
        return score, reason

    def _yfinance_trend_score(self, symbol: str) -> tuple[float, str]:
        series = self._fetch_close_series(symbol)
        if len(series) < 15:
            return 0.0, f"{symbol}:insufficient"
        score, reason = self._trend_score_from_closes(series, label=symbol)
        return score, reason

    def _yfinance_delta_score(self, symbol: str, higher_is_risk_off: bool = False) -> tuple[float, str]:
        series = self._fetch_close_series(symbol)
        if len(series) < 2:
            return 0.0, f"{symbol}:insufficient"
        prev = series[-2]
        last = series[-1]
        chg = (last - prev) / max(prev, 1e-9)
        if higher_is_risk_off:
            score = max(-1.0, min(1.0, chg * 8.0))
        else:
            score = max(-1.0, min(1.0, chg * 8.0))
        return score, f"{symbol}:chg={chg:+.2%}"

    def _vix_risk_score(self) -> tuple[float, str]:
        series = self._fetch_close_series(_SYMBOLS["vix"])
        if len(series) < 2:
            return 0.0, "VIX:insufficient"
        prev = series[-2]
        last = series[-1]
        chg = (last - prev) / max(prev, 1e-9)
        score = 0.0
        if last >= 20:
            score += min(0.5, (last - 20) / 20.0)
        if chg > 0:
            score += min(0.35, chg * 5.0)
        return max(0.0, min(1.0, score)), f"VIX={last:.2f} chg={chg:+.2%}"

    def _trend_score_from_closes(self, closes: List[float], label: str) -> tuple[float, str]:
        closes = [float(x) for x in closes if isinstance(x, (int, float)) and x > 0]
        if len(closes) < 10:
            return 0.0, f"{label}:insufficient"
        short_n = min(20, len(closes))
        long_n = min(50, len(closes))
        short_ma = statistics.fmean(closes[-short_n:])
        long_ma = statistics.fmean(closes[-long_n:])
        last = closes[-1]
        trend = (short_ma - long_ma) / max(long_ma, 1e-9)
        slope = (closes[-1] - closes[max(0, len(closes) - 6)]) / max(closes[max(0, len(closes) - 6)], 1e-9)
        price_pos = (last - min(closes)) / max(max(closes) - min(closes), 1e-9)
        score = trend * 18.0 + slope * 8.0 + (price_pos - 0.5) * 0.6
        score = max(-1.0, min(1.0, score))
        return score, f"{label}:trend={trend:+.2%} slope={slope:+.2%} pos={price_pos:.2f}"

    def _fetch_close_series(self, symbol: str) -> List[float]:
        if not self._breaker.allow():
            return []

        def _fetch() -> List[float]:
            from coinbase.src.yahoo_chart import fetch_closes
            return fetch_closes(symbol, period="3mo", interval="1d")

        try:
            series = retry_call(_fetch, attempts=2, base_delay=0.4, max_delay=2.0)
            self._breaker.on_success()
            return series[-self.lookback :]
        except Exception as e:
            self._breaker.on_failure(e)
            log.debug("Cross-asset fetch failed for %s: %s", symbol, e)
            return []
