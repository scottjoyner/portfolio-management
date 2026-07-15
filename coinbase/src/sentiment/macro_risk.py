import time
import logging
import threading
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

from ..resilience import SourceCircuitBreaker, retry_call

logger = logging.getLogger(__name__)

MACRO_TICKERS = {
    "DX-Y.NYB": "DXY",
    "^TNX": "10Y",
    "^VIX": "VIX",
    "GC=F": "GOLD",
}


@dataclass
class MacroSignal:
    action: str
    confidence: float
    macro_score: float
    components: Dict[str, float]
    product_id: str = "MACRO"

    def to_opportunity(self) -> Dict:
        return {
            "action": self.action,
            "strategy": "macro_risk",
            "confidence": round(self.confidence, 3),
            "reason": f"macro:score={self.macro_score:.2f} "
                      f"DXY={self.components.get('DXY',0):.1f} "
                      f"10Y={self.components.get('10Y',0):.2f}% "
                      f"VIX={self.components.get('VIX',0):.1f} "
                      f"GOLD={self.components.get('GOLD',0):.0f}",
            "macro_score": self.macro_score,
            "components": self.components,
        }


class MacroRiskEngine:
    def __init__(self, cache_ttl: int = 600):
        self._cache_ttl = cache_ttl
        self._cached: Optional[MacroSignal] = None
        self._last_fetch: float = 0.0
        self._lock = threading.Lock()
        self._breaker = SourceCircuitBreaker("macro:yfinance", failure_threshold=2, reset_timeout_s=300.0)

    def get_signal(self) -> Optional[MacroSignal]:
        with self._lock:
            now = time.time()
            if self._cached and now - self._last_fetch < self._cache_ttl:
                return self._cached
            if not self._breaker.allow():
                logger.debug("Macro risk breaker open; using cached signal if available")
                return self._cached
            try:
                from coinbase.src.yahoo_chart import fetch_closes
                components: Dict[str, float] = {}
                for ticker, name in MACRO_TICKERS.items():
                    try:
                        closes = retry_call(
                            lambda: fetch_closes(ticker, period="5d", interval="1d"),
                            attempts=2,
                            base_delay=0.4,
                            max_delay=2.0,
                        )
                        if len(closes) < 2:
                            continue
                        last = closes[-1]
                        prev = closes[-2]
                        if prev > 0:
                            components[name] = last
                            components[f"{name}_chg"] = (last - prev) / prev * 100.0
                    except Exception:
                        continue

                # Composite macro risk score: higher = risk-off
                risk_score = 0.0
                n = 0

                # DXY rising → risk-off
                dxy = components.get("DXY", 0)
                dxy_chg = components.get("DXY_chg", 0)
                if dxy > 0:
                    dxy_z = (dxy - 100.0) / 5.0
                    risk_score += dxy_z * 0.3 + (dxy_chg * 0.05)
                    n += 1

                # 10Y yield rising → risk-off
                y10 = components.get("10Y", 0)
                y10_chg = components.get("10Y_chg", 0)
                if y10 > 0:
                    y10_z = (y10 - 4.0) / 1.0
                    risk_score += y10_z * 0.25 + (y10_chg * 0.04)
                    n += 1

                # VIX rising → risk-off
                vix = components.get("VIX", 0)
                vix_chg = components.get("VIX_chg", 0)
                if vix > 0:
                    vix_z = (vix - 18.0) / 8.0
                    risk_score += vix_z * 0.3 + (vix_chg * 0.03)
                    n += 1

                # Gold rising → risk-off (flight to safety)
                gold = components.get("GOLD", 0)
                gold_chg = components.get("GOLD_chg", 0)
                if gold > 0:
                    gold_z = (gold - 2000.0) / 500.0
                    risk_score += gold_z * 0.15 + (gold_chg * 0.03)
                    n += 1

                macro_score = risk_score / max(n, 1)

                # Determine signal
                # Extreme risk-off (>1.5) → sell crypto
                # Extreme risk-on (<-1.5) → buy crypto
                confidence = min(1.0, abs(macro_score) / 2.0) * 0.6 + 0.2
                action: str
                if macro_score >= 1.5:
                    action = "SELL"
                elif macro_score <= -1.5:
                    action = "BUY"
                else:
                    action = "HOLD"

                sig = MacroSignal(
                    action=action,
                    confidence=min(1.0, confidence),
                    macro_score=round(macro_score, 3),
                    components=components,
                )
                self._cached = sig
                self._last_fetch = now
                self._breaker.on_success()
                if action == "HOLD":
                    return None
                return sig
            except ImportError:  # pragma: no cover - yahoo_chart import cannot fail in-process
                logger.debug("yfinance not available for macro risk")
                return None
            except Exception as e:
                self._breaker.on_failure(e)
                logger.debug("Macro risk fetch error: %s", e)
                return None
