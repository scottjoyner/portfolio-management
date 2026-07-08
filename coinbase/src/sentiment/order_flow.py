import time
import logging
import threading
from collections import defaultdict
from dataclasses import dataclass
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)


@dataclass
class OrderFlowSignal:
    product_id: str
    action: str
    confidence: float
    spread_bps: float
    volume_24h: float
    spread_z: float
    spread_tight: bool

    def to_opportunity(self) -> Dict:
        return {
            "action": self.action,
            "strategy": "order_flow",
            "confidence": round(self.confidence, 3),
            "reason": f"orderflow:spread={self.spread_bps:.1f}bps "
                      f"z={self.spread_z:.1f} vol={self.volume_24h:.0f}",
            "spread_bps": self.spread_bps,
            "spread_z": round(self.spread_z, 2),
            "volume_24h": self.volume_24h,
        }


class OrderFlowEngine:
    """Evaluates market microstructure conditions per product.

    Runs every `eval_interval` seconds per product. Uses a rolling
    window of spreads to compute z-score. When spread is tight vs
    its recent history and volume is high → favorable for bull continuation.
    When spread blows out → liquidity stress → bearish.
    """

    def __init__(self, window: int = 100, eval_interval: float = 10.0):
        self._window = window
        self._eval_interval = eval_interval
        self._spread_history: Dict[str, List[float]] = defaultdict(list)
        self._signals: Dict[str, OrderFlowSignal] = {}
        self._last_eval: Dict[str, float] = defaultdict(float)
        self._lock = threading.Lock()

    def evaluate(self, product_id: str, bid: float, ask: float, price: float, volume_24h: float) -> Optional[OrderFlowSignal]:
        if bid <= 0 or ask <= 0 or bid >= ask or price <= 0:
            return None
        now = time.time()
        if now - self._last_eval.get(product_id, 0.0) < 1.0:
            return None

        spread_bps = ((ask - bid) / bid) * 10000.0
        if spread_bps <= 0:
            return None

        with self._lock:
            history = self._spread_history[product_id]
            history.append(spread_bps)
            if len(history) > self._window:
                history.pop(0)
            self._last_eval[product_id] = now

            n = len(history)
            if n < 20:
                return None

            mean = sum(history) / n
            var = sum((x - mean) ** 2 for x in history) / n
            std = var ** 0.5 if var > 0 else 1.0
            spread_z = (spread_bps - mean) / std

        is_tight = spread_z < -1.5
        is_wide = spread_z > 1.5

        confidence = 0.0
        action: Optional[str] = None

        if is_tight:
            # Tight spread: high liquidity, favorable for bulls
            # More confident if volume is also above avg
            vol_conf = min(1.0, volume_24h / 5_000_000.0)  # scale up to $5M
            spread_conf = min(1.0, abs(spread_z) / 3.0)
            confidence = spread_conf * 0.6 + vol_conf * 0.4
            action = "BUY"
        elif is_wide:
            # Wide spread: liquidity stress, bearish
            spread_conf = min(1.0, abs(spread_z) / 3.0)
            confidence = spread_conf * 0.7
            action = "SELL"

        if action is None or confidence < 0.3:
            return None

        sig = OrderFlowSignal(
            product_id=product_id,
            action=action,
            confidence=min(1.0, confidence),
            spread_bps=round(spread_bps, 1),
            volume_24h=volume_24h,
            spread_z=round(spread_z, 2),
            spread_tight=is_tight,
        )
        with self._lock:
            self._signals[product_id] = sig
        return sig

    def get_signal(self, product_id: str) -> Optional[OrderFlowSignal]:
        with self._lock:
            return self._signals.get(product_id)
