from __future__ import annotations

import logging
import time
from typing import Any, Dict, List, Optional
from collections import defaultdict

log = logging.getLogger(__name__)


class ScalpingStrategy:
    """Fee-tier volume scalping strategy.

    Scans high-volume pairs for micro pullbacks (0.2-0.8%) and generates
    quick breakeven-to-small-profit signals. The primary goal is volume
    generation for Coinbase fee tier improvement, not maximising per-trade P&L.

    Aims for:
      - Entry: pullback from recent micro-high
      - Stop: 1.5× ATR or 0.4% (whichever is smaller)
      - Target: 1.0× ATR or 0.3% (whichever is smaller)
      - Max hold: 600s (10 min)
      - Auto-cycle: after exit, re-enter after 300s cooldown
    """

    def __init__(
        self,
        min_volume_usd: float = 500_000,
        max_spread_bps: float = 15.0,
        min_confidence: float = 0.30,
        product_cooldown_s: float = 300.0,
        max_positions: int = 3,
    ):
        self.min_volume_usd = min_volume_usd
        self.max_spread_bps = max_spread_bps
        self.min_confidence = min_confidence
        self.product_cooldown_s = product_cooldown_s
        self.max_positions = max_positions
        self._last_signal: Dict[str, float] = {}
        self._active_scalps: Dict[str, float] = {}

    def get_signals(
        self,
        product_id: str,
        price: float,
        closes: List[float],
        volumes: List[float],
        volume_24h: float,
        bid: float,
        ask: float,
    ) -> Optional[Dict[str, Any]]:
        now = time.time()
        if product_id in self._last_signal:
            if now - self._last_signal[product_id] < self.product_cooldown_s:
                return None

        if len(closes) < 20:
            return None

        if volume_24h < self.min_volume_usd:
            return None

        spread_bps = ((ask - bid) / max(bid, 1e-9)) * 10_000 if bid > 0 and ask > 0 else 99
        if spread_bps > self.max_spread_bps:
            return None

        recent = closes[-20:]
        recent_high = max(recent)
        recent_low = min(recent)
        current = closes[-1]

        pullback_pct = (recent_high - current) / max(current, 1e-9)
        if not (0.002 <= pullback_pct <= 0.015):
            return None

        recent_volumes = volumes[-10:]
        avg_vol = sum(recent_volumes) / max(len(recent_volumes), 1)
        if avg_vol > 0 and volumes[-1] < avg_vol * 0.5:
            return None

        atr_val = self._estimate_atr(closes)
        stop_dist = min(atr_val * 1.5, current * 0.004) if atr_val > 0 else current * 0.004
        target_dist = min(atr_val * 1.0, current * 0.003) if atr_val > 0 else current * 0.003

        if stop_dist <= 0 or target_dist <= 0:
            return None

        confidence = min(0.65, max(self.min_confidence, pullback_pct * 40))
        rr = target_dist / max(stop_dist, 1e-9)

        self._last_signal[product_id] = now

        return {
            "currency": product_id.split("-")[0],
            "product_id": product_id,
            "strategy": "scalping",
            "action": "BUY",
            "confidence": round(confidence, 4),
            "price": current,
            "win_rate": 0.55,
            "sharpe": 0.8,
            "regime": "ranging",
            "regime_conf": 0.5,
            "atr_14": atr_val,
            "stop_price": current - stop_dist,
            "target_price": current + target_dist,
            "risk_reward": round(rr, 2),
            "pullback_pct": round(pullback_pct * 100, 2),
            "reason": f"scalp:pullback={pullback_pct*100:.2f}%",
        }

    def on_exit(self, product_id: str, pnl: float) -> None:
        self._last_signal[product_id] = time.time()

    @staticmethod
    def _estimate_atr(closes: List[float], period: int = 14) -> float:
        if len(closes) < period + 1:
            return 0.0
        trs = []
        for i in range(len(closes) - period, len(closes)):
            if i == 0:
                continue
            trs.append(abs(closes[i] - closes[i - 1]))
        return sum(trs) / max(len(trs), 1) if trs else 0.0
