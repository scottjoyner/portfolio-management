from __future__ import annotations

import json
import logging
import time
import urllib.request
import urllib.error
from typing import Any, Dict, List, Optional
from pathlib import Path

from ..resilience import SourceCircuitBreaker, retry_call

log = logging.getLogger(__name__)

# Mapping from Coinbase product IDs to CoinGecko asset IDs
PRODUCT_TO_COINGECKO: Dict[str, str] = {
    "BTC-USD": "bitcoin",
    "ETH-USD": "ethereum",
    "SOL-USD": "solana",
    "XRP-USD": "ripple",
    "ADA-USD": "cardano",
    "DOGE-USD": "dogecoin",
    "AVAX-USD": "avalanche-2",
    "DOT-USD": "polkadot",
    "LINK-USD": "chainlink",
    "UNI-USD": "uniswap",
    "POL-USD": "matic-network",
    "ATOM-USD": "cosmos",
    "LTC-USD": "litecoin",
    "BCH-USD": "bitcoin-cash",
    "NEAR-USD": "near",
    "APT-USD": "aptos",
    "SUI-USD": "sui",
    "ARB-USD": "arbitrum",
    "OP-USD": "optimism",
    "FIL-USD": "filecoin",
    "INJ-USD": "injective-protocol",
    "SEI-USD": "sei-network",
    "TIA-USD": "celestia",
    "ALGO-USD": "algorand",
    "XLM-USD": "stellar",
    "STX-USD": "blockstack",
    "HBAR-USD": "hedera-hashgraph",
    "ICP-USD": "internet-computer",
    "GRT-USD": "the-graph",
    "SHIB-USD": "shiba-inu",
    "PEPE-USD": "pepe",
    "BONK-USD": "bonk",
    "TRUMP-USD": "official-trump",
    "FLOKI-USD": "floki",
    "AAVE-USD": "aave",
    "CRV-USD": "curve-dao-token",
    "MKR-USD": "maker",
    "COMP-USD": "compound-governance-token",
    "SNX-USD": "synthetix-network-token",
    "YFI-USD": "yearn-finance",
}

COINGECKO_EXCHANGE_FLOWS_URL = "https://api.coingecko.com/api/v3/coins/{}/market_chart?vs_currency=usd&days=2"
COINGECKO_API_HEADERS = {
    "User-Agent": "PortfolioTrader/1.0",
    "Accept": "application/json",
}


class OnChainFlowStrategy:
    """On-chain exchange flow signal generator.

    Uses CoinGecko free API to fetch exchange inflow/outflow proxies
    (price + volume data over last 2 days). Detects anomalies:
      - Large volume spikes (>3x avg) → potential exchange movement activity
      - Sustained price divergence from market → whale accumulation/distribution

    Signals:
      - SELL when volume spike + price decline (exchange inflows = selling pressure)
      - BUY when volume spike + price stable/rising (accumulation)
      - Confidence weighted by volume anomaly magnitude

    Caches results for 300s to avoid rate limiting.
    """

    def __init__(
        self,
        cache_ttl: float = 300.0,
        volume_spike_threshold: float = 3.0,
        min_confidence: float = 0.30,
    ):
        self.cache_ttl = cache_ttl
        self.volume_spike_threshold = volume_spike_threshold
        self.min_confidence = min_confidence
        self._cache: Dict[str, Dict[str, Any]] = {}
        self._cache_ts: Dict[str, float] = {}
        self._breaker = SourceCircuitBreaker("coingecko:onchain", failure_threshold=3, reset_timeout_s=300.0)

    def get_signals(
        self, product_ids: List[str]
    ) -> List[Dict[str, Any]]:
        signals: List[Dict[str, Any]] = []
        for pid in product_ids:
            base = pid.split("-")[0]
            cg_id = PRODUCT_TO_COINGECKO.get(pid)
            if not cg_id:
                continue

            data = self._fetch_flow_proxy(cg_id)
            if not data:
                continue

            confidence = data.get("confidence", 0.0)
            if confidence < self.min_confidence:
                continue

            signals.append({
                "currency": base,
                "product_id": pid,
                "strategy": "onchain_flow",
                "action": data.get("action", "HOLD"),
                "confidence": round(confidence, 4),
                "price": data.get("current_price", 0.0),
                "win_rate": 0.50,
                "sharpe": 0.6,
                "regime": "ranging",
                "regime_conf": 0.4,
                "atr_14": 0.0,
                "volume_anomaly": round(data.get("volume_anomaly", 0.0), 1),
                "price_trend": round(data.get("price_trend", 0.0), 4),
                "reason": f"onchain:{data.get('action','HOLD')}_vol_anomaly={data.get('volume_anomaly',0):.1f}x",
            })

        return signals

    def _fetch_flow_proxy(self, cg_id: str) -> Optional[Dict[str, Any]]:
        now = time.time()
        if cg_id in self._cache:
            if now - self._cache_ts.get(cg_id, 0.0) < self.cache_ttl:
                return self._cache[cg_id]

        if not self._breaker.allow():
            log.debug("CoinGecko on-chain breaker open; using cache for %s", cg_id)
            return self._cache.get(cg_id)

        url = f"https://api.coingecko.com/api/v3/coins/{cg_id}/market_chart?vs_currency=usd&days=2"
        try:
            req = urllib.request.Request(url, headers=COINGECKO_API_HEADERS)
            def _fetch() -> Dict[str, Any]:
                with urllib.request.urlopen(req, timeout=10) as resp:
                    return json.loads(resp.read().decode())

            body = retry_call(_fetch, attempts=3, base_delay=0.5, max_delay=4.0)
            self._breaker.on_success()
        except Exception as e:
            self._breaker.on_failure(e)
            log.debug("CoinGecko fetch failed for %s: %s", cg_id, e)
            return None

        prices = body.get("prices", [])
        volumes = body.get("total_volumes", [])

        if len(prices) < 12 or len(volumes) < 12:
            return None

        price_values = [p[1] for p in prices[-48:]]
        volume_values = [v[1] for v in volumes[-48:]]

        if not price_values or not volume_values:
            return None

        current_price = price_values[-1]
        avg_volume = sum(volume_values[:-1]) / max(len(volume_values) - 1, 1)
        latest_volume = volume_values[-1] if len(volume_values) > 0 else 0
        volume_anomaly = latest_volume / max(avg_volume, 1e-9) if avg_volume > 0 else 0.0

        price_change_24h = (price_values[-1] - price_values[0]) / max(price_values[0], 1e-9)

        result: Dict[str, Any] = {
            "current_price": current_price,
            "volume_anomaly": volume_anomaly,
            "price_trend": price_change_24h,
            "confidence": 0.0,
            "action": "HOLD",
        }

        if volume_anomaly >= self.volume_spike_threshold:
            if price_change_24h < -0.03:
                confidence = min(0.70, 0.20 + (volume_anomaly - self.volume_spike_threshold) * 0.10)
                result["confidence"] = confidence
                result["action"] = "SELL"
            elif price_change_24h > 0.01:
                confidence = min(0.65, 0.20 + (volume_anomaly - self.volume_spike_threshold) * 0.08)
                result["confidence"] = confidence
                result["action"] = "BUY"
            else:
                result["action"] = "HOLD"
        elif volume_anomaly < 0.3 and price_change_24h < -0.05:
            result["confidence"] = 0.30
            result["action"] = "BUY"

        self._cache[cg_id] = result
        self._cache_ts[cg_id] = now
        return result

    def invalidate_cache(self) -> None:
        self._cache.clear()
        self._cache_ts.clear()
