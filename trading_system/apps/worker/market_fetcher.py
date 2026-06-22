"""
Market Data Fetcher - Live Price Feed from Coinbase v3.

Pulls current prices, order book depth, and 24h stats for configured symbols.
Designed to run as a background loop feeding the signal processor.
"""

from __future__ import annotations

import logging
import time
from typing import Any

logger = logging.getLogger(__name__)


class MarketDataFetcher:
    """Fetches live market data from Coinbase v3 connector."""

    def __init__(self, products: list[str] | None = None) -> None:
        self.products = products or ["BTC-USD", "ETH-USD"]
        self._connector = None  # lazy init on first fetch
        self._last_prices: dict[str, Any] = {}

    def _get_connector(self):
        if self._connector is None:
            from trading_system.connectors.coinbase_v3 import CoinbaseConnectorV3

            self._connector = CoinbaseConnectorV3()
        return self._connector

    def fetch_all(self) -> dict[str, Any]:
        """Fetch market data for all configured products. Returns a flat dict keyed by product."""
        connector = self._get_connector()
        result: dict[str, Any] = {}

        for pid in self.products:
            try:
                price_data = connector.get_price(pid)
                orderbook = connector.get_order_book(pid, level=1)

                # Compute spread from best bid/ask
                bids = orderbook.get("bids", [])
                asks = orderbook.get("asks", [])
                best_bid: float = 0.0
                best_ask: float = 0.0
                if bids and asks:
                    best_bid = float(bids[0].get("price", 0))
                    best_ask = float(asks[0].get("price", 0))
                    mid_price = (best_bid + best_ask) / 2
                    spread_bps = (
                        ((best_ask - best_bid) / mid_price * 10000)
                        if mid_price > 0
                        else 0.0
                    )
                else:
                    mid_price = float(price_data.get("price", 0))
                    spread_bps = 0.0

                result[pid] = {
                    "price": mid_price,
                    "spread": spread_bps / 10000,  # as fraction
                    "bid": best_bid if bids else None,
                    "ask": best_ask if asks else None,
                    "volume_24h": float(price_data.get("quote_volume", 0)),
                    "change_pct": float(price_data.get("price_percent_change_24h", 0))
                    / 100.0,
                }
                self._last_prices[pid] = result[pid]

            except Exception as e:
                logger.warning("Failed to fetch %s: %s", pid, e)
                if pid in self._last_prices:
                    result[pid] = self._last_prices[pid]  # stale fallback
                else:
                    result[pid] = {"price": 0.0, "spread": 0.0}

        return result

    def fetch_single(self, product_id: str) -> dict[str, Any]:
        """Fetch market data for a single product."""
        all_data = self.fetch_all()
        return all_data.get(product_id, {"price": 0.0, "spread": 0.0})


def build_market_state(
    prices: dict[str, Any],
    product_id: str,
    regime: str = "neutral",
    sentiment_score: float = 0.0,
    global_consensus: float = 0.0,
) -> dict[str, Any]:
    """Build a market_state dict for the signal processor."""
    data = prices.get(product_id, {})
    return {
        "product_id": product_id,
        "price": data.get("price", 0),
        "spread": data.get("spread", 0.0),
        "volume_24h": data.get("volume_24h", 0),
        "change_pct": data.get("change_pct", 0.0),
        "regime": regime,
        "sentiment_score": sentiment_score,
        "global_consensus": global_consensus,
        # Market leaders for cross-correlation check
        "market_leaders": ["BTC-USD"],
    }
