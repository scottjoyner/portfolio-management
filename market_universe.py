#!/usr/bin/env python3
"""Unified market-universe discovery helpers.

Provides a single place to enumerate Coinbase products, prediction markets,
and a stock/ETF watchlist so scanners can review all available opportunities.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, List, Optional


COINBASE_QUOTE_PRIORITY = {
    "USD": 0,
    "USDC": 1,
    "USDT": 2,
    "DAI": 3,
    "BTC": 4,
    "ETH": 5,
}

DEFAULT_STOCK_WATCHLIST = [
    "AAPL", "MSFT", "NVDA", "AMZN", "META", "GOOGL", "TSLA",
    "SPY", "QQQ", "VTI", "IWM", "XLK", "XLF", "XLE",
]


@dataclass(slots=True)
class MarketUniverseEntry:
    source: str
    symbol: str
    market_id: str
    asset_class: str
    market_kind: str
    quote_currency: str = ""
    base_currency: str = ""
    priority: float = 0.0
    actionable: bool = True
    metadata: Dict[str, Any] = field(default_factory=dict)


def _quote_priority(quote_currency: str) -> int:
    return COINBASE_QUOTE_PRIORITY.get(quote_currency.upper(), len(COINBASE_QUOTE_PRIORITY) + 1)


def discover_coinbase_products(connector: Any, max_pairs: int = 0) -> List[MarketUniverseEntry]:
    """Discover every active Coinbase spot product."""
    raw = connector.list_products("SPOT")
    products = raw if isinstance(raw, list) else raw.get("products", []) if isinstance(raw, dict) else []

    entries: List[MarketUniverseEntry] = []
    for product in products:
        if not isinstance(product, dict):
            continue
        if product.get("trading_disabled"):
            continue
        pid = product.get("product_id") or ""
        if not pid:
            continue
        base = product.get("base_currency", pid.split("-")[0])
        quote = product.get("quote_currency", pid.split("-")[-1])
        entries.append(MarketUniverseEntry(
            source="coinbase",
            symbol=pid,
            market_id=pid,
            asset_class=base,
            market_kind="spot",
            quote_currency=quote,
            base_currency=base,
            priority=100.0 - _quote_priority(quote),
            actionable=True,
            metadata={
                "status": product.get("status", ""),
                "base_increment": product.get("base_increment"),
                "quote_increment": product.get("quote_increment"),
                "supported_order_types": product.get("supported_order_types", []),
            },
        ))

    entries.sort(key=lambda e: (e.priority, e.symbol), reverse=True)
    return entries[:max_pairs] if max_pairs and max_pairs > 0 else entries


def discover_prediction_markets(client: Any, limit_per_platform: int = 15) -> Dict[str, List[MarketUniverseEntry]]:
    """Discover Kalshi and Polymarket markets across all categories."""
    result: Dict[str, List[MarketUniverseEntry]] = {}
    categories = client.search_all_categories(limit_per_platform=limit_per_platform, min_volume=0, max_spread=0.25)
    for category, markets in categories.items():
        result[category] = [
            MarketUniverseEntry(
                source=f"prediction:{m.platform}",
                symbol=m.question[:80],
                market_id=m.market_id,
                asset_class=category,
                market_kind="prediction",
                quote_currency="YES/NO",
                base_currency="",
                priority=float(m.volume) * float(m.liquidity_score or 1.0),
                actionable=(category in {"crypto", "economics", "technology"}),
                metadata={
                    "platform": m.platform,
                    "question": m.question,
                    "probability": m.mid_price,
                    "volume": m.volume,
                    "spread": m.spread,
                    "liquidity_score": m.liquidity_score,
                },
            )
            for m in markets
        ]
    return result


def discover_stock_watchlist() -> List[MarketUniverseEntry]:
    """Return a broad stock/ETF watchlist for Alpaca-based scanning."""
    return [
        MarketUniverseEntry(
            source="alpaca",
            symbol=sym,
            market_id=sym,
            asset_class="equity" if sym not in {"SPY", "QQQ", "VTI", "IWM", "XLK", "XLF", "XLE"} else "etf",
            market_kind="stock",
            quote_currency="USD",
            base_currency=sym,
            priority=50.0,
            actionable=True,
            metadata={},
        )
        for sym in DEFAULT_STOCK_WATCHLIST
    ]


def build_master_universe(
    coinbase_connector: Any,
    prediction_market_client: Any | None = None,
    max_coinbase_pairs: int = 0,
    prediction_limit_per_platform: int = 15,
) -> Dict[str, Any]:
    """Build a unified view of all accessible market surfaces."""
    universe: Dict[str, Any] = {
        "coinbase": discover_coinbase_products(coinbase_connector, max_pairs=max_coinbase_pairs),
        "stocks": discover_stock_watchlist(),
        "prediction_markets": {},
    }
    if prediction_market_client is not None:
        universe["prediction_markets"] = discover_prediction_markets(
            prediction_market_client, limit_per_platform=prediction_limit_per_platform
        )
    return universe


def main() -> None:
    import argparse

    parser = argparse.ArgumentParser(description="Inspect the unified market universe")
    parser.add_argument("--coinbase", action="store_true", help="Show Coinbase products")
    parser.add_argument("--predictions", action="store_true", help="Show prediction markets")
    parser.add_argument("--stocks", action="store_true", help="Show stock/ETF watchlist")
    args = parser.parse_args()

    show_all = not (args.coinbase or args.predictions or args.stocks)
    if args.coinbase or show_all:
        print("Coinbase products: discovery requires a live connector")
    if args.predictions or show_all:
        print("Prediction markets: discovery requires unified prediction market client")
    if args.stocks or show_all:
        print(f"Stock/ETF watchlist ({len(DEFAULT_STOCK_WATCHLIST)}): {', '.join(DEFAULT_STOCK_WATCHLIST)}")


if __name__ == "__main__":
    main()
