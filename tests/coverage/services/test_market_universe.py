"""Coverage tests for market_universe helpers."""

from unittest.mock import MagicMock, patch

import pytest

import market_universe as mu
from market_universe import (
    MarketUniverseEntry,
    _quote_priority,
    build_master_universe,
    discover_coinbase_products,
    discover_prediction_markets,
    discover_stock_watchlist,
)


def test_quote_priority():
    assert _quote_priority("USD") == mu.COINBASE_QUOTE_PRIORITY["USD"]
    assert _quote_priority("UNKNOWN") == len(mu.COINBASE_QUOTE_PRIORITY) + 1
    assert _quote_priority("btc") == mu.COINBASE_QUOTE_PRIORITY["BTC"]


def test_discover_coinbase_products():
    class Conn:
        def list_products(self, kind):
            return {
                "products": [
                    {"product_id": "BTC-USD", "base_currency": "BTC", "quote_currency": "USD",
                     "status": "online", "trading_disabled": False},
                    {"product_id": "ETH-USDC", "base_currency": "ETH", "quote_currency": "USDC",
                     "status": "online", "trading_disabled": False},
                    {"product_id": "DISABLED-USD", "trading_disabled": True},
                    "not-a-dict",  # skipped
                    {"quote_currency": "USD"},  # no product_id -> skipped
                ]
            }

    entries = discover_coinbase_products(Conn(), max_pairs=0)
    pids = {e.symbol for e in entries}
    assert "BTC-USD" in pids and "ETH-USDC" in pids
    assert "DISABLED-USD" not in pids
    # USD (priority 0) ranks above USDC (priority 1)
    assert entries[0].symbol == "BTC-USD"
    # max_pairs limits
    limited = discover_coinbase_products(Conn(), max_pairs=1)
    assert len(limited) == 1


def test_discover_coinbase_list_return():
    class Conn:
        def list_products(self, kind):
            return [
                {"product_id": "BTC-USD", "quote_currency": "USD", "trading_disabled": False},
            ]

    entries = discover_coinbase_products(Conn())
    assert entries[0].source == "coinbase"
    assert entries[0].market_kind == "spot"


def test_discover_stock_watchlist():
    entries = discover_stock_watchlist()
    assert len(entries) == len(mu.DEFAULT_STOCK_WATCHLIST)
    # ETFs classified correctly
    etfs = {e.symbol for e in entries if e.asset_class == "etf"}
    assert {"SPY", "QQQ", "VTI", "IWM", "XLK", "XLF", "XLE"} <= etfs
    for e in entries:
        assert e.source == "alpaca"


def test_discover_prediction_markets():
    class Client:
        def search_all_categories(self, limit_per_platform=15, min_volume=0, max_spread=0.25):
            return {
                "crypto": [
                    MagicMock(market_id="c1", platform="kalshi", question="Will BTC go up?",
                              mid_price=0.6, volume=1000.0, liquidity_score=0.5, spread=0.02),
                ],
                "sports": [
                    MagicMock(market_id="s1", platform="polymarket", question="Who wins?",
                              mid_price=0.5, volume=500.0, liquidity_score=0.3, spread=0.05),
                ],
                "politics": [],
            }

    res = discover_prediction_markets(Client())
    assert res["crypto"][0].actionable is True
    assert res["crypto"][0].source == "prediction:kalshi"
    assert res["sports"][0].actionable is False
    assert res["politics"] == []


def test_build_master_universe_with_and_without_pm():
    class Conn:
        def list_products(self, kind):
            return [{"product_id": "BTC-USD", "quote_currency": "USD", "trading_disabled": False}]

    uni = build_master_universe(Conn())
    assert "coinbase" in uni and "stocks" in uni and "prediction_markets" in uni
    assert uni["prediction_markets"] == {}

    class PMClient:
        def search_all_categories(self, **kw):
            return {"crypto": []}

    uni2 = build_master_universe(Conn(), prediction_market_client=PMClient(),
                                 max_coinbase_pairs=1, prediction_limit_per_platform=3)
    assert uni2["prediction_markets"] == {"crypto": []}
    assert len(uni2["coinbase"]) == 1


def test_market_universe_entry_dataclass():
    e = MarketUniverseEntry(source="x", symbol="s", market_id="m", asset_class="ac",
                            market_kind="mk")
    assert e.actionable is True
    assert e.metadata == {}


def test_main_prints(monkeypatch, capsys):
    monkeypatch.setattr("sys.argv", ["market_universe"])
    mu.main()
    out = capsys.readouterr().out
    assert "Stock/ETF watchlist" in out
    assert "14" in out  # len(DEFAULT_STOCK_WATCHLIST)


def test_main_flags(monkeypatch, capsys):
    for flag in ("--coinbase", "--predictions", "--stocks"):
        monkeypatch.setattr("sys.argv", ["market_universe", flag])
        mu.main()
        out = capsys.readouterr().out
        assert out.strip()
