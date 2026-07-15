import asyncio
import pytest

import trading_system.unified_price_fetcher as upf
from trading_system.unified_price_fetcher import UnifiedPriceFetcher


class FakeProvider:
    def __init__(self, name, prices=None, connect_fail=False, fetch_fail=False):
        self._name = name
        self._prices = prices if prices is not None else {}
        self._connect_fail = connect_fail
        self._fetch_fail = fetch_fail

    async def connect(self):
        if self._connect_fail:
            raise RuntimeError(f"{self._name} connect failed")
        return None

    def get_name(self):
        return self._name

    async def get_current_prices(self, symbols):
        if self._fetch_fail:
            raise RuntimeError(f"{self._name} fetch failed")
        return {s: self._prices.get(s) for s in symbols}


@pytest.fixture
def patched_providers(monkeypatch):
    def make_factory(name, **kw):
        def factory(*a, **k):
            return FakeProvider(name, **kw)
        return factory

    monkeypatch.setattr(upf, "AlpacaProvider", make_factory("alpaca", prices={"AAPL": 1.0}))
    monkeypatch.setattr(upf, "CoinbaseProvider", make_factory("coinbase", prices={"BTC-USD": 50000.0}))
    monkeypatch.setattr(upf, "KrakenProvider", make_factory("kraken", connect_fail=True))
    monkeypatch.setattr(upf, "BinanceProvider", make_factory("binance", connect_fail=True))
    monkeypatch.setattr(upf, "KalshiProvider", make_factory("kalshi"))
    monkeypatch.setattr(upf, "PolymarketProvider", make_factory("polymarket", prices={"m1": 0.4}))


def test_initialize(patched_providers):
    f = UnifiedPriceFetcher()
    asyncio.run(f.initialize())
    assert "alpaca" in f.providers
    assert "coinbase" in f.providers
    assert "kalshi" in f.providers
    assert "polymarket" in f.providers
    assert "kraken" not in f.providers
    assert "binance" not in f.providers


def test_fetch_stock_price_success(patched_providers):
    f = UnifiedPriceFetcher()
    asyncio.run(f.initialize())
    assert asyncio.run(f.fetch_stock_price("AAPL", "alpaca")) == 1.0


def test_fetch_stock_price_fetch_fail(patched_providers, capsys):
    f = UnifiedPriceFetcher()
    asyncio.run(f.initialize())
    f.providers["alpaca"] = FakeProvider("alpaca", fetch_fail=True)
    assert asyncio.run(f.fetch_stock_price("AAPL", "alpaca")) is None
    assert "Error fetching stock price" in capsys.readouterr().out


def test_fetch_stock_price_no_provider(patched_providers, capsys):
    f = UnifiedPriceFetcher()
    asyncio.run(f.initialize())
    assert asyncio.run(f.fetch_stock_price("AAPL", "nonexistent")) is None
    assert "not found" in capsys.readouterr().out


def test_fetch_crypto_price_success(patched_providers):
    f = UnifiedPriceFetcher()
    asyncio.run(f.initialize())
    assert asyncio.run(f.fetch_crypto_price("BTC-USD")) == 50000.0


def test_fetch_crypto_price_fetch_fail(patched_providers, capsys):
    f = UnifiedPriceFetcher()
    asyncio.run(f.initialize())
    f.providers["coinbase"] = FakeProvider("coinbase", fetch_fail=True)
    assert asyncio.run(f.fetch_crypto_price("BTC-USD")) is None
    assert "Error fetching crypto price" in capsys.readouterr().out


def test_fetch_crypto_price_no_provider(patched_providers):
    f = UnifiedPriceFetcher()
    asyncio.run(f.initialize())
    f.providers.pop("coinbase", None)
    assert asyncio.run(f.fetch_crypto_price("BTC-USD")) is None


def test_fetch_prediction_market_price_success(patched_providers):
    f = UnifiedPriceFetcher()
    asyncio.run(f.initialize())
    out = asyncio.run(f.fetch_prediction_market_price("m1", 0, "polymarket"))
    assert out["market"] == "m1"
    assert out["price"] == 0.4


def test_fetch_prediction_market_price_fetch_fail(patched_providers, capsys):
    f = UnifiedPriceFetcher()
    asyncio.run(f.initialize())
    f.providers["polymarket"] = FakeProvider("polymarket", fetch_fail=True)
    out = asyncio.run(f.fetch_prediction_market_price("m1", 0, "polymarket"))
    assert out is None
    assert "Error fetching prediction market" in capsys.readouterr().out


def test_fetch_prediction_market_price_no_provider(patched_providers):
    f = UnifiedPriceFetcher()
    asyncio.run(f.initialize())
    f.providers.pop("polymarket", None)
    assert asyncio.run(f.fetch_prediction_market_price("m1")) is None


def test_fetch_all_prices(patched_providers):
    f = UnifiedPriceFetcher()
    asyncio.run(f.initialize())
    prices = asyncio.run(f.fetch_all_prices(
        stocks=["AAPL", "TSLA"],
        cryptos=["BTC-USD", "ETH-USD"],
        predictions=[{"market_id": "m1", "outcome": 0, "platform": "polymarket"}],
    ))
    assert prices["stocks"]["AAPL"] == 1.0
    assert prices["cryptos"]["BTC-USD"] == 50000.0
    assert prices["predictions"]["m1"]["price"] == 0.4


def test_fetch_all_prices_empty(patched_providers):
    f = UnifiedPriceFetcher()
    asyncio.run(f.initialize())
    prices = asyncio.run(f.fetch_all_prices())
    assert prices["stocks"] == {}
    assert prices["cryptos"] == {}
    assert prices["predictions"] == {}


def test_initialize_loads_env_file(patched_providers, monkeypatch):
    loaded = {}

    class FakePath:
        def __init__(self, *args):
            self.parts = args
        @classmethod
        def home(cls):
            return cls("/home/scott")
        def __truediv__(self, other):
            return FakePath(*self.parts, other)
        def exists(self):
            return True

    monkeypatch.setattr(upf, "Path", FakePath)
    monkeypatch.setattr(upf.dotenv, "load_dotenv",
                        lambda p: loaded.setdefault("loaded", p))

    f = UnifiedPriceFetcher()
    asyncio.run(f.initialize())
    assert "loaded" in loaded
    assert "alpaca" in f.providers


def test_fetch_all_prices_missing_market_id(patched_providers):
    f = UnifiedPriceFetcher()
    asyncio.run(f.initialize())
    prices = asyncio.run(f.fetch_all_prices(
        predictions=[{"platform": "polymarket"}],
    ))
    assert prices["predictions"] == {}


def test_test_unified_fetcher_runs():
    class _P:
        async def connect(self):
            return None
        def get_name(self):
            return "coinbase"
        async def get_current_prices(self, syms):
            return {s: 1.0 for s in syms}

    upf.AlpacaProvider = lambda **k: _P()
    upf.CoinbaseProvider = lambda **k: _P()
    upf.KrakenProvider = lambda **k: _P()
    upf.BinanceProvider = lambda **k: _P()
    upf.KalshiProvider = lambda **k: _P()
    upf.PolymarketProvider = lambda **k: _P()
    asyncio.run(upf.test_unified_fetcher())
