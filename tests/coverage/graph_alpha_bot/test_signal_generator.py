import sys
import types
import builtins
from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch

import pandas as pd

# ---------------------------------------------------------------------------
# Inject a lightweight stub for the heavy `unified_signal_generator` module so
# importing `app.strategies.signal_generator` does not pull in the root
# backtester / yfinance stack. Must happen BEFORE the module import below.
# ---------------------------------------------------------------------------
_fake = types.ModuleType("app.strategies.unified_signal_generator")


class UnifiedSignalConfig:
    def __init__(self, **kw):
        self.__dict__.update(kw)


class UnifiedSignalGenerator:
    def __init__(self, config):
        self.config = config

    def generate_signals(self):
        return []


_fake.UnifiedSignalConfig = UnifiedSignalConfig
_fake.UnifiedSignalGenerator = UnifiedSignalGenerator
sys.modules["app.strategies.unified_signal_generator"] = _fake

from app.strategies import signal_generator as m
from app.strategies.signal_generator import SignalConfig, TradingSignal, SignalGenerator


def test_signal_config_defaults():
    c = SignalConfig()
    assert c.symbols == ["BTC-USD", "ETH-USD", "SOL-USD"]
    assert c.sentiment_threshold_long == 0.3
    assert c.signal_cooldown_minutes == 15


def test_trading_signal_to_dict():
    s = TradingSignal(
        symbol="BTC-USD", direction="LONG", confidence=0.77,
        sentiment_score=0.5, price_change_pct=1.2, signal_reason="r",
        timestamp=datetime(2024, 1, 1),
    )
    d = s.to_dict()
    assert d["symbol"] == "BTC-USD" and d["confidence"] == 0.77


def test_init_loads_cache(tmp_path, monkeypatch):
    cache = tmp_path / ".signal_cache.json"
    cache.write_text('{"signals": [{"symbol": "BTC-USD"}]}')
    monkeypatch.setattr(m.SignalGenerator, "__init__", lambda self, config=None: None)
    sg = SignalGenerator.__new__(SignalGenerator)
    sg.signal_cache_file = str(cache)
    sg.cached_signals = {}
    sg._load_cache()
    assert sg.cached_signals["signals"][0]["symbol"] == "BTC-USD"


def test_load_cache_missing(tmp_path):
    sg = SignalGenerator.__new__(SignalGenerator)
    sg.signal_cache_file = str(tmp_path / "absent.json")
    sg.cached_signals = {}
    sg._load_cache()
    assert sg.cached_signals == {"signals": []}


def test_load_cache_corrupt(tmp_path):
    cache = tmp_path / ".signal_cache.json"
    cache.write_text("{not json")
    sg = SignalGenerator.__new__(SignalGenerator)
    sg.signal_cache_file = str(cache)
    sg.cached_signals = {}
    sg._load_cache()
    assert sg.cached_signals == {"signals": []}


def test_save_cache(tmp_path):
    sg = SignalGenerator.__new__(SignalGenerator)
    sg.signal_cache_file = str(tmp_path / ".signal_cache.json")
    sg.cached_signals = {"signals": [{"symbol": "BTC-USD"}]}
    sg._save_cache()
    assert (tmp_path / ".signal_cache.json").exists()


def test_save_cache_error(tmp_path):
    sg = SignalGenerator.__new__(SignalGenerator)
    sg.signal_cache_file = str(tmp_path / "no" / "dir" / ".signal_cache.json")
    sg.cached_signals = {"signals": []}
    sg._save_cache()  # should not raise


# ---------------------------------------------------------------------------
# _get_recent_news_sentiment
# ---------------------------------------------------------------------------

def test_news_sentiment_neo4j_healthy(tmp_path, monkeypatch):
    sg = SignalGenerator(SignalConfig())
    conn = MagicMock()
    conn.is_healthy.return_value = True
    conn.execute_query.return_value = [[0.6], [("t", "a", 0.5)]]
    monkeypatch.setattr("app.db.neo4j_connection.get_connection", lambda: conn)
    score, arts = sg._get_recent_news_sentiment("BTC-USD")
    assert score == 0.6
    assert arts[0]["title"] == "a"


def test_news_sentiment_neo4j_falls_back_to_local(tmp_path, monkeypatch):
    sg = SignalGenerator(SignalConfig())
    conn = MagicMock()
    conn.is_healthy.return_value = False
    monkeypatch.setattr("app.db.neo4j_connection.get_connection", lambda: conn)
    monkeypatch.chdir(tmp_path)
    import json
    kg = {"articles": [{"published_at": datetime.utcnow().isoformat() + "Z",
                        "tickers": ["BTC-USD"], "sentiment_score": 0.8}]}
    with open("knowledge_graph.json", "w") as f:
        json.dump(kg, f)
    score, arts = sg._get_recent_news_sentiment("BTC-USD")
    assert score == 0.8


def test_news_sentiment_no_source(tmp_path, monkeypatch):
    sg = SignalGenerator(SignalConfig())
    conn = MagicMock()
    conn.is_healthy.return_value = False
    monkeypatch.setattr("app.db.neo4j_connection.get_connection", lambda: conn)
    monkeypatch.chdir(tmp_path)  # no knowledge_graph.json
    score, arts = sg._get_recent_news_sentiment("BTC-USD")
    assert score == 0.0 and arts == []


# ---------------------------------------------------------------------------
# _get_price_data
# ---------------------------------------------------------------------------

def test_get_price_data_import_fallback(monkeypatch):
    sg = SignalGenerator(SignalConfig())
    real = builtins.__import__

    def fake_import(name, *a, **k):
        if name == "yfinance":
            raise ImportError("no yfinance")
        return real(name, *a, **k)

    monkeypatch.setattr(builtins, "__import__", fake_import)
    price, change = sg._get_price_data("BTC-USD")
    assert price == 68500.0 and change == 0.0


def test_get_price_data_success(monkeypatch):
    sg = SignalGenerator(SignalConfig())

    class FakeTicker:
        def __init__(self, symbol):
            self.symbol = symbol

        def history(self, period):
            return pd.DataFrame({"Close": [100.0, 110.0]})

        @property
        def info(self):
            return {"currentPrice": 105.0, "regularMarketChangePercent": 3.0}

    class FakeYf:
        Ticker = FakeTicker

    monkeypatch.setitem(sys.modules, "yfinance", FakeYf)
    price, change = sg._get_price_data("BTC-USD")
    assert price == 110.0 and change == 10.0


def test_get_price_data_empty_history(monkeypatch):
    sg = SignalGenerator(SignalConfig())

    class FakeTicker:
        def __init__(self, symbol):
            self.symbol = symbol

        def history(self, period):
            return pd.DataFrame()

        @property
        def info(self):
            return {"currentPrice": 105.0, "regularMarketChangePercent": 3.0}

    class FakeYf:
        Ticker = FakeTicker

    monkeypatch.setitem(sys.modules, "yfinance", FakeYf)
    price, change = sg._get_price_data("BTC-USD")
    assert price == 105.0 and change == 3.0


# ---------------------------------------------------------------------------
# _analyze_sentiment_for_signal
# ---------------------------------------------------------------------------

def test_analyze_news_count_too_low():
    sg = SignalGenerator(SignalConfig())
    assert sg._analyze_sentiment_for_signal(0.9, 1.0, 1) is None


def test_analyze_long_with_boost():
    sg = SignalGenerator(SignalConfig())
    sig = sg._analyze_sentiment_for_signal(0.5, 3.0, 3)
    assert sig is not None
    assert sig.direction == "LONG"
    assert sig.confidence > 0.2


def test_analyze_short():
    sg = SignalGenerator(SignalConfig())
    sig = sg._analyze_sentiment_for_signal(-0.5, -3.0, 3)
    assert sig is not None
    assert sig.direction == "SHORT"
    assert sig.confidence > 0.2


def test_analyze_neutral_none():
    sg = SignalGenerator(SignalConfig())
    assert sg._analyze_sentiment_for_signal(0.1, 1.0, 3) is None


# ---------------------------------------------------------------------------
# _get_target_symbol
# ---------------------------------------------------------------------------

def test_get_target_symbol_fresh():
    sg = SignalGenerator(SignalConfig())
    sg.last_signal_times = {}
    assert sg._get_target_symbol() == "BTC-USD"


def test_get_target_symbol_least_recent():
    sg = SignalGenerator(SignalConfig())
    now = datetime.utcnow()
    sg.last_signal_times = {
        "BTC-USD": now,
        "ETH-USD": now - timedelta(minutes=60),
        "SOL-USD": now - timedelta(minutes=120),
    }
    assert sg._get_target_symbol() == "SOL-USD"


# ---------------------------------------------------------------------------
# generate_signals / main
# ---------------------------------------------------------------------------

def test_generate_signals_saves_cache(tmp_path, monkeypatch):
    sg = SignalGenerator(SignalConfig())
    sg.signal_cache_file = str(tmp_path / ".signal_cache.json")
    monkeypatch.setattr(sg, "_get_recent_news_sentiment", lambda s, h=6: (0.9, [{"title": "x"}, {"title": "y"}]))
    monkeypatch.setattr(sg, "_get_price_data", lambda s: (100.0, 1.0))
    sigs = sg.generate_signals()
    assert len(sigs) >= 1
    assert (tmp_path / ".signal_cache.json").exists()


def test_main(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(m.SignalGenerator, "generate_signals",
                        lambda self: [TradingSignal(symbol="BTC-USD", direction="LONG",
                                                    confidence=0.7, sentiment_score=0.5,
                                                    price_change_pct=1.0, signal_reason="r")])
    out = m.main()
    assert len(out) == 1


def test_analyze_short_no_boost():
    sg = SignalGenerator(SignalConfig())
    sig = sg._analyze_sentiment_for_signal(-0.5, -1.0, 3)
    assert sig is not None
    assert sig.direction == "SHORT"


def test_gen_signals_neutral_no_signal(tmp_path, monkeypatch):
    sg = SignalGenerator(SignalConfig())
    sg.signal_cache_file = str(tmp_path / ".signal_cache.json")
    monkeypatch.setattr(sg, "_get_recent_news_sentiment", lambda s, h=6: (0.1, [{"title": "x"}, {"title": "y"}]))
    monkeypatch.setattr(sg, "_get_price_data", lambda s: (100.0, 1.0))
    sigs = sg.generate_signals()
    assert sigs == []


def test_news_sentiment_neo4j_empty(tmp_path, monkeypatch):
    sg = SignalGenerator(SignalConfig())
    conn = MagicMock()
    conn.is_healthy.return_value = True
    conn.execute_query.return_value = []  # healthy but no rows
    monkeypatch.setattr("app.db.neo4j_connection.get_connection", lambda: conn)
    monkeypatch.chdir(tmp_path)  # no local kg
    score, arts = sg._get_recent_news_sentiment("BTC-USD")
    assert score == 0.0 and arts == []


def test_news_sentiment_local_no_match(tmp_path, monkeypatch):
    sg = SignalGenerator(SignalConfig())
    conn = MagicMock()
    conn.is_healthy.return_value = False
    monkeypatch.setattr("app.db.neo4j_connection.get_connection", lambda: conn)
    monkeypatch.chdir(tmp_path)
    import json
    kg = {"articles": [{"published_at": datetime.utcnow().isoformat() + "Z",
                        "tickers": ["ETH-USD"], "sentiment_score": 0.8}]}
    with open("knowledge_graph.json", "w") as f:
        json.dump(kg, f)
    score, arts = sg._get_recent_news_sentiment("BTC-USD")
    assert score == 0.0 and arts == []


def test_news_sentiment_neo4j_short_row(tmp_path, monkeypatch):
    # Neo4j row with fewer than 3 columns -> empty article dict branch (127-128)
    sg = SignalGenerator(SignalConfig())
    conn = MagicMock()
    conn.is_healthy.return_value = True
    conn.execute_query.return_value = [[0.8], [("title", "art")]]  # len 2 -> else branch
    monkeypatch.setattr("app.db.neo4j_connection.get_connection", lambda: conn)
    score, arts = sg._get_recent_news_sentiment("BTC-USD")
    assert score == 0.8
    assert arts == [{}]


def test_news_sentiment_neo4j_raises_falls_back(tmp_path, monkeypatch):
    # conn healthy but execute_query raises -> hits except handler (127-128) -> local fallback
    sg = SignalGenerator(SignalConfig())
    conn = MagicMock()
    conn.is_healthy.return_value = True
    conn.execute_query.side_effect = RuntimeError("db down")
    monkeypatch.setattr("app.db.neo4j_connection.get_connection", lambda: conn)
    monkeypatch.chdir(tmp_path)  # no local kg -> neutral
    score, arts = sg._get_recent_news_sentiment("BTC-USD")
    assert score == 0.0 and arts == []


def test_news_sentiment_local_corrupt(tmp_path, monkeypatch):
    # knowledge_graph.json exists but is invalid JSON -> local read exception (148-149)
    sg = SignalGenerator(SignalConfig())
    conn = MagicMock()
    conn.is_healthy.return_value = False
    monkeypatch.setattr("app.db.neo4j_connection.get_connection", lambda: conn)
    monkeypatch.chdir(tmp_path)
    (tmp_path / "knowledge_graph.json").write_text("{broken json")
    score, arts = sg._get_recent_news_sentiment("BTC-USD")
    assert score == 0.0 and arts == []
