import json
import sys
import types
import importlib.util as _ilu
from datetime import datetime
from unittest.mock import MagicMock, patch, mock_open

import unittest.mock as _mock

# The strategy classes (BTCVolatilityStacking, ...) are referenced by this module
# but live only in archive/root/backtester.py in the current codebase (the live
# root backtester.py was removed). Provide lightweight stubs so the module imports
# and its pure logic can be exercised. Real strategy behavior is covered elsewhere.
_STRATEGY_NAMES = [
    "BTCVolatilityStacking", "BTCVolatilityBreakout", "BTCVolatilityMeanReversion",
    "BTCVolatilityMomentum", "CoinbaseMomentumStrategy", "CoinbaseMeanReversionStrategy",
    "VolatilityBreakoutStrategy", "RegimeAwareAdaptiveStrategy", "VolumeProfileStrategy",
    "MultiTimeframeConfluenceStrategy", "OrderFlowPressureStrategy",
    "VolatilityContractionExpansionStrategy", "StatisticalArbitrageZScorePairStrategy",
    "LiquidationHeatmapStrategy",
]
_STUB_SRC = "\n".join(
    f"class {n}:\n    def __init__(self, *a, **k):\n        pass\n    def generate_signals(self, data):\n        return []\n"
    for n in _STRATEGY_NAMES
)
import tempfile as _tempfile
_STUB_PATH = _tempfile.mktemp(suffix=".py")
with open(_STUB_PATH, "w") as _f:
    _f.write(_STUB_SRC)
_orig_spec = _ilu.spec_from_file_location
def _patched_spec(name, location, *a, **k):
    return _orig_spec(name, _STUB_PATH, *a, **k)
_mock.patch.object(_ilu, "spec_from_file_location", _patched_spec).start()

import graph_alpha_bot.app.strategies.unified_signal_generator as m
from graph_alpha_bot.app.strategies.unified_signal_generator import (
    UnifiedSignalGenerator,
    UnifiedSignalConfig,
    NewsSentimentAnalyzer,
    StrategySignalGenerator,
    UnifiedTradingSignal,
    SignalDirection,
)


def _kg():
    return {
        "articles": [
            {"title": "BTC adoption", "tickers": ["BTC-USD"], "sentiment_score": 0.9,
             "topic": "adoption", "is_breaking": True, "freshness": 0.9},
            {"title": "BTC tech upgrade", "tickers": ["BTC-USD"], "sentiment_score": 0.6,
             "topic": "technology", "is_breaking": False, "freshness": 0.5},
            {"title": "BTC more adoption", "tickers": ["BTC-USD"], "sentiment_score": 0.8,
             "topic": "adoption", "is_breaking": True, "freshness": 0.9},
            {"title": "ETH hack", "tickers": ["ETH-USD"], "sentiment_score": 0.1,
             "topic": "hacks_security", "is_breaking": False, "freshness": 0.5},
            {"title": "ETH hack2", "tickers": ["ETH-USD"], "sentiment_score": 0.1,
             "topic": "hacks_security", "is_breaking": False, "freshness": 0.5},
            {"title": "SOL regulation", "tickers": ["SOL-USD"], "sentiment_score": 0.2,
             "topic": "regulation", "is_breaking": False, "freshness": 0.5},
            {"title": "SOL regulation2", "tickers": ["SOL-USD"], "sentiment_score": 0.2,
             "topic": "regulation", "is_breaking": False, "freshness": 0.5},
            {"title": "XRP weak", "tickers": ["XRP-USD"], "sentiment_score": 0.1,
             "topic": "adoption", "is_breaking": False, "freshness": 0.5},
            {"title": "XRP weak2", "tickers": ["XRP-USD"], "sentiment_score": 0.1,
             "topic": "adoption", "is_breaking": False, "freshness": 0.5},
            {"title": "BTC no topic", "tickers": ["BTC-USD"], "sentiment_score": 0.5,
             "topic": None, "is_breaking": False, "freshness": 0.5},
            {"title": "BTC macro news", "tickers": ["BTC-USD"], "sentiment_score": 0.4,
             "topic": "macro", "is_breaking": False, "freshness": 0.5},
        ]
    }


def test_analyze_full(monkeypatch):
    kg = _kg()
    monkeypatch.setattr(m.os.path, "exists", lambda p: True)
    with patch("builtins.open", mock_open(read_data=json.dumps(kg))):
        res = NewsSentimentAnalyzer(["btc", "eth", "sol", "xrp", "ada"]).analyze_full()
    assert "btc" in res
    assert res["btc"]["adoption_count"] == 2
    assert res["btc"]["breaking_ratio"] > 0.3
    assert "eth" in res and res["eth"]["hack_count"] == 2


def test_analyze_sentiment(monkeypatch):
    kg = _kg()
    monkeypatch.setattr(m.os.path, "exists", lambda p: True)
    with patch("builtins.open", mock_open(read_data=json.dumps(kg))):
        res = NewsSentimentAnalyzer(["btc"]).analyze_sentiment()
    assert res["btc"][0] > 0


def test_analyze_full_no_file(monkeypatch):
    monkeypatch.setattr(m.os.path, "exists", lambda p: False)
    assert NewsSentimentAnalyzer(["btc"]).analyze_full() == {}


def test_strategy_signal_generator_init():
    sg = StrategySignalGenerator()
    assert len(sg.strategies) == 14


def test_generate_strategy_signals_conversion(monkeypatch):
    sg = StrategySignalGenerator()
    fake = MagicMock()
    fake.generate_signals.return_value = [("BUY", 100.0), ("SELL", 90.0)]
    sg.strategies = {"MyStrat": fake}
    sigs = sg.generate_strategy_signals("BTC-USD", [{"close": 100.0}])
    assert len(sigs) == 2
    assert sigs[0].direction == "LONG"
    assert sigs[1].direction == "SHORT"


def test_generate_strategy_signals_raises(monkeypatch):
    sg = StrategySignalGenerator()
    bad = MagicMock()
    bad.generate_signals.side_effect = RuntimeError("boom")
    none = MagicMock()
    none.generate_signals.return_value = []
    sg.strategies = {"Bad": bad, "None": none}
    # should not raise; bad skipped, none returns nothing
    assert sg.generate_strategy_signals("BTC-USD", []) == []


def test_calculate_confidence_groups():
    sg = StrategySignalGenerator()
    sigs = [("BUY", 1.0), ("SELL", 2.0)]
    assert sg._calculate_strategy_confidence("BTCVolatilityStacking", sigs) > 0
    assert sg._calculate_strategy_confidence("BTCVolatilityBreakout", sigs) > 0
    assert sg._calculate_strategy_confidence("BTCVolatilityMomentum", sigs) > 0
    assert sg._calculate_strategy_confidence("VolumeProfile", sigs) > 0
    assert sg._calculate_strategy_confidence("ZScorePairArb", sigs) > 0
    assert sg._calculate_strategy_confidence("UnknownStrat", sigs) == 0.7
    assert sg._calculate_strategy_confidence("X", []) == 0.0


def test_calculate_technical_score():
    sg = StrategySignalGenerator()
    assert sg._calculate_technical_score("X", [("BUY", 1), ("BUY", 2)]) > 0
    assert sg._calculate_technical_score("X", [("SELL", 1), ("SELL", 2)]) < 0
    assert sg._calculate_technical_score("X", [("BUY", 1), ("SELL", 1)]) == 0
    assert sg._calculate_technical_score("X", []) == 0


def test_analyze_news_sentiment_news_count_lt2():
    g = UnifiedSignalGenerator(UnifiedSignalConfig(symbols=["BTC-USD"]))
    assert g._analyze_news_sentiment_for_signal(0.9, 5.0, 1, "BTC-USD") is None


def test_analyze_news_sentiment_long_boost():
    g = UnifiedSignalGenerator(UnifiedSignalConfig(symbols=["BTC-USD"]))
    sig = g._analyze_news_sentiment_for_signal(0.9, 5.0, 3, "BTC-USD")
    assert sig.direction == "LONG"
    assert sig.confidence > 0.2 + 0.5 * 0.9  # boosted by price_change>2


def test_analyze_news_sentiment_short_boost():
    g = UnifiedSignalGenerator(UnifiedSignalConfig(symbols=["BTC-USD"]))
    sig = g._analyze_news_sentiment_for_signal(-0.9, -5.0, 3, "BTC-USD")
    assert sig.direction == "SHORT"
    assert sig.confidence > 0.2


def test_analyze_news_sentiment_neutral_none():
    g = UnifiedSignalGenerator(UnifiedSignalConfig(symbols=["BTC-USD"]))
    assert g._analyze_news_sentiment_for_signal(0.1, 1.0, 3, "BTC-USD") is None


def _rich():
    return {
        "btc": {"avg_sentiment": 0.85, "topic_adjusted_sentiment": 0.85, "count": 3,
                "breaking_ratio": 0.6, "hack_count": 0, "regulation_count": 0,
                "technology_count": 1, "adoption_count": 2, "topics": ["adoption", "technology"]},
        "eth": {"avg_sentiment": 0.1, "count": 2, "breaking_ratio": 0,
                "hack_count": 2, "regulation_count": 0, "technology_count": 0,
                "adoption_count": 0, "topics": ["hacks_security"]},
        "sol": {"avg_sentiment": 0.2, "count": 2, "breaking_ratio": 0,
                "hack_count": 0, "regulation_count": 1, "technology_count": 0,
                "adoption_count": 0, "topics": ["regulation"]},
        "xrp": {"avg_sentiment": 0.1, "count": 2, "breaking_ratio": 0,
                "hack_count": 0, "regulation_count": 0, "technology_count": 0,
                "adoption_count": 1, "topics": ["adoption"]},
        "ada": {"avg_sentiment": 0.85, "count": 2, "breaking_ratio": 0.0,
                "hack_count": 0, "regulation_count": 0, "technology_count": 0,
                "adoption_count": 0, "topics": []},
    }


def test_generate_signals_integration(monkeypatch, tmp_path):
    cfg = UnifiedSignalConfig(symbols=["BTC-USD", "ETH-USD", "SOL-USD", "XRP-USD", "ADA-USD"])
    g = UnifiedSignalGenerator(cfg)
    g.signal_cache_file = str(tmp_path / ".cache.json")
    g.news_analyzer.analyze_full = lambda: _rich()
    g._get_price_data = lambda symbol: (100.0, 5.0)
    g.strategy_generator.generate_strategy_signals = lambda symbol, data: [
        UnifiedTradingSignal(symbol=symbol, direction="LONG", confidence=0.9,
                             sentiment_score=0.0, technical_score=0.5,
                             price_change_pct=5.0, news_count=0,
                             signal_reason="x", strategy_name="Strat",
                             timestamp=datetime.now())
    ]
    sigs = g.generate_signals()
    # BTC-USD: base + strategy + adoption + tech -> some pass cooldown
    assert any(s.symbol == "BTC-USD" for s in sigs)
    assert any(s.symbol == "ETH-USD" for s in sigs)
    # strategy signal present
    assert any(s.strategy_name == "Strat" for s in sigs)


def test_generate_signals_news_only(monkeypatch, tmp_path):
    kg = {
        "articles": [
            {"title": "BTC adoption", "tickers": ["BTC-USD"], "sentiment_score": 0.9,
             "topic": "adoption", "is_breaking": True, "freshness": 0.9},
            {"title": "BTC tech", "tickers": ["BTC-USD"], "sentiment_score": 0.6,
             "topic": "technology", "is_breaking": True, "freshness": 0.5},
            {"title": "ADA quiet", "tickers": ["ADA-USD"], "sentiment_score": 0.9,
             "topic": "adoption", "is_breaking": False, "freshness": 0.5},
            {"title": "ADA quiet2", "tickers": ["ADA-USD"], "sentiment_score": 0.8,
             "topic": None, "is_breaking": False, "freshness": 0.5},
        ]
    }
    monkeypatch.setattr(m.os.path, "exists", lambda p: True)
    with patch("builtins.open", mock_open(read_data=json.dumps(kg))):
        cfg = UnifiedSignalConfig(symbols=["BTC-USD", "ADA-USD"], enable_strategy_signals=False)
        g = UnifiedSignalGenerator(cfg)
        g.signal_cache_file = str(tmp_path / ".cache.json")
        g.cached_signals = {'signals': []}
        g._get_price_data = lambda symbol: (100.0, 5.0)
        sigs = g.generate_signals()
    assert any(s.symbol == "BTC-USD" for s in sigs)
    assert any(s.symbol == "ADA-USD" for s in sigs)


def test_strategy_signal_exception(monkeypatch, tmp_path):
    cfg = UnifiedSignalConfig(symbols=["BTC-USD"], enable_news_signals=False)
    g = UnifiedSignalGenerator(cfg)
    g.signal_cache_file = str(tmp_path / ".cache.json")
    g._get_price_data = lambda symbol: (100.0, 0.0)
    g.strategy_generator.generate_strategy_signals = lambda symbol, data: (_ for _ in ()).throw(RuntimeError("boom"))
    # should not raise
    assert g.generate_signals() == []


def test_get_price_data_yfinance(monkeypatch):
    g = UnifiedSignalGenerator(UnifiedSignalConfig(symbols=["BTC-USD"]))
    close = MagicMock()
    close.iloc.__getitem__.side_effect = lambda i: 110.0 if i == -1 else 100.0
    hist = MagicMock(); hist.empty = False; hist.__getitem__.return_value = close
    ticker = MagicMock(); ticker.info = {}; ticker.history.return_value = hist
    yf = MagicMock(); yf.Ticker.return_value = ticker
    monkeypatch.setitem(sys.modules, "yfinance", yf)
    price, chg = g._get_price_data("BTC-USD")
    assert price == 110.0
    assert round(chg, 4) == 10.0


def test_analyze_news_sentiment_short_no_boost():
    g = UnifiedSignalGenerator(UnifiedSignalConfig(symbols=["BTC-USD"]))
    sig = g._analyze_news_sentiment_for_signal(-0.9, -1.0, 3, "BTC-USD")
    assert sig.direction == "SHORT"


def test_should_generate_signal_direct():
    g = UnifiedSignalGenerator(UnifiedSignalConfig(symbols=["BTC-USD"]))
    sig = UnifiedTradingSignal(symbol="BTC-USD", direction="LONG", confidence=0.5,
                               sentiment_score=0.5, technical_score=0.0,
                               price_change_pct=1.0, news_count=2,
                               signal_reason="x", strategy_name="NewsSentimentAnalyzer",
                               timestamp=datetime.now())
    assert g._should_generate_signal(sig) is True
    # cooldown
    g.last_signal_times["BTC-USD"] = datetime.now()
    assert g._should_generate_signal(sig) is False
    # low confidence
    g.last_signal_times.clear()
    sig.confidence = 0.1
    assert g._should_generate_signal(sig) is False
    # news sentiment below threshold
    sig.confidence = 0.5
    sig.sentiment_score = 0.1
    assert g._should_generate_signal(sig) is False


def test_get_price_data_fallback(monkeypatch):
    g = UnifiedSignalGenerator(UnifiedSignalConfig(symbols=["BTC-USD"]))
    monkeypatch.setitem(sys.modules, "yfinance", None)
    price, chg = g._get_price_data("BTC-USD")
    assert price == 68500.0
    price2, chg2 = g._get_price_data("UNKNOWN-USD")
    assert price2 == 100.0


def test_load_cache_exists(tmp_path):
    g = UnifiedSignalGenerator(UnifiedSignalConfig(symbols=["BTC-USD"]))
    cache = tmp_path / ".uc.json"
    cache.write_text(json.dumps({"signals": [{"a": 1}]}))
    g.signal_cache_file = str(cache)
    g._load_cache()
    assert g.cached_signals == {"signals": [{"a": 1}]}


def test_load_cache_invalid(tmp_path):
    g = UnifiedSignalGenerator(UnifiedSignalConfig(symbols=["BTC-USD"]))
    cache = tmp_path / ".uc.json"
    cache.write_text("{bad")
    g.signal_cache_file = str(cache)
    g._load_cache()
    assert g.cached_signals == {"signals": []}


def test_save_cache_exception():
    g = UnifiedSignalGenerator(UnifiedSignalConfig(symbols=["BTC-USD"]))
    g.signal_cache_file = "/nonexistent_dir_xyz/cache.json"
    g._save_cache()


def test_analyze_full_exception(monkeypatch):
    monkeypatch.setattr(m.os.path, "exists", lambda p: True)
    with patch("builtins.open", mock_open(read_data="{not json")):
        assert NewsSentimentAnalyzer(["btc"]).analyze_full() == {}


def test_get_strategy_signals():
    g = UnifiedSignalGenerator(UnifiedSignalConfig(symbols=["BTC-USD"]))
    info = g.get_strategy_signals()
    assert "BTCVolatilityStacking" in info["available_strategies"]
    assert g._get_strategy_description("BTCVolatilityStacking").startswith("BTC")
    assert g._get_strategy_description("Nope") == "Unknown strategy"


def test_main(monkeypatch):
    monkeypatch.setattr(m, "COINBASE_SPOT_PAIRS", ["BTC-USD", "ETH-USD"])
    monkeypatch.setattr(m.UnifiedSignalGenerator, "generate_signals",
                        lambda self: [])
    monkeypatch.setattr(m.UnifiedSignalGenerator, "get_strategy_signals",
                        lambda self: {"available_strategies": [], "supported_symbols": [],
                                      "strategy_details": {}})
    assert m.main() == []
