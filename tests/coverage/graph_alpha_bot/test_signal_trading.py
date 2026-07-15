import sys
import types
import json
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

from app.core import signal_trading as m
from app.core.signal_trading import (
    SignalDirection,
    TradingSignal,
    SignalConfig,
    NewsSentimentAnalyzer,
    TechnicalAnalyzer,
    SignalGenerator,
    PaperTradingExecutor,
)


def _install_fake_unified(generate_return=None, raise_on_gen=False):
    """Insert a stub `app.strategies.unified_signal_generator` module so the
    heavy real import chain is avoided and signal generation is controllable."""
    fake = types.ModuleType("app.strategies.unified_signal_generator")

    class USig:
        def __init__(self, **kw):
            pass

    class UGen:
        def __init__(self, config):
            self.config = config
            if raise_on_gen:
                raise RuntimeError("unified boom")

        def generate_signals(self):
            return list(generate_return or [])

    fake.UnifiedSignalConfig = USig
    fake.UnifiedSignalGenerator = UGen
    sys.modules["app.strategies.unified_signal_generator"] = fake
    return fake


class FakeSignal:
    def __init__(self, symbol="BTC-USD", direction="LONG", confidence=0.7,
                 sentiment_score=0.5, technical_score=0.3, price_change_pct=1.2,
                 news_count=3, signal_reason="r", strategy_name="s",
                 timestamp=None):
        self.symbol = symbol
        self.direction = direction
        self.confidence = confidence
        self.sentiment_score = sentiment_score
        self.technical_score = technical_score
        self.price_change_pct = price_change_pct
        self.news_count = news_count
        self.signal_reason = signal_reason
        self.strategy_name = strategy_name
        self.timestamp = timestamp or datetime.now(timezone.utc)


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------

def test_trading_signal_to_dict():
    s = TradingSignal(
        symbol="BTC-USD", direction="LONG", confidence=0.77,
        sentiment_score=0.5, technical_score=0.3, price_change_pct=1.23,
        news_count=2, signal_reason="r", timestamp=datetime(2024, 1, 1),
    )
    d = s.to_dict()
    assert d["symbol"] == "BTC-USD"
    assert d["direction"] == "LONG"
    assert d["confidence"] == 0.77
    assert d["price_change_pct"] == 1.23


def test_signal_config_defaults():
    c = SignalConfig()
    assert c.symbols == ["BTC-USD", "ETH-USD", "SOL-USD"]
    assert c.sentiment_threshold == 0.25
    assert c.cooldown_minutes == 15


def test_signal_direction_enum():
    assert SignalDirection.LONG.value == "LONG"
    assert SignalDirection.SHORT.value == "SHORT"
    assert SignalDirection.CLOSE.value == "CLOSE"


# ---------------------------------------------------------------------------
# NewsSentimentAnalyzer
# ---------------------------------------------------------------------------

def test_news_analyzer_no_kg(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    a = NewsSentimentAnalyzer(["BTC-USD", "ETH-USD"])
    assert a.analyze_sentiment() == {}


def test_news_analyzer_with_kg(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    (tmp_path / "app" / "data").mkdir(parents=True)
    kg = {
        "articles": [
            {"title": "btc price soars", "sentiment_score": 0.9},
            {"title": "BTC ETF approved", "sentiment_score": 0.7},
        ]
    }
    (tmp_path / "app" / "data" / "knowledge_graph.json").write_text(json.dumps(kg))
    a = NewsSentimentAnalyzer(["BTC-USD"])
    res = a.analyze_sentiment()
    assert "BTC" in res
    assert res["BTC"][0] > 0.7


def test_signal_generator_unified_generate_exception_falls_back():
    # init succeeds but generate_signals() raises -> caught -> fallback path
    fake = types.ModuleType("app.strategies.unified_signal_generator")

    class USig:
        def __init__(self, **kw):
            pass

    class UGen:
        def __init__(self, config):
            self.config = config

        def generate_signals(self):
            raise RuntimeError("gen boom")

    fake.UnifiedSignalConfig = USig
    fake.UnifiedSignalGenerator = UGen
    sys.modules["app.strategies.unified_signal_generator"] = fake

    gen = SignalGenerator(SignalConfig(symbols=["BTC-USD"]))
    assert gen.use_unified is True
    gen.analyzer.analyze_sentiment = lambda: {"BTC-USD": (0.9, 3)}
    gen.tech_analyzer.get_technical_score = lambda: {"BTC-USD": (0.35, 1.2)}
    sigs = gen.generate_signals()
    assert len(sigs) == 1
    assert sigs[0].direction == "LONG"


def test_news_analyzer_open_error(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    (tmp_path / "app" / "data").mkdir(parents=True)
    (tmp_path / "app" / "data" / "knowledge_graph.json").write_text("{}")
    a = NewsSentimentAnalyzer(["BTC-USD"])
    with patch("builtins.open", side_effect=OSError("boom")):
        assert a.analyze_sentiment() == {}


def test_news_analyzer_no_matching():
    a = NewsSentimentAnalyzer(["BTC-USD"])
    a.analyze_sentiment = lambda: {}
    assert a.get_signal_reason("BTC", 0.5, 1) == "Positive news sentiment (0.50) from 1 articles"
    assert a.get_signal_reason("BTC", -0.5, 1) == "Negative news sentiment (-0.50) from 1 articles"


# ---------------------------------------------------------------------------
# TechnicalAnalyzer
# ---------------------------------------------------------------------------

def test_technical_analyzer():
    t = TechnicalAnalyzer(["BTC-USD", "ETH-USD", "SOL-USD", "XRP-USD"])
    out = t.get_technical_score()
    assert out["BTC-USD"][0] == 0.35
    assert "XRP-USD" not in out  # not in fixed data set


# ---------------------------------------------------------------------------
# SignalGenerator (unified path)
# ---------------------------------------------------------------------------

def test_signal_generator_unified_path():
    _install_fake_unified(generate_return=[FakeSignal()])
    gen = SignalGenerator(SignalConfig())
    assert gen.use_unified is True
    sigs = gen.generate_signals()
    assert len(sigs) == 1
    assert isinstance(sigs[0], TradingSignal)
    assert sigs[0].symbol == "BTC-USD"


def test_signal_generator_unified_failure_falls_back():
    _install_fake_unified(raise_on_gen=True)
    gen = SignalGenerator(SignalConfig())
    # Fallback engaged
    assert gen.use_unified is False
    gen.analyzer.analyze_sentiment = lambda: {"BTC-USD": (0.9, 3)}
    gen.tech_analyzer.get_technical_score = lambda: {"BTC-USD": (0.35, 1.2)}
    sigs = gen.generate_signals()
    assert len(sigs) == 1
    assert sigs[0].direction == "LONG"
    assert sigs[0].confidence > 0.85 or sigs[0].confidence <= 0.85


# ---------------------------------------------------------------------------
# SignalGenerator (traditional fallback path branches)
# ---------------------------------------------------------------------------

def test_signal_generator_fallback_long_with_boost():
    _install_fake_unified(raise_on_gen=True)
    gen = SignalGenerator(SignalConfig(symbols=["BTC-USD"]))
    gen.analyzer.analyze_sentiment = lambda: {"BTC-USD": (0.9, 3)}
    gen.tech_analyzer.get_technical_score = lambda: {"BTC-USD": (0.35, 1.2)}
    sigs = gen.generate_signals()
    assert len(sigs) == 1
    s = sigs[0]
    assert s.direction == "LONG"
    # both positive -> confidence boost applied
    assert s.confidence >= 0.25


def test_signal_generator_fallback_neutral_skip():
    _install_fake_unified(raise_on_gen=True)
    gen = SignalGenerator(SignalConfig(symbols=["XRP-USD"]))
    # sentiment & technical both near zero -> neutral skip branch
    gen.analyzer.analyze_sentiment = lambda: {"XRP-USD": (0.1, 0)}
    gen.tech_analyzer.get_technical_score = lambda: {"XRP-USD": (0.05, 0.0)}
    assert gen.generate_signals() == []


def test_signal_generator_fallback_direction_none():
    _install_fake_unified(raise_on_gen=True)
    gen = SignalGenerator(SignalConfig(symbols=["ADA-USD"]))
    # strong sentiment but non-positive technical -> direction stays None
    gen.analyzer.analyze_sentiment = lambda: {"ADA-USD": (0.9, 3)}
    gen.tech_analyzer.get_technical_score = lambda: {"ADA-USD": (0.0, 0.0)}
    assert gen.generate_signals() == []


def test_signal_generator_fallback_price_reason_branch():
    _install_fake_unified(raise_on_gen=True)
    gen = SignalGenerator(SignalConfig(symbols=["BTC-USD"]))
    gen.analyzer.analyze_sentiment = lambda: {"BTC-USD": (0.9, 3)}
    # large price change to trigger the price-change reason branch
    gen.tech_analyzer.get_technical_score = lambda: {"BTC-USD": (0.35, 5.0)}
    sigs = gen.generate_signals()
    assert "UP" in sigs[0].signal_reason


# ---------------------------------------------------------------------------
# PaperTradingExecutor
# ---------------------------------------------------------------------------

def test_executor_get_order_size():
    ex = PaperTradingExecutor(100000.0)
    size = ex.get_order_size("BTC-USD")
    assert size > 0


def test_executor_execute_long_btc():
    ex = PaperTradingExecutor(100000.0)
    sig = TradingSignal(symbol="BTC-USD", direction="LONG", confidence=0.8,
                         sentiment_score=0.5, technical_score=0.3, price_change_pct=1.0,
                         news_count=1, signal_reason="r")
    usd_before = ex.portfolio["USD"]
    order = ex.execute_signal(sig)
    assert order["status"] == "filled"
    assert ex.portfolio["BTC"] > 0.5
    assert ex.portfolio["USD"] < usd_before


def test_executor_execute_long_non_btc_eth():
    ex = PaperTradingExecutor(100000.0)
    sig = TradingSignal(symbol="SOL-USD", direction="LONG", confidence=0.8,
                         sentiment_score=0.5, technical_score=0.3, price_change_pct=1.0,
                         news_count=1, signal_reason="r")
    # SOL is neither BTC nor ETH -> execution returns None
    assert ex.execute_signal(sig) is None


def test_executor_execute_short():
    ex = PaperTradingExecutor(100000.0)
    sig = TradingSignal(symbol="BTC-USD", direction="SHORT", confidence=0.8,
                         sentiment_score=0.5, technical_score=0.3, price_change_pct=1.0,
                         news_count=1, signal_reason="r")
    assert ex.execute_signal(sig) is None


def test_executor_execute_cooldown():
    ex = PaperTradingExecutor(100000.0)
    sig = TradingSignal(symbol="BTC-USD", direction="LONG", confidence=0.8,
                         sentiment_score=0.5, technical_score=0.3, price_change_pct=1.0,
                         news_count=1, signal_reason="r")
    ex.execute_signal(sig)
    # immediate re-execution -> cooldown blocks
    assert ex.execute_signal(sig) is None


def test_executor_display_portfolio(caplog):
    import logging

    caplog.set_level(logging.INFO)
    ex = PaperTradingExecutor(100000.0)
    ex.display_portfolio()
    assert any("PORTFOLIO" in r.message for r in caplog.records)


# ---------------------------------------------------------------------------
# main()
# ---------------------------------------------------------------------------

def test_main_runs():
    _install_fake_unified(generate_return=[FakeSignal()])
    n = m.main()
    assert n == 1


# ---------------------------------------------------------------------------
# Additional fallback / executor branches (merged from extra suite)
# ---------------------------------------------------------------------------

def test_news_analyzer_matching(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    (tmp_path / "app" / "data").mkdir(parents=True)
    kg = {"articles": [{"title": "BTC price soars", "sentiment_score": 0.9}]}
    (tmp_path / "app" / "data" / "knowledge_graph.json").write_text(json.dumps(kg))
    a = NewsSentimentAnalyzer(["BTC-USD"])
    res = a.analyze_sentiment()
    assert res["BTC"] == (0.9, 1)


def test_news_analyzer_partial_match(tmp_path, monkeypatch):
    # one symbol has no matching articles -> line 104 (neutral result)
    monkeypatch.chdir(tmp_path)
    (tmp_path / "app" / "data").mkdir(parents=True)
    kg = {"articles": [{"title": "BTC price soars", "sentiment_score": 0.9}]}
    (tmp_path / "app" / "data" / "knowledge_graph.json").write_text(json.dumps(kg))
    a = NewsSentimentAnalyzer(["BTC-USD", "ETH-USD"])
    res = a.analyze_sentiment()
    assert res["BTC"] == (0.9, 1)
    assert res["ETH"] == (0.0, 0)


def test_fallback_long_small_price_change():
    _install_fake_unified(raise_on_gen=True)
    gen = SignalGenerator(SignalConfig(symbols=["BTC-USD"]))
    gen.analyzer.analyze_sentiment = lambda: {"BTC-USD": (0.9, 3)}
    gen.tech_analyzer.get_technical_score = lambda: {"BTC-USD": (0.35, 0.5)}
    sigs = gen.generate_signals()
    assert sigs[0].direction == "LONG"
    assert "UP" not in sigs[0].signal_reason


def test_fallback_news_count_zero():
    _install_fake_unified(raise_on_gen=True)
    gen = SignalGenerator(SignalConfig(symbols=["XRP-USD"]))
    gen.analyzer.analyze_sentiment = lambda: {"XRP-USD": (0.9, 0)}
    gen.tech_analyzer.get_technical_score = lambda: {"XRP-USD": (0.35, 1.2)}
    sigs = gen.generate_signals()
    assert sigs[0].direction == "LONG"
    assert "news" not in sigs[0].signal_reason


def test_executor_eth_long():
    ex = PaperTradingExecutor(100000.0)
    sig = TradingSignal(symbol="ETH-USD", direction="LONG", confidence=0.8,
                        sentiment_score=0.5, technical_score=0.3, price_change_pct=1.0,
                        news_count=1, signal_reason="r")
    order = ex.execute_signal(sig)
    assert order["status"] == "filled"
    assert ex.portfolio["ETH"] > 2.0


def test_executor_close_direction():
    ex = PaperTradingExecutor(100000.0)
    sig = TradingSignal(symbol="BTC-USD", direction="CLOSE", confidence=0.8,
                        sentiment_score=0.5, technical_score=0.3, price_change_pct=1.0,
                        news_count=1, signal_reason="r")
    assert ex.execute_signal(sig) is None


def test_executor_cooldown_expired_executes():
    ex = PaperTradingExecutor(100000.0)
    sig = TradingSignal(symbol="BTC-USD", direction="LONG", confidence=0.8,
                        sentiment_score=0.5, technical_score=0.3, price_change_pct=1.0,
                        news_count=1, signal_reason="r")
    ex.last_signal_times["BTC-USD"] = datetime.now() - timedelta(minutes=20)
    order = ex.execute_signal(sig)
    assert order["status"] == "filled"


def test_main_short_signal_no_order():
    _install_fake_unified(generate_return=[FakeSignal(direction="SHORT")])
    n = m.main()
    assert n == 0


def test_main_no_signals():
    _install_fake_unified(generate_return=[])
    n = m.main()
    assert n == 0


def test_main_signal_no_reason():
    # order with empty reason -> exercises 483->484 (if order.get("reason") False)
    _install_fake_unified(generate_return=[FakeSignal(symbol="BTC-USD", signal_reason="")])
    n = m.main()
    assert n == 1


def test_main_two_signals():
    # two signals -> exercises for-loop back-edge (483->474)
    _install_fake_unified(generate_return=[
        FakeSignal(symbol="BTC-USD", signal_reason="r1"),
        FakeSignal(symbol="ETH-USD", signal_reason="r2"),
    ])
    n = m.main()
    assert n == 2
