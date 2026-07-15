import json
import subprocess
import sys
import time
import types
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

import graph_alpha_bot.app.strategies.unified_signal_accumulator as usa_mod
from graph_alpha_bot.app.strategies.unified_signal_accumulator import (
    AccumulatedSignal,
    CoinbasePriceProvider,
    DaemonSnapshotAdapter,
    MultiStrategyAdapter,
    NewsSentimentAdapter,
    StrategySignalAdapter,
    UnifiedSignalAccumulator,
    main,
)


MOD = "graph_alpha_bot.app.strategies.unified_signal_accumulator"


def _sig(symbol, action="BUY", conf=0.6, score=0.3, strat="Strat", vol=600):
    return AccumulatedSignal(
        symbol=symbol, action=action, base_confidence=conf, final_confidence=conf,
        opportunity_score=score, strategy_name=strat, signal_reason="r",
        estimated_volume_usd=vol,
    )


# ── AccumulatedSignal ──────────────────────────────────────────────

def test_accumulated_signal_to_dict():
    s = _sig("BTC-USD")
    d = s.to_dict()
    assert d["symbol"] == "BTC-USD"
    assert d["action"] == "BUY"
    assert 0 <= d["final_confidence"] <= 1
    assert "timestamp" in d


# ── CoinbasePriceProvider ─────────────────────────────────────────

def test_price_provider_cache_empty_not_fresh():
    p = CoinbasePriceProvider()
    assert p._is_cache_fresh() is False


def test_price_provider_fresh_cache():
    p = CoinbasePriceProvider()
    p._cache = {"BTC-USD": {"price": 1.0}}
    p._cache_ts = __import__("time").time()
    assert p._is_cache_fresh() is True


def test_price_provider_get_price_none():
    p = CoinbasePriceProvider()
    with patch("subprocess.run", side_effect=RuntimeError("down")):
        assert CoinbasePriceProvider().get_price("BTC-USD") == 0.0


def test_price_provider_market_data_parses_json():
    p = CoinbasePriceProvider()
    out = json.dumps({"ticker": {"price": "123.4", "price_percentage_change_24h": "2.5", "volume_24h": "1000"}})
    res = subprocess.CompletedProcess(("x",), 0, stdout=out)
    with patch("subprocess.run", return_value=res):
        md = p.get_market_data(["BTC-USD"])
    assert md["BTC-USD"]["price"] == 123.4
    assert md["BTC-USD"]["change_pct"] == 2.5
    # now cached
    assert p._is_cache_fresh() is True


def test_price_provider_market_data_cli_error():
    res = subprocess.CompletedProcess(("x",), 1, stderr="err")
    with patch("subprocess.run", return_value=res):
        md = CoinbasePriceProvider().get_market_data(["BTC-USD"])
    assert md["BTC-USD"]["price"] == 0.0


def test_price_provider_market_data_exception():
    with patch("subprocess.run", side_effect=RuntimeError("boom")):
        md = CoinbasePriceProvider().get_market_data(["BTC-USD"])
    assert md["BTC-USD"]["price"] == 0.0


# ── DaemonSnapshotAdapter ─────────────────────────────────────────

def _write_state(path: Path):
    state = {
        "marketIntelligence": {
            "prediction_markets": {
                "rankings": [
                    {"mid_price": 0.8, "probability_extremity": 0.3, "question": "Will bitcoin hit 100k?",
                     "category": "crypto", "platform": "kalshi", "heat_score": 50, "volume": 1000, "spread": 0.02},
                    {"mid_price": 0.2, "probability_extremity": 0.4, "question": "Ethereum ETF approval",
                     "category": "crypto", "platform": "polymarket", "heat_score": 10, "volume": 500, "spread": 0.03},
                    {"mid_price": 0.5, "probability_extremity": 0.3, "question": "neutral coin",
                     "category": "crypto", "platform": "kalshi", "heat_score": 1, "volume": 10, "spread": 0.01},
                    {"mid_price": 0.9, "probability_extremity": 0.05, "question": "low extremity",
                     "category": "crypto", "platform": "kalshi", "heat_score": 1, "volume": 10, "spread": 0.01},
                    {"mid_price": 0.0, "probability_extremity": 0.3, "question": "zero price skip",
                     "category": "crypto", "platform": "kalshi", "heat_score": 1, "volume": 10, "spread": 0.01},
                ]
            },
            "arbitrage": {
                "opportunities": [
                    {"edge_pct": 0.05, "confidence": 0.7, "platform_buy": "K", "platform_hedge": "P",
                     "total_cost": 200.0, "guaranteed_payout": 230.0, "event_key": "e1", "category": "gen"},
                    {"edge_pct": 0.0, "confidence": 0.5, "platform_buy": "K", "platform_hedge": "P",
                     "total_cost": 100.0, "guaranteed_payout": 100.0, "event_key": "e2", "category": "gen"},
                ]
            },
            "coinbase": {
                "last_updates": {
                    "BTC-USD": {"price": 70000.0, "timestamp": "t", "channel": "ticker"},
                    "ETH-USD": {"price": 0.0, "timestamp": "t", "channel": "ticker"},
                }
            },
        }
    }
    path.write_text(json.dumps(state))


def test_daemon_adapter_no_file():
    d = DaemonSnapshotAdapter(state_path="/nonexistent/path.json")
    assert d.get_signals() == []


def test_daemon_adapter_all_branches(tmp_path):
    f = tmp_path / "operator-state.json"
    _write_state(f)
    d = DaemonSnapshotAdapter(state_path=f)
    sigs = d.get_signals()
    actions = {s.action for s in sigs}
    # BUY from high YES, SELL from low YES, HOLD from live feed, BUY from arb
    assert "BUY" in actions and "SELL" in actions and "HOLD" in actions
    # arb signal present
    assert any("Arb" in s.strategy_name for s in sigs)


def test_daemon_adapter_map_question():
    d = DaemonSnapshotAdapter()
    assert d._map_question("Will bitcoin go up?", "crypto") == "BTC-USD"
    assert d._map_question("Ethereum news", "crypto") == "ETH-USD"
    assert d._map_question("a story about dogs", "sports") == "BTC-USD"  # category fallback
    assert d._map_question("random", "technology") == "NVDA"


# ── UnifiedSignalAccumulator (adapters mocked) ───────────────────

class _FakeStrat:
    def get_signals(self, symbol, price, history):
        return [_sig(symbol)]


class _FakeNews:
    def get_signals(self, price_map):
        return []


class _FakeMulti:
    def get_signals(self, symbol, price_data, history):
        return []


class _FakeDaemon:
    def get_signals(self, price_map=None):
        return []


def _build_acc(monkeypatch):
    mod = "graph_alpha_bot.app.strategies.unified_signal_accumulator"
    monkeypatch.setattr(f"{mod}.StrategySignalAdapter", _FakeStrat)
    monkeypatch.setattr(f"{mod}.NewsSentimentAdapter", _FakeNews)
    monkeypatch.setattr(f"{mod}.MultiStrategyAdapter", _FakeMulti)
    monkeypatch.setattr(f"{mod}.DaemonSnapshotAdapter", _FakeDaemon)
    acc = UnifiedSignalAccumulator(max_queue_size=50)
    acc.prediction_market_adapter = None
    # prevent network calls during accumulate
    acc.price_provider.get_market_data = lambda symbols: {s: {"price": 100.0, "change_pct": 1.0} for s in symbols}
    acc._ensure_history = lambda symbol: []
    return acc


def test_accumulator_apply_cross_consensus_boost():
    acc = UnifiedSignalAccumulator.__new__(UnifiedSignalAccumulator)
    sigs = [
        _sig("BTC-USD", action="BUY", conf=0.6, score=0.3, strat="A"),
        _sig("BTC-USD", action="BUY", conf=0.6, score=0.3, strat="B"),
        _sig("ETH-USD", action="SELL", conf=0.5, score=0.2, strat="C"),
    ]
    out = acc._apply_cross_consensus(sigs)
    btc = [s for s in out if s.symbol == "BTC-USD" and s.action == "BUY"]
    assert len(btc) == 2
    # boosted: 0.6 * (1 + (2-1)*0.15) = 0.69
    assert abs(btc[0].final_confidence - 0.69) < 1e-9
    eth = [s for s in out if s.symbol == "ETH-USD"][0]
    assert abs(eth.final_confidence - 0.5) < 1e-9  # no boost


def test_accumulator_fee_tier_boost_skipped(monkeypatch):
    # multi_strategy_paper_trading is not importable -> branch is skipped gracefully
    acc = UnifiedSignalAccumulator.__new__(UnifiedSignalAccumulator)
    sigs = [_sig("BTC-USD")]
    out = acc._apply_fee_tier_boost(sigs)
    assert out[0].final_confidence == 0.6


def test_accumulator_strategy_breakdown():
    acc = UnifiedSignalAccumulator.__new__(UnifiedSignalAccumulator)
    sigs = [_sig("BTC-USD", strat="A"), _sig("ETH-USD", strat="A"), _sig("SOL-USD", strat="B")]
    bd = acc._strategy_breakdown(sigs)
    assert bd == {"A": 2, "B": 1}


def test_accumulator_accumulate(monkeypatch):
    acc = _build_acc(monkeypatch)
    sigs = acc.accumulate()
    # one BUY signal per spot pair
    from graph_alpha_bot.app.strategies.coinbase_universe import COINBASE_SPOT_PAIRS
    assert len(sigs) == len(COINBASE_SPOT_PAIRS)
    assert all(s.action == "BUY" for s in sigs)
    # ranked descending by opportunity_score
    scores = [s.opportunity_score for s in sigs]
    assert scores == sorted(scores, reverse=True)


def test_accumulator_accumulate_and_report(monkeypatch):
    acc = _build_acc(monkeypatch)
    report = acc.accumulate_and_report()
    assert report["status"] == "ok"
    assert report["total_signals"] == len(acc.symbols)
    assert report["buy_signals"] == len(acc.symbols)
    assert report["sell_signals"] == 0
    assert report["top_signal"] is not None
    assert "Strat" in report["strategy_breakdown"]


def test_accumulator_accumulate_empty(monkeypatch):
    class _EmptyStrat:
        def get_signals(self, *a, **k):
            return []
    class _EmptyNews:
        def get_signals(self, *a, **k):
            return []
    class _EmptyMulti:
        def get_signals(self, *a, **k):
            return []
    class _EmptyDaemon:
        def get_signals(self, *a, **k):
            return []
    mod = "graph_alpha_bot.app.strategies.unified_signal_accumulator"
    monkeypatch.setattr(f"{mod}.StrategySignalAdapter", _EmptyStrat)
    monkeypatch.setattr(f"{mod}.NewsSentimentAdapter", _EmptyNews)
    monkeypatch.setattr(f"{mod}.MultiStrategyAdapter", _EmptyMulti)
    monkeypatch.setattr(f"{mod}.DaemonSnapshotAdapter", _EmptyDaemon)
    acc = UnifiedSignalAccumulator(max_queue_size=50)
    acc.prediction_market_adapter = None
    acc.price_provider.get_market_data = lambda symbols: {s: {"price": 100.0, "change_pct": 1.0} for s in symbols}
    acc._ensure_history = lambda symbol: []
    report = acc.accumulate_and_report()
    assert report["status"] == "no_signals"
    assert report["total_signals"] == 0
    assert report["top_signal"] is None


# ======================================================================
# Fake external modules for the adapter classes (mock all imports)
# ======================================================================

class _Raw:
    def __init__(self, direction, confidence=0.8, strategy_name="RawStrat",
                 signal_reason="reason", price_change_pct=1.0):
        self.direction = direction
        self.confidence = confidence
        self.strategy_name = strategy_name
        self.signal_reason = signal_reason
        self.price_change_pct = price_change_pct


def _make_usg_module(rich_data=None):
    mod = types.ModuleType("unified_signal_generator")

    class UnifiedSignalConfig:
        pass

    class UnifiedSignalGenerator:
        def __init__(self, *a, **k):
            pass

    class StrategySignalGenerator:
        def generate_strategy_signals(self, symbol, history):
            return [
                _Raw("LONG"), _Raw("SHORT"), _Raw("WEIRD"),  # -> BUY/SELL/CLOSE
            ]

    class NewsSentimentAnalyzer:
        def __init__(self, symbols):
            self.symbols = symbols

        def analyze_full(self):
            return rich_data or {}

    mod.UnifiedSignalConfig = UnifiedSignalConfig
    mod.UnifiedSignalGenerator = UnifiedSignalGenerator
    mod.StrategySignalGenerator = StrategySignalGenerator
    mod.NewsSentimentAnalyzer = NewsSentimentAnalyzer
    return mod


def _make_multi_module(sig=None, raise_on=None, with_fees=True):
    mod = types.ModuleType("multi_strategy_paper_trading")

    class _MultiSig:
        def __init__(self, action="BUY", strength=0.7, reason="r"):
            self.action = action
            self.strength = strength
            self.reason = reason

    def _make_strategy(name):
        class _Strat:
            def __init__(self, *a, **k):
                self.name = name

            def generate_signal(self, symbol, price_data, history):
                if raise_on == name:
                    raise RuntimeError("boom")
                return sig if sig is not None else _MultiSig()

        return _Strat

    mod.MomentumStrategy = _make_strategy("Momentum")
    mod.MeanReversionStrategy = _make_strategy("MeanReversion")
    mod.RSIStrategy = _make_strategy("RSI")
    mod.BreakoutStrategy = _make_strategy("Breakout")
    mod.VolatilityStrategy = _make_strategy("Volatility")
    mod.ScalpingStrategy = _make_strategy("Scalping")

    if with_fees:
        class FeeTierManager:
            def volume_to_next_tier(self):
                return 5000.0

        class VolumeOptimizer:
            def __init__(self, ftm):
                self.ftm = ftm

            def volume_boost(self, vol):
                return 1.2

        mod.FeeTierManager = FeeTierManager
        mod.VolumeOptimizer = VolumeOptimizer
    return mod


# ── StrategySignalAdapter ─────────────────────────────────────────

def test_strategy_signal_adapter(monkeypatch):
    monkeypatch.setitem(sys.modules, "unified_signal_generator", _make_usg_module())
    adapter = StrategySignalAdapter()
    sigs = adapter.get_signals("BTC-USD", 100.0, [{"close": 100.0}])
    actions = [s.action for s in sigs]
    assert actions == ["BUY", "SELL", "CLOSE"]
    assert all(s.symbol == "BTC-USD" for s in sigs)


# ── NewsSentimentAdapter ──────────────────────────────────────────

def _rich():
    return {
        "BTC": {"avg_sentiment": 0.85, "count": 3, "breaking_ratio": 0.6,
                "hack_count": 0, "regulation_count": 0, "adoption_count": 3,
                "technology_count": 3, "topics": ["adoption"]},
        "ETH": {"avg_sentiment": -0.8, "count": 3, "breaking_ratio": 0.0,
                "hack_count": 2, "regulation_count": 2, "adoption_count": 0,
                "technology_count": 0, "topics": ["hacks_security"]},
        "SOL": {"avg_sentiment": 0.1, "count": 1, "breaking_ratio": 0.0,
                "hack_count": 0, "regulation_count": 0, "adoption_count": 0,
                "technology_count": 0, "topics": []},
    }


def test_news_sentiment_adapter(monkeypatch):
    monkeypatch.setitem(sys.modules, "unified_signal_generator", _make_usg_module(_rich()))
    adapter = NewsSentimentAdapter()
    sigs = adapter.get_signals({"BTC-USD": 100.0, "ETH-USD": 50.0})
    # dedup: at most one signal per (symbol, action)
    keys = [(s.symbol, s.action) for s in sigs]
    assert len(keys) == len(set(keys))
    # BTC -> BUY (breaking boost applied), ETH -> SELL
    btc = [s for s in sigs if s.symbol == "BTC-USD"]
    assert btc and btc[0].action == "BUY"
    assert "[BREAKING]" in btc[0].signal_reason
    eth = [s for s in sigs if s.symbol == "ETH-USD"]
    assert eth and eth[0].action == "SELL"


# ── MultiStrategyAdapter ──────────────────────────────────────────

def test_multi_strategy_adapter(monkeypatch):
    monkeypatch.setitem(sys.modules, "multi_strategy_paper_trading", _make_multi_module())
    adapter = MultiStrategyAdapter()
    sigs = adapter.get_signals("BTC-USD", {"price": 100.0}, [{"close": 100.0}])
    assert len(sigs) == 6
    assert all(s.strategy_name.startswith("Multi:") for s in sigs)


def test_multi_strategy_adapter_none_and_exception(monkeypatch):
    # one strategy raises, all others return None -> no signals
    mod = _make_multi_module(sig=False, raise_on="Momentum")

    # override generate_signal to return None for non-raising ones
    class _NoneSig:
        pass

    for cls_name in ("MeanReversionStrategy", "RSIStrategy", "BreakoutStrategy",
                     "VolatilityStrategy", "ScalpingStrategy"):
        cls = getattr(mod, cls_name)
        cls.generate_signal = lambda self, symbol, pd, hist: None
    monkeypatch.setitem(sys.modules, "multi_strategy_paper_trading", mod)
    adapter = MultiStrategyAdapter()
    sigs = adapter.get_signals("BTC-USD", {"price": 100.0}, [])
    assert sigs == []


# ── _init_pm_adapter ──────────────────────────────────────────────

def test_init_pm_adapter_success(monkeypatch):
    fake = types.ModuleType("event_markets.signal_adapter")

    class PredictionMarketAdapter:
        def __init__(self, **k):
            self.k = k

    fake.PredictionMarketAdapter = PredictionMarketAdapter
    monkeypatch.setitem(sys.modules, "event_markets.signal_adapter", fake)
    acc = UnifiedSignalAccumulator.__new__(UnifiedSignalAccumulator)
    result = acc._init_pm_adapter()
    assert isinstance(result, PredictionMarketAdapter)


def test_init_pm_adapter_failure(monkeypatch):
    def _boom(name, *a, **k):
        if name == "event_markets.signal_adapter":
            raise ImportError("nope")
        return _orig(name, *a, **k)

    import builtins
    _orig = builtins.__import__
    monkeypatch.setattr(builtins, "__import__", _boom)
    acc = UnifiedSignalAccumulator.__new__(UnifiedSignalAccumulator)
    assert acc._init_pm_adapter() is None


# ── _ensure_history ───────────────────────────────────────────────

def _bare_acc():
    acc = UnifiedSignalAccumulator.__new__(UnifiedSignalAccumulator)
    acc._history_cache = {}
    acc._last_fetch = {}
    return acc


def test_ensure_history_cache_hit():
    acc = _bare_acc()
    acc._history_cache["BTC-USD"] = [{"close": 1.0}]
    acc._last_fetch["BTC-USD"] = time.time()
    assert acc._ensure_history("BTC-USD") == [{"close": 1.0}]


def test_ensure_history_json_dict_candles():
    acc = _bare_acc()
    payload = {"candles": [
        {"start": "1", "open": "1", "high": "2", "low": "0.5", "close": "1.5", "volume": "10"},
    ]}
    res = subprocess.CompletedProcess(("x",), 0, stdout=json.dumps(payload))
    with patch("subprocess.run", return_value=res):
        candles = acc._ensure_history("BTC-USD")
    assert candles and candles[0]["close"] == 1.5
    # cached now
    assert acc._history_cache["BTC-USD"] == candles


def test_ensure_history_json_list():
    acc = _bare_acc()
    payload = [{"open": "1", "high": "2", "low": "0.5", "close": "1.5", "volume": "10", "time": "t"}]
    res = subprocess.CompletedProcess(("x",), 0, stdout=json.dumps(payload))
    with patch("subprocess.run", return_value=res):
        candles = acc._ensure_history("ETH-USD")
    assert candles and candles[0]["close"] == 1.5


def test_ensure_history_line_fallback():
    acc = _bare_acc()
    out = "header\n1 1.0 2.0 0.5 1.5 10\n2 1.5 2.5 1.0 2.0 20\n"
    res = subprocess.CompletedProcess(("x",), 0, stdout=out)
    with patch("subprocess.run", return_value=res):
        candles = acc._ensure_history("SOL-USD")
    assert len(candles) == 2
    assert candles[0]["open"] == 1.0


def test_ensure_history_bad_return_code():
    acc = _bare_acc()
    res = subprocess.CompletedProcess(("x",), 1, stdout="", stderr="err")
    with patch("subprocess.run", return_value=res):
        assert acc._ensure_history("XRP-USD") == []


def test_ensure_history_exception():
    acc = _bare_acc()
    with patch("subprocess.run", side_effect=RuntimeError("boom")):
        assert acc._ensure_history("ADA-USD") == []


# ── _apply_fee_tier_boost success path ────────────────────────────

def test_apply_fee_tier_boost_success(monkeypatch):
    monkeypatch.setitem(sys.modules, "multi_strategy_paper_trading", _make_multi_module())
    acc = UnifiedSignalAccumulator.__new__(UnifiedSignalAccumulator)
    sigs = [_sig("BTC-USD", conf=0.5, score=0.2)]
    out = acc._apply_fee_tier_boost(sigs)
    # boost 1.2 -> final_confidence = 0.5 * 1.2 = 0.6
    assert abs(out[0].final_confidence - 0.6) < 1e-9
    assert abs(out[0].opportunity_score - 0.24) < 1e-9


# ── accumulate: prediction-market dict results + exception path ────

def test_accumulate_dict_results_and_exception(monkeypatch):
    class _RaisingStrat:
        def get_signals(self, symbol, price, history):
            raise RuntimeError("strategy down")

    class _EmptyNews:
        def get_signals(self, price_map):
            return []

    class _EmptyMulti:
        def get_signals(self, symbol, price_data, history):
            return []

    class _EmptyDaemon:
        def get_signals(self, price_map=None):
            return []

    monkeypatch.setattr(f"{MOD}.StrategySignalAdapter", _RaisingStrat)
    monkeypatch.setattr(f"{MOD}.NewsSentimentAdapter", _EmptyNews)
    monkeypatch.setattr(f"{MOD}.MultiStrategyAdapter", _EmptyMulti)
    monkeypatch.setattr(f"{MOD}.DaemonSnapshotAdapter", _EmptyDaemon)

    acc = UnifiedSignalAccumulator(max_queue_size=5)
    acc.symbols = ["BTC-USD"]
    acc.price_provider.get_market_data = lambda symbols: {s: {"price": 100.0, "change_pct": 1.0} for s in symbols}
    acc._ensure_history = lambda symbol: []

    # prediction market adapter returns a list of dicts -> AccumulatedSignal(**r)
    class _PM:
        def get_signals(self, price_map):
            return [{
                "symbol": "BTC-USD", "action": "BUY", "base_confidence": 0.9,
                "final_confidence": 0.9, "opportunity_score": 0.8,
                "strategy_name": "PM", "signal_reason": "r",
                "estimated_volume_usd": 100.0,
            }]

    acc.prediction_market_adapter = _PM()
    sigs = acc.accumulate()
    # strategy raised (caught), PM dict signal present
    assert any(s.strategy_name == "PM" for s in sigs)


# ── CoinbasePriceProvider extra branches ──────────────────────────

def test_price_provider_line_parse_fallback():
    p = CoinbasePriceProvider()
    out = "some header\nprice: 123.45\nchange stuff\n"
    res = subprocess.CompletedProcess(("x",), 0, stdout=out)
    with patch("subprocess.run", return_value=res):
        md = p.get_market_data(["BTC-USD"])
    assert md["BTC-USD"]["price"] == 123.45


def test_price_provider_cache_fresh_subset():
    p = CoinbasePriceProvider()
    out = json.dumps({"price": "100.0", "price_percentage_change_24h": "1.0", "volume_24h": "5"})
    res = subprocess.CompletedProcess(("x",), 0, stdout=out)
    with patch("subprocess.run", return_value=res):
        p.get_market_data(["BTC-USD"])
        # second call: cache fresh + all symbols present -> returns cached without subprocess
        md = p.get_market_data(["BTC-USD"])
    assert md["BTC-USD"]["price"] == 100.0


def test_get_price_returns_value():
    p = CoinbasePriceProvider()
    out = json.dumps({"price": "250.0", "price_percentage_change_24h": "1.0", "volume_24h": "5"})
    res = subprocess.CompletedProcess(("x",), 0, stdout=out)
    with patch("subprocess.run", return_value=res):
        assert p.get_price("BTC-USD") == 250.0


# ── DaemonSnapshotAdapter read error ──────────────────────────────

def test_daemon_read_snapshot_invalid_json(tmp_path):
    f = tmp_path / "operator-state.json"
    f.write_text("{not valid json")
    d = DaemonSnapshotAdapter(state_path=f)
    assert d.get_signals() == []


def test_daemon_snapshot_no_market_intelligence(tmp_path):
    f = tmp_path / "operator-state.json"
    f.write_text(json.dumps({"other": {}}))
    d = DaemonSnapshotAdapter(state_path=f)
    assert d.get_signals() == []


# ── main() CLI ────────────────────────────────────────────────────

def _report_with_queue(n):
    queue = [
        {"action": "BUY", "symbol": "BTC-USD", "opportunity_score": 0.9,
         "final_confidence": 0.8, "strategy_name": "S", "signal_reason": "reason text"}
        for _ in range(n)
    ]
    return {
        "status": "ok", "timestamp": "t", "total_signals": n,
        "buy_signals": n, "sell_signals": 0,
        "top_signal": queue[0], "queue": queue,
        "strategy_breakdown": {"S": n},
    }


def test_main_text_mode(monkeypatch):
    monkeypatch.setattr(sys, "argv", ["prog", "--max-signals", "5"])
    monkeypatch.setattr(UnifiedSignalAccumulator, "accumulate_and_report",
                        lambda self: _report_with_queue(12))
    monkeypatch.setattr(UnifiedSignalAccumulator, "__init__", lambda self, max_queue_size=50: None)
    report = main()
    assert report["total_signals"] == 12


def test_main_json_mode(monkeypatch):
    monkeypatch.setattr(sys, "argv", ["prog", "--json"])
    monkeypatch.setattr(UnifiedSignalAccumulator, "accumulate_and_report",
                        lambda self: _report_with_queue(2))
    monkeypatch.setattr(UnifiedSignalAccumulator, "__init__", lambda self, max_queue_size=50: None)
    report = main()
    assert report["status"] == "ok"


# ── extra branch coverage ─────────────────────────────────────────

def test_price_provider_line_parse_unparseable_then_good():
    p = CoinbasePriceProvider()
    out = "header\nprice: notanumber\nprice: 55.5\n"
    res = subprocess.CompletedProcess(("x",), 0, stdout=out)
    with patch("subprocess.run", return_value=res):
        md = p.get_market_data(["BTC-USD"])
    assert md["BTC-USD"]["price"] == 55.5


def test_ensure_history_scalar_json():
    acc = _bare_acc()
    res = subprocess.CompletedProcess(("x",), 0, stdout="123")
    with patch("subprocess.run", return_value=res):
        assert acc._ensure_history("BTC-USD") == []


def test_ensure_history_dict_entry_missing_keys():
    acc = _bare_acc()
    payload = {"candles": [
        {"start": "1", "open": "1", "high": "2"},  # missing low/close -> skipped
        {"start": "2", "open": "1", "high": "2", "low": "0.5", "close": "1.5"},
    ]}
    res = subprocess.CompletedProcess(("x",), 0, stdout=json.dumps(payload))
    with patch("subprocess.run", return_value=res):
        candles = acc._ensure_history("ETH-USD")
    assert len(candles) == 1


def test_ensure_history_dict_empty_then_line_fallback():
    acc = _bare_acc()
    # dict with candles that are all invalid -> falls to line-by-line parse
    payload = {"candles": [{"foo": "bar"}]}
    out = json.dumps(payload)
    res = subprocess.CompletedProcess(("x",), 0, stdout=out)
    with patch("subprocess.run", return_value=res):
        # JSON parses but yields no candles; line fallback also yields none
        assert acc._ensure_history("SOL-USD") == []


def test_ensure_history_line_short_and_bad():
    acc = _bare_acc()
    out = "header\nx y\n1 bad hi lo cl vol\n2 1.0 2.0 0.5 1.5 10\n"
    res = subprocess.CompletedProcess(("x",), 0, stdout=out)
    with patch("subprocess.run", return_value=res):
        candles = acc._ensure_history("XRP-USD")
    # only the last well-formed numeric line is parsed
    assert len(candles) == 1
    assert candles[0]["close"] == 1.5


def test_main_no_top_signal(monkeypatch):
    report = {
        "status": "ok", "timestamp": "t", "total_signals": 3,
        "buy_signals": 3, "sell_signals": 0, "top_signal": None,
        "queue": [
            {"action": "BUY", "symbol": "BTC-USD", "opportunity_score": 0.9,
             "final_confidence": 0.8, "strategy_name": "S", "signal_reason": "r"}
            for _ in range(3)
        ],
        "strategy_breakdown": {"S": 3},
    }
    monkeypatch.setattr(sys, "argv", ["prog"])
    monkeypatch.setattr(UnifiedSignalAccumulator, "accumulate_and_report", lambda self: report)
    monkeypatch.setattr(UnifiedSignalAccumulator, "__init__", lambda self, max_queue_size=50: None)
    out = main()
    assert out["top_signal"] is None
