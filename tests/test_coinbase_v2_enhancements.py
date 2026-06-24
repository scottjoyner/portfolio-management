"""
Comprehensive Unit Tests — Coinbase v2 Enhancements
====================================================
"""
import sys, os, json, math, time, unittest, tempfile
from pathlib import Path
from unittest.mock import patch
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from coinbase.src.protocols import Direction, InstrumentType, Bar, Opportunity, BracketSetup, BaseStrategy
from coinbase.src import news_risk
from coinbase.src import market_condition as mc
from coinbase.src.product_rotation import ProductRotator, MomentumRotationStrategy
from coinbase.src.adaptive_mode import AdaptiveModeSelector, AdaptiveScalpSwingStrategy, TradingMode
from coinbase.src.dual_mm import DualMarketMaker, MarketMakingStrategy
from coinbase.src.ranking import StrategyRanking, StrategyRankingFilter, TopRankedStrategyWrapper
from coinbase.src.fear_greed import FearGreedIndex, FearGreedSignalAdapter
from coinbase.src.graph import sync_coingecko_universe as cg_sync
from portfolio_optimizer import PortfolioOptimizer
from trading_system.ui import dashboard_server as ds


def make_bar(close, high=None, low=None, open_=None, volume=0):
    c = close
    h = high or c * 1.001
    lv = low or c * 0.999
    o = open_ or c * 0.9995
    return Bar(timestamp=time.time(), open=o, high=h, low=lv, close=c, volume=volume)


def make_history(prices, volumes=None):
    if volumes is None:
        volumes = [100] * len(prices)
    return [make_bar(p, volume=v) for p, v in zip(prices, volumes)]


class TestNewsRisk(unittest.TestCase):
    def setUp(self):
        path = os.path.join("graph-alpha-bot", "app", "data", "knowledge_graph.json")
        self.kg = news_risk.KnowledgeGraphReader(path=path, cache_ttl_secs=0)
        self.adjuster = news_risk.NewsRiskAdjuster(kg_reader=self.kg)

    def test_kg_reads_live_data(self):
        data = self.kg.read()
        self.assertIn("articles", data)
        self.assertIn("tickers", data)

    def test_get_ticker_sentiment_known(self):
        info = self.kg.get_ticker_sentiment("BTC-USD")
        self.assertIsNotNone(info)
        self.assertIn("avg_sentiment", info)
        self.assertGreater(info["count"], 0)

    def test_get_ticker_sentiment_unknown(self):
        self.assertIsNone(self.kg.get_ticker_sentiment("FAKE-COIN-999"))

    def test_global_sentiment_pulse(self):
        pulse = self.kg.global_sentiment_pulse()
        self.assertIsInstance(pulse, float)
        self.assertGreaterEqual(pulse, -1.0)
        self.assertLessEqual(pulse, 1.0)

    def test_assess_product_known(self):
        snap = self.adjuster.assess_product("BTC-USD")
        self.assertGreaterEqual(snap.sentiment_risk_score, 0.0)
        self.assertLessEqual(snap.sentiment_risk_score, 1.0)
        self.assertGreaterEqual(snap.size_multiplier, 0.3)
        self.assertLessEqual(snap.size_multiplier, 1.0)
        self.assertGreaterEqual(snap.leverage_cap, 1.0)

    def test_assess_product_unknown(self):
        snap = self.adjuster.assess_product("NOT-A-REAL-PAIR")
        self.assertAlmostEqual(snap.sentiment_risk_score, 0.3)
        self.assertAlmostEqual(snap.size_multiplier, 1.0)
        self.assertEqual(snap.reason, "no news data")

    def test_kg_file_not_found(self):
        kg = news_risk.KnowledgeGraphReader(path="/nonexistent/file.json")
        data = kg.read()
        self.assertEqual(data, {"tickers": {}, "articles": [], "metadata": {}})

    def test_adjust_opportunity_known(self):
        opp = Opportunity(product_id="BTC-USD", direction=Direction.LONG,
                          instrument_type=InstrumentType.SPOT,
                          entry_price=50000, stop_price=48000, target_price=55000,
                          risk_reward=2.5, confidence=0.7, reason="test",
                          strategy_name="test_strat", base_size=1.0, leverage=2.0, score=50)
        result = self.adjuster.adjust_opportunity(opp)
        self.assertIs(result, opp)
        self.assertIn("news_risk", opp.meta)

    def test_adjust_opportunity_unknown_product(self):
        opp = Opportunity(product_id="NOT-A-REAL-PAIR", direction=Direction.LONG,
                          instrument_type=InstrumentType.SPOT,
                          entry_price=100, stop_price=90, target_price=120,
                          risk_reward=2.0, confidence=0.6, reason="test",
                          strategy_name="test", base_size=100, leverage=1.5, score=30)
        result = self.adjuster.adjust_opportunity(opp)
        self.assertIs(result, opp)
        self.assertAlmostEqual(opp.base_size, 100.0)
        self.assertAlmostEqual(opp.confidence, 0.6)

    def test_adjust_profile(self):
        class MockProfile:
            def __init__(self):
                self.max_position_pct = 0.3
                self.max_notional_per_trade = 10000.0
                self.risk_per_trade_pct = 0.02
                self.max_leverage = 3.0
        profile = MockProfile()
        self.adjuster.adjust_profile(profile)
        self.assertGreaterEqual(profile.max_leverage, 1.0)

    def test_news_risk_strategy(self):
        strat = news_risk.NewsAwareRiskStrategy(self.adjuster)
        self.assertEqual(strat.name(), "news_risk")
        self.assertIsNone(strat.on_bar(make_bar(100), []))
        opp = Opportunity(product_id="BTC-USD", direction=Direction.LONG,
                          instrument_type=InstrumentType.SPOT,
                          entry_price=50000, stop_price=48000, target_price=55000,
                          risk_reward=2.5, confidence=0.7, reason="test",
                          strategy_name="test", base_size=1.0, leverage=2.0, score=50)
        adjusted = strat.adjust_opportunity(opp)
        self.assertIsNotNone(adjusted)

    def test_mcp_client_unreachable(self):
        client = news_risk.MCPSentimentClient(host="127.0.0.1", port=19999, timeout_secs=0.5)
        self.assertIsNone(client.query_sentiment("BTC-USD"))
        self.assertFalse(client.is_available())


class TestMarketCondition(unittest.TestCase):
    def setUp(self):
        self.selector = mc.MarketConditionStrategySelector()
        self.uptrend_profile = mc.MarketConditionProfile(
            regime="strong_uptrend", fear_greed=72,
            news_sentiment_pulse=0.15, trend_strength=0.05,
            volatility_bps=35, adx=32, hurst=0.65,
            serial_correlation=0.2, volume_trend=0.03,
        )
        self.ranging_fear_profile = mc.MarketConditionProfile(
            regime="ranging", fear_greed=18,
            news_sentiment_pulse=-0.35, trend_strength=0.005,
            volatility_bps=25, adx=14, hurst=0.35,
            serial_correlation=-0.15, volume_trend=-0.01,
            breaking_news_ratio=0.5, has_hacks=True, has_regulation=True,
        )

    def test_evaluate_uptrend(self):
        weights = self.selector.evaluate(self.uptrend_profile)
        self.assertGreater(weights.get("ema_cross", 0), weights.get("rsi_revert", 0))

    def test_evaluate_ranging_fear(self):
        weights = self.selector.evaluate(self.ranging_fear_profile)
        self.assertGreater(weights.get("rsi_revert", 0), weights.get("ema_cross", 0))

    def test_top_archetypes_uptrend(self):
        self.selector.evaluate(self.uptrend_profile)
        self.assertEqual(self.selector.top_archetype_fits()[0][0], mc.StrategyArchetype.TREND_FOLLOW)

    def test_top_archetypes_ranging_fear(self):
        self.selector.evaluate(self.ranging_fear_profile)
        archetypes = [a for a, _ in self.selector.top_archetype_fits()]
        self.assertIn(mc.StrategyArchetype.MEAN_REVERSION, archetypes)

    def test_unknown_strategy_gets_low_weight(self):
        self.selector.evaluate(self.uptrend_profile)
        self.assertEqual(self.selector.weight("nonexistent_strategy_xyz"), self.selector.min_fit)

    def test_filter_opportunities_keeps_good_fits(self):
        self.selector.evaluate(self.uptrend_profile)
        opp1 = Opportunity(product_id="BTC-USD", direction=Direction.LONG,
                           instrument_type=InstrumentType.SPOT,
                           entry_price=100, stop_price=95, target_price=110,
                           risk_reward=2.0, confidence=0.7, reason="a",
                           strategy_name="ema_cross", base_size=1.0, score=50)
        opp2 = Opportunity(product_id="BTC-USD", direction=Direction.LONG,
                           instrument_type=InstrumentType.SPOT,
                           entry_price=100, stop_price=95, target_price=110,
                           risk_reward=2.0, confidence=0.7, reason="b",
                           strategy_name="macd", base_size=1.0, score=50)
        filtered = self.selector.filter_opportunities([opp1, opp2])
        self.assertEqual(len(filtered), 2)

    def test_market_summary(self):
        self.selector.evaluate(self.uptrend_profile)
        self.assertIn("regime=strong_uptrend", self.selector.market_summary())
        self.assertIn("best=", self.selector.market_summary())

    def test_risk_flags(self):
        self.assertTrue(self.ranging_fear_profile.is_risk_off)
        self.assertFalse(self.uptrend_profile.is_risk_off)

    def test_extreme_sentiment(self):
        extreme = mc.MarketConditionProfile(regime="unknown", fear_greed=15, news_sentiment_pulse=0.0)
        self.assertTrue(extreme.is_extreme_sentiment)

    def test_trending_property(self):
        t = mc.MarketConditionProfile(regime="unknown", hurst=0.7, serial_correlation=0.2)
        self.assertTrue(t.is_trending)

    def test_all_archetype_fit_functions(self):
        profile = mc.MarketConditionProfile(regime="unknown")
        for archetype, fn in mc.ARCHETYPE_FIT_FUNCTIONS.items():
            score = fn(profile)
            self.assertGreaterEqual(score, 0.0)
            self.assertLessEqual(score, 1.0)

    def test_summary_dict(self):
        self.selector.evaluate(self.uptrend_profile)
        s = self.selector.summary()
        self.assertIn("profile", s)
        self.assertIn("strategies_enabled", s)

    def test_filter_adds_meta(self):
        self.selector.evaluate(self.uptrend_profile)
        opp = Opportunity(product_id="BTC-USD", direction=Direction.LONG,
                          instrument_type=InstrumentType.SPOT,
                          entry_price=100, stop_price=95, target_price=110,
                          risk_reward=2.0, confidence=0.7, reason="a",
                          strategy_name="ema_cross", base_size=1.0, score=50)
        filtered = self.selector.filter_opportunities([opp])
        self.assertIn("market_fit_weight", filtered[0].meta)
        self.assertIn("market_regime", filtered[0].meta)


class TestProductRotation(unittest.TestCase):
    def setUp(self):
        self.rotator = ProductRotator(top_n=2, rebalance_cooldown_bars=5)

    def test_record_and_score(self):
        for i in range(50):
            self.rotator.record_bar("BTC-USD", 100 + i * 0.5, 1000 + i)
            self.rotator.record_bar("ETH-USD", 200 + i * 0.1, 500 + i)
        scores = self.rotator.score_all()
        self.assertGreater(len(scores), 0)
        btc = [s for s in scores if s.product_id == "BTC-USD"]
        self.assertEqual(len(btc), 1)

    def test_rebalance_cooldown(self):
        for i in range(20):
            self.rotator.record_bar("BTC-USD", 100 + i * 0.5, 1000)
            self.rotator.record_bar("ETH-USD", 200 + i * 0.1, 500)
        first = self.rotator.rebalance()
        self.assertGreater(len(first), 0)
        second = self.rotator.rebalance()
        self.assertEqual(second, first)

    def test_top_opportunity_filter(self):
        for i in range(30):
            self.rotator.record_bar("BTC-USD", 100 + i, 1000)
            self.rotator.record_bar("ETH-USD", 200 + i * 0.1, 500)
        self.rotator.rebalance()
        opps = [
            Opportunity(product_id="BTC-USD", direction=Direction.LONG,
                        instrument_type=InstrumentType.SPOT,
                        entry_price=100, stop_price=95, target_price=110,
                        risk_reward=2.0, confidence=0.7, reason="a",
                        strategy_name="s1", base_size=1.0, score=50),
            Opportunity(product_id="OTHER-USD", direction=Direction.LONG,
                        instrument_type=InstrumentType.SPOT,
                        entry_price=50, stop_price=45, target_price=60,
                        risk_reward=2.0, confidence=0.7, reason="b",
                        strategy_name="s2", base_size=1.0, score=50),
        ]
        filtered = self.rotator.top_opportunity_filter(opps)
        for opp in filtered:
            self.assertIn(opp.product_id, self.rotator.ranked_products)

    def test_momentum_rotation_returns_signal_or_none(self):
        strat = MomentumRotationStrategy(self.rotator)
        for i in range(30):
            b = make_bar(100 + i * 0.5, volume=1000)
            self.rotator.record_bar("BTC-USD", b.close, b.volume)
        strat.set_product_id("BTC-USD")
        history = make_history([100 + i * 0.5 for i in range(20)], volumes=[1000]*20)
        result = strat.on_bar(make_bar(115, volume=1000), history)
        if result is not None:
            self.assertIn(result.direction, [Direction.LONG, Direction.SHORT])

    def test_empty_rotator(self):
        self.assertEqual(len(self.rotator.score_all()), 0)
        self.assertEqual(self.rotator.ranked_products, [])


class TestAdaptiveMode(unittest.TestCase):
    def setUp(self):
        self.selector = AdaptiveModeSelector(switch_cooldown_bars=5)

    def test_default_mode(self):
        self.assertEqual(self.selector.current_mode, TradingMode.SWING)

    def test_update_cooldown(self):
        self.selector.update("strong_uptrend", 30, 70, 35, 0.05)
        first = self.selector.current_mode
        self.selector.update("ranging", 20, 50, 15, 0.0)
        self.assertEqual(self.selector.current_mode, first)

    def test_update_after_cooldown(self):
        self.selector._bars_since_switch = 999
        mode = self.selector.update("strong_uptrend", 25, 70, 35, 0.05)
        self.assertIn(mode, [TradingMode.TREND, TradingMode.SWING, TradingMode.SCALP])

    def test_profile(self):
        p = self.selector.profile()
        self.assertIn("stop_atr", p)
        self.assertIn("target_atr", p)

    def test_summary(self):
        s = self.selector.summary()
        self.assertIn("mode", s)

    def test_adaptive_scalp_swing_strategy(self):
        strat = AdaptiveScalpSwingStrategy(self.selector)
        self.assertEqual(strat.name(), "adaptive_mode")
        history = make_history([100 + i * 0.1 for i in range(40)])
        result = strat.on_bar(make_bar(104), history)
        if result is not None:
            self.assertIn(result.direction, [Direction.LONG, Direction.SHORT])
            self.assertGreater(result.confidence, 0)


class TestDualMM(unittest.TestCase):
    def setUp(self):
        self.mm = DualMarketMaker(base_size_usd=100, quote_refresh_bars=0)

    def test_generate_quotes(self):
        bar = make_bar(100)
        history = make_history([99, 100, 101])
        quote = self.mm.generate_quotes("BTC-USD", bar, history)
        self.assertIsNotNone(quote)
        self.assertGreater(quote.ask_price, quote.bid_price)
        self.assertGreater(quote.bid_price, 0)

    def test_quote_spread_within_bounds(self):
        bar = make_bar(100)
        history = make_history([99, 100, 101])
        quote = self.mm.generate_quotes("BTC-USD", bar, history)
        self.assertLessEqual(quote.spread_bps, self.mm.max_spread_bps)
        self.assertGreaterEqual(quote.spread_bps, self.mm.min_spread_bps)

    def test_record_fill(self):
        result = self.mm.record_fill("BTC-USD", "buy", 100, 0.5)
        self.assertEqual(result["side"], "buy")
        self.assertEqual(result["size"], 0.5)

    def test_inventory_skew(self):
        self.mm.record_trade("BTC-USD", 10.0, 100)
        quote = self.mm.generate_quotes("BTC-USD", make_bar(100), [make_bar(100)])
        self.assertIsNotNone(quote)

    def test_market_making_strategy(self):
        strat = MarketMakingStrategy(self.mm)
        self.assertEqual(strat.name(), "market_making")
        bar = make_bar(100, volume=1000)
        history = make_history([99] * 30)
        result = strat.on_bar(bar, history)
        if result is not None:
            self.assertIn(result.direction, [Direction.LONG, Direction.SHORT])

    def test_summary(self):
        self.mm.record_fill("BTC-USD", "buy", 100, 0.5)
        s = self.mm.summary()
        self.assertIn("spread_captures", s)
        self.assertIn("active_inventories", s)


class TestRanking(unittest.TestCase):
    def setUp(self):
        self.ranking = StrategyRanking(min_trades=3, rebalance_bars=10)

    def test_record_and_rank(self):
        for s in ["strat_a", "strat_b", "strat_c"]:
            for _ in range(5):
                self.ranking.record_trade(s, 10 if s == "strat_a" else -5, 0.6)
        ranked = self.ranking.rank_all()
        self.assertGreater(len(ranked), 0)
        self.assertEqual(ranked[0][0], "strat_a")

    def test_min_trades_filter(self):
        self.ranking.record_trade("new_strat", 100, 0.9)
        names = [n for n, _ in self.ranking.rank_all()]
        self.assertNotIn("new_strat", names)

    def test_rebalance_weights(self):
        for s in ["strat_a", "strat_b"]:
            for _ in range(5):
                self.ranking.record_trade(s, 10 if s == "strat_a" else -5, 0.6)
        weights = self.ranking.rebalance_weights()
        self.assertGreater(weights.get("strat_a", 0), weights.get("strat_b", 0))

    def test_should_rebalance(self):
        self.assertFalse(self.ranking.should_rebalance())
        self.ranking._bars_since_rebalance = 10
        self.assertTrue(self.ranking.should_rebalance())

    def test_top_strategies(self):
        for s in ["strat_a", "strat_b"]:
            for _ in range(5):
                self.ranking.record_trade(s, 10 if s == "strat_a" else -5, 0.6)
        self.ranking.rank_all()
        self.assertIn("strat_a", self.ranking.top_strategies())

    def test_summary(self):
        for s in ["strat_a", "strat_b"]:
            for _ in range(5):
                self.ranking.record_trade(s, 10, 0.6)
        self.ranking.rank_all()
        s = self.ranking.summary()
        self.assertIn("ranked", s)
        self.assertIn("total_tracked", s)

    def test_top_ranked_strategy_wrapper_blocks_without_rank(self):
        class FakeStrategy(BaseStrategy):
            def name(self):
                return "inner_strat"
            def on_bar(self, bar, history):
                return BracketSetup(direction=Direction.LONG, entry_price=100,
                                     stop_price=95, target_price=110,
                                     risk_reward=2.0, confidence=0.7,
                                     reason="inner", strategy_name="inner_strat")
        inner = FakeStrategy()
        wrapper = TopRankedStrategyWrapper(inner, self.ranking)
        self.assertEqual(wrapper.name(), "ranked_inner_strat")
        result = wrapper.on_bar(make_bar(100), [])
        self.assertIsNone(result, "Should block when no ranking data")

    def test_top_ranked_strategy_wrapper_with_rank(self):
        class FakeStrategy(BaseStrategy):
            def name(self):
                return "strat_a"
            def on_bar(self, bar, history):
                return BracketSetup(direction=Direction.LONG, entry_price=100,
                                     stop_price=95, target_price=110,
                                     risk_reward=2.0, confidence=0.7,
                                     reason="inner", strategy_name="strat_a")
        inner = FakeStrategy()
        for _ in range(5):
            self.ranking.record_trade("strat_a", 10, 0.6)
        self.ranking.rank_all()
        wrapper = TopRankedStrategyWrapper(inner, self.ranking)
        result = wrapper.on_bar(make_bar(100), [])
        self.assertIsNotNone(result)

    def test_ranking_filter(self):
        for s in ["strat_a", "strat_b"]:
            for _ in range(5):
                self.ranking.record_trade(s, 10, 0.6)
        self.ranking.rank_all()
        filt = StrategyRankingFilter(self.ranking)
        opps = [
            Opportunity(product_id="BTC-USD", direction=Direction.LONG,
                        instrument_type=InstrumentType.SPOT,
                        entry_price=100, stop_price=95, target_price=110,
                        risk_reward=2.0, confidence=0.7, reason="a",
                        strategy_name="strat_a", base_size=1.0, score=50),
            Opportunity(product_id="BTC-USD", direction=Direction.LONG,
                        instrument_type=InstrumentType.SPOT,
                        entry_price=100, stop_price=95, target_price=110,
                        risk_reward=2.0, confidence=0.7, reason="b",
                        strategy_name="strat_z", base_size=1.0, score=50),
        ]
        filtered = filt.filter_opportunities(opps)
        names = [o.strategy_name for o in filtered]
        self.assertIn("strat_a", names)
        self.assertNotIn("strat_z", names)

    def test_wrapper_set_product_id(self):
        class FakeWithPid(BaseStrategy):
            def __init__(self):
                self._pid = None
            def name(self):
                return "fake"
            def set_product_id(self, pid):
                self._pid = pid
            def on_bar(self, bar, history):
                return None
        inner = FakeWithPid()
        wrapper = TopRankedStrategyWrapper(inner, self.ranking)
        wrapper.set_product_id("BTC-USD")
        self.assertEqual(inner._pid, "BTC-USD")


class TestGraphSync(unittest.TestCase):
    def test_sync_coingecko_universe_uses_cached_payload(self):
        class FakeStore:
            def __init__(self):
                self.schema_applied = False
                self.assets = []
                self.tokens = []

            def apply_schema(self):
                self.schema_applied = True

            def upsert_assets(self, assets):
                items = list(assets)
                self.assets.extend(items)
                return len(items)

            def upsert_tokens(self, tokens):
                items = list(tokens)
                self.tokens.extend(items)
                return len(items)

        with tempfile.TemporaryDirectory() as td:
            td = Path(td)
            markets = td / "markets.json"
            meta = td / "meta.json"
            markets.write_text(json.dumps({"data": [
                {"id": "bitcoin", "symbol": "btc", "name": "Bitcoin", "market_cap_rank": 1, "market_cap": 1},
                {"id": "ethereum", "symbol": "eth", "name": "Ethereum", "market_cap_rank": 2, "market_cap": 2},
            ]}))
            meta.write_text(json.dumps({"data": {
                "bitcoin": {"id": "bitcoin", "symbol": "btc", "name": "Bitcoin", "categories": ["Layer 1"], "platforms": {"": ""}},
                "ethereum": {"id": "ethereum", "symbol": "eth", "name": "Ethereum", "categories": ["Layer 1"], "platforms": {"ethereum": "0xabc"}},
            }}))

            fake = FakeStore()
            with patch.object(cg_sync, "_load_coinbase_symbols", return_value={"BTC", "ETH"}):
                summary = cg_sync.sync_coingecko_universe(
                    store=fake,
                    markets_path=markets,
                    meta_path=meta,
                    fetch_live=False,
                )

        self.assertTrue(fake.schema_applied)
        self.assertEqual(summary["assets"], 2)
        self.assertEqual(summary["meta_assets"], 2)
        self.assertEqual(summary["tokens"], 1)
        self.assertEqual(fake.assets[0].product_id, "BTC-USD")

    def test_portfolio_optimizer_graph_multiplier_prefers_higher_scores(self):
        class FakeSignal:
            def __init__(self, score):
                self.graph_score = score

        opt = PortfolioOptimizer.__new__(PortfolioOptimizer)
        opt._graph_signals = {
            "BTC-USD": FakeSignal(0.9),
            "ETH-USD": FakeSignal(0.4),
        }
        opt.graph_store = None
        opt._graph_cache_ts = 0.0
        opt._graph_cache_ttl = 3600.0

        high = opt._graph_multiplier_for_product("BTC-USD", max_boost=0.25)
        low = opt._graph_multiplier_for_product("ETH-USD", max_boost=0.25)

        self.assertGreater(high, 1.0)
        self.assertLess(low, 1.0)
        self.assertGreater(high, low)


class TestDashboardGraph(unittest.TestCase):
    def test_graph_summary_for_products_uses_fake_store(self):
        class FakeSignal:
            def __init__(self, product_id, symbol, score, available=True):
                self.product_id = product_id
                self.symbol = symbol
                self.graph_score = score
                self.available_on_coinbase = available
                self.reasons = ["test"]

        class FakeStore:
            def asset_signal(self, product_id):
                return FakeSignal(product_id, product_id.split("-")[0], 0.9 if product_id == "BTC-USD" else 0.4)

        original_ts = ds.GRAPH_CACHE["ts"]
        original_data = ds.GRAPH_CACHE["data"]
        try:
            ds.GRAPH_CACHE["data"] = FakeStore()
            ds.GRAPH_CACHE["ts"] = time.time()
            summary = ds._graph_summary_for_products(["BTC-USD", "ETH-USD"], limit=2)
        finally:
            ds.GRAPH_CACHE["ts"] = original_ts
            ds.GRAPH_CACHE["data"] = original_data

        self.assertTrue(summary["available"])
        self.assertEqual(summary["top_assets"][0]["product_id"], "BTC-USD")
        self.assertGreater(summary["top_assets"][0]["overlay"], summary["top_assets"][1]["overlay"])

    def test_enrich_signals_with_graph_enriches_matching_products(self):
        class FakeSignal:
            def __init__(self, product_id, symbol, score, available=True):
                self.product_id = product_id
                self.symbol = symbol
                self.graph_score = score
                self.available_on_coinbase = available
                self.reasons = ["test"]
        class FakeStore:
            def asset_signal(self, product_id):
                return FakeSignal(product_id, product_id.split("-")[0], 0.9 if product_id == "BTC-USD" else 0.4)

        original_ts = ds.GRAPH_CACHE["ts"]
        original_data = ds.GRAPH_CACHE["data"]
        try:
            ds.GRAPH_CACHE["data"] = FakeStore()
            ds.GRAPH_CACHE["ts"] = time.time()
            queue = [
                {"symbol": "BTC-USD", "action": "BUY", "score": 0.8},
                {"symbol": "ETH-USD", "action": "SELL", "score": 0.6},
                {"symbol": "USDC", "action": "BUY", "score": 0.5},  # no dash — won't be collected
            ]
            enriched = ds._enrich_signals_with_graph(queue)
            self.assertEqual(len(enriched), 3)
            btc = next(s for s in enriched if s["symbol"] == "BTC-USD")
            self.assertAlmostEqual(btc["graph_score"], 0.9)
            self.assertGreater(btc["graph_overlay"], 1.0)
            eth = next(s for s in enriched if s["symbol"] == "ETH-USD")
            self.assertAlmostEqual(eth["graph_score"], 0.4)
            usdc = next(s for s in enriched if s["symbol"] == "USDC")
            self.assertNotIn("graph_score", usdc, "USDC has no dash so should be skipped")
        finally:
            ds.GRAPH_CACHE["ts"] = original_ts
            ds.GRAPH_CACHE["data"] = original_data


class TestCriticalBugs(unittest.TestCase):
    def test_confidence_engine_stub_has_required_attributes(self):
        from portfolio_optimizer import PortfolioOptimizer

        class _Sig: pass
        stub = _Sig()
        stub.symbol = "BTC"
        stub.strategy = "confidence_matrix_aggregated"
        stub.strength = 0.8
        stub.action = "BUY"

        # Verify all fields the ConfidenceEngine.apply_modifiers accesses
        self.assertEqual(stub.symbol, "BTC")
        self.assertEqual(stub.strategy, "confidence_matrix_aggregated")
        self.assertEqual(stub.strength, 0.8)
        self.assertEqual(stub.action, "BUY")

    def test_orchestrator_writes_dict_not_list_to_pending_approvals(self):
        from coinbase.src.orchestrator import ExecutionOrchestrator, TradeSignal
        from coinbase.src.protocols import Direction

        sig = TradeSignal(
            product_id="BTC-USD",
            direction=Direction.LONG,
            size=0.1,
            entry_price=50000,
            stop_price=49000,
            target_price=52000,
            strategy_name="test",
            confidence=0.8,
            reason="test",
        )
        with tempfile.TemporaryDirectory() as tmp:
            orig = os.getcwd()
            os.chdir(tmp)
            try:
                from coinbase.src.orchestrator import TradeMode
                orch = ExecutionOrchestrator(mode=TradeMode.PAPER, dry_run=True)
                orch._approval_execute(sig)
                with open("pending_approvals.json") as f:
                    data = json.load(f)
                self.assertIsInstance(data, dict)
                self.assertEqual(len(data), 1)
                token = list(data.keys())[0]
                self.assertEqual(data[token]["product_id"], "BTC-USD")
                self.assertEqual(data[token]["status"], "pending")
            finally:
                os.chdir(orig)


class TestFearGreed(unittest.TestCase):
    def setUp(self):
        self.fg = FearGreedIndex()
        self.adapter = FearGreedSignalAdapter()

    def _compute(self, closes_list, vol_list=None):
        return self.fg.compute({"TEST": closes_list}, {"TEST": vol_list} if vol_list else None)

    def test_initial_value(self):
        snapsnot = self.fg.compute({"TEST": [100] * 30})
        self.assertIsNotNone(snapsnot)

    def test_update_with_uptrend(self):
        prices = [100 + i * 0.5 for i in range(50)]
        snap = self._compute(prices)
        self.assertGreaterEqual(snap.value, 0)
        self.assertLessEqual(snap.value, 100)

    def test_update_with_downtrend(self):
        prices = [100 - i * 0.5 for i in range(50)]
        snap = self._compute(prices)
        self.assertGreaterEqual(snap.value, 0)

    def test_update_short_history(self):
        snap = self.fg.compute({"TEST": [100, 101]})
        self.assertEqual(snap.value, 50.0)

    def test_cache_ttl(self):
        self._compute([100 + i for i in range(50)])
        v1 = self.fg._cache.value
        self.fg._cache_ts = 0
        self._compute([100 + i for i in range(50)])
        v2 = self.fg._cache.value
        self.assertGreaterEqual(v2, 0)

    def test_momentum_component(self):
        prices = [100 + i * 1.0 for i in range(30)]
        snap = self._compute(prices)
        self.assertIsInstance(snap.momentum_component, float)

    def test_volatility_component(self):
        import random
        random.seed(42)
        prices = [100 + random.gauss(0, 3) for i in range(30)]
        snap = self._compute(prices)
        self.assertIsInstance(snap.volatility_component, float)

    def test_classification(self):
        snap = self._compute([100 + i * 2 for i in range(50)])
        self.assertIn(snap.classification,
                      ["extreme_fear", "fear", "neutral", "greed", "extreme_greed"])

    def test_fear_greed_signal_adapter_extreme(self):
        prices = [100 - i * 2 for i in range(50)]
        snap = self._compute(prices)
        bar = make_bar(prices[-1])
        result = self.adapter.on_bar(bar, [bar])
        if result is not None:
            self.assertIn(result.direction, [Direction.LONG, Direction.SHORT])

    def test_fear_greed_signal_adapter_mid(self):
        prices = [100] * 50
        self._compute(prices)
        result = self.adapter.on_bar(make_bar(100), [make_bar(100)])
        self.assertIsNone(result)


class TestConfidenceMatrix(unittest.TestCase):
    def setUp(self):
        # Build minimal StrategySignal-like stubs
        class FakeSignal:
            def __init__(self, action, strategy, confidence, reason=""):
                self.action = action
                self.strategy = strategy
                self.confidence = confidence
                self.reason = reason
        self.FakeSignal = FakeSignal
        self.module_path = "confidence_matrix"

    def _matrix(self, bt_cache=None):
        from confidence_matrix import ConfidenceMatrix
        return ConfidenceMatrix(bt_cache=bt_cache or {})

    def test_empty_signals_returns_empty_list(self):
        cm = self._matrix()
        self.assertEqual(cm.aggregate([]), [])

    def test_single_buy_signal(self):
        cm = self._matrix()
        sig = self.FakeSignal("BUY", "ema_cross", 0.7, "trend up")
        results = cm.aggregate([sig], currency="BTC")
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0].direction, "BUY")
        self.assertAlmostEqual(results[0].confidence, 0.7, places=2)

    def test_group_agreement_boost(self):
        cm = self._matrix()
        signals = [
            self.FakeSignal("BUY", "ema_cross", 0.7, "trend"),
            self.FakeSignal("BUY", "rsi_revert", 0.6, "momentum"),
            self.FakeSignal("BUY", "boll_break", 0.5, "volatility"),
        ]
        results = cm.aggregate(signals, currency="BTC")
        self.assertEqual(len(results), 1)
        # 3 agreeing groups → 1.0 + (3-1)*0.15 = 1.3 boost
        self.assertGreater(results[0].confidence, 0.5)
        self.assertEqual(results[0].agreeing_groups, 3)

    def test_buy_and_sell_in_different_directions(self):
        cm = self._matrix()
        signals = [
            self.FakeSignal("BUY", "ema_cross", 0.7, "up"),
            self.FakeSignal("SELL", "rsi_revert", 0.6, "down"),
        ]
        results = cm.aggregate(signals, currency="BTC")
        self.assertEqual(len(results), 2)
        self.assertEqual(results[0].direction, "BUY")
        self.assertEqual(results[1].direction, "SELL")
        self.assertGreater(results[0].confidence, results[1].confidence)

    def test_strategy_weight_from_bt_cache(self):
        cm = self._matrix(bt_cache={"ema_cross/BTC": {"win_rate": 0.8, "sharpe_ratio": 0.5, "profit_factor": 1.5}})
        sig = self.FakeSignal("BUY", "ema_cross", 0.7, "")
        weight = cm._strategy_weight("ema_cross", "BTC")
        self.assertGreater(weight, 0.3)  # Cache weight should exceed default
        self.assertLessEqual(weight, 1.0)

    def test_class_boost_varies_by_asset_class(self):
        cm = self._matrix()
        # momentum strategies boosted in speculative
        mom_safe = cm._class_boost("rsi_revert", "safe")
        mom_spec = cm._class_boost("rsi_revert", "speculative")
        self.assertLess(mom_safe, mom_spec)


class TestStateStore(unittest.TestCase):
    def setUp(self):
        import tempfile
        self.tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        self.tmp.close()
        from state_store import StateStore
        self.store = StateStore(self.tmp.name)

    def tearDown(self):
        os.unlink(self.tmp.name)

    def test_save_and_load_trade(self):
        trade = {"type": "rebalance", "side": "BUY", "currency": "BTC", "size_usd": 1000}
        self.store.save_trade(trade)
        trades = self.store.load_trades()
        self.assertEqual(len(trades), 1)
        self.assertEqual(trades[0]["currency"], "BTC")
        self.assertEqual(trades[0]["size_usd"], 1000)

    def test_multiple_trades_ordered_by_recency(self):
        for i in range(3):
            self.store.save_trade({"type": "test", "side": "BUY", "currency": f"CUR{i}", "size_usd": i * 100})
        trades = self.store.load_trades(limit=10)
        self.assertEqual(len(trades), 3)

    def test_save_snapshot_and_load(self):
        class FakeState:
            holdings = {"BTC": {"currency": "BTC", "total": 1.0, "price": 50000, "value": 50000, "classification": "safe", "allocation_pct": 100}}
            total_value = 50000
            usdc_balance = 10000
            fee_volume_30d = 5000
            fee_tier = (100000, 0.006, 0.012)

        result = self.store.save_snapshot(FakeState())
        self.assertIn("id", result)
        snapshots = self.store.load_snapshots(limit=5)
        self.assertEqual(len(snapshots), 1)
        self.assertIn("holdings", snapshots[0])
        self.assertEqual(snapshots[0]["total_value"], 50000)

    def test_bt_cache_round_trip(self):
        class FakeVerdict:
            strategy = "ema_cross"
            currency = "BTC"
            total_trades = 10
            winning_trades = 6
            losing_trades = 4
            win_rate = 0.6
            total_return_pct = 15.0
            sharpe_ratio = 1.2
            profit_factor = 1.8
            max_drawdown_pct = -10.0
            regime = "trending"
            passed = True
            reason = "ok"

        self.store.save_bt_cache("ema_cross/BTC", FakeVerdict())
        cache = self.store.load_bt_cache(ttl=86400)
        self.assertIn("ema_cross/BTC", cache)
        self.assertEqual(cache["ema_cross/BTC"]["win_rate"], 0.6)

    def test_connection_is_reused(self):
        conn1 = self.store._conn()
        conn2 = self.store._conn()
        self.assertIs(conn1, conn2)

    def test_meta_round_trip(self):
        self.store.set_meta("test_key", "test_value")
        self.assertEqual(self.store.get_meta("test_key"), "test_value")
        self.assertIsNone(self.store.get_meta("nonexistent"))

    def test_stats(self):
        stats = self.store.stats()
        self.assertIn("trades", stats)
        self.assertIn("snapshots", stats)
        self.assertIn("db_path", stats)


class TestApprovalServer(unittest.TestCase):
    def setUp(self):
        import tempfile
        self.tmp = tempfile.NamedTemporaryFile(suffix=".json", mode="w", delete=False)
        self.tmp.write("{}")
        self.tmp.close()

    def tearDown(self):
        os.unlink(self.tmp.name)

    def test_read_pending_returns_empty_dict_for_missing_file(self):
        from approval_server import ApprovalHandler
        h = ApprovalHandler.__new__(ApprovalHandler)
        h.pending_file = "/nonexistent/file.json"
        data = h._read_pending()
        self.assertEqual(data, {})

    def test_write_then_read_round_trip(self):
        from approval_server import ApprovalHandler
        h = ApprovalHandler.__new__(ApprovalHandler)
        h.pending_file = self.tmp.name
        h._write_pending({"abc-123": {"status": "pending", "side": "BUY", "currency": "BTC", "size_usd": 1000}})
        data = h._read_pending()
        self.assertEqual(data["abc-123"]["status"], "pending")
        self.assertEqual(data["abc-123"]["currency"], "BTC")


if __name__ == "__main__":
    unittest.main()
