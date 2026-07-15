import unittest
from coinbase.src.market_condition import (
    MarketConditionProfile, StrategyArchetype, MarketConditionStrategySelector,
    _fit_trend_follow, _fit_momentum, _fit_mean_reversion, _fit_breakout,
    _fit_volatility, _fit_scalping, _fit_grid, _fit_market_making, _fit_pairs,
    _fit_sentiment_driven, _fit_accumulation, _fit_dca, _fit_funding_capture,
    _fit_momentum_acceleration, ARCHETYPE_FIT_FUNCTIONS, StrategyArchetype,
)
from coinbase.src.protocols import Direction, Opportunity


FIT_FUNCS = [_fit_trend_follow, _fit_momentum, _fit_mean_reversion, _fit_breakout,
             _fit_volatility, _fit_scalping, _fit_grid, _fit_market_making, _fit_pairs,
             _fit_sentiment_driven, _fit_accumulation, _fit_dca, _fit_funding_capture,
             _fit_momentum_acceleration]


def prof(**kw):
    base = dict(regime="mixed", fear_greed=50.0, news_sentiment_pulse=0.0,
                trend_strength=0.0, volatility_bps=30.0, adx=25.0, hurst=0.5,
                serial_correlation=0.0, volume_trend=0.0, breaking_news_ratio=0.0,
                has_hacks=False, has_regulation=False)
    base.update(kw)
    return MarketConditionProfile(**base)


class TestProfile(unittest.TestCase):
    def test_post_init_regime(self):
        p = MarketConditionProfile(regime="ranging")
        self.assertEqual(p.regime, "ranging")

    def test_post_init_regime_enum(self):
        p = MarketConditionProfile(regime=StrategyArchetype.TREND_FOLLOW)
        self.assertEqual(p.regime, "trend_follow")

    def test_properties(self):
        hi = prof(fear_greed=90, news_sentiment_pulse=0.5, adx=40, trend_strength=0.1,
                  volatility_bps=90, hurst=0.2, serial_correlation=0.3,
                  regime="strong_uptrend", has_hacks=False)
        self.assertTrue(hi.is_extreme_sentiment)
        self.assertTrue(hi.is_trending_strongly)
        self.assertTrue(hi.is_high_volatility)
        self.assertFalse(hi.is_ranging)
        self.assertFalse(hi.is_mean_reverting)
        self.assertFalse(hi.is_trending)
        self.assertFalse(hi.is_risk_off)
        self.assertTrue(hi.is_risk_on)

        lo = prof(fear_greed=10, news_sentiment_pulse=-0.5, adx=10, trend_strength=-0.1,
                  volatility_bps=10, hurst=0.3, serial_correlation=-0.3,
                  regime="strong_downtrend", has_hacks=False)
        self.assertTrue(lo.is_ranging)
        self.assertTrue(lo.is_mean_reverting)
        self.assertFalse(lo.is_trending)
        self.assertTrue(lo.is_risk_off)
        self.assertFalse(lo.is_risk_on)


class TestFitFunctions(unittest.TestCase):
    def test_all_fit_funcs_various_profiles(self):
        profiles = [
            prof(fear_greed=90, news_sentiment_pulse=0.5, trend_strength=0.1,
                 volatility_bps=90, adx=40, hurst=0.2, serial_correlation=0.3,
                 breaking_news_ratio=0.5, has_hacks=True, has_regulation=True,
                 regime="strong_uptrend", volume_trend=0.1),
            prof(fear_greed=10, news_sentiment_pulse=-0.5, trend_strength=-0.1,
                 volatility_bps=10, adx=10, hurst=0.7, serial_correlation=-0.3,
                 regime="ranging", volume_trend=-0.1),
            prof(fear_greed=20, news_sentiment_pulse=-0.4, trend_strength=-0.05,
                 volatility_bps=50, adx=35, hurst=0.7, serial_correlation=0.0,
                 regime="strong_downtrend"),
            prof(fear_greed=50, news_sentiment_pulse=0.2, trend_strength=0.0,
                 volatility_bps=45, adx=22, hurst=0.7, serial_correlation=0.0,
                 regime="weak_uptrend"),
            prof(fear_greed=50),
        ]
        for fn in FIT_FUNCS:
            for p in profiles:
                score = fn(p)
                self.assertGreaterEqual(score, 0.0)
                self.assertLessEqual(score, 1.0)


class TestSelector(unittest.TestCase):
    def test_evaluate_sets_scores(self):
        sel = MarketConditionStrategySelector()
        sel.evaluate(prof(fear_greed=90, adx=40, volatility_bps=90, trend_strength=0.1,
                           regime="strong_uptrend", has_hacks=True))
        self.assertIn(StrategyArchetype.TREND_FOLLOW, sel._last_archetype_scores)

    def test_weight_before_evaluate(self):
        sel = MarketConditionStrategySelector()
        self.assertEqual(sel.weight("ema_cross"), sel.min_fit)

    def test_is_enabled(self):
        sel = MarketConditionStrategySelector()
        sel.evaluate(prof(fear_greed=90, adx=40, volatility_bps=90, trend_strength=0.1,
                          regime="strong_uptrend"))
        self.assertTrue(sel.is_enabled("ema_cross"))

    def test_top_archetype_fits_empty(self):
        sel = MarketConditionStrategySelector()
        self.assertEqual(sel.top_archetype_fits(), [])

    def test_top_strategies_limit(self):
        sel = MarketConditionStrategySelector()
        sel.evaluate(prof(fear_greed=90, adx=40, volatility_bps=90, trend_strength=0.1,
                          regime="strong_uptrend"))
        top = sel.top_strategies(limit=3)
        self.assertLessEqual(len(top), 3)

    def test_market_summary_no_profile(self):
        sel = MarketConditionStrategySelector()
        self.assertEqual(sel.market_summary(), "no market data")

    def test_market_summary_with_profile(self):
        sel = MarketConditionStrategySelector()
        sel.evaluate(prof(fear_greed=90, news_sentiment_pulse=0.5, adx=40, volatility_bps=90,
                          trend_strength=0.1, hurst=0.2, regime="strong_uptrend", has_hacks=False))
        s = sel.market_summary()
        self.assertIn("RISK_ON", s)

    def test_filter_opportunities(self):
        sel = MarketConditionStrategySelector()
        sel.evaluate(prof(fear_greed=90, adx=40, volatility_bps=90, trend_strength=0.1,
                          regime="strong_uptrend"))
        opps = [
            Opportunity(product_id="BTC-USD", direction=Direction.LONG, instrument_type=None,
                        entry_price=100, stop_price=90, target_price=110, risk_reward=2,
                        confidence=0.5, reason="r", strategy_name="ema_cross"),
            Opportunity(product_id="BTC-USD", direction=Direction.LONG, instrument_type=None,
                        entry_price=100, stop_price=90, target_price=110, risk_reward=2,
                        confidence=0.5, reason="r", strategy_name="unknown_strat"),
        ]
        filtered = sel.filter_opportunities(opps)
        self.assertEqual(len(filtered), 2)
        self.assertIn("market_fit_weight", filtered[0].meta)

    def test_summary(self):
        sel = MarketConditionStrategySelector()
        sel.evaluate(prof(fear_greed=90, adx=40, volatility_bps=90, trend_strength=0.1,
                          regime="strong_uptrend"))
        s = sel.summary()
        self.assertIn("top_archetypes", s)
        self.assertGreaterEqual(s["strategies_total"], 1)


class TestMarketConditionEdge(unittest.TestCase):
    def test_fit_trend_follow_downtrend_fg(self):
        p = prof(regime="strong_downtrend", fear_greed=20, adx=40, trend_strength=-0.1)
        self.assertGreater(_fit_trend_follow(p), 0.0)

    def test_fit_momentum_low_adx(self):
        p = prof(adx=10)
        self.assertLess(_fit_momentum(p), 0.5)

    def test_fit_breakout_hurst_high(self):
        p = prof(hurst=0.7, regime="strong_uptrend", volatility_bps=70, adx=30, volume_trend=0.05)
        self.assertGreater(_fit_breakout(p), 0.0)

    def test_fit_volatility_low_hurst(self):
        p = prof(hurst=0.2, volatility_bps=70, regime="high_volatility")
        self.assertGreater(_fit_volatility(p), 0.0)

    def test_fit_grid_trending_strongly_penalty(self):
        p = prof(adx=40, trend_strength=0.1, volatility_bps=10)
        self.assertLess(_fit_grid(p), 0.5)

    def test_market_summary_risk_off(self):
        sel = MarketConditionStrategySelector()
        sel.evaluate(prof(fear_greed=10, news_sentiment_pulse=-0.4,
                          regime="strong_downtrend", has_hacks=True))
        self.assertIn("RISK_OFF", sel.market_summary())

    def test_filter_strategy_below_min_fit_excluded(self):
        sel = MarketConditionStrategySelector(min_fit_threshold=0.4)
        # a neutral profile yields ~0 fit for most strategies -> excluded
        sel.evaluate(prof(fear_greed=50, volatility_bps=30, adx=25, hurst=0.5,
                          regime="mixed", trend_strength=0.0))
        opp = Opportunity(product_id="BTC-USD", direction=Direction.LONG, instrument_type=None,
                          entry_price=100, stop_price=90, target_price=110, risk_reward=2,
                          confidence=0.5, reason="r", strategy_name="ema_cross")
        filtered = sel.filter_opportunities([opp])
        self.assertEqual(len(filtered), 0)

    def test_filter_reason_tag_already_present(self):
        sel = MarketConditionStrategySelector()
        sel.evaluate(prof(fear_greed=90, adx=40, volatility_bps=90, trend_strength=0.1,
                          regime="strong_uptrend"))
        opp = Opportunity(product_id="BTC-USD", direction=Direction.LONG, instrument_type=None,
                          entry_price=100, stop_price=90, target_price=110, risk_reward=2,
                          confidence=0.5, reason="r [fit=0.60]", strategy_name="ema_cross")
        filtered = sel.filter_opportunities([opp])
        self.assertEqual(len(filtered), 1)
        self.assertIn("[fit=0.60]", filtered[0].reason)


if __name__ == "__main__":
    unittest.main()
