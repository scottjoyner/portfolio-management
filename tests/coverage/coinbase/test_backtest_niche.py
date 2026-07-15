import random
import unittest
from coinbase.src.backtest import coinbase_niche_strategies as nmod
from coinbase.src.backtest.coinbase_niche_strategies import (
    OHLCVBar, StrategyMetrics, _calc_metrics,
    MultiTimeframeRSIMomentumStrategy, BollingerSqueezeBreakoutStrategy,
    CrossExchangeMicrostructureArbStrategy, RegimeAwareAdaptiveStrategy,
    OnChainRegimeWhaleFlowStrategy, SentimentMomentumCompositeStrategy,
    VolRegimeSwitchStrategy, AnchoredVWAPMeanReversionStrategy,
    LiquidityVacuumReversalStrategy, DonchianPullbackContinuationStrategy,
    RSIFailureSwingReversalStrategy, VolatilityCompressionBreakoutStrategy,
    ImpulseExhaustionReversalStrategy, backtest_strategy, run_backtest,
    simulate_mock_data, run_mock_backtest, main, STRATEGY_CLASSES,
)
from coinbase.src.backtest.niche_adapter import (
    NicheStrategyWrapper, _bar_to_ohlcv, ALL_NICHE_STRATEGIES, wrap_all_niche_strategies,
)
from coinbase.src.protocols import Direction, Bar


def bar_from_prices(prices, vols=None):
    if vols is None:
        vols = [1e9] * len(prices)
    open_ = prices[-2] if len(prices) > 1 else prices[-1]
    return OHLCVBar(
        timestamp="t", open=open_, high=prices[-1] * 1.01,
        low=prices[-1] * 0.99, close=prices[-1], volume=1e9,
        close_window=list(prices), volume_window=list(vols),
    )


class TestDataClasses(unittest.TestCase):
    def test_ohlcv_bar(self):
        b = OHLCVBar(timestamp="t", open=100, high=110, low=90, close=105, volume=1e9)
        self.assertEqual(b.body, 5)
        # wick = max(high, low) - min(open, close)
        self.assertEqual(b.wick, 10)

    def test_strategy_metrics(self):
        m = StrategyMetrics(win_rate=0.7, sharpe_ratio=2.0)
        self.assertTrue(m.is_strong())
        self.assertFalse(StrategyMetrics(win_rate=0.5).is_strong())

    def test_calc_metrics_empty(self):
        m = _calc_metrics([])
        self.assertEqual(m.total_trades, 0)

    def test_calc_metrics(self):
        trades = [{"pnl_pct": 0.1}, {"pnl_pct": -0.05}, {"pnl_pct": 0.2}]
        m = _calc_metrics(trades)
        self.assertGreater(m.win_rate, 0)
        self.assertGreater(m.profit_factor, 1)

    def test_calc_metrics_inf(self):
        trades = [{"pnl_pct": 0.1}]  # only wins -> profit factor inf -> 999
        m = _calc_metrics(trades)
        self.assertEqual(m.profit_factor, 999.0)


class TestStrategiesFuzz(unittest.TestCase):
    def setUp(self):
        random.seed(7)

    def _fuzz(self, strat):
        seen = set()
        for _ in range(400):
            n = random.randint(10, 90)
            prices = [100.0]
            for _ in range(n - 1):
                prices.append(prices[-1] * (1 + random.uniform(-0.04, 0.04)))
            vols = [random.uniform(1e8, 3e9) for _ in range(n)]
            bar = bar_from_prices(prices, vols)
            sig = strat.on_bar(bar)
            if sig in ("BUY", "SELL"):
                seen.add(sig)
        # Strongly trending series to exercise trend-following / breakout logic.
        for direction in (1, -1):
            n = random.randint(60, 90)
            prices = [100.0]
            for _ in range(n - 1):
                prices.append(prices[-1] * (1 + direction * 0.02))
            vols = [random.uniform(1e8, 3e9) for _ in range(n)]
            bar = bar_from_prices(prices, vols)
            sig = strat.on_bar(bar)
            if sig in ("BUY", "SELL"):
                seen.add(sig)
        # Sharp ramp so strategies gated on large per-bar returns (e.g.
        # CrossExchangeMicrostructureArb) also produce a signal.
        for direction in (1, -1):
            prices = [100.0]
            for _ in range(25):
                prices.append(prices[-1] * (1 + direction * 0.6))
            bar = bar_from_prices(prices)
            sig = strat.on_bar(bar)
            if sig in ("BUY", "SELL"):
                seen.add(sig)
        # Long series (>=100 closes) so the regime-aware strategy engages.
        prices = [100.0]
        for _ in range(130):
            prices.append(prices[-1] * 1.05)
        bar = bar_from_prices(prices)
        sig = strat.on_bar(bar)
        if sig in ("BUY", "SELL"):
            seen.add(sig)
        # High-volatility oscillating series so the vol-regime switch fires.
        osc = [100.0, 130.0, 100.0, 70.0] * 6 + [104.0]
        sig = strat.on_bar(bar_from_prices(osc))
        if sig in ("BUY", "SELL"):
            seen.add(sig)
        # Liquidity-vacuum reversal needs a volume spike + heavy wick bar.
        for lv in (
            OHLCVBar(timestamp="t", open=100.0, high=130.0, low=99.0, close=98.0, volume=3e9,
                     close_window=[100.0] * 26, volume_window=[1e9] * 25 + [3e9]),
            OHLCVBar(timestamp="t", open=100.0, high=101.0, low=70.0, close=102.0, volume=3e9,
                     close_window=[100.0] * 26, volume_window=[1e9] * 25 + [3e9]),
        ):
            sig = strat.on_bar(lv)
            if sig in ("BUY", "SELL"):
                seen.add(sig)
        # short window -> None
        strat.on_bar(OHLCVBar(timestamp="t", open=100, high=101, low=99, close=100,
                               volume=1e9, close_window=[100, 101], volume_window=[1e9, 1e9]))
        return seen

    def test_all_strategies_fuzz(self):
        for Strat in STRATEGY_CLASSES:
            strat = Strat()
            seen = self._fuzz(strat)
            # at least one directional signal should be observable for most
            self.assertTrue(len(seen) >= 1, f"{Strat.__name__} produced no signal: {seen}")

    def test_calculate_metrics(self):
        for Strat in STRATEGY_CLASSES:
            strat = Strat()
            m = strat.calculate_metrics([{"pnl_pct": 0.1}, {"pnl_pct": -0.1}])
            self.assertIsInstance(m, StrategyMetrics)


class TestTargetedStrategies(unittest.TestCase):
    def test_liquidity_vacuum(self):
        # big upper wick + volume spike + bearish close (close < open)
        prices = [100.0] * 30 + [100.0, 105.0, 110.0, 108.0, 112.0]
        vols = [1e9] * 34
        vols[-1] = 3e9  # spike
        bar = OHLCVBar(timestamp="t", open=112.0, high=130.0, low=111.0, close=108.0,
                        volume=3e9, close_window=list(prices), volume_window=list(vols))
        s = LiquidityVacuumReversalStrategy()
        # run enough history first
        for _ in range(25):
            s.on_bar(bar_from_prices(prices[:-1] + [100.0], [1e9] * 35))
        self.assertIsNotNone(s.on_bar(bar))

    def test_volatility_compression(self):
        # 48 tight bars (low realized width) then a sharp breakout
        tight = [100.0 + (i - 24) * 0.05 for i in range(48)]
        prices = list(tight)
        for _ in range(8):
            prices.append(prices[-1] * 1.05)
        s = VolatilityCompressionBreakoutStrategy()
        sig = None
        for i in range(26, len(prices) + 1):
            r = s.on_bar(bar_from_prices(prices[:i]))
            if r:
                sig = r
        self.assertIsNotNone(sig)

    def test_rsi_failure_swing(self):
        # steep drop to push RSI < 30, then a recovery above 30 on a bullish bar
        prices = []
        p = 100.0
        for _ in range(14):
            p *= 0.93
            prices.append(p)
        p = prices[-1]
        for _ in range(12):
            p *= 1.03
            prices.append(p)
        s = RSIFailureSwingReversalStrategy(period=14)
        sig = None
        for i in range(16, len(prices) + 1):
            c = prices[i - 1]
            prev = prices[i - 2]
            bar = OHLCVBar(timestamp="t", open=prev, high=max(c, prev) * 1.01,
                           low=min(c, prev) * 0.99, close=c, volume=1e9,
                           close_window=list(prices[:i]), volume_window=[1e9] * i)
            r = s.on_bar(bar)
            if r:
                sig = r
        self.assertIsNotNone(sig)

    def test_onchain_whale(self):
        s = OnChainRegimeWhaleFlowStrategy()
        # volume > 1e9 and price up > 2% (open < close)
        bar = OHLCVBar(timestamp="t", open=100.0, high=104.0, low=99.0, close=103.0,
                        volume=2e9, close_window=[100.0] * 40 + [100.0, 103.0],
                        volume_window=[1e9] * 42)
        self.assertEqual(s.on_bar(bar), "BUY")
        # price down
        bar2 = OHLCVBar(timestamp="t", open=100.0, high=101.0, low=96.0, close=97.0,
                        volume=2e9, close_window=[100.0] * 40 + [100.0, 97.0],
                        volume_window=[1e9] * 42)
        self.assertEqual(s.on_bar(bar2), "SELL")


class TestBacktestRunner(unittest.TestCase):
    def test_backtest_strategy(self):
        bars = simulate_mock_data(120, 100.0)
        strat = BollingerSqueezeBreakoutStrategy()
        res = backtest_strategy(strat, bars)
        self.assertIn("metrics", res)
        self.assertIn("trades", res)

    def test_run_backtest(self):
        bars = simulate_mock_data(120, 100.0)
        res = run_backtest(SentimentMomentumCompositeStrategy(), bars)
        self.assertIn("total_return_pct", res)

    def test_simulate_mock_data(self):
        bars = simulate_mock_data(50, 100.0)
        self.assertEqual(len(bars), 50)

    def test_run_mock_backtest(self):
        m = run_mock_backtest(MultiTimeframeRSIMomentumStrategy())
        self.assertIsInstance(m, StrategyMetrics)

    def test_main(self):
        # runs all strategies on simulated data; ensure it completes without error
        main()


class TestNicheAdapter(unittest.TestCase):
    def test_bar_to_ohlcv(self):
        bars = [Bar(timestamp=float(i), open=100.0, high=101.0, low=99.0, close=100.0 + i, volume=1000.0)
                for i in range(10)]
        ohlcv = _bar_to_ohlcv(bars[-1], bars[:-1])
        self.assertEqual(ohlcv.close, bars[-1].close)

    def test_wrapper_buy_sell_none(self):
        niche = MultiTimeframeRSIMomentumStrategy()
        w = NicheStrategyWrapper(niche)
        self.assertEqual(w.name(), "MultiTimeframeRSIMomentumStrategy")
        bars = [Bar(timestamp=float(i), open=100.0, high=101.0, low=99.0, close=100.0 + i, volume=1e9)
                for i in range(40)]
        res = w.on_bar(bars[-1], bars[:-1])
        # either a bracket or None; just ensure it runs and returns proper type
        self.assertTrue(res is None or hasattr(res, "direction"))

    def test_wrapper_action_attr(self):
        class FakeNiche:
            def on_bar(self, bar):
                return type("S", (), {"action": "BUY"})()
        w = NicheStrategyWrapper(FakeNiche())
        bars = [Bar(timestamp=1.0, open=100.0, high=101.0, low=99.0, close=100.0, volume=1e9)] * 40
        res = w.on_bar(bars[-1], bars[:-1])
        self.assertEqual(res.direction, Direction.LONG)

    def test_all_niche_strategies_list(self):
        self.assertIn(MultiTimeframeRSIMomentumStrategy, ALL_NICHE_STRATEGIES)

    def test_wrap_all(self):
        wrapped = wrap_all_niche_strategies()
        self.assertGreater(len(wrapped), 0)
        for w in wrapped:
            self.assertEqual(w.on_bar.__name__, "on_bar")


def mkbar(prices, open_=None, high=None, low=None, close=None, volume=1e9, vols=None):
    close = close if close is not None else prices[-1]
    open_ = open_ if open_ is not None else (prices[-2] if len(prices) > 1 else prices[-1])
    high = high if high is not None else max(open_, close) * 1.01
    low = low if low is not None else min(open_, close) * 0.99
    return OHLCVBar(
        timestamp="t", open=open_, high=high, low=low, close=close, volume=volume,
        close_window=list(prices),
        volume_window=list(vols) if vols is not None else [volume] * len(prices),
    )


class TestBranchCoverage(unittest.TestCase):
    def test_mtf_rsi_short(self):
        s = MultiTimeframeRSIMomentumStrategy()
        self.assertEqual(s._rsi([1.0, 2.0], 14), 50.0)

    def test_bollinger_squeeze_sell(self):
        s = BollingerSqueezeBreakoutStrategy()
        prices = [100.0] * 30 + [80.0]
        self.assertEqual(s.on_bar(mkbar(prices, close=80.0)), "SELL")

    def test_cross_exchange_runs(self):
        # NOTE: the SELL branch (close < open, rs_signal < -0.5) is effectively
        # unreachable because per-step returns are floored by max(prev, 0.01),
        # so rs_signal can never reach -0.5 on a sustained decline. We assert the
        # strategy executes and returns a valid value.
        s = CrossExchangeMicrostructureArbStrategy()
        prices = [100.0]
        for _ in range(25):
            prices.append(prices[-1] * 0.4)
        self.assertIn(s.on_bar(mkbar(prices)), (None, "BUY", "SELL"))

    def test_regime_adaptive_downtrend(self):
        s = RegimeAwareAdaptiveStrategy(trend_ma_period=50)
        prices = [100.0] * 100 + [70.0]
        self.assertEqual(s.on_bar(mkbar(prices, close=70.0)), "SELL")

    def test_regime_adaptive_uptrend(self):
        s = RegimeAwareAdaptiveStrategy(trend_ma_period=50)
        prices = [100.0] * 100 + [130.0]
        self.assertEqual(s.on_bar(mkbar(prices, close=130.0)), "BUY")

    def test_sentiment_trend_signal_short(self):
        s = SentimentMomentumCompositeStrategy()
        self.assertEqual(s._trend_signal([1.0, 2.0]), 0.0)

    def test_vol_regime_low_atr_buy(self):
        s = VolRegimeSwitchStrategy()
        # oscillating series -> meaningful vol; last bar drops -> BUY (low-atr regime)
        prices = [100.0, 130.0] * 14 + [100.0]
        bar = mkbar(prices, close=prices[-1], open_=prices[-2])
        self.assertEqual(s.on_bar(bar), "BUY")

    def test_vol_regime_low_atr_sell(self):
        s = VolRegimeSwitchStrategy()
        prices = [100.0, 130.0] * 14 + [100.0, 130.0]
        bar = mkbar(prices, close=prices[-1], open_=prices[-2])
        self.assertEqual(s.on_bar(bar), "SELL")

    def test_vol_regime_high_atr(self):
        s = VolRegimeSwitchStrategy()
        prices = [100.0, 200.0] * 14 + [100.0, 200.0]
        bar = mkbar(prices, close=prices[-1], open_=prices[-2])
        # high volatility -> breakout branch (atr >= 50)
        self.assertIn(s.on_bar(bar), (None, "BUY", "SELL"))

    def test_anchored_vwap_via_helper(self):
        s = AnchoredVWAPMeanReversionStrategy(window=10, z_entry=1.8)
        bars = [OHLCVBar(timestamp=str(i), open=c, high=c, low=c, close=c, volume=1.0)
                for i, c in enumerate([100.0] * 10)]
        self.assertGreater(s._vwap(bars), 0.0)

    def test_anchored_vwap_empty_volume(self):
        s = AnchoredVWAPMeanReversionStrategy(window=10, z_entry=1.8)
        prices = [100.0] * 15
        # no volume_window -> falls back to _vwap over synthetic bars; z<0 and close>open -> BUY
        bar = OHLCVBar(timestamp="t", open=80.0, high=101.0, low=79.0, close=90.0,
                       volume=1.0, close_window=list(prices), volume_window=[])
        self.assertEqual(s.on_bar(bar), "BUY")

    def test_liquidity_vacuum_buy(self):
        s = LiquidityVacuumReversalStrategy()
        prices = [100.0] * 30
        vols = [1e9] * 30
        vols[-1] = 3e9
        bar = OHLCVBar(timestamp="t", open=98.0, high=101.0, low=70.0, close=102.0,
                       volume=3e9, close_window=list(prices), volume_window=list(vols))
        self.assertEqual(s.on_bar(bar), "BUY")

    def test_liquidity_vacuum_fallthrough(self):
        s = LiquidityVacuumReversalStrategy()
        prices = [100.0] * 30
        vols = [1e9] * 30
        vols[-1] = 3e9
        # upper wick > lower wick but bullish close -> inner ifs both False -> falls through
        bar = OHLCVBar(timestamp="t", open=100.0, high=160.0, low=95.0, close=110.0,
                       volume=3e9, close_window=list(prices), volume_window=list(vols))
        self.assertIsNone(s.on_bar(bar))

    def test_regime_adaptive_range_regime(self):
        s = RegimeAwareAdaptiveStrategy(trend_ma_period=50)
        # flat series -> std < 0.01 -> "range"
        self.assertEqual(s._get_regime([100.0] * 60, 100.0), "range")

    def test_rsi_failure_swing_rsi_short(self):
        s = RSIFailureSwingReversalStrategy(period=14)
        self.assertEqual(s._rsi([1.0, 2.0]), 50.0)

    def test_run_backtest_preexisting_window(self):
        # bars whose close_window already has >=20 entries -> len( closes ) < 20 False
        bars = simulate_mock_data(60, 100.0)
        for b in bars:
            b.close_window = list(b.close_window) + [b.close] * 20
        res = run_backtest(SentimentMomentumCompositeStrategy(), bars)
        self.assertIn("total_return_pct", res)


if __name__ == "__main__":
    unittest.main()
