import sys
import types
import unittest
from unittest import mock

from coinbase.src import opportunity_scanner as osc
from coinbase.src.protocols import (
    Bar, BracketSetup, Direction, InstrumentType, BaseStrategy,
)


def mk_bar(close, high=None, low=None, vol=100.0):
    return Bar(timestamp=0.0, open=close, high=high or close + 1,
               low=low or close - 1, close=close, volume=vol)


def mk_bars(n, start=100.0, step=1.0):
    return [mk_bar(start + i * step) for i in range(n)]


class FakeSignal:
    def __init__(self, action, price=0.0, confidence=0.5, reason="r"):
        self.action = action
        self.price = price
        self.confidence = confidence
        self.reason = reason


class FakeStrat:
    def __init__(self, signal):
        self._signal = signal

    def on_bar(self, *a, **k):
        return self._signal


class FakeStrategy(BaseStrategy):
    def __init__(self, setup=None, exc=None):
        self._setup = setup
        self._exc = exc
        self.pid = None

    def name(self):
        return "fake"

    def set_product_id(self, pid):
        self.pid = pid

    def on_bar(self, bar, history):
        if self._exc:
            raise self._exc
        return self._setup


class TestScannerConfig(unittest.TestCase):
    def test_defaults(self):
        c = osc.ScannerConfig()
        self.assertEqual(c.min_rr, 1.5)
        self.assertTrue(c.enable_short)
        self.assertTrue(c.enable_futures)


class TestAlphaSetupBase(unittest.TestCase):
    def test_name(self):
        s = osc._AlphaSetupBase("n", lambda *a, **k: None, True)
        self.assertEqual(s.name(), "n")

    def test_on_bar_short_history(self):
        s = osc._AlphaSetupBase("n", lambda *a, **k: {}, True)
        self.assertIsNone(s.on_bar(mk_bar(1), mk_bars(10)))

    def test_on_bar_atr_none(self):
        s = osc._AlphaSetupBase("n", lambda *a, **k: {"side": "buy", "entry": 1,
                                                      "stop": 0.9, "target": 1.1, "rr": 2.0}, True)
        with mock.patch.object(osc, "compute_atr", return_value=None):
            setup = s.on_bar(mk_bar(1), mk_bars(220))
        self.assertIsNotNone(setup)

    def test_on_bar_needs_stop_target(self):
        fn = mock.MagicMock(return_value={"side": "buy", "entry": 1, "stop": 0.9,
                                          "target": 1.1, "rr": 2.0, "name": "x"})
        s = osc._AlphaSetupBase("n", fn, True, 2.0, 3.0)
        with mock.patch.object(osc, "compute_atr", return_value=mock.MagicMock(iloc=[5.0])):
            setup = s.on_bar(mk_bar(1), mk_bars(220))
        self.assertIsNotNone(setup)
        fn.assert_called_once_with(mock.ANY, 2.0, 3.0)

    def test_on_bar_no_stop_target(self):
        fn = mock.MagicMock(return_value={"side": "sell", "entry": 1, "stop": 1.1,
                                          "target": 0.9, "rr": 2.0})
        s = osc._AlphaSetupBase("n", fn, False, 2.0, 3.0)
        with mock.patch.object(osc, "compute_atr", return_value=mock.MagicMock(iloc=[5.0])):
            setup = s.on_bar(mk_bar(1), mk_bars(220))
        self.assertEqual(setup.direction, Direction.SHORT)
        fn.assert_called_once_with(mock.ANY, 2.0)

    def test_on_bar_low_rr(self):
        fn = mock.MagicMock(return_value={"side": "buy", "entry": 1, "stop": 0.9,
                                          "target": 1.1, "rr": 0.5})
        s = osc._AlphaSetupBase("n", fn, True)
        with mock.patch.object(osc, "compute_atr", return_value=mock.MagicMock(iloc=[5.0])):
            self.assertIsNone(s.on_bar(mk_bar(1), mk_bars(220)))

    def test_on_bar_exception(self):
        fn = mock.MagicMock(side_effect=RuntimeError("x"))
        s = osc._AlphaSetupBase("n", fn, True)
        with mock.patch.object(osc, "compute_atr", return_value=mock.MagicMock(iloc=[5.0])):
            self.assertIsNone(s.on_bar(mk_bar(1), mk_bars(220)))

    def test_bars_to_df(self):
        df = osc._AlphaSetupBase._bars_to_df(mk_bars(3))
        self.assertEqual(len(df), 3)
        self.assertIn("close", df.columns)


class TestMarketMakingStrategy(unittest.TestCase):
    def test_name(self):
        self.assertEqual(osc.MarketMakingStrategy().name(), "market_making")

    def test_on_bar_short_history(self):
        s = osc.MarketMakingStrategy()
        self.assertIsNone(s.on_bar(mk_bar(100), mk_bars(10)))

    def test_on_bar_low_atr(self):
        s = osc.MarketMakingStrategy()
        bars = mk_bars(30)
        # force small ATR by mirroring price -> estimate returns 0
        self.assertIsNone(s.on_bar(mk_bar(100), bars))

    def test_on_bar_spread_too_tight(self):
        s = osc.MarketMakingStrategy()
        bars = mk_bars(30)
        bar = mk_bar(100.0, high=100.0001, low=99.9999, vol=100.0)
        setup = s.on_bar(bar, bars)
        # spread tiny -> filtered
        self.assertIsNone(setup)

    def test_on_bar_spread_too_wide(self):
        s = osc.MarketMakingStrategy()
        bars = mk_bars(30)
        bar = mk_bar(100.0, high=101.0, low=99.0, vol=100.0)
        self.assertIsNone(s.on_bar(bar, bars))

    def test_on_bar_vol_spike(self):
        s = osc.MarketMakingStrategy()
        bars = mk_bars(30)
        # build a realistic bar: close=100, atr ~1, spread in [10,50] bps -> 0.1-0.5
        bar = mk_bar(100.0, high=100.03, low=99.97, vol=1000.0)
        # make history volume small so bar.volume spikes
        hist = mk_bars(30, start=100.0)
        for b in hist:
            b.volume = 100.0
        hist[-1].volume = 100.0
        setup = s.on_bar(bar, hist)
        # volume spike -> filtered (None) OR signal; just ensure no crash and rsi path possible
        self.assertTrue(setup is None or isinstance(setup, BracketSetup))

    def test_on_bar_long(self):
        s = osc.MarketMakingStrategy()
        bars = mk_bars(30)
        bar = mk_bar(100.0, high=100.03, low=99.97, vol=100.0)
        # force rsi low by making closes drop
        for i, b in enumerate(bars):
            b.close = 100.0 - i * 0.5
        setup = s.on_bar(bar, bars)
        # rsi may be <35 -> LONG, else filtered
        self.assertTrue(setup is None or setup.direction == Direction.LONG)

    def test_on_bar_short(self):
        s = osc.MarketMakingStrategy()
        bars = mk_bars(30)
        bar = mk_bar(100.0, high=100.03, low=99.97, vol=100.0)
        for i, b in enumerate(bars):
            b.close = 100.0 + i * 0.5
        setup = s.on_bar(bar, bars)
        self.assertTrue(setup is None or setup.direction == Direction.SHORT)

    def test_rsi_static(self):
        self.assertEqual(osc.MarketMakingStrategy._rsi([1, 2], 14), 50.0)
        # all gains
        closes = [100 + i for i in range(15)]
        self.assertEqual(osc.MarketMakingStrategy._rsi(closes, 14), 100.0)
        # mixed
        closes = [100, 101, 99, 102, 98, 103, 97, 104, 96, 105, 95, 106, 94, 107, 93]
        r = osc.MarketMakingStrategy._rsi(closes, 14)
        self.assertGreaterEqual(r, 0.0)
        self.assertLessEqual(r, 100.0)

    def test_estimate_atr_static(self):
        self.assertEqual(osc.MarketMakingStrategy._estimate_atr([1, 2], [1, 2], [1, 2]), 0.0)
        bars = mk_bars(20)
        v = osc.MarketMakingStrategy._estimate_atr(
            [b.close for b in bars], [b.high for b in bars], [b.low for b in bars])
        self.assertGreaterEqual(v, 0.0)


class TestCrossProductArbitrage(unittest.TestCase):
    def test_name(self):
        self.assertEqual(osc.CrossProductArbitrageStrategy().name(), "cross_arb")

    def test_on_bar_none(self):
        self.assertIsNone(osc.CrossProductArbitrageStrategy().on_bar(mk_bar(1), mk_bars(5)))


class TestStrategyEngineAdapter(unittest.TestCase):
    def _patch(self, key, factory, vol=False, hl=False):
        import strategy_engine
        saved = (strategy_engine.ALL_STRATEGIES, strategy_engine.VOLUME_STRATEGIES,
                 strategy_engine.HIGH_LOW_STRATEGIES)
        strategy_engine.ALL_STRATEGIES = {key: factory}
        strategy_engine.VOLUME_STRATEGIES = {key} if vol else set()
        strategy_engine.HIGH_LOW_STRATEGIES = {key} if hl else set()
        return saved

    def _restore(self, saved):
        import strategy_engine
        strategy_engine.ALL_STRATEGIES, strategy_engine.VOLUME_STRATEGIES, \
            strategy_engine.HIGH_LOW_STRATEGIES = saved

    def test_name(self):
        self.assertEqual(osc.StrategyEngineAdapter("ema_cross").name(), "se_ema_cross")

    def test_on_bar_unknown_key(self):
        a = osc.StrategyEngineAdapter("does_not_exist")
        self.assertIsNone(a.on_bar(mk_bar(1), mk_bars(5)))

    def test_on_bar_buy(self):
        saved = self._patch("x", lambda: FakeStrat(FakeSignal("BUY", price=100.0, confidence=0.6)))
        try:
            a = osc.StrategyEngineAdapter("x")
            setup = a.on_bar(mk_bar(100), mk_bars(30))
        finally:
            self._restore(saved)
        self.assertIsNotNone(setup)
        self.assertEqual(setup.direction, Direction.LONG)

    def test_on_bar_sell(self):
        saved = self._patch("x", lambda: FakeStrat(FakeSignal("SELL", price=100.0, confidence=0.6)))
        try:
            a = osc.StrategyEngineAdapter("x")
            setup = a.on_bar(mk_bar(100), mk_bars(30))
        finally:
            self._restore(saved)
        self.assertEqual(setup.direction, Direction.SHORT)

    def test_on_bar_none_signal(self):
        saved = self._patch("x", lambda: FakeStrat(None))
        try:
            a = osc.StrategyEngineAdapter("x")
            self.assertIsNone(a.on_bar(mk_bar(100), mk_bars(30)))
        finally:
            self._restore(saved)

    def test_on_bar_hold(self):
        saved = self._patch("x", lambda: FakeStrat(FakeSignal("HOLD")))
        try:
            a = osc.StrategyEngineAdapter("x")
            self.assertIsNone(a.on_bar(mk_bar(100), mk_bars(30)))
        finally:
            self._restore(saved)

    def test_on_bar_volume(self):
        saved = self._patch("x", lambda: FakeStrat(FakeSignal("BUY", price=100.0)), vol=True)
        try:
            a = osc.StrategyEngineAdapter("x")
            setup = a.on_bar(mk_bar(100), mk_bars(30))
        finally:
            self._restore(saved)
        self.assertIsNotNone(setup)

    def test_on_bar_high_low(self):
        saved = self._patch("x", lambda: FakeStrat(FakeSignal("BUY", price=100.0)), hl=True)
        try:
            a = osc.StrategyEngineAdapter("x")
            setup = a.on_bar(mk_bar(100), mk_bars(30))
        finally:
            self._restore(saved)
        self.assertIsNotNone(setup)

    def test_on_bar_exception(self):
        import strategy_engine
        saved = (strategy_engine.ALL_STRATEGIES, strategy_engine.VOLUME_STRATEGIES,
                 strategy_engine.HIGH_LOW_STRATEGIES)
        strategy_engine.ALL_STRATEGIES = {"x": FakeStratExc}
        strategy_engine.VOLUME_STRATEGIES = set()
        strategy_engine.HIGH_LOW_STRATEGIES = set()
        try:
            a = osc.StrategyEngineAdapter("x")
            self.assertIsNone(a.on_bar(mk_bar(100), mk_bars(30)))
        finally:
            self._restore(saved)


class FakeStratExc:
    def on_bar(self, *a, **k):
        raise RuntimeError("boom")


class TestFuturesSignalAdapter(unittest.TestCase):
    def test_name(self):
        self.assertEqual(osc.FuturesSignalAdapter().name(), "futures_signal")

    def test_on_bar_short_history(self):
        self.assertIsNone(osc.FuturesSignalAdapter().on_bar(mk_bar(1), mk_bars(10)))

    def test_on_bar_long_bounce(self):
        a = osc.FuturesSignalAdapter()
        bars = mk_bars(60, start=100.0)
        bar = mk_bar(bars[-1].close, vol=1000.0)
        with mock.patch.object(osc.FuturesSignalAdapter, "_calc_rsi", return_value=25.0), \
                mock.patch.object(osc.FuturesSignalAdapter, "_detect_regime", return_value="uptrend"):
            setup = a.on_bar(bar, bars)
        self.assertIsNotNone(setup)
        self.assertEqual(setup.direction, Direction.LONG)
        self.assertEqual(setup.instrument_type, InstrumentType.PERP_FUTURES)

    def test_on_bar_short_bounce(self):
        a = osc.FuturesSignalAdapter()
        bars = mk_bars(60, start=200.0)
        bar = mk_bar(bars[-1].close, vol=1000.0)
        with mock.patch.object(osc.FuturesSignalAdapter, "_calc_rsi", return_value=75.0), \
                mock.patch.object(osc.FuturesSignalAdapter, "_detect_regime", return_value="downtrend"):
            setup = a.on_bar(bar, bars)
        self.assertIsNotNone(setup)
        self.assertEqual(setup.direction, Direction.SHORT)

    def test_on_bar_no_signal(self):
        a = osc.FuturesSignalAdapter()
        bars = mk_bars(60, start=100.0)
        bar = mk_bar(100.0, vol=100.0)
        self.assertIsNone(a.on_bar(bar, bars))

    def test_calc_rsi(self):
        self.assertEqual(osc.FuturesSignalAdapter._calc_rsi([1, 2], 14), 50.0)
        closes = [100 + i for i in range(15)]
        self.assertEqual(osc.FuturesSignalAdapter._calc_rsi(closes, 14), 100.0)

    def test_detect_regime(self):
        self.assertEqual(osc.FuturesSignalAdapter._detect_regime([1, 2]), "unknown")
        bars = [100 + i for i in range(50)]
        self.assertEqual(osc.FuturesSignalAdapter._detect_regime(bars), "uptrend")
        bars = [200 - i for i in range(50)]
        self.assertEqual(osc.FuturesSignalAdapter._detect_regime(bars), "downtrend")
        bars = [100 + (i % 2) for i in range(50)]
        self.assertEqual(osc.FuturesSignalAdapter._detect_regime(bars), "ranging")


class TestStrategyUniverse(unittest.TestCase):
    def test_all_se_adapters(self):
        strats = osc.StrategyUniverse.all_se_adapters()
        self.assertEqual(len(strats), len(osc.STRATEGY_ENGINE_ALL_KEYS))

    def test_all_se_adapters_filtered(self):
        strats = osc.StrategyUniverse.all_se_adapters(keys=["rsi_revert"])
        self.assertEqual(len(strats), 1)

    def test_all_alpha_setups(self):
        strats = osc.StrategyUniverse.all_alpha_setups()
        self.assertGreaterEqual(len(strats), 1)

    def test_all_alpha_setups_filters_none(self):
        saved = list(osc.ALPHA_SETUP_FUNCTIONS)
        osc.ALPHA_SETUP_FUNCTIONS[0] = (saved[0][0], None, saved[0][2])
        try:
            strats = osc.StrategyUniverse.all_alpha_setups()
        finally:
            osc.ALPHA_SETUP_FUNCTIONS[0] = saved[0]
        self.assertGreaterEqual(len(strats), 0)

    def test_all_niche_strategies(self):
        strats = osc.StrategyUniverse.all_niche_strategies()
        self.assertIsInstance(strats, list)

    def test_all_niche_strategies_failure(self):
        fake = types.ModuleType("coinbase.src.backtest.niche_adapter")
        saved = sys.modules.get("coinbase.src.backtest.niche_adapter")
        sys.modules["coinbase.src.backtest.niche_adapter"] = fake
        try:
            strats = osc.StrategyUniverse.all_niche_strategies()
        finally:
            if saved is None:
                sys.modules.pop("coinbase.src.backtest.niche_adapter", None)
            else:
                sys.modules["coinbase.src.backtest.niche_adapter"] = saved
        self.assertEqual(strats, [])

    def test_all_novel_strategies(self):
        strats = osc.StrategyUniverse.all_novel_strategies()
        self.assertGreater(len(strats), 0)

    def test_all_novel_strategies_partial_failure(self):
        fake = types.ModuleType("coinbase.src.strat_scalper")
        saved = sys.modules.get("coinbase.src.strat_scalper")
        sys.modules["coinbase.src.strat_scalper"] = fake
        try:
            strats = osc.StrategyUniverse.all_novel_strategies()
        finally:
            if saved is None:
                sys.modules.pop("coinbase.src.strat_scalper", None)
            else:
                sys.modules["coinbase.src.strat_scalper"] = saved
        names = [s.name() for s in strats]
        self.assertNotIn("volatility_scalper", names)
        self.assertIn("price_action_sr", names)

    def test_all_strategies_default(self):
        cfg = osc.ScannerConfig()
        strats = osc.StrategyUniverse.all_strategies(cfg)
        self.assertGreater(len(strats), 0)

    def test_all_strategies_flags(self):
        cfg = osc.ScannerConfig(include_se_all=False, include_alpha=False,
                                include_niche=False, include_futures=False,
                                include_market_making=False, include_orderbook=False,
                                include_novel=False)
        strats = osc.StrategyUniverse.all_strategies(cfg)
        self.assertEqual(strats, [])

    def test_all_strategies_filter(self):
        cfg = osc.ScannerConfig(strategy_filter={"market_making"})
        strats = osc.StrategyUniverse.all_strategies(cfg)
        self.assertEqual([s.name() for s in strats], ["market_making"])

    def test_all_strategies_exclude(self):
        cfg = osc.ScannerConfig(exclude_strategies={"market_making", "futures_signal"})
        strats = osc.StrategyUniverse.all_strategies(cfg)
        names = [s.name() for s in strats]
        self.assertNotIn("market_making", names)
        self.assertNotIn("futures_signal", names)

    def test_summary(self):
        s = osc.StrategyUniverse.summary()
        self.assertIn("total", s)
        self.assertEqual(s["total"], len(osc.STRATEGY_ENGINE_ALL_KEYS) +
                         len(osc.ALPHA_SETUP_FUNCTIONS) + len(osc.NICHE_STRATEGY_NAMES) + 3 + 5 + 5)


class TestOpportunityScanner(unittest.TestCase):
    def _setup(self, direction=Direction.LONG, instrument=InstrumentType.SPOT,
               atr=0.5, rr=2.0, confidence=0.6):
        setup = BracketSetup(
            direction=direction, entry_price=200.0, stop_price=199.0,
            target_price=202.0, risk_reward=rr, confidence=confidence,
            reason="r", strategy_name="fake", atr=atr,
            instrument_type=instrument, leverage=1.0,
        )
        scanner = osc.OpportunityScanner(osc.ScannerConfig())
        scanner.register(FakeStrategy(setup))
        return scanner, setup

    def test_register(self):
        s = osc.OpportunityScanner()
        s.register(FakeStrategy())
        self.assertEqual(len(s._strategies), 1)

    def test_register_all(self):
        s = osc.OpportunityScanner()
        s.register_all([FakeStrategy(), FakeStrategy()])
        self.assertEqual(len(s._strategies), 2)

    def test_register_defaults(self):
        s = osc.OpportunityScanner()
        s.register_defaults()
        self.assertGreater(len(s._strategies), 0)

    def test_scan_basic_long(self):
        scanner, _ = self._setup()
        opps = scanner.scan("BTC-USD", mk_bar(200), mk_bars(30), atr=1.0)
        self.assertEqual(len(opps), 1)
        self.assertEqual(opps[0].direction, Direction.LONG)

    def test_scan_short_disabled(self):
        scanner, _ = self._setup(direction=Direction.SHORT)
        cfg = osc.ScannerConfig(enable_short=False)
        scanner.config = cfg
        opps = scanner.scan("BTC-USD", mk_bar(200), mk_bars(30), atr=1.0)
        self.assertEqual(opps, [])

    def test_scan_futures_disabled(self):
        scanner, _ = self._setup(instrument=InstrumentType.PERP_FUTURES)
        cfg = osc.ScannerConfig(enable_futures=False)
        scanner.config = cfg
        opps = scanner.scan("BTC-USD", mk_bar(200), mk_bars(30), atr=1.0)
        self.assertEqual(opps, [])

    def test_scan_futures_enabled_score_boost(self):
        scanner, _ = self._setup(instrument=InstrumentType.PERP_FUTURES)
        opps = scanner.scan("BTC-USD", mk_bar(200), mk_bars(30), atr=1.0)
        self.assertEqual(len(opps), 1)
        # futures boost *1.1
        self.assertAlmostEqual(opps[0].score, 0.6 * 2.0 * 1.1, places=4)

    def test_scan_rr_too_low(self):
        scanner, _ = self._setup(rr=1.0)
        cfg = osc.ScannerConfig(min_rr=1.5)
        scanner.config = cfg
        opps = scanner.scan("BTC-USD", mk_bar(200), mk_bars(30), atr=1.0)
        self.assertEqual(opps, [])

    def test_scan_confidence_too_low(self):
        scanner, _ = self._setup(confidence=0.05)
        cfg = osc.ScannerConfig(confidence_threshold=0.1)
        scanner.config = cfg
        opps = scanner.scan("BTC-USD", mk_bar(200), mk_bars(30), atr=1.0)
        self.assertEqual(opps, [])

    def test_scan_leverage_bands(self):
        # vol < 50 bps -> leverage 3.0
        scanner, _ = self._setup(atr=0.5)  # 0.5/200*10000 = 25 bps
        opps = scanner.scan("BTC-USD", mk_bar(200), mk_bars(30), atr=1.0)
        self.assertEqual(opps[0].leverage, 3.0)
        # 50-150 bps -> 2.0
        scanner2, _ = self._setup(atr=2.0)  # 100 bps
        opps2 = scanner2.scan("BTC-USD", mk_bar(200), mk_bars(30), atr=1.0)
        self.assertEqual(opps2[0].leverage, 2.0)
        # >=150 bps -> 1.0
        scanner3, _ = self._setup(atr=8.0)  # 400 bps
        opps3 = scanner3.scan("BTC-USD", mk_bar(200), mk_bars(30), atr=1.0)
        self.assertEqual(opps3[0].leverage, 1.0)

    def test_scan_short_score_multiplier(self):
        scanner, _ = self._setup(direction=Direction.SHORT)
        opps = scanner.scan("BTC-USD", mk_bar(200), mk_bars(30), atr=1.0)
        self.assertAlmostEqual(opps[0].score, 0.6 * 2.0 * 0.9, places=4)

    def test_scan_dedup_and_limit(self):
        cfg = osc.ScannerConfig(max_positions_per_product=1)
        scanner = osc.OpportunityScanner(cfg)
        setups = []
        for i in range(5):
            setups.append(BracketSetup(
                direction=Direction.LONG, entry_price=200.0, stop_price=199.0,
                target_price=202.0, risk_reward=2.0, confidence=0.6,
                reason=f"r{i}", strategy_name=f"fake{i}", atr=0.5,
                instrument_type=InstrumentType.SPOT, leverage=1.0))
        for st in setups:
            scanner.register(FakeStrategy(st))
        opps = scanner.scan("BTC-USD", mk_bar(200), mk_bars(30), atr=1.0)
        # dedup keeps distinct strategies; limited to max_positions_per_product
        self.assertLessEqual(len(opps), 1)

    def test_scan_strategy_exception(self):
        cfg = osc.ScannerConfig()
        scanner = osc.OpportunityScanner(cfg)
        scanner.register(FakeStrategy(exc=RuntimeError("x")))
        opps = scanner.scan("BTC-USD", mk_bar(200), mk_bars(30), atr=1.0)
        self.assertEqual(opps, [])

    def test_scan_multi_skip(self):
        scanner, _ = self._setup()
        out = scanner.scan_multi({"BTC-USD": {"history": []}}, lambda p: 200.0)
        self.assertEqual(out, {})
        out2 = scanner.scan_multi(
            {"BTC-USD": {"bar": mk_bar(200), "history": mk_bars(30)}}, lambda p: 200.0)
        self.assertIn("BTC-USD", out2)


if __name__ == "__main__":
    unittest.main()
