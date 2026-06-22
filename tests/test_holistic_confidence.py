"""
End-to-end tests for the holistic confidence scoring pipeline.

Validates that all 8 confidence modifiers work together to produce
a unified holistic trade confidence score, and that the integration
points (regime detection, consensus, cross-correlation) are correct.
"""

import unittest
from dataclasses import dataclass
from trading_system.signal_confidence import ConfidenceEngine


@dataclass
class MockSignal:
    symbol: str
    action: str
    strength: float
    strategy: str


# -----------------------------------------------------------------------
# Unit: regime detection
# -----------------------------------------------------------------------

def detect_regime(price_data: dict) -> str:
    change_pct = abs(float(price_data.get("change_pct", 0)))
    if change_pct > 5.0:
        return "volatile"
    if change_pct > 2.0:
        return "trending"
    if change_pct < 0.5:
        return "quiet"
    return "neutral"


# -----------------------------------------------------------------------
# Unit: global consensus
# -----------------------------------------------------------------------

def compute_global_consensus(signals: list) -> float:
    if not signals:
        return 0.0
    buys = sum(1 for s in signals if s.action == "BUY")
    sells = sum(1 for s in signals if s.action == "SELL")
    majority = max(buys, sells)
    return majority / len(signals)


# -----------------------------------------------------------------------
# Configuration fixture matching the production HolisticConfidenceConfig
# -----------------------------------------------------------------------

LIQUIDITY_TIERS = {
    "BTC-USD": 1, "ETH-USD": 1, "SOL-USD": 1,
    "GARBAGE-USD": 5, "SHIB-USD": 3, "FIL-USD": 3,
}

REGIME_CAPS = {"volatile": 0.4, "quiet": 0.6, "trending": 1.0, "neutral": 0.8}

MARKET_LEADERS = ["BTC-USD", "ETH-USD"]


# -----------------------------------------------------------------------
# Tests
# -----------------------------------------------------------------------

class TestHolisticConfidencePipeline(unittest.TestCase):
    """Full pipeline: all 8 modifiers applied together."""

    def setUp(self):
        self.engine = ConfidenceEngine(
            liquidity_tiers=LIQUIDITY_TIERS,
            regime_caps=REGIME_CAPS,
        )

    # ------------------------------------------------------------------
    # Modifier interaction tests
    # ------------------------------------------------------------------

    def test_all_modifiers_high_quality_signal(self):
        """Top-tier liquid product, no spread, trending, aligned sentiment, high consensus."""
        signal = MockSignal("BTC-USD", "BUY", 0.8, "momentum")
        result = self.engine.apply_modifiers(
            signal=signal,
            market_data={"spread": 0.0001},
            regime="trending",
            market_leaders=MARKET_LEADERS,
            sentiment_score=0.7,
            global_consensus=0.85,
        )
        self.assertGreaterEqual(result.modified_confidence, 0.0)
        self.assertLessEqual(result.modified_confidence, 1.0)
        self.assertGreater(len(result.modifiers_applied), 0)
        # Should NOT have liquidity_tier penalty (tier 1)
        self.assertNotIn("liquidity_tier", result.modifiers_applied)
        # Should NOT have regime gate (trending cap = 1.0)
        self.assertNotIn("regime_gate", result.modifiers_applied)

    def test_all_modifiers_low_quality_signal(self):
        """Junk product, wide spread, volatile regime, negative sentiment, low consensus."""
        signal = MockSignal("GARBAGE-USD", "BUY", 0.8, "momentum")
        result = self.engine.apply_modifiers(
            signal=signal,
            market_data={"spread": 0.02},
            regime="volatile",
            market_leaders=MARKET_LEADERS,
            sentiment_score=-0.6,
            global_consensus=0.25,
        )
        self.assertLess(result.modified_confidence, 0.4)
        # Penalties compound: the regime gate may not fire if other
        # modifiers already brought confidence below the volatile cap (0.4)
        self.assertIn("liquidity_tier", result.modifiers_applied)
        self.assertIn("spread_adjustment", result.modifiers_applied)
        self.assertIn("sentiment_penalty", result.modifiers_applied)
        self.assertIn("global_consensus_penalty", result.modifiers_applied)
        # Overall modifiers applied >= 4
        self.assertGreaterEqual(len(result.modifiers_applied), 4)

    def test_regime_gate_fires_when_needed(self):
        """High-confidence signal in volatile regime: regime gate caps it."""
        signal = MockSignal("BTC-USD", "BUY", 0.9, "momentum")
        result = self.engine.apply_modifiers(
            signal=signal,
            market_data={"spread": 0.0},
            regime="volatile",
        )
        self.assertEqual(result.modified_confidence, 0.4)
        self.assertIn("regime_gate", result.modifiers_applied)

    # ------------------------------------------------------------------
    # Regime detection
    # ------------------------------------------------------------------

    def test_detect_regime_volatile(self):
        self.assertEqual(detect_regime({"change_pct": 8.0}), "volatile")
        self.assertEqual(detect_regime({"change_pct": -6.5}), "volatile")

    def test_detect_regime_trending(self):
        self.assertEqual(detect_regime({"change_pct": 3.0}), "trending")
        self.assertEqual(detect_regime({"change_pct": -2.5}), "trending")

    def test_detect_regime_quiet(self):
        self.assertEqual(detect_regime({"change_pct": 0.3}), "quiet")

    def test_detect_regime_neutral(self):
        self.assertEqual(detect_regime({"change_pct": 1.0}), "neutral")

    # ------------------------------------------------------------------
    # Global consensus
    # ------------------------------------------------------------------

    def test_consensus_high(self):
        sigs = [
            MockSignal("BTC-USD", "BUY", 0.5, "mom"),
            MockSignal("ETH-USD", "BUY", 0.6, "mom"),
            MockSignal("SOL-USD", "SELL", 0.4, "mom"),
        ]
        self.assertAlmostEqual(compute_global_consensus(sigs), 2 / 3)

    def test_consensus_empty(self):
        self.assertEqual(compute_global_consensus([]), 0.0)

    def test_consensus_unanimous(self):
        sigs = [
            MockSignal("BTC-USD", "SELL", 0.5, "mom"),
            MockSignal("ETH-USD", "SELL", 0.6, "mom"),
        ]
        self.assertEqual(compute_global_consensus(sigs), 1.0)

    # ------------------------------------------------------------------
    # Cross-correlation penalty
    # ------------------------------------------------------------------

    def test_cross_correlation_buy_penalty_when_leaders_dumping(self):
        """BUY signal when BTC dumped -> penalty."""
        signal = MockSignal("SOL-USD", "BUY", 0.8, "momentum")
        result = self.engine.apply_modifiers(
            signal=signal,
            market_data={
                "spread": 0.0,
                "BTC-USD": {"change_pct": -2.0},
                "ETH-USD": {"change_pct": -1.5},
            },
            regime="neutral",
            market_leaders=["BTC-USD", "ETH-USD"],
        )
        # BTC change is -2.0% which is < -1.0%, so cross_correlation should fire
        self.assertIn("cross_correlation", result.modifiers_applied)

    def test_cross_correlation_no_penalty_when_leaders_flat(self):
        """No penalty when leaders are flat."""
        signal = MockSignal("SOL-USD", "BUY", 0.8, "momentum")
        result = self.engine.apply_modifiers(
            signal=signal,
            market_data={
                "spread": 0.0,
                "BTC-USD": {"change_pct": 0.1},
            },
            regime="neutral",
            market_leaders=["BTC-USD"],
        )
        self.assertNotIn("cross_correlation", result.modifiers_applied)

    # ------------------------------------------------------------------
    # Sequential modifier compounding
    # ------------------------------------------------------------------

    def test_holistic_score_is_compounded(self):
        """Multiple modifiers compound: start at 0.8, end reduced."""
        signal = MockSignal("FIL-USD", "BUY", 0.8, "momentum")
        result = self.engine.apply_modifiers(
            signal=signal,
            market_data={"spread": 0.005, "volume_24h": 500_000},
            regime="volatile",
            market_leaders=["BTC-USD", "ETH-USD"],
            sentiment_score=0.0,
            global_consensus=0.5,
        )
        # FIL-USD tier 3, spread 50bps, volatile cap 0.4
        self.assertLessEqual(result.modified_confidence, 0.4)
        self.assertIn("spread_adjustment", result.modifiers_applied)
        self.assertIn("regime_gate", result.modifiers_applied)

    def test_consecutive_signal_boost(self):
        """Same action twice in a row gets a boost."""
        signal = MockSignal("BTC-USD", "BUY", 0.5, "momentum")
        r1 = self.engine.apply_modifiers(
            signal=signal,
            market_data={"spread": 0.0},
        )
        prev_conf = r1.modified_confidence
        r2 = self.engine.apply_modifiers(
            signal=signal,
            market_data={"spread": 0.0},
        )
        self.assertIn("consecutive_confirmation", r2.modifiers_applied)
        self.assertGreaterEqual(r2.modified_confidence, prev_conf)

    # ------------------------------------------------------------------
    # Signal processor integration (MockSignal -> ConfidenceEngine)
    # ------------------------------------------------------------------

    def test_signal_processor_wire_format(self):
        """Validate that the Signal dataclass used in multi_strategy_paper_trading
        works with ConfidenceEngine.apply_modifiers (symbol, action, strength, strategy)."""
        from multi_strategy_paper_trading import Signal
        sig = Signal(symbol="BTC-USD", action="BUY", strength=0.7, reason="test", strategy="momentum")
        result = self.engine.apply_modifiers(
            signal=sig,
            market_data={"spread": 0.001},
            regime="trending",
            market_leaders=["BTC-USD"],
            sentiment_score=0.5,
            global_consensus=0.8,
        )
        self.assertIsInstance(result.modified_confidence, float)
        self.assertGreater(len(result.modifiers_applied), 0)

    def test_final_confidence_in_range(self):
        """All modifier combinations must produce a [0, 1] result."""
        from multi_strategy_paper_trading import Signal
        cases = [
            (Signal("BTC-USD", "BUY", 0.9, "test", "mom"), {"spread": 0.0}, "trending", 0.9),
            (Signal("BTC-USD", "SELL", 0.3, "test", "mom"), {"spread": 0.02}, "volatile", -0.5),
            (Signal("GARBAGE-USD", "BUY", 0.7, "test", "mom"), {"spread": 0.01}, "quiet", 0.0),
            (Signal("ETH-USD", "BUY", 0.5, "test", "rv"), {"spread": 0.001}, "neutral", 0.6),
        ]
        for sig, md, regime, consensus in cases:
            result = self.engine.apply_modifiers(
                signal=sig, market_data=md, regime=regime,
                market_leaders=["BTC-USD"], sentiment_score=0.0,
                global_consensus=consensus,
            )
            self.assertGreaterEqual(result.modified_confidence, 0.0,
                                    f"{sig.symbol} {sig.action} fell below 0")
            self.assertLessEqual(result.modified_confidence, 1.0,
                                 f"{sig.symbol} {sig.action} exceeded 1")


# -----------------------------------------------------------------------
# Opportunity cost scoring & allocation
# -----------------------------------------------------------------------

class TestOpportunityCost(unittest.TestCase):
    """Test the cross-market opportunity cost ranking."""

    def setUp(self):
        self.engine = ConfidenceEngine()

    def _make_signal(self, symbol: str, action: str = "BUY", strength: float = 0.7, strategy: str = "mom"):
        from multi_strategy_paper_trading import Signal
        return Signal(symbol=symbol, action=action, strength=strength,
                      reason="test", strategy=strategy)

    def test_liquid_scores_higher_than_illiquid(self):
        """High volume, low spread should dominate low volume, wide spread."""
        from multi_strategy_paper_trading import score_opportunity
        liquid_sig = self._make_signal("BTC-USD")
        liquid_data = {"spread": 0.0005, "volume": 500_000_000}
        liquid_result = self.engine.apply_modifiers(liquid_sig, liquid_data)

        junk_sig = self._make_signal("JUNK-USD")
        junk_data = {"spread": 0.02, "volume": 10_000}
        junk_result = self.engine.apply_modifiers(junk_sig, junk_data)

        liquid_score = score_opportunity(liquid_sig, liquid_data, liquid_result)
        junk_score = score_opportunity(junk_sig, junk_data, junk_result)
        self.assertGreater(liquid_score, junk_score)

    def test_high_confidence_beats_low_confidence(self):
        """Same market, stronger signal beats weaker."""
        from multi_strategy_paper_trading import score_opportunity
        md = {"spread": 0.001, "volume": 100_000_000}
        strong = self._make_signal("BTC-USD", strength=0.9)
        weak = self._make_signal("BTC-USD", strength=0.2)

        score_strong = score_opportunity(strong, md,
            self.engine.apply_modifiers(strong, md))
        score_weak = score_opportunity(weak, md,
            self.engine.apply_modifiers(weak, md))
        self.assertGreater(score_strong, score_weak)

    def test_allocate_capital_top_signal_gets_most(self):
        """Best signal should receive largest allocation."""
        from multi_strategy_paper_trading import (
            score_opportunity, allocate_capital, ScoredSignal,
        )
        md_best = {"spread": 0.0005, "volume": 1_000_000_000}
        md_worst = {"spread": 0.03, "volume": 1_000}

        best_sig = self._make_signal("BTC-USD", strength=0.9)
        worst_sig = self._make_signal("JUNK-USD", strength=0.3)

        best_result = self.engine.apply_modifiers(best_sig, md_best)
        worst_result = self.engine.apply_modifiers(worst_sig, md_worst)

        scored = [
            ScoredSignal(best_sig, md_best, best_result,
                         score_opportunity(best_sig, md_best, best_result)),
            ScoredSignal(worst_sig, md_worst, worst_result,
                         score_opportunity(worst_sig, md_worst, worst_result)),
        ]
        allocs = allocate_capital(scored, 10000.0, max_positions=2)
        self.assertEqual(len(allocs), 2)
        self.assertEqual(allocs[0]["signal"].symbol, "BTC-USD")
        self.assertGreater(allocs[0]["allocated_usd"], allocs[1]["allocated_usd"])

    def test_allocate_capital_respects_max_risk(self):
        """No single position exceeds max_risk_per_position."""
        from multi_strategy_paper_trading import (
            score_opportunity, allocate_capital, ScoredSignal,
        )
        md = {"spread": 0.001, "volume": 100_000_000}
        sigs = [self._make_signal(f"COIN-{i}", strength=0.8) for i in range(5)]
        scored = []
        for s in sigs:
            r = self.engine.apply_modifiers(s, md)
            scored.append(ScoredSignal(s, md, r, score_opportunity(s, md, r)))
        allocs = allocate_capital(scored, 10000.0, max_positions=5, max_risk_per_position=0.20)
        for a in allocs:
            self.assertLessEqual(a["allocated_usd"], 2000.0,  # 20% of 10K
                                 f"{a['signal'].symbol} exceeds max risk")

    def test_assign_liquidity_tiers_distribution(self):
        """assign_liquidity_tiers should produce tiers 1-5 with more 1s for top volume."""
        from multi_strategy_paper_trading import assign_liquidity_tiers
        data = {
            "BTC-USD": {"volume": 1_000_000_000},
            "ETH-USD": {"volume": 500_000_000},
            "SOL-USD": {"volume": 100_000_000},
            "ADA-USD": {"volume": 50_000_000},
            "DOGE-USD": {"volume": 10_000_000},
            "JUNK-USD": {"volume": 1_000},
            "TRASH-USD": {"volume": 500},
            "SKIP-USD": {"volume": 100},
        }
        tiers = assign_liquidity_tiers(data)
        self.assertEqual(tiers["BTC-USD"], 1)
        self.assertGreaterEqual(tiers["JUNK-USD"], 4)

    def test_compute_global_consensus_all_buy(self):
        """100% buy consensus = 1.0."""
        from multi_strategy_paper_trading import compute_global_consensus, Signal
        sigs = [
            Signal("A", "BUY", 0.5, "t", "s"),
            Signal("B", "BUY", 0.6, "t", "s"),
        ]
        self.assertEqual(compute_global_consensus(sigs), 1.0)

    def test_compute_global_consensus_split(self):
        """50/50 split = 0.5."""
        from multi_strategy_paper_trading import compute_global_consensus, Signal
        sigs = [
            Signal("A", "BUY", 0.5, "t", "s"),
            Signal("B", "SELL", 0.5, "t", "s"),
        ]
        self.assertEqual(compute_global_consensus(sigs), 0.5)


class TestFeeTierIntegration(unittest.TestCase):
    """Fee tier progression, volume optimization, scalping strategy."""

    def setUp(self):
        from trading_system.signal_confidence import ConfidenceEngine
        self.engine = ConfidenceEngine()

    def test_fee_tier_manager_starts_at_bottom(self):
        """Initial $0 volume -> tier 0 ($0+, 0.6% maker)."""
        from multi_strategy_paper_trading import FeeTierManager
        fm = FeeTierManager(initial_volume_30d=0)
        self.assertAlmostEqual(fm.maker_rate(), 0.006)
        self.assertAlmostEqual(fm.taker_rate(), 0.012)

    def test_fee_tier_progresses_with_volume(self):
        """$1,100 volume -> tier 1 ($1k+, 0.35% maker)."""
        from multi_strategy_paper_trading import FeeTierManager
        fm = FeeTierManager(initial_volume_30d=500)
        fm.record_trade(600)
        self.assertAlmostEqual(fm.maker_rate(), 0.0035)

    def test_fee_tier_volume_to_next(self):
        """volume_to_next_tier reports remaining to next tier."""
        from multi_strategy_paper_trading import FeeTierManager
        fm = FeeTierManager(initial_volume_30d=500)
        self.assertEqual(fm.volume_to_next_tier(), 500.0)  # $500 from $1k tier
        fm.record_trade(400)
        self.assertEqual(fm.volume_to_next_tier(), 100.0)  # $100 from $1k tier
        fm.record_trade(200)
        # now at $1,100, next tier is $10k
        self.assertEqual(fm.volume_to_next_tier(), 8900.0)

    def test_fee_cost_calculated_correctly(self):
        """$1000 trade at entry tier costs $12 taker or $6 maker."""
        from multi_strategy_paper_trading import FeeTierManager
        fm = FeeTierManager()
        self.assertAlmostEqual(fm.fee_cost(1000, is_maker=False), 12.0)
        self.assertAlmostEqual(fm.fee_cost(1000, is_maker=True), 6.0)

    def test_volume_optimizer_boost_near_tier(self):
        """Trade near next tier threshold gets up to 40% boost."""
        from multi_strategy_paper_trading import FeeTierManager, VolumeOptimizer
        fm = FeeTierManager(initial_volume_30d=9_500)  # $500 from $10k tier
        vo = VolumeOptimizer(fm)
        boost = vo.volume_boost(500)
        self.assertAlmostEqual(boost, 1.4)  # full 40% boost when trade covers gap
        boost_small = vo.volume_boost(250)
        self.assertAlmostEqual(boost_small, 1.2)  # partial boost

    def test_volume_optimizer_no_boost_at_top_tier(self):
        """No boost when already at top fee tier."""
        from multi_strategy_paper_trading import FeeTierManager, VolumeOptimizer
        fm = FeeTierManager(initial_volume_30d=50_000_000)
        vo = VolumeOptimizer(fm)
        self.assertEqual(vo.volume_boost(1000), 1.0)

    def test_volume_optimizer_in_opportunity_score(self):
        """score_opportunity with VolumeOptimizer returns higher score for volume-boosting trades."""
        from multi_strategy_paper_trading import (
            FeeTierManager, VolumeOptimizer, score_opportunity, Signal,
        )
        engine = self.engine
        fm = FeeTierManager(initial_volume_30d=9_500)
        vo = VolumeOptimizer(fm)
        sig = Signal("BTC-USD", "BUY", 0.5, "test", "momentum")
        md = {"spread": 0.001, "volume": 100_000_000}
        result = engine.apply_modifiers(sig, md)

        score_without = score_opportunity(sig, md, result)
        score_with = score_opportunity(sig, md, result, volume_optimizer=vo)
        self.assertGreater(score_with, score_without)

    def test_scalping_strategy_fires_on_micro_dip(self):
        """Scalper signals BUY on small dips that cover spread."""
        from multi_strategy_paper_trading import ScalpingStrategy, Signal
        ss = ScalpingStrategy()
        signal = ss.generate_signal("BTC-USD",
            {"price_percentage_change_24h": -0.8, "spread": 0.001}, [])
        self.assertIsNotNone(signal)
        self.assertEqual(signal.action, "BUY")
        self.assertEqual(signal.strategy, "scalper")

    def test_scalping_strategy_no_signal_when_spread_too_wide(self):
        """Scalper skips when spread cost exceeds expected move."""
        from multi_strategy_paper_trading import ScalpingStrategy
        ss = ScalpingStrategy()
        signal = ss.generate_signal("JUNK-USD",
            {"price_percentage_change_24h": -0.3, "spread": 0.02}, [])
        self.assertIsNone(signal)

    def test_scalping_strategy_sell_on_micro_rise(self):
        """Scalper signals SELL on small rises."""
        from multi_strategy_paper_trading import ScalpingStrategy
        ss = ScalpingStrategy()
        signal = ss.generate_signal("BTC-USD",
            {"price_percentage_change_24h": 0.9, "spread": 0.001}, [])
        self.assertIsNotNone(signal)
        self.assertEqual(signal.action, "SELL")

    def test_tier_display_contains_tier_info(self):
        """tier_display includes maker/taker rates and next tier info."""
        from multi_strategy_paper_trading import FeeTierManager
        fm = FeeTierManager(initial_volume_30d=500)
        display = fm.tier_display()
        self.assertIn("maker=", display)
        self.assertIn("taker=", display)
        self.assertIn("to next tier", display)

    def test_system_tracks_volume_on_trades(self):
        """PaperTradingSystem volume_tracker records buy/sell volume."""
        from multi_strategy_paper_trading import PaperTradingSystem, Signal
        system = PaperTradingSystem(10000.0)
        sig = Signal("BTC-USD", "BUY", 0.5, "test", "momentum")
        system.execute_buy(sig, 50.0, 1000.0)  # price=50, qty=20
        self.assertGreater(system.volume_tracker.rolling_30d_volume, 0)
        self.assertEqual(len(system.trades), 1)
        self.assertEqual(system.trades[0]["action"], "BUY")


if __name__ == "__main__":
    unittest.main()
