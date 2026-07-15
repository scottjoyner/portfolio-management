import unittest

from analytics.metrics.live_transfer import (
    BacktestRealismScorer,
    LiveTransferAssessment,
    RealismPenaltyBreakdown,
    SimulationAssumptions,
)


class TestLiveTransfer(unittest.TestCase):
    def _assumptions(self, **kw):
        base = dict(
            latency_ms=10.0,
            queue_fill_probability=0.9,
            stale_quote_decay=0.1,
            maker_ratio=0.5,
            cancel_ratio=0.9,
            rejection_rate=0.05,
            outage_rate=0.05,
        )
        base.update(kw)
        return SimulationAssumptions(**base)

    def test_realism_penalty(self):
        a = self._assumptions()
        b = BacktestRealismScorer.realism_penalty(a)
        self.assertIsInstance(b, RealismPenaltyBreakdown)
        # all penalties should be in [0, cap]
        self.assertGreaterEqual(b.latency_penalty, 0.0)
        self.assertLessEqual(b.latency_penalty, 0.2)

    def test_total_property_clamped_high(self):
        b = RealismPenaltyBreakdown(
            latency_penalty=0.2, fill_optimism_penalty=0.25, stale_quote_penalty=0.2,
            turnover_penalty=0.15, rejection_penalty=0.1, outage_penalty=0.1,
        )
        self.assertAlmostEqual(b.total, 1.0)

    def test_total_property_clamped_low(self):
        b = RealismPenaltyBreakdown(
            latency_penalty=-0.5, fill_optimism_penalty=-0.5, stale_quote_penalty=-0.5,
            turnover_penalty=-0.5, rejection_penalty=-0.5, outage_penalty=-0.5,
        )
        self.assertAlmostEqual(b.total, 0.0)

    def test_total_property_in_range(self):
        b = RealismPenaltyBreakdown(
            latency_penalty=0.1, fill_optimism_penalty=0.1, stale_quote_penalty=0.1,
            turnover_penalty=0.05, rejection_penalty=0.02, outage_penalty=0.02,
        )
        self.assertAlmostEqual(b.total, 0.39)

    def test_assess_strategy_sub(self):
        a = self._assumptions()
        res = BacktestRealismScorer.assess_strategy("s1", 1.0, 2.0, a, "sub-minute")
        self.assertIsInstance(res, LiveTransferAssessment)
        self.assertGreaterEqual(res.fragility_score, 0.0)
        self.assertLessEqual(res.fragility_score, 1.0)

    def test_assess_strategy_intraday(self):
        a = self._assumptions()
        res = BacktestRealismScorer.assess_strategy("s2", 1.0, 2.0, a, "intraday")
        self.assertIsInstance(res, LiveTransferAssessment)

    def test_assess_strategy_other(self):
        a = self._assumptions()
        res = BacktestRealismScorer.assess_strategy("s3", 1.0, 2.0, a, "daily")
        self.assertIsInstance(res, LiveTransferAssessment)

    def test_assess_strategy_low_sharpe(self):
        a = self._assumptions()
        res = BacktestRealismScorer.assess_strategy("s4", 1.0, 0.0, a, "daily")
        self.assertIsInstance(res, LiveTransferAssessment)


if __name__ == "__main__":
    unittest.main()
