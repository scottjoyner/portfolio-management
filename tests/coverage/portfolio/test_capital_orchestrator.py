import unittest

from portfolio.allocator.capital_orchestrator import (
    AllocationDecision,
    StrategyAllocationInput,
    _clip,
    allocate_capital,
)


def strat(**kw):
    base = dict(
        strategy_id="s",
        sleeve="growth",
        risk_tier="TIER_1_LOW_RISK",
        requested_fraction=0.5,
        quality_score=0.8,
        drawdown=0.0,
        live_safe=True,
        paper_ready=True,
    )
    base.update(kw)
    return StrategyAllocationInput(**base)


class TestClip(unittest.TestCase):
    def test_clip(self):
        self.assertEqual(_clip(5, 0, 1), 1)
        self.assertEqual(_clip(-1, 0, 1), 0)
        self.assertEqual(_clip(0.5, 0, 1), 0.5)


class TestAllocateCapital(unittest.TestCase):
    def test_negative_locked_reserve_raises(self):
        with self.assertRaises(ValueError):
            allocate_capital([strat()], locked_reserve_fraction=-0.1,
                             cash_buffer_fraction=0.0, hedge_reserve_fraction=0.0)

    def test_negative_cash_buffer_raises(self):
        with self.assertRaises(ValueError):
            allocate_capital([strat()], locked_reserve_fraction=0.0,
                             cash_buffer_fraction=-0.1, hedge_reserve_fraction=0.0)

    def test_negative_hedge_raises(self):
        with self.assertRaises(ValueError):
            allocate_capital([strat()], locked_reserve_fraction=0.0,
                             cash_buffer_fraction=0.0, hedge_reserve_fraction=-0.1)

    def test_deployable_not_positive_raises(self):
        with self.assertRaises(ValueError):
            allocate_capital([strat()], locked_reserve_fraction=0.5,
                             cash_buffer_fraction=0.5, hedge_reserve_fraction=0.0)

    def test_allocated_under_tier_cap(self):
        dec = allocate_capital([strat()], locked_reserve_fraction=0.0,
                               cash_buffer_fraction=0.0, hedge_reserve_fraction=0.0)
        self.assertEqual(len(dec), 1)
        self.assertEqual(dec[0].reason, "allocated under tier cap")
        self.assertAlmostEqual(dec[0].approved_fraction, 0.30 * 0.9)

    def test_paper_readiness_throttle(self):
        dec = allocate_capital(
            [strat(paper_ready=False, risk_tier="TIER_2_MODERATE_RISK")],
            locked_reserve_fraction=0.0, cash_buffer_fraction=0.0,
            hedge_reserve_fraction=0.0)
        self.assertEqual(dec[0].reason, "paper-readiness throttle applied")

    def test_live_safety_throttle(self):
        dec = allocate_capital(
            [strat(live_safe=False, paper_ready=True, risk_tier="TIER_2_MODERATE_RISK")],
            locked_reserve_fraction=0.0, cash_buffer_fraction=0.0,
            hedge_reserve_fraction=0.0)
        self.assertEqual(dec[0].reason, "live-safety throttle applied")

    def test_expert_hard_cap(self):
        dec = allocate_capital(
            [strat(risk_tier="TIER_4_EXPERT_HIGH_RISK", requested_fraction=0.5,
                   quality_score=1.0, drawdown=0.0)],
            locked_reserve_fraction=0.0, cash_buffer_fraction=0.0,
            hedge_reserve_fraction=0.0)
        self.assertEqual(dec[0].reason, "expert/research hard cap applied")
        self.assertAlmostEqual(dec[0].approved_fraction, 0.02)

    def test_research_hard_cap(self):
        dec = allocate_capital(
            [strat(risk_tier="TIER_5_RESEARCH_ONLY", requested_fraction=0.5,
                   quality_score=1.0, drawdown=0.0)],
            locked_reserve_fraction=0.0, cash_buffer_fraction=0.0,
            hedge_reserve_fraction=0.0)
        self.assertAlmostEqual(dec[0].approved_fraction, 0.01)

    def test_unknown_tier_zero(self):
        dec = allocate_capital(
            [strat(risk_tier="NO_SUCH_TIER")],
            locked_reserve_fraction=0.0, cash_buffer_fraction=0.0,
            hedge_reserve_fraction=0.0)
        self.assertEqual(dec[0].approved_fraction, 0.0)

    def test_granted_total_zero_returns_early(self):
        dec = allocate_capital(
            [strat(risk_tier="NO_SUCH_TIER"), strat(risk_tier="NO_SUCH_TIER")],
            locked_reserve_fraction=0.0, cash_buffer_fraction=0.0,
            hedge_reserve_fraction=0.0)
        self.assertEqual(len(dec), 2)
        self.assertTrue(all(d.approved_fraction == 0.0 for d in dec))

    def test_scaling_to_deployable(self):
        dec = allocate_capital(
            [strat(strategy_id="a", risk_tier="TIER_0_CAPITAL_PRESERVATION",
                   requested_fraction=1.0, quality_score=1.0, drawdown=0.0),
             strat(strategy_id="b", risk_tier="TIER_0_CAPITAL_PRESERVATION",
                   requested_fraction=1.0, quality_score=1.0, drawdown=0.0)],
            locked_reserve_fraction=0.0, cash_buffer_fraction=0.0,
             hedge_reserve_fraction=0.5)
        total = sum(d.approved_fraction for d in dec)
        self.assertAlmostEqual(total, 0.5)

    def test_quality_scale_extremes(self):
        d1 = allocate_capital(
            [strat(quality_score=-1.0, risk_tier="TIER_1_LOW_RISK")],
            locked_reserve_fraction=0.0, cash_buffer_fraction=0.0,
            hedge_reserve_fraction=0.0)[0]
        d2 = allocate_capital(
            [strat(quality_score=2.0, risk_tier="TIER_1_LOW_RISK")],
            locked_reserve_fraction=0.0, cash_buffer_fraction=0.0,
            hedge_reserve_fraction=0.0)[0]
        self.assertNotEqual(d1.approved_fraction, d2.approved_fraction)

    def test_drawdown_zero_out(self):
        dec = allocate_capital(
            [strat(drawdown=2.0, risk_tier="TIER_1_LOW_RISK")],
            locked_reserve_fraction=0.0, cash_buffer_fraction=0.0,
            hedge_reserve_fraction=0.0)
        self.assertEqual(dec[0].approved_fraction, 0.0)

    def test_approved_fraction_nonnegative(self):
        dec = allocate_capital(
            [strat(requested_fraction=-5.0, risk_tier="TIER_1_LOW_RISK")],
            locked_reserve_fraction=0.0, cash_buffer_fraction=0.0,
            hedge_reserve_fraction=0.0)
        self.assertGreaterEqual(dec[0].approved_fraction, 0.0)

    def test_decision_dataclass(self):
        d = AllocationDecision(strategy_id="x", approved_fraction=0.1, reason="r")
        self.assertEqual(d.strategy_id, "x")


if __name__ == "__main__":
    unittest.main()
