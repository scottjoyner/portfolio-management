import unittest
from decimal import Decimal

from onchain.dex.clmm.range_selector import (
    widen_for_volatility,
    widen_for_gas,
    skew_range_for_inventory,
    retreat_range_under_stress,
    score_range_candidate,
    optimize_range_candidates,
    select_range,
)
from onchain.dex.clmm.schemas import RangeCandidate, RangeSelectionInput


class TestRangeSelector(unittest.TestCase):
    def test_widen_vol(self):
        self.assertEqual(widen_for_volatility(Decimal("100"), Decimal("0.5")), Decimal("150"))

    def test_widen_gas(self):
        self.assertEqual(widen_for_gas(Decimal("100"), Decimal("0.2")), Decimal("110"))

    def test_skew(self):
        lo, hi = skew_range_for_inventory(100, 200, Decimal("0.5"), 10)
        self.assertEqual(lo, 105)
        self.assertEqual(hi, 205)

    def test_retreat(self):
        lo, hi = retreat_range_under_stress(1000, 10, Decimal("1"))
        # width = (200 + 1*500)*10 = 7000; 1000 +- 7000
        self.assertEqual(lo, 1000 - 7000)
        self.assertEqual(hi, 1000 + 7000)

    def test_score_and_optimize(self):
        c1 = RangeCandidate(lower_tick=0, upper_tick=10, width_bps=Decimal("100"),
                            expected_utilization=Decimal("0.5"), expected_fee_density=Decimal("1"),
                            expected_rebalance_pressure=Decimal("0"), expected_il_pressure=Decimal("0"),
                            expected_hedge_drag=Decimal("0"), confidence_score=Decimal("0.5"))
        c2 = RangeCandidate(lower_tick=0, upper_tick=10, width_bps=Decimal("100"),
                            expected_utilization=Decimal("0.9"), expected_fee_density=Decimal("2"),
                            expected_rebalance_pressure=Decimal("0"), expected_il_pressure=Decimal("0"),
                            expected_hedge_drag=Decimal("0"), confidence_score=Decimal("0.9"))
        self.assertGreater(score_range_candidate(c2), score_range_candidate(c1))
        self.assertEqual(optimize_range_candidates([c1, c2]), c2)

    def test_select_range_calm(self):
        data = RangeSelectionInput(mark_price=Decimal("2000"), tick_spacing=10,
                                   realized_vol=Decimal("0.1"), short_vol=Decimal("0.1"),
                                   market_regime="NORMAL", inventory_skew=Decimal("0"),
                                   gas_regime_score=Decimal("0.1"))
        cand = select_range(data)
        self.assertIsInstance(cand, RangeCandidate)
        self.assertGreater(cand.upper_tick, cand.lower_tick)

    def test_select_range_crisis(self):
        data = RangeSelectionInput(mark_price=Decimal("2000"), tick_spacing=10,
                                   realized_vol=Decimal("0.5"), short_vol=Decimal("0.5"),
                                   market_regime="CRISIS", inventory_skew=Decimal("0"),
                                   gas_regime_score=Decimal("0.1"))
        cand = select_range(data)
        self.assertEqual(cand.confidence_score, Decimal("0.6"))


if __name__ == "__main__":
    unittest.main()
