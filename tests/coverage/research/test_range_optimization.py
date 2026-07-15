import unittest

import trading_system.research.lp.range_optimization as m
from onchain.dex.clmm.range_selector import optimize_range_candidates


class TestRangeOptimization(unittest.TestCase):
    def test_re_export(self):
        self.assertIs(m.optimize_range_candidates, optimize_range_candidates)
        self.assertIn("optimize_range_candidates", m.__all__)


if __name__ == "__main__":
    unittest.main()
