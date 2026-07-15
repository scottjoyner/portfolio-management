import unittest

import trading_system.backtest.models as m


class TestBacktestModels(unittest.TestCase):
    def test_models_imported(self):
        # ORM model class bodies execute at import (mapped_column calls),
        # so importing the module exercises every statement.
        for name in [
            "Base",
            "BacktestResult",
            "EquityCurvePoint",
            "BacktestTrade",
            "PerformanceSignal",
            "StrategyCertification",
            "BacktestConfiguration",
            "StrategyComparison",
        ]:
            self.assertTrue(hasattr(m, name), name)


if __name__ == "__main__":
    unittest.main()
