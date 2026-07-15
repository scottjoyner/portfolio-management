import unittest
from unittest import mock

from coinbase.src import exec_algo
from coinbase.src.exec_algo import (
    TWAPAlgo, VWAPAlgo, IcebergAlgo, ExecutionSlice, ExecutionResult,
)
from coinbase.src.protocols import Direction


def fake_prices(value=100.0):
    return value


class TestExecutionResult(unittest.TestCase):
    def test_vwap(self):
        r = ExecutionResult(slices=[
            ExecutionSlice(timestamp=0, price=100.0, size=1.0),
            ExecutionSlice(timestamp=0, price=200.0, size=1.0),
        ])
        r.total_size = 2.0
        self.assertEqual(r.vwap, 150.0)

    def test_vwap_empty(self):
        r = ExecutionResult()
        self.assertEqual(r.vwap, 0.0)

    def test_implementation_shortfall(self):
        r = ExecutionResult(slices=[
            ExecutionSlice(timestamp=0, price=100.0, size=1.0),
            ExecutionSlice(timestamp=0, price=101.0, size=1.0),
        ])
        r.total_size = 2.0
        self.assertAlmostEqual(r.implementation_shortfall, 0.5)

    def test_implementation_shortfall_empty(self):
        self.assertEqual(ExecutionResult().implementation_shortfall, 0.0)


class TestTWAP(unittest.TestCase):
    def test_execute_defaults(self):
        with mock.patch.object(exec_algo.time, "sleep"):
            algo = TWAPAlgo(total_size=10.0, duration_secs=0, n_slices=3)
            res = algo.execute(Direction.LONG, fake_prices)
        self.assertEqual(len(res.slices), 3)
        self.assertAlmostEqual(res.total_size, 10.0)
        self.assertGreater(res.completion_pct, 0)

    def test_execute_with_bid_ask_volume(self):
        with mock.patch.object(exec_algo.time, "sleep"):
            algo = TWAPAlgo(total_size=4.0, duration_secs=0, n_slices=2)
            res = algo.execute(Direction.SHORT, lambda: 100.0,
                               get_bid_ask=lambda: (99.0, 101.0),
                               get_volume=lambda: 500.0)
        self.assertEqual(len(res.slices), 2)


class TestVWAP(unittest.TestCase):
    def test_execute_skips_zero_weight(self):
        with mock.patch.object(exec_algo.time, "sleep"):
            algo = VWAPAlgo(total_size=4.0, expected_volume_profile=[1.0, 0.0, 1.0])
            res = algo.execute(Direction.LONG, fake_prices)
        # zero-weight slice skipped -> 2 slices
        self.assertEqual(len(res.slices), 2)

    def test_execute_all(self):
        with mock.patch.object(exec_algo.time, "sleep"):
            algo = VWAPAlgo(total_size=3.0, expected_volume_profile=[1.0, 1.0, 1.0])
            res = algo.execute(Direction.LONG, fake_prices)
        self.assertEqual(len(res.slices), 3)


class TestIceberg(unittest.TestCase):
    def test_execute_long_with_callback(self):
        with mock.patch.object(exec_algo.time, "sleep"):
            algo = IcebergAlgo(total_size=10.0, visible_size=3.0)
            calls = []
            res = algo.execute(Direction.LONG, fake_prices,
                               get_bid_ask=lambda: (99.0, 101.0),
                               on_fill=lambda f: calls.append(f))
        self.assertEqual(len(res.slices), 4)  # 3+3+3+1
        self.assertEqual(len(calls), 4)
        self.assertGreater(res.completion_pct, 0)

    def test_execute_short(self):
        with mock.patch.object(exec_algo.time, "sleep"):
            algo = IcebergAlgo(total_size=6.0, visible_size=3.0)
            res = algo.execute(Direction.SHORT, fake_prices,
                               get_bid_ask=lambda: (99.0, 101.0))
        self.assertEqual(len(res.slices), 2)


if __name__ == "__main__":
    unittest.main()
