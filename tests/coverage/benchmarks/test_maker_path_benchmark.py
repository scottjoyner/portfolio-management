import unittest

from trading_system.benchmarks.maker_path_benchmark import run


class TestMakerPathBenchmark(unittest.TestCase):
    def test_run_default(self):
        res = run(iterations=10)
        self.assertEqual(res["iterations"], 10)
        self.assertEqual(res["quotes_generated"], 10)
        self.assertGreaterEqual(res["elapsed_s"], 0.0)
        self.assertGreater(res["ops_per_sec"], 0.0)

    def test_run_large(self):
        res = run(iterations=1000)
        self.assertEqual(res["quotes_generated"], 1000)


if __name__ == "__main__":
    unittest.main()
