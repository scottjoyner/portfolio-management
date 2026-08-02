import unittest

from trading_system.benchmarks.maker_path_benchmark import run


class TestMakerPathBenchmark(unittest.TestCase):
    def test_run_default(self):
        result = run(iterations=10)
        self.assertEqual(result["iterations"], 10)
        self.assertGreaterEqual(result["quotes_generated"], 10)
        self.assertEqual(result["quotes_generated"] % 10, 0)
        self.assertGreaterEqual(result["elapsed_s"], 0.0)
        self.assertGreater(result["ops_per_sec"], 0.0)

    def test_run_large(self):
        result = run(iterations=1000)
        self.assertGreaterEqual(result["quotes_generated"], 1000)
        self.assertEqual(result["quotes_generated"] % 1000, 0)


if __name__ == "__main__":
    unittest.main()
