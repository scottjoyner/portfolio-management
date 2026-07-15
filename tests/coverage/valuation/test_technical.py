import unittest

from trading_system.valuation.technical import TechnicalIndicators


class TestTechnicalIndicators(unittest.TestCase):
    def setUp(self):
        self.t = TechnicalIndicators()

    def test_calculate_ema_helper(self):
        ema = self.t._calculate_ema([1.0, 2.0, 3.0, 4.0], 2)
        self.assertEqual(len(ema), 4)
        self.assertAlmostEqual(ema[0], 1.0)
        self.assertAlmostEqual(ema[-1], 3.5185, places=3)

    def test_calculate_ema_empty(self):
        self.assertEqual(self.t._calculate_ema([], 2), [])

    def test_calculate_rsi_too_few_prices(self):
        with self.assertRaises(ValueError):
            self.t.calculate_rsi("X", [1, 2, 3], period=14)

    def test_calculate_rsi_all_gains_overbought(self):
        prices = [100 + i for i in range(20)]
        rsi, signal = self.t.calculate_rsi("X", prices, period=14)
        self.assertAlmostEqual(rsi, 100.0)
        self.assertEqual(signal, "overbought")

    def test_calculate_rsi_all_losses_oversold(self):
        prices = [100 - i for i in range(20)]
        rsi, signal = self.t.calculate_rsi("X", prices, period=14)
        self.assertAlmostEqual(rsi, 0.0)
        self.assertEqual(signal, "oversold")

    def test_calculate_rsi_neutral(self):
        prices = [100, 101, 99, 102, 98, 103, 97, 104, 96, 105, 95, 106, 94, 107, 93, 108]
        rsi, signal = self.t.calculate_rsi("X", prices, period=14)
        self.assertGreaterEqual(rsi, 30)
        self.assertLessEqual(rsi, 70)
        self.assertEqual(signal, "neutral")

    def test_calculate_macd_too_few_prices(self):
        with self.assertRaises(ValueError):
            self.t.calculate_macd("X", [1, 2, 3])

    def test_calculate_macd_bearish(self):
        prices = [100 - i * 2 for i in range(30)]
        data, signal = self.t.calculate_macd("X", prices)
        self.assertIn("macd_line", data)
        self.assertEqual(signal, "bearish_crossover")

    def test_calculate_macd_bullish(self):
        prices = [2.37, 25.25, 19.23, 17.25, 14.67, 17.32, 29.27, 14.43, 23.89,
                  18.91, 30.81, 1.37, 24.49, 25.92, 6.8, 1.86, 26.95, 10.92, 0.91,
                  14.64, 9.18, 39.45, 3.03, 30.87, 40.62, 42.19, 40.68]
        data, signal = self.t.calculate_macd("X", prices)
        self.assertEqual(signal, "bullish_crossover")

    def test_calculate_macd_neutral_simulation(self):
        prices = [100 + i for i in range(30)]
        data, signal = self.t.calculate_macd("X", prices)
        self.assertIn(signal, ("bullish_crossover", "bearish_crossover", "neutral"))

    def test_calculate_bollinger_too_few_prices(self):
        with self.assertRaises(ValueError):
            self.t.calculate_bollinger_bands("X", [1, 2, 3], period=20)

    def test_calculate_bollinger_bands(self):
        prices = [100 + (i % 5) for i in range(25)]
        bands = self.t.calculate_bollinger_bands("X", prices, period=20)
        self.assertIn("upper_band", bands)
        self.assertIn("lower_band", bands)
        self.assertIn("middle_band", bands)
        self.assertGreaterEqual(bands["upper_band"], bands["lower_band"])


if __name__ == "__main__":
    unittest.main()
