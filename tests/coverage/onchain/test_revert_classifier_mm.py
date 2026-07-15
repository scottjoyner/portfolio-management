import unittest

from onchain.simulation.revert_classifier_mm import classify_revert


class TestRevertClassifier(unittest.TestCase):
    def test_insufficient(self):
        self.assertEqual(classify_revert("insufficient funds"), "INSUFFICIENT_FUNDS")

    def test_slippage(self):
        self.assertEqual(classify_revert("Slippage exceeded"), "SLIPPAGE")

    def test_deadline(self):
        self.assertEqual(classify_revert("deadline expired"), "DEADLINE")

    def test_unknown(self):
        self.assertEqual(classify_revert("weird error"), "UNKNOWN")


if __name__ == "__main__":
    unittest.main()
