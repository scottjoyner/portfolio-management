import unittest
from decimal import Decimal

from onchain.strategies.amm_lp.adverse_selection_defense import adverse_selection_trigger


class TestAdverseSelectionDefense(unittest.TestCase):
    def test_trigger(self):
        self.assertTrue(adverse_selection_trigger(Decimal("0.8")))

    def test_no_trigger(self):
        self.assertFalse(adverse_selection_trigger(Decimal("0.5")))

    def test_custom_threshold(self):
        self.assertFalse(adverse_selection_trigger(Decimal("0.5"), Decimal("0.9")))


if __name__ == "__main__":
    unittest.main()
