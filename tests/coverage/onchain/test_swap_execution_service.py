import unittest
from decimal import Decimal

from onchain.dex.swap_execution.service import SwapExecution, SwapExecutor


class TestSwapExecution(unittest.TestCase):
    def test_execution_default(self):
        e = SwapExecution()
        self.assertEqual(e.status, "pending")

    def test_executor(self):
        r = SwapExecutor().execute(None, Decimal("1"), Decimal("0.9"))
        self.assertIsInstance(r, SwapExecution)
        self.assertEqual(r.tx_hash, "")


if __name__ == "__main__":
    unittest.main()
