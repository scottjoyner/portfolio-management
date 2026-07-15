import unittest
from unittest import mock

from trading_system.api.databases import valuation as valuation_mod


def mock_exec(session, rows):
    res = mock.MagicMock()
    res.mapped.return_value = rows
    session.execute.return_value = res


class TestValuation(unittest.IsolatedAsyncioTestCase):
    async def test_get_price_estimates(self):
        session = mock.AsyncMock()
        mock_exec(session, [{"id": 1}])
        self.assertEqual(len(await valuation_mod.get_price_estimates(session)), 1)

    async def test_get_price_estimates_empty(self):
        session = mock.AsyncMock()
        mock_exec(session, [])
        self.assertEqual(await valuation_mod.get_price_estimates(session), [])

    async def test_get_valuation_for_instrument_present(self):
        session = mock.AsyncMock()
        mock_exec(session, [("a", 1)])
        self.assertEqual(await valuation_mod.get_valuation_for_instrument(session, "BTC"), {"a": 1})

    async def test_get_valuation_for_instrument_empty(self):
        session = mock.AsyncMock()
        mock_exec(session, [])
        self.assertEqual(await valuation_mod.get_valuation_for_instrument(session, "BTC"), {})

    async def test_get_price_history(self):
        session = mock.AsyncMock()
        mock_exec(session, [{"x": 1}])
        self.assertEqual(len(await valuation_mod.get_price_history(session, "BTC", 7)), 1)


if __name__ == "__main__":
    unittest.main()
