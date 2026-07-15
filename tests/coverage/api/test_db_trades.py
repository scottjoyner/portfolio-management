import unittest
from unittest import mock

from trading_system.api.databases import trades as trades_mod


def mock_exec(session, rows):
    res = mock.MagicMock()
    res.mapped.return_value = rows
    session.execute.return_value = res


class TestTrades(unittest.IsolatedAsyncioTestCase):
    async def test_get_executed_trades(self):
        session = mock.AsyncMock()
        mock_exec(session, [{"id": 1}])
        self.assertEqual(len(await trades_mod.get_executed_trades(session)), 1)

    async def test_get_executed_trades_empty(self):
        session = mock.AsyncMock()
        mock_exec(session, [])
        self.assertEqual(await trades_mod.get_executed_trades(session), [])

    async def test_get_trades_by_strategy(self):
        session = mock.AsyncMock()
        mock_exec(session, [{"id": 2}])
        self.assertEqual(len(await trades_mod.get_trades_by_strategy(session, "s1")), 1)

    async def test_get_trades_for_period(self):
        session = mock.AsyncMock()
        mock_exec(session, [{"id": 3}])
        self.assertEqual(len(await trades_mod.get_trades_for_period(session, "2024-01-01", "2024-02-01")), 1)


if __name__ == "__main__":
    unittest.main()
