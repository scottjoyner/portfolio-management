import unittest
from unittest import mock

from trading_system.api.databases import positions as positions_mod


def mock_exec(session, rows):
    res = mock.MagicMock()
    res.mapped.return_value = rows
    session.execute.return_value = res


class TestPositions(unittest.IsolatedAsyncioTestCase):
    async def test_get_active_positions(self):
        session = mock.AsyncMock()
        mock_exec(session, [{"id": 1}])
        self.assertEqual(len(await positions_mod.get_active_positions(session)), 1)

    async def test_get_active_positions_empty(self):
        session = mock.AsyncMock()
        mock_exec(session, [])
        self.assertEqual(await positions_mod.get_active_positions(session), [])

    async def test_get_positions_by_asset(self):
        session = mock.AsyncMock()
        mock_exec(session, [{"id": 2}])
        self.assertEqual(len(await positions_mod.get_positions_by_asset(session, "BTC")), 1)

    async def test_get_positions_with_unrealized_pnl(self):
        session = mock.AsyncMock()
        mock_exec(session, [{"id": 3}])
        self.assertEqual(len(await positions_mod.get_positions_with_unrealized_pnl(session)), 1)


if __name__ == "__main__":
    unittest.main()
