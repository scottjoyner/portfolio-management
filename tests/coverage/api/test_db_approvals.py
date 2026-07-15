import unittest
from unittest import mock

from trading_system.api.databases import approvals as approvals_mod


def mock_exec(session, rows):
    res = mock.MagicMock()
    res.mapped.return_value = rows
    session.execute.return_value = res


class TestApprovals(unittest.IsolatedAsyncioTestCase):
    async def test_get_approval_requests(self):
        session = mock.AsyncMock()
        mock_exec(session, [{"id": 1}])
        self.assertEqual(len(await approvals_mod.get_approval_requests(session)), 1)

    async def test_get_approval_requests_empty(self):
        session = mock.AsyncMock()
        mock_exec(session, [])
        self.assertEqual(await approvals_mod.get_approval_requests(session), [])

    async def test_get_pending_approvals(self):
        session = mock.AsyncMock()
        mock_exec(session, [{"id": 2}])
        self.assertEqual(len(await approvals_mod.get_pending_approvals(session)), 1)

    async def test_get_auto_approved_trades(self):
        session = mock.AsyncMock()
        mock_exec(session, [{"id": 3}])
        self.assertEqual(len(await approvals_mod.get_auto_approved_trades(session)), 1)


if __name__ == "__main__":
    unittest.main()
