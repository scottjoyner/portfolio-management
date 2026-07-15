import unittest
from unittest import mock

from trading_system.api.databases import accounts as accounts_mod


def mock_exec(session, rows):
    res = mock.MagicMock()
    res.mapped.return_value = rows
    session.execute.return_value = res


class TestAccounts(unittest.IsolatedAsyncioTestCase):
    async def test_get_all_accounts(self):
        session = mock.AsyncMock()
        mock_exec(session, [{"id": 1, "name": "x"}])
        self.assertEqual(await accounts_mod.get_all_accounts(session), [{"id": 1, "name": "x"}])

    async def test_get_all_accounts_empty(self):
        session = mock.AsyncMock()
        mock_exec(session, [])
        self.assertEqual(await accounts_mod.get_all_accounts(session), [])

    async def test_get_accounts_by_institution(self):
        session = mock.AsyncMock()
        mock_exec(session, [{"id": 1}])
        self.assertEqual(len(await accounts_mod.get_accounts_by_institution(session, "Plaid")), 1)

    async def test_get_accounts_with_positions(self):
        session = mock.AsyncMock()
        mock_exec(session, [{"id": 2}])
        self.assertEqual(len(await accounts_mod.get_accounts_with_positions(session)), 1)


if __name__ == "__main__":
    unittest.main()
