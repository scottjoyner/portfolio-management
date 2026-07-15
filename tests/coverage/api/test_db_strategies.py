import unittest
from unittest import mock

from trading_system.api.databases import strategies as strategies_mod


def mock_exec(session, rows):
    res = mock.MagicMock()
    res.mapped.return_value = rows
    session.execute.return_value = res


class TestStrategies(unittest.IsolatedAsyncioTestCase):
    async def test_get_strategy_performance(self):
        session = mock.AsyncMock()
        mock_exec(session, [{"id": 1}])
        self.assertEqual(len(await strategies_mod.get_strategy_performance(session)), 1)

    async def test_get_strategy_performance_empty(self):
        session = mock.AsyncMock()
        mock_exec(session, [])
        self.assertEqual(await strategies_mod.get_strategy_performance(session), [])

    async def test_get_strategy_backtest_results_present(self):
        session = mock.AsyncMock()
        mock_exec(session, [("a", 1)])
        self.assertEqual(await strategies_mod.get_strategy_backtest_results(session, "s1"), {"a": 1})

    async def test_get_strategy_backtest_results_empty(self):
        session = mock.AsyncMock()
        mock_exec(session, [])
        self.assertEqual(await strategies_mod.get_strategy_backtest_results(session, "s1"), {})


if __name__ == "__main__":
    unittest.main()
