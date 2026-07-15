"""Coverage tests for trading_system.strategies.grid_trading.bot.

Drives the GridTradingBot through every public method and branch, including
constructor/config validation, grid creation, single-trade execution,
position-limit enforcement, monitoring/rebalancing, health checks, performance
stats, state accessors, and the module-level main() entry point.

Network access is avoided entirely: the market-price fetch path is mocked and
the Coinbase read-only client is stubbed where the price grid is generated.
"""

import asyncio
import unittest
from unittest.mock import AsyncMock, MagicMock, patch

from trading_system.connectors.coinboard.rest.circuit_breaker import CircuitBreakerError
from trading_system.strategies.grid_trading.bot import GridTradingBot, GridConfig


def make_bot(**overrides):
    cfg = {
        'pair': 'BTC-USD',
        'grid_levels': 50,
        'grid_step_pct': 1.0,
        'initial_capital_usd': 1000.0,
        'exchange': {},
        'risk_management': {'max_position_btc': 0.1, 'enabled': True},
    }
    cfg.update(overrides)
    return GridTradingBot(cfg)


class TestGridConfig(unittest.TestCase):
    def test_from_dict_defaults(self):
        c = GridConfig.from_dict({})
        self.assertEqual(c.grid_levels, 50)
        self.assertEqual(c.grid_step_pct, 1.0)
        self.assertEqual(c.initial_capital_usd, 1000.0)
        self.assertEqual(c.risk_management, {})

    def test_from_dict_explicit(self):
        c = GridConfig.from_dict({
            'grid_levels': 30,
            'grid_step_pct': 2.0,
            'initial_capital_usd': 2000.0,
            'risk_management': {'enabled': False},
        })
        self.assertEqual(c.grid_levels, 30)
        self.assertEqual(c.grid_step_pct, 2.0)
        self.assertEqual(c.initial_capital_usd, 2000.0)
        self.assertEqual(c.risk_management, {'enabled': False})

    def test_from_dict_risk_management_falsy(self):
        # The `or {}` branch when risk_management is None.
        c = GridConfig.from_dict({'risk_management': None})
        self.assertEqual(c.risk_management, {})


class TestInit(unittest.TestCase):
    def test_init_basic(self):
        bot = make_bot()
        self.assertEqual(bot.pair, 'BTC-USD')
        self.assertEqual(bot.config.grid_levels, 50)
        self.assertEqual(bot.order_map, {})
        self.assertIsNone(bot.current_price)
        self.assertIsNotNone(bot.fee_calculator)
        self.assertIsNotNone(bot.strategy_circuit_breaker)

    def test_init_default_pair(self):
        cfg = {'grid_levels': 50, 'grid_step_pct': 1.0}
        bot = GridTradingBot(cfg)
        self.assertEqual(bot.pair, 'BTC-USD')
        self.assertEqual(bot.exchange_config, {})


class TestInitializeValidation(unittest.TestCase):
    def test_initialize_valid(self):
        bot = make_bot()
        bot._fetch_market_price = AsyncMock(return_value=50000.0)
        result = asyncio.run(bot.initialize())
        self.assertEqual(result['status'], 'initialized')
        self.assertEqual(result['orders_placed'], len(bot.order_map))
        self.assertEqual(bot.current_price, 50000.0)

    def test_initialize_grid_levels_too_low(self):
        bot = make_bot(grid_levels=5)
        with self.assertRaises(ValueError):
            asyncio.run(bot.initialize())

    def test_initialize_grid_levels_too_high(self):
        bot = make_bot(grid_levels=600)
        with self.assertRaises(ValueError):
            asyncio.run(bot.initialize())

    def test_initialize_step_too_low(self):
        bot = make_bot(grid_step_pct=0.05)
        with self.assertRaises(ValueError):
            asyncio.run(bot.initialize())

    def test_initialize_step_too_high(self):
        bot = make_bot(grid_step_pct=60.0)
        with self.assertRaises(ValueError):
            asyncio.run(bot.initialize())

    def test_initialize_inner_raise_no_token(self):
        # _initialize_grid raises a non-CB error -> initialize() except (no token)
        bot = make_bot()
        bot._generate_price_grid = AsyncMock(side_effect=ValueError("boom"))
        with self.assertRaises(ValueError):
            asyncio.run(bot.initialize())

    def test_initialize_inner_raise_with_token(self):
        # _initialize_grid raises; message contains 'access_token' -> masked branch
        bot = make_bot(exchange={'access_token': 'SECRET'})
        bot._generate_price_grid = AsyncMock(
            side_effect=ValueError("failure with access_token=SECRET inside")
        )
        with self.assertRaises(ValueError):
            asyncio.run(bot.initialize())


class TestGeneratePriceGrid(unittest.TestCase):
    def test_generate_price_grid_normal(self):
        bot = make_bot()
        bot._fetch_market_price = AsyncMock(return_value=50000.0)
        asyncio.run(bot._generate_price_grid())
        self.assertEqual(bot.current_price, 50000.0)

    def test_generate_price_grid_circuit_open(self):
        # call_if_closed raises CircuitBreakerError -> falls back to mock price
        bot = make_bot()
        bot._fetch_market_price = AsyncMock(return_value=50000.0)
        bot.strategy_circuit_breaker.call_if_closed = AsyncMock(
            side_effect=CircuitBreakerError("open")
        )
        asyncio.run(bot._generate_price_grid())
        self.assertEqual(bot.current_price, 50000.0)


class TestFetchMarketPrice(unittest.TestCase):
    def test_fetch_returns_btc_estimate(self):
        bot = make_bot()
        fake_client = MagicMock()
        fake_client.fetch_account = AsyncMock(
            return_value=({'currency': 'BTC', 'available': 1.0, 'last_refreshed': 2.0}, None)
        )
        with patch(
            'trading_system.connectors.coinboard.rest.create_read_only_client',
            new=AsyncMock(return_value=fake_client),
        ):
            price = asyncio.run(bot._fetch_market_price())
        self.assertEqual(price, 100000.0)

    def test_fetch_returns_non_btc(self):
        bot = make_bot()
        bot.current_price = 12345.0
        fake_client = MagicMock()
        fake_client.fetch_account = AsyncMock(
            return_value=({'currency': 'USD', 'available': 5.0}, None)
        )
        with patch(
            'trading_system.connectors.coinboard.rest.create_read_only_client',
            new=AsyncMock(return_value=fake_client),
        ):
            price = asyncio.run(bot._fetch_market_price())
        self.assertEqual(price, 12345.0)

    def test_fetch_raises_falls_back(self):
        bot = make_bot()
        bot.current_price = 7777.0
        with patch(
            'trading_system.connectors.coinboard.rest.create_read_only_client',
            new=AsyncMock(side_effect=RuntimeError("network down")),
        ):
            price = asyncio.run(bot._fetch_market_price())
        self.assertEqual(price, 7777.0)


class TestInitializeGrid(unittest.TestCase):
    def test_initialize_grid_builds_orders(self):
        bot = make_bot()
        bot._fetch_market_price = AsyncMock(return_value=50000.0)
        asyncio.run(bot._initialize_grid())
        # 50 buy + 50 sell levels
        self.assertEqual(len(bot.order_map), 100)
        buys = [o for o in bot.order_map.values() if o['type'] == 'buy']
        sells = [o for o in bot.order_map.values() if o['type'] == 'sell']
        self.assertEqual(len(buys), 50)
        self.assertEqual(len(sells), 50)
        # lower levels (<=20) pending, higher filled
        self.assertTrue(any(o['status'] == 'pending' for o in buys))
        self.assertTrue(any(o['status'] == 'filled' for o in buys))

    def test_initialize_grid_inner_raises(self):
        bot = make_bot()
        bot._generate_price_grid = AsyncMock(side_effect=RuntimeError("x"))
        with self.assertRaises(RuntimeError):
            asyncio.run(bot._initialize_grid())


class TestExecuteSingleTrade(unittest.TestCase):
    def _ready_bot(self):
        bot = make_bot()
        bot.current_price = 50000.0
        return bot

    def test_buy_default_amount(self):
        bot = self._ready_bot()
        res = asyncio.run(bot._execute_single_trade('buy', None))
        self.assertEqual(res['side'], 'buy')
        self.assertAlmostEqual(res['price'], 25000.0)
        self.assertGreater(res['fees'], 0)
        self.assertLess(res['net'], res['amount_usd'])

    def test_sell_default_amount(self):
        bot = self._ready_bot()
        res = asyncio.run(bot._execute_single_trade('sell', None))
        self.assertEqual(res['side'], 'sell')
        self.assertAlmostEqual(res['price'], 75000.0)

    def test_explicit_amount(self):
        bot = self._ready_bot()
        res = asyncio.run(bot._execute_single_trade('buy', 200.0))
        self.assertEqual(res['amount_usd'], 200.0)

    def test_position_limit_exceeded(self):
        bot = make_bot(risk_management={'max_position_btc': 0.0001, 'enabled': True})
        bot.current_price = 50000.0
        with self.assertRaises(ValueError):
            asyncio.run(bot._execute_single_trade('buy', None))


class TestExecuteGridTrade(unittest.TestCase):
    def test_success(self):
        bot = make_bot()
        bot.current_price = 50000.0
        asyncio.run(bot.execute_grid_trade('buy', None))
        # No return value is produced by the current implementation.

    def test_circuit_breaker_open(self):
        bot = make_bot()
        bot.strategy_circuit_breaker.call_if_closed = AsyncMock(
            side_effect=CircuitBreakerError("open")
        )
        with self.assertRaises(CircuitBreakerError):
            asyncio.run(bot.execute_grid_trade('buy', None))

    def test_generic_exception_no_token(self):
        bot = make_bot()
        bot.strategy_circuit_breaker.call_if_closed = AsyncMock(
            side_effect=RuntimeError("boom")
        )
        with self.assertRaises(CircuitBreakerError):
            asyncio.run(bot.execute_grid_trade('buy', None))

    def test_generic_exception_with_token(self):
        bot = make_bot(exchange={'access_token': 'TOPSECRET'})
        bot.strategy_circuit_breaker.call_if_closed = AsyncMock(
            side_effect=RuntimeError("leaked access_token=TOPSECRET here")
        )
        with self.assertRaises(CircuitBreakerError):
            asyncio.run(bot.execute_grid_trade('buy', None))


class TestMonitorAndRebalance(unittest.TestCase):
    def test_empty_order_map(self):
        bot = make_bot()
        res = asyncio.run(bot.monitor_and_rebalance())
        self.assertEqual(res, {'rebalanced': False, 'orders_adjusted': 0})

    def test_rebalance_triggered(self):
        bot = make_bot()
        bot._fetch_market_price = AsyncMock(return_value=50000.0)
        asyncio.run(bot._initialize_grid())
        # 50 levels -> 60 of 100 orders filled (ratio 0.6 > 0.3)
        res = asyncio.run(bot.monitor_and_rebalance())
        self.assertTrue(res['rebalanced'])
        self.assertEqual(res['orders_adjusted'], len(bot.order_map))

    def test_rebalance_exception(self):
        bot = make_bot()
        bot._fetch_market_price = AsyncMock(return_value=50000.0)
        asyncio.run(bot._initialize_grid())
        bot._rebalance_grid = AsyncMock(side_effect=RuntimeError("oops"))
        res = asyncio.run(bot.monitor_and_rebalance())
        self.assertEqual(res, {'rebalanced': False, 'orders_adjusted': 0})


class TestRebalanceGrid(unittest.TestCase):
    def test_rebalance_success(self):
        bot = make_bot()
        bot._fetch_market_price = AsyncMock(return_value=50000.0)
        asyncio.run(bot._rebalance_grid())
        self.assertEqual(len(bot.order_map), 100)
        self.assertTrue(all(o.get('rebalance_adjusted') for o in bot.order_map.values()))

    def test_rebalance_generate_raises(self):
        bot = make_bot()
        bot._generate_price_grid = AsyncMock(side_effect=RuntimeError("boom"))
        asyncio.run(bot._rebalance_grid())
        # Original order map preserved (rebuild failed)
        self.assertTrue(isinstance(bot.order_map, dict))


class TestAccessors(unittest.TestCase):
    def test_get_grid_info_active(self):
        bot = make_bot()
        bot.current_price = 50000.0
        info = bot.get_grid_info()
        self.assertEqual(info['status'], 'active')
        self.assertEqual(info['position_limit'], 0.1)

    def test_get_grid_info_inactive(self):
        bot = make_bot()
        bot.current_price = None
        info = bot.get_grid_info()
        self.assertEqual(info['status'], 'inactive')

    def test_get_grid_info_no_position_limit(self):
        bot = make_bot(risk_management={})
        bot.current_price = 1.0
        self.assertEqual(bot.get_grid_info()['position_limit'], 0.1)

    def test_get_order_map_empty(self):
        bot = make_bot()
        self.assertEqual(bot.get_order_map(), {})

    def test_get_order_map_nonempty(self):
        bot = make_bot()
        bot.order_map = {'a': {'status': 'filled'}}
        self.assertEqual(bot.get_order_map(), {'a': {'status': 'filled'}})

    def test_get_health_check_enabled_default(self):
        bot = make_bot(risk_management={})
        hc = bot.get_health_check()
        self.assertEqual(hc['status'], 'healthy')
        self.assertTrue(hc['components']['position_limits_enforced'])

    def test_get_health_check_enabled_false(self):
        bot = make_bot(risk_management={'enabled': False})
        hc = bot.get_health_check()
        self.assertFalse(hc['components']['position_limits_enforced'])


class TestHealthCheck(unittest.TestCase):
    def test_success(self):
        bot = make_bot()
        res = asyncio.run(bot.health_check())
        self.assertEqual(res[1], False)

    def test_circuit_breaker_open(self):
        bot = make_bot()
        bot.strategy_circuit_breaker.call_if_closed = AsyncMock(
            side_effect=CircuitBreakerError("open")
        )
        with self.assertRaises(CircuitBreakerError):
            asyncio.run(bot.health_check())

    def test_generic_exception_no_token(self):
        bot = make_bot()
        bot.strategy_circuit_breaker.call_if_closed = AsyncMock(
            side_effect=RuntimeError("boom")
        )
        with self.assertRaises(CircuitBreakerError):
            asyncio.run(bot.health_check())

    def test_generic_exception_with_token(self):
        bot = make_bot(exchange={'access_token': 'X'})
        bot.strategy_circuit_breaker.call_if_closed = AsyncMock(
            side_effect=RuntimeError("access_token=X leaked")
        )
        with self.assertRaises(CircuitBreakerError):
            asyncio.run(bot.health_check())


class TestPerformanceStats(unittest.TestCase):
    def test_empty(self):
        bot = make_bot()
        stats = bot.get_performance_stats()
        self.assertEqual(stats['total_trades'], 0)
        self.assertEqual(stats['win_rate'], 0)
        self.assertEqual(stats['total_profit_usd'], 0)
        self.assertEqual(stats['avg_profit_per_trade'], 0)

    def test_with_filled_mixed(self):
        bot = make_bot()
        bot.order_map = {
            'a': {'status': 'filled', 'net': 5.0},
            'b': {'status': 'filled', 'net': -1.0},
            'c': {'status': 'pending', 'net': 99.0},
        }
        stats = bot.get_performance_stats()
        self.assertEqual(stats['total_trades'], 2)
        self.assertEqual(stats['winning_trades'], 1)
        self.assertEqual(stats['win_rate'], 0.5)
        self.assertEqual(stats['total_profit_usd'], 4.0)
        self.assertEqual(stats['avg_profit_per_trade'], 2.0)


class TestMain(unittest.TestCase):
    def test_main_runs(self):
        with patch.object(GridTradingBot, '_fetch_market_price',
                          new=AsyncMock(return_value=50000.0)):
            # Import lazily so the module-level sys.path tweak is active.
            from trading_system.strategies.grid_trading import bot as botmod
            asyncio.run(botmod.main())


if __name__ == '__main__':
    unittest.main()
