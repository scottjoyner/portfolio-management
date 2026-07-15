"""Direct-call coverage for the CoinbaseCLI wrapper methods.  The CLI shells
out to the `coinbase` binary via subprocess; we fake a successful JSON stdout
so each command path is exercised without a real binary."""
from __future__ import annotations

from unittest import mock

import portfolio_optimizer as P


class _FakeResult:
    def __init__(self, stdout):
        self.stdout = stdout
        self.stderr = ""
        self.returncode = 0


def _json_run(stdout):
    return mock.patch("subprocess.run", return_value=_FakeResult(stdout))


def test_cli_products_and_price():
    with _json_run('{"products": [{"product_id": "BTC-USD"}, {"product_id": "ETH-USD"}]}'):
        cli = P.CoinbaseCLI(environment="live")
        prods = cli.get_products()
        assert "BTC-USD" in prods
        assert cli.get_product("BTC-USD")["product_id"] == "BTC-USD"
    with _json_run('{"price": 100.0}'):
        cli = P.CoinbaseCLI(environment="live")
        assert cli.get_price("BTC-USD")["price"] == 100.0
    with _json_run('[[1, 2, 3, 4, 5, 6]]'):
        cli = P.CoinbaseCLI(environment="live")
        assert isinstance(cli.get_candles("BTC-USD", granularity="1h", limit=10), list)


def test_cli_balances_fees_fills():
    with _json_run('[{"currency": "USDC", "available_balance": {"value": "1"}}]'):
        cli = P.CoinbaseCLI(environment="live")
        assert isinstance(cli.get_balances(), list)
    with _json_run('{"advanced_trade_only_volume": 100.0}'):
        cli = P.CoinbaseCLI(environment="live")
        assert cli.get_fees()["advanced_trade_only_volume"] == 100.0
    with _json_run('[{"product_id": "BTC-USD", "side": "BUY", "size": "1", "price": "1"}]'):
        cli = P.CoinbaseCLI(environment="live")
        assert isinstance(cli.get_fills(), list)


def test_cli_preview_create_order():
    with _json_run('{"preview": {"total_fee": 0.5, "total_cost": 100.0}}'):
        cli = P.CoinbaseCLI(environment="live")
        pv = cli.preview_order("BTC-USD", "BUY", 100.0, is_quote=True)
        assert pv["preview"]["total_fee"] == 0.5
    with _json_run('{"id": "order-1", "status": "done"}'):
        cli = P.CoinbaseCLI(environment="live")
        od = cli.create_order("BTC-USD", "BUY", 0.001, is_quote=False)
        assert od["id"] == "order-1"
        assert cli.get_order("order-1")["id"] == "order-1"


def test_cli_best_product_and_round():
    with _json_run('{"products": [{"product_id": "BTC-USD"}, {"product_id": "ETH-USD"}]}'):
        cli = P.CoinbaseCLI(environment="live")
        assert cli.best_product("BTC", "BUY") == "BTC-USD"
        # unknown currency -> no products match -> None
        assert cli.best_product("ZZZ", "SELL") is None
    cli2 = P.CoinbaseCLI(environment="live")
    assert isinstance(cli2._round_quote("BTC-USD", 1.2345), float)
    assert isinstance(cli2._round_base("BTC-USD", 1.2345), float)
