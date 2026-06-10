from trading_system.core.exchange.coinbase_service import CoinbaseService


class FakeConnector:
    def __init__(self, fail=False):
        self.fail = fail

    def get_balances(self):
        if self.fail:
            raise RuntimeError("auth failed with privateKey=SHOULD_NOT_LEAK")
        return {
            "accounts": [
                {
                    "uuid": "acct-1",
                    "name": "USDC Wallet",
                    "currency": "USDC",
                    "available_balance": {"value": "100.00", "currency": "USDC"},
                    "hold": {"value": "0", "currency": "USDC"},
                    "active": True,
                    "ready": True,
                }
            ],
            "size": 1,
        }

    def get_price(self, product_id):
        return {"product_id": product_id, "price": "60716.93"}


def test_coinbase_service_reports_connection_status_green():
    service = CoinbaseService(connector=FakeConnector())

    status = service.get_connection_status()

    assert status["connected"] is True
    assert status["account_count"] == 1
    assert status["error"] is None


def test_coinbase_service_sanitizes_connection_errors():
    service = CoinbaseService(connector=FakeConnector(fail=True))

    status = service.get_connection_status()

    assert status["connected"] is False
    assert "privateKey" not in status["error"]
    assert "SHOULD_NOT_LEAK" not in status["error"]


def test_coinbase_service_returns_normalized_balance_snapshot():
    service = CoinbaseService(connector=FakeConnector())

    snapshot = service.get_balances_snapshot()
    data = snapshot.to_dict()

    assert data["account_count"] == 1
    assert data["accounts"][0]["currency"] == "USDC"
    assert data["accounts"][0]["available"] == "100.00"


def test_coinbase_service_returns_normalized_price():
    service = CoinbaseService(connector=FakeConnector())

    price = service.get_price("BTC-USD")

    assert price["product_id"] == "BTC-USD"
    assert price["price"] == "60716.93"
