from trading_system.apps.api import main


class FakeCoinbaseService:
    def get_connection_status(self):
        return {"connected": True, "environment": "live", "account_count": 1, "error": None}

    def get_balances_snapshot(self):
        class Snapshot:
            def to_dict(self):
                return {"account_count": 1, "accounts": [{"currency": "USDC", "available": "100"}]}

        return Snapshot()

    def get_price(self, product_id):
        return {"product_id": product_id, "price": "60716.93"}


def test_coinbase_status_endpoint(monkeypatch):
    monkeypatch.setattr(main, "get_coinbase_service", lambda: FakeCoinbaseService())

    data = main.coinbase_status()

    assert data["connected"] is True
    assert data["account_count"] == 1


def test_coinbase_balances_endpoint(monkeypatch):
    monkeypatch.setattr(main, "get_coinbase_service", lambda: FakeCoinbaseService())

    data = main.coinbase_balances()

    assert data["account_count"] == 1
    assert data["accounts"][0]["currency"] == "USDC"


def test_coinbase_price_endpoint(monkeypatch):
    monkeypatch.setattr(main, "get_coinbase_service", lambda: FakeCoinbaseService())

    data = main.coinbase_price("BTC-USD")

    assert data["product_id"] == "BTC-USD"
    assert data["price"] == "60716.93"
