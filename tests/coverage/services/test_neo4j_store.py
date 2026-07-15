"""Coverage tests for neo4j_store.Neo4jStore (driver mocked)."""

from unittest.mock import MagicMock, patch

import pytest

import neo4j_store as n4j


class FakeResult:
    def __init__(self, rows=None, single=None, consume_val=None):
        self._rows = rows or []
        self._single = single
        self._consume = consume_val if consume_val is not None else object()

    def consume(self):
        return self._consume

    def single(self):
        return self._single

    def __iter__(self):
        return iter(self._rows)


class FakeSession:
    def __init__(self, handler):
        self._handler = handler

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def run(self, q, **params):
        return self._handler(q, params)


class FakeDriver:
    def __init__(self, handler):
        self.handler = handler
        self.closed = False

    def session(self, **kw):
        return FakeSession(self.handler)

    def close(self):
        self.closed = True


class _FakeState:
    def __init__(self):
        self.holdings = {
            "BTC-USD": {
                "currency": "BTC-USD", "total": 1.0, "price": 50000.0,
                "value": 50000.0, "classification": "safe", "allocation_pct": 50.0,
            }
        }
        self.total_value = 100000.0
        self.usdc_balance = 50000.0
        self.fee_volume_30d = 1000000.0
        self.fee_tier = (1000000.0, 0.001, 0.002)


@pytest.fixture
def store():
    recorded = []
    default_handler = lambda q, p: FakeResult(consume_val=True)
    driver = FakeDriver(default_handler)

    def handler(q, p):
        recorded.append((q, p))
        if "RETURN count" in q:
            return FakeResult(single={"c": 0})
        return FakeResult(consume_val=True)

    driver.handler = handler
    with patch("neo4j_store.GraphDatabase") as GD:
        GD.driver.return_value = driver
        s = n4j.Neo4jStore(uri="bolt://x", user="u", password="p", database="trading")
    s._fake_driver = driver
    s._recorded = recorded
    yield s
    s.close()


def _verbose_handler(q, p):
    if "RETURN count" in q:
        return FakeResult(single={"c": 0})
    return FakeResult(consume_val=True)


def test_init_schema_and_connect(store):
    # init ran (constraints) without error
    assert any("CREATE CONSTRAINT" in q for q, _ in store._recorded)


def test_ensure_database_falls_back():
    def raiser(q, p):
        if "CREATE DATABASE" in q:
            raise RuntimeError("no system db")
        return FakeResult(consume_val=True)
    driver = FakeDriver(raiser)
    with patch("neo4j_store.GraphDatabase") as GD:
        GD.driver.return_value = driver
        s = n4j.Neo4jStore(uri="bolt://x", user="u", password="p", database="trading")
    assert s._database == "neo4j"
    s.close()


def test_save_and_load_trades(store):
    store.save_trade({"type": "rebalance", "side": "BUY", "currency": "BTC-USD",
                      "size_usd": 1000, "fee": 2, "reason": "r", "order_id": "o1", "dry_run": True})
    store.save_trade({"type": "rebalance", "side": "SELL", "currency": "ETH-USD",
                      "size_usd": 500, "order_id": ""})  # no order_id -> uuid id
    rows = [
        {"id": "o1", "timestamp": "2026-01-01T00:00:00", "type": "rebalance", "side": "BUY",
         "currency": "BTC-USD", "size_usd": 1000, "fee": 2, "reason": "r", "order_id": "o1", "dry_run": True},
        {"id": "o2", "timestamp": "2026-01-02T00:00:00", "type": "rebalance", "side": "SELL",
         "currency": "ETH-USD", "size_usd": 500, "fee": 1, "reason": "r2", "order_id": "o2", "dry_run": 0},
    ]
    store._fake_driver.handler = lambda q, p: FakeResult(rows=rows)
    out = store.load_trades(limit=10)
    assert len(out) == 2
    assert out[0]["id"] == "o1"
    assert out[0]["dry_run"] == 1


def test_save_and_load_snapshots(store):
    res = store.save_snapshot(_FakeState())
    assert "id" in res
    rows = [{
        "id": "s1", "timestamp": "2026-01-01T00:00:00", "total_value": 100000.0,
        "holding_count": 1, "usdc_balance": 50000.0, "fee_volume_30d": 1000000.0,
        "fee_tier_min_volume": 1000000.0, "fee_tier_maker": 0.001, "fee_tier_taker": 0.002,
        "holdings_json": '{"BTC-USD": {"currency": "BTC-USD"}}',
    }]
    store._fake_driver.handler = lambda q, p: FakeResult(rows=rows)
    out = store.load_snapshots(limit=10)
    assert len(out) == 1
    assert out[0]["holdings"]["BTC-USD"]["currency"] == "BTC-USD"


def test_bt_cache(store):
    class V:
        strategy = "ema_cross"; currency = "BTC-USD"; total_trades = 10
        winning_trades = 6; losing_trades = 4; win_rate = 0.6
        total_return_pct = 10.0; sharpe_ratio = 1.2; profit_factor = 1.5
        max_drawdown_pct = 5.0; regime = "trending"; passed = True; reason = "ok"

    store.save_bt_cache("k", V())
    rows = [{"key": "k", "strategy": "ema_cross", "currency": "BTC-USD", "total_trades": 10,
             "winning_trades": 6, "losing_trades": 4, "win_rate": 0.6, "total_return_pct": 10.0,
             "sharpe_ratio": 1.2, "profit_factor": 1.5, "max_drawdown_pct": 5.0,
             "regime": "trending", "passed": True, "reason": "ok"}]
    store._fake_driver.handler = lambda q, p: FakeResult(rows=rows)
    out = store.load_bt_cache(ttl=3600)
    assert out["k"]["win_rate"] == 0.6
    # prune uses consume
    store.prune_bt_cache(ttl=86400)
    assert store._recorded  # some query ran


def test_position_ages(store):
    store.save_position_ages({"BTC-USD": 3.5})
    rows = [{"currency": "BTC-USD", "age": 3.5}]
    store._fake_driver.handler = lambda q, p: FakeResult(rows=rows)
    out = store.load_position_ages()
    assert out["BTC-USD"] == 3.5


def test_meta(store):
    assert store.get_meta("missing", default="d") == "d"
    # no rows -> single() returns None branch already covered by default above
    store.set_meta("k", "v")
    store._fake_driver.handler = lambda q, p: FakeResult(single={"value": "v"})
    assert store.get_meta("k") == "v"


def test_stats(store):
    out = store.stats()
    assert out["trades"] == 0
    assert out["uri"] == "bolt://x"


def test_context_manager():
    driver = FakeDriver(_verbose_handler)
    with patch("neo4j_store.GraphDatabase") as GD:
        GD.driver.return_value = driver
        with n4j.Neo4jStore(uri="bolt://x", user="u", password="p") as s:
            assert s is not None
    assert driver.closed is True
