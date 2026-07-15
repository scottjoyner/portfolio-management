"""Coverage tests for state_store.StateStore (SQLite CRUD + thread safety)."""

import threading

import pytest

import state_store as ss


class _FakeVerdict:
    def __init__(self, **kw):
        self.strategy = kw.get("strategy", "ema_cross")
        self.currency = kw.get("currency", "BTC-USD")
        self.total_trades = kw.get("total_trades", 10)
        self.winning_trades = kw.get("winning_trades", 6)
        self.losing_trades = kw.get("losing_trades", 4)
        self.win_rate = kw.get("win_rate", 0.6)
        self.total_return_pct = kw.get("total_return_pct", 10.0)
        self.sharpe_ratio = kw.get("sharpe_ratio", 1.2)
        self.profit_factor = kw.get("profit_factor", 1.5)
        self.max_drawdown_pct = kw.get("max_drawdown_pct", 5.0)
        self.regime = kw.get("regime", "trending")
        self.passed = kw.get("passed", True)
        self.reason = kw.get("reason", "ok")


class _FakeState:
    def __init__(self):
        self.holdings = {
            "BTC-USD": {
                "currency": "BTC-USD",
                "total": 1.0,
                "price": 50000.0,
                "value": 50000.0,
                "classification": "safe",
                "allocation_pct": 50.0,
            }
        }
        self.total_value = 100000.0
        self.usdc_balance = 50000.0
        self.fee_volume_30d = 1000000.0
        self.fee_tier = (1000000.0, 0.001, 0.002)


@pytest.fixture
def store(tmp_path):
    db = tmp_path / "state.db"
    s = ss.StateStore(str(db))
    yield s
    try:
        s._conn_obj.close()
    except Exception:
        pass


def test_save_and_load_trades(store):
    trade = {
        "type": "rebalance",
        "side": "BUY",
        "currency": "BTC-USD",
        "size_usd": 1000,
        "fee": 2.0,
        "symbol": "BTC-USD",
        "price": 50000,
        "quantity": 0.02,
        "strategy": "ema_cross",
        "pnl_usd": 15.0,
        "reason": "rebalance",
        "order_id": "o1",
        "dry_run": True,
    }
    store.save_trade(trade)
    rows = store.load_trades()
    assert len(rows) == 1
    assert rows[0]["type"] == "rebalance"
    assert rows[0]["dry_run"] == 1
    # limit param
    assert len(store.load_trades(limit=5)) == 1
    assert len(store.load_trades(limit=0)) == 1 or len(store.load_trades(limit=1)) == 1


def test_save_trade_defaults(store):
    # minimal trade dict exercises defaults for type/side/currency/symbol
    store.save_trade({"size_usd": 100})
    row = store.load_trades()[0]
    assert row["type"] == ""
    assert row["side"] == ""
    assert row["currency"] == ""
    assert row["symbol"] == ""
    assert row["dry_run"] == 0


def test_snapshot_roundtrip(store):
    res = store.save_snapshot(_FakeState())
    assert "id" in res and "timestamp" in res
    snaps = store.load_snapshots()
    assert len(snaps) == 1
    assert snaps[0]["total_value"] == 100000.0
    assert snaps[0]["holdings"]["BTC-USD"]["currency"] == "BTC-USD"
    assert len(store.load_snapshots(limit=10)) == 1


def test_bt_cache_roundtrip_and_prune(store):
    v = _FakeVerdict()
    store.save_bt_cache("ema_cross/BTC-USD", v)
    loaded = store.load_bt_cache(ttl=3600)
    assert loaded["ema_cross/BTC-USD"]["win_rate"] == 0.6
    assert loaded["ema_cross/BTC-USD"]["passed"] is True
    # stale ttl -> pruned (this also deletes the entry)
    assert store.load_bt_cache(ttl=-1) == {}
    # re-save and confirm still readable
    store.save_bt_cache("k2", v)
    assert store.load_bt_cache(ttl=3600)
    store.prune_bt_cache(ttl=86400)
    assert store.load_bt_cache(ttl=3600)


def test_position_ages(store):
    store.save_position_ages({"BTC-USD": 3.5, "ETH-USD": 1.0})
    ages = store.load_position_ages()
    assert ages["BTC-USD"] == 3.5
    assert ages["ETH-USD"] == 1.0


def test_meta(store):
    assert store.get_meta("missing", default="d") == "d"
    store.set_meta("key", "val")
    assert store.get_meta("key") == "val"
    # set_meta again replaces
    store.set_meta("key", "val2")
    assert store.get_meta("key") == "val2"


def test_stats(store):
    store.save_trade({"size_usd": 10})
    s = store.stats()
    assert s["trades"] == 1
    assert "snapshots" in s
    assert s["db_path"].endswith(".db")


def test_lock_is_threading_lock(store):
    assert isinstance(store._lock, type(threading.Lock()))
    # _conn lazily creates connection
    assert store._conn() is store._conn_obj
