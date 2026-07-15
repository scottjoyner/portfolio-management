import os
import tempfile

# Point the feed cache at a temp dir BEFORE importing the module (env is read
# at import time).
_TMP = tempfile.mkdtemp(prefix="feed_cache_test_")
os.environ["NAS_FEED_ROOT"] = _TMP

from data.feed_cache import (  # noqa: E402
    save_candles,
    load_candles,
    new_since,
    record,
    save_records,
    load_records,
)

CANDLES = [
    [1000, 10.0, 11.0, 9.5, 10.5, 100.0],
    [2000, 10.5, 12.0, 10.0, 11.5, 120.0],
    [3000, 11.5, 12.5, 11.0, 12.0, 90.0],
]


def test_save_load_roundtrip():
    n = save_candles("coinbase_candles", "BTC-USD", 3600, CANDLES)
    assert n == 3
    out = load_candles("coinbase_candles", "BTC-USD", 3600)
    assert len(out) == 3
    assert out[0][0] == 1000 and out[-1][0] == 3000  # oldest-first
    assert out[1][4] == 11.5  # close preserved


def test_save_dedup_and_append():
    save_candles("coinbase_candles", "ETH-USD", 3600, CANDLES)
    # overlapping + one new bar
    extra = [
        [2000, 99, 99, 99, 99, 99],  # duplicate ts -> ignored
        [4000, 12.0, 13.0, 11.5, 12.5, 80.0],
    ]
    save_candles("coinbase_candles", "ETH-USD", 3600, extra)
    out = load_candles("coinbase_candles", "ETH-USD", 3600)
    assert len(out) == 4
    # duplicate ts keeps original row, not the 99s overwrite
    row2000 = [r for r in out if r[0] == 2000][0]
    assert row2000[4] == 11.5


def test_new_since():
    save_candles("coinbase_candles", "SOL-USD", 3600, CANDLES)
    newer = new_since("coinbase_candles", "SOL-USD", 3600, 2000)
    assert [r[0] for r in newer] == [3000]


def test_limit():
    save_candles("coinbase_candles", "XRP-USD", 3600, CANDLES)
    out = load_candles("coinbase_candles", "XRP-USD", 3600, limit=2)
    assert len(out) == 2
    assert out[-1][0] == 3000


def test_empty_save_noop():
    assert save_candles("coinbase_candles", "ZERO-USD", 3600, []) == 0
    assert load_candles("coinbase_candles", "ZERO-USD", 3600) == []


def test_record_alias():
    assert record("coinbase_candles", "ADA-USD", 3600, CANDLES) == 3


def test_non_ohlcv_records():
    recs = [
        {"ts": 1, "metric": "netflow", "value": -5.2},
        {"ts": 2, "metric": "netflow", "value": 3.1},
    ]
    n = save_records("onchain", "exchange_netflow", recs)
    assert n == 2
    out = load_records("onchain", "exchange_netflow")
    assert len(out) == 2
    assert out[0]["value"] == -5.2
    # append dedup by content is not expected; ensure append works
    save_records("onchain", "exchange_netflow", [{"ts": 3, "metric": "netflow", "value": 0.0}])
    assert len(load_records("onchain", "exchange_netflow")) == 3
