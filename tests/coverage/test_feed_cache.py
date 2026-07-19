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


def test_metrics_increment():
    from data.feed_cache import get_metrics, reset_metrics, save_candles, load_candles

    reset_metrics()
    save_candles("coinbase_candles", "METRIC-USD", 3600, CANDLES)
    m = get_metrics()
    assert m["candle_saves"] == 3
    # mem cache hit
    load_candles("coinbase_candles", "METRIC-USD", 3600)
    assert get_metrics()["candle_hits"] == 1
    # different symbol -> disk miss path still counts a miss
    load_candles("coinbase_candles", "MISS-USD", 3600)
    assert get_metrics()["candle_misses"] >= 1


def test_compact_retention():
    from data.feed_cache import save_candles, compact, reset_metrics, get_metrics

    reset_metrics()
    # 1-minute candles: retention is 7 days (10080). Write 20000 to force trim.
    many = [[1000 + i * 60, float(i), float(i), float(i), float(i), 1.0] for i in range(20000)]
    save_candles("coinbase_candles", "COMPACT-USD", 60, many)
    assert get_metrics()["candle_saves"] == 20000
    summary = compact(dry_run=False)
    # the compacted file should have been trimmed to the retention max
    loaded = load_candles("coinbase_candles", "COMPACT-USD", 60)
    assert len(loaded) <= 10080
    assert any("COMPACT-USD" in k for k in summary)


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


def test_inspect_cache_reports_existing_tree_without_creating_or_probing(tmp_path, monkeypatch):
    """Status inspection must be observational: no fallback resolution or writes."""
    import data.feed_cache as fc

    configured = tmp_path / "nas-cache"
    selected = tmp_path / "local-cache"
    (selected / "coinbase_candles" / "BTC-USD").mkdir(parents=True)
    candle = selected / "coinbase_candles" / "BTC-USD" / "3600.parquet"
    candle.write_bytes(b"abc")
    (selected / "news").mkdir()
    (selected / "news" / "wire.jsonl").write_bytes(b"news")
    monkeypatch.setattr(fc, "NAS_FEED_ROOT", str(configured))
    monkeypatch.setattr(fc, "_RESOLVED_ROOT", str(selected))
    monkeypatch.setattr(fc, "ensure_root", lambda: (_ for _ in ()).throw(AssertionError("must not resolve")))

    report = fc.inspect_cache(now=candle.stat().st_mtime + 10)

    assert report["configured_root"] == str(configured)
    assert report["resolved_root"] == str(selected)
    assert report["selected_root"] == str(selected)
    assert report["fallback"] is True
    assert report["readable"] is True
    assert report["totals"] == {"files": 2, "bytes": 7, "truncated": False, "unreadable_entries": 0}
    assert report["kinds"]["coinbase_candles"]["files"] == 1
    assert report["kinds"]["coinbase_candles"]["freshness"] == "fresh"
    assert not configured.exists(), "inspection must not create the configured root"


def test_inspect_cache_missing_or_unavailable_is_unknown_and_bounded(tmp_path, monkeypatch):
    import data.feed_cache as fc

    missing = tmp_path / "missing"
    monkeypatch.setattr(fc, "NAS_FEED_ROOT", str(missing))
    monkeypatch.setattr(fc, "_RESOLVED_ROOT", None)
    report = fc.inspect_cache(max_files=1)

    assert report["selected_root"] == str(missing)
    assert report["readable"] is False
    assert report["status"] == "unknown"
    assert report["kinds"] == {}
    assert not missing.exists()


def test_inspect_cache_caps_streamed_directory_entries_and_regular_files(tmp_path, monkeypatch):
    import data.feed_cache as fc

    root = tmp_path / "cache"
    root.mkdir()
    for index in range(5):
        (root / f"file-{index}.jsonl").write_text("x")
    monkeypatch.setattr(fc, "NAS_FEED_ROOT", str(root))
    monkeypatch.setattr(fc, "_RESOLVED_ROOT", None)

    entry_limited = fc.inspect_cache(max_files=10, max_entries=2)
    file_limited = fc.inspect_cache(max_files=1, max_entries=10)

    assert entry_limited["totals"]["files"] == 2
    assert entry_limited["totals"]["truncated"] is True
    assert entry_limited["status"] == "warn"
    assert entry_limited["truncation_reasons"] == ["directory_entries_limit"]
    assert file_limited["totals"]["files"] == 1
    assert file_limited["totals"]["truncated"] is True
    assert file_limited["status"] == "warn"
    assert file_limited["truncation_reasons"] == ["regular_files_limit"]


def test_inspect_cache_zero_file_limit_records_no_files(tmp_path, monkeypatch):
    import data.feed_cache as fc

    root = tmp_path / "cache"
    root.mkdir()
    (root / "file.jsonl").write_text("x")
    monkeypatch.setattr(fc, "NAS_FEED_ROOT", str(root))
    monkeypatch.setattr(fc, "_RESOLVED_ROOT", None)

    report = fc.inspect_cache(max_files=0, max_entries=10)

    assert report["totals"]["files"] == 0
    assert report["totals"]["truncated"] is True
    assert report["truncation_reasons"] == ["regular_files_limit"]
    assert report["status"] == "warn"


def test_inspect_cache_reports_depth_truncation(tmp_path, monkeypatch):
    import data.feed_cache as fc

    root = tmp_path / "cache"
    deep = root
    for depth in range(14):
        deep = deep / f"level-{depth}"
    deep.mkdir(parents=True)
    (deep / "hidden.jsonl").write_text("x")
    monkeypatch.setattr(fc, "NAS_FEED_ROOT", str(root))
    monkeypatch.setattr(fc, "_RESOLVED_ROOT", None)

    report = fc.inspect_cache(max_files=10, max_entries=100)

    assert report["totals"]["files"] == 0
    assert report["totals"]["truncated"] is True
    assert report["truncation_reasons"] == ["max_depth"]
    assert report["status"] == "warn"


# ── collector normalization (scripts/collect_backtest_data.py) ────────────────

def test_collector_yahoo_normalization():
    import scripts.collect_backtest_data as C
    fake = {
        "chart": {"result": [{
            "timestamp": [1700000000, 1700086400],
            "indicators": {"quote": [{
                "open": [100.0, 101.0],
                "high": [105.0, 106.0],
                "low": [99.0, 100.0],
                "close": [104.0, 105.5],
                "volume": [1000, 1100],
            }]},
        }]},
    }
    # patch the network call
    C._get_json = lambda url, timeout=20: fake
    rows = C._yahoo_candles("AAPL", 30)
    assert len(rows) == 2
    t, o, h, lo, c, v = rows[0]
    assert (o, h, lo, c, v) == (100.0, 105.0, 99.0, 104.0, 1000)
    # writes through feed_cache into the temp root
    saved = C.save_candles("yahoo_candles", "AAPL", 86400, rows)
    assert saved == 2
    assert len(load_candles("yahoo_candles", "AAPL", 86400)) == 2


def test_collector_coingecko_normalization():
    import scripts.collect_backtest_data as C
    fake = {
        "prices": [[1700000000000, 42000.0], [1700086400000, 42100.0]],
        "total_volumes": [[1700000000000, 500.0], [1700086400000, 600.0]],
    }
    C._get_json = lambda url, timeout=20: fake
    rows = C._coingecko_candles("bitcoin", 30)
    assert len(rows) == 2
    t, o, h, lo, c, v = rows[0]
    # CoinGecko prices are ms -> converted to seconds; candle-form collapses OHLC to close
    assert abs(t - 1700000000.0) < 1.0
    assert c == 42000.0
    assert v == 500.0
    saved = C.save_candles("coingecko_candles", "BTC", 86400, rows)
    assert saved == 2


def test_collector_binance_snapshot_shape():
    import scripts.collect_backtest_data as C
    fake = [
        {"symbol": "BTCUSDT", "lastFundingRate": "0.0001",
         "markPrice": "42000.0", "indexPrice": "41999.0"},
        {"symbol": "ETHUSDT", "lastFundingRate": "-0.0002",
         "markPrice": "2200.0", "indexPrice": "2199.0"},
    ]
    C._get_json = lambda url, timeout=20: fake
    snap = C._binance_funding_snapshot()
    assert snap is not None
    assert len(snap["rates"]) == 2
    assert snap["rates"][0]["symbol"] == "BTCUSDT"
    assert snap["rates"][0]["lastFundingRate"] == 0.0001
    n = C.save_records("binance_funding", "premium_index", [snap])
    assert n == 1


def test_backtest_from_cache_reads_harvested_parquet():
    """The backtest loader must read harvested candles from feed_cache and
    return strategy verdicts without any network call."""
    import scripts.backtest_from_cache as B

    # Synthesize ~200 bars of a gentle uptrend for BTC-USD at 1h.
    candles = []
    t0 = 1_700_000_000
    price = 100.0
    for i in range(200):
        price += 0.2
        candles.append([float(t0 + i * 3600), price - 0.5, price + 0.5,
                         price - 1.0, price, 1000.0 + i])
    save_candles("coinbase_candles", "BTC-USD", 3600, candles)

    summary = B.run(B._args_for(granularity=3600, symbols="BTC-USD",
                                strategies="ema_cross,rsi_revert", max_bars=5000))
    assert summary["n_signals"] >= 1
    assert summary["symbols"] == ["BTC-USD"]
    first = summary["results"][0]
    assert "sharpe" in first and "win_rate" in first and "passed" in first


def test_backtest_writes_bt_cache(tmp_path):
    """--bt-cache-db must persist verdicts into a StateStore so ConfidenceMatrix
    can consume them as per-strategy weights."""
    import scripts.backtest_from_cache as B
    from state_store import StateStore

    candles = []
    t0 = 1_700_000_000
    price = 100.0
    for i in range(200):
        price += 0.2
        candles.append([float(t0 + i * 3600), price - 0.5, price + 0.5,
                         price - 1.0, price, 1000.0 + i])
    save_candles("coinbase_candles", "BTC-USD", 3600, candles)

    db = str(tmp_path / "btcache.db")
    summary = B.run(B._args_for(granularity=3600, symbols="BTC-USD",
                                strategies="ema_cross,rsi_revert", max_bars=5000,
                                bt_cache_db=db))
    assert summary["n_signals"] >= 1
    store = StateStore(db_path=db)
    rows = store.load_bt_cache(ttl=10 ** 9)
    assert len(rows) == summary["n_signals"]
    assert any(k.startswith("ema_cross/BTC") for k in rows)
    # the optimizer's ConfidenceMatrix consumes win_rate/sharpe_ratio keys
    v = rows["ema_cross/BTC"]
    assert "win_rate" in v and "sharpe_ratio" in v
