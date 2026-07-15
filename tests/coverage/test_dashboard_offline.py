import os
import tempfile
from unittest import mock

import trading_system.ui.dashboard_server as ds


def test_simple_regime():
    assert ds._simple_regime([100, 100, 100, 100, 101]) == "bull"   # up, low vol
    assert ds._simple_regime([101, 101, 101, 101, 100]) == "bear"   # down, low vol
    assert ds._simple_regime([100, 105, 100, 105, 100]) == "chop"  # flat slope
    assert ds._simple_regime([1]) == "n/a"


def test_watchlist_offline_fallback():
    """When the live feed is down, the watchlist serves from the durable cache (E7)."""
    tmp = tempfile.mkdtemp()
    os.environ["NAS_FEED_ROOT"] = tmp
    import data.feed_cache as fc
    fc._RESOLVED_ROOT = None  # force re-resolution to temp NAS root

    ds._WL_CACHE["data"] = None
    ds._WL_CACHE["ts"] = 0.0

    # seed the durable cache for BTC-USD
    fc.save_candles("coinbase_candles", "BTC-USD", 3600, [
        [1000, 60000, 60100, 59900, 60050, 100],
        [13600, 60050, 60200, 60000, 60100, 120],
    ])

    candles = [
        [1000, 60000, 60100, 59900, 60050, 100],
        [13600, 60050, 60200, 60000, 60100, 120],
    ]
    with mock.patch.object(ds, "_ensure_project_root_on_path", lambda: None), \
         mock.patch("coinbase.src.pair_discovery.top_coinbase_pairs", return_value=[("BTC-USD", "BTC")]), \
         mock.patch("coinbase.src.rest_feed.fetch_candles_batch_sync", side_effect=RuntimeError("down")):
        res = ds.api_market_watchlist(limit_pairs=1)

    assert res["offline"] is True
    assert len(res["watchlist"]) == 1
    row = res["watchlist"][0]
    assert row["symbol"] == "BTC-USD"
    assert row["last"] is not None
    assert row["regime"] in ("bull", "bear", "chop", "n/a")
