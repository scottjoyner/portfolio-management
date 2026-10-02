"""Offline/dashboard behaviour tests.

Removed 2026-10-01: the eight `test_system_truth_*`, `test_safe_terminal_url_contract`
and `test_trader_health_probe_*` cases. They tested the "portfolio system truth
layer", added in 85d2da22 and removed in 295b6380. The module they drove
(`api_system_truth`, `_safe_terminal_url`, `_probe_trader_health`,
`PAPER_TRADER_PATH`, `FRESHNESS_STALE_SECONDS`, `_inspect_feed_cache`) no longer
exists anywhere in the repository, and `api_system_truth` was referenced only by
this file. They could not be repaired -- there is nothing left to test.

Also removed: `test_dashboard_system_truth_strip_static_contract`, which asserted
the same layer's markup. dashboard.html contains zero references to system-truth,
truth-mode, truth-terminal or renderSystemTruth, so the UI went with the module.

RECORDED, because it is a security requirement that nothing currently enforces:
`_safe_terminal_url` rejected unsafe schemes on the operator-facing terminal URL --
`javascript:alert(1)` and protocol-relative `//host/path` both had to resolve to
None, falling back to "/dashboard" with a warning. The sink is gone, so there is no
live vulnerability today. But anyone re-introducing a terminal URL from
configuration or an environment variable must restore that sanitisation; a
`javascript:` value rendered into the dashboard is script injection. The same
sanitisation existed client-side as a `safeTerminalUrl` helper in dashboard.html;
that is gone too, so re-introducing a terminal URL means restoring both.

The remaining tests cover live behaviour: the simple regime classifier, the
durable-cache watchlist fallback, and static contracts in dashboard.html.
"""

import os
import re
import os
import tempfile
import time
from pathlib import Path
from unittest import mock

import trading_system.ui.dashboard_server as ds


def test_simple_regime():
    assert ds._simple_regime([100, 100, 100, 100, 101]) == "bull"   # up, low vol
    assert ds._simple_regime([101, 101, 101, 101, 100]) == "bear"   # down, low vol
    assert ds._simple_regime([100, 105, 100, 105, 100]) == "chop"  # flat slope
    assert ds._simple_regime([1]) == "n/a"


def test_watchlist_offline_fallback(monkeypatch):
    """When the live feed is down, the watchlist serves from the durable cache (E7)."""
    tmp = tempfile.mkdtemp()
    monkeypatch.setenv("NAS_FEED_ROOT", tmp)
    import data.feed_cache as fc
    # The module reads its configured root at import time; make this test's
    # cache root explicit so it remains isolated when run with cache tests.
    monkeypatch.setattr(fc, "NAS_FEED_ROOT", tmp)
    monkeypatch.setattr(fc, "_RESOLVED_ROOT", None)  # force re-resolution to temp NAS root

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
    # The live fetch moved to coinbase.src.rest_feed; api_market_watchlist imports
    # fetch_candles_batch_sync from it inside the function. Patch it there, since
    # the previous module-local _fetch_candles_batch_http no longer exists.
    with mock.patch("coinbase.src.rest_feed.fetch_candles_batch_sync",
                    side_effect=RuntimeError("down")):
        res = ds.api_market_watchlist(limit_pairs=1)

    assert res["offline"] is True
    assert len(res["watchlist"]) == 1
    row = res["watchlist"][0]
    assert row["symbol"] == "BTC-USD"
    assert row["last"] is not None
    assert row["regime"] in ("bull", "bear", "chop", "n/a")


def test_dashboard_terminal_renders_on_a_dark_surface():
    """The operator terminal must not follow the OS light theme.

    Kept from a test that also asserted uPlot configuration. The chart library has
    since been replaced -- dashboard.html no longer references uPlot at all -- so the
    uPlot-specific assertions were removed rather than mechanically rewritten onto
    the new implementation, which would have asserted trivia.

    What survives is the intent and it is library-independent: the terminal is
    always dark, never themed by prefers-color-scheme, and the accent colour is
    actually applied to a stroke.
    """
    page = Path(ds.__file__).with_name("dashboard.html").read_text()

    # Never follow the OS into a light theme; the terminal is a dark surface.
    assert "@media (prefers-color-scheme: light)" not in page
    assert "--bg:#0a0e14" in page

    # The chart is drawn, in the page's own accent colour.
    assert "renderChart" in page
    accent = re.search(r"--accent:\s*(#[0-9a-fA-F]{3,8})", page)
    assert accent, "the page must define an --accent colour"
    accent_hex = accent.group(1)
    assert re.search(rf"stroke(?:Style)?\s*[=:]\s*[\"']{re.escape(accent_hex)}[\"']", page), (
        f"the chart must draw in the accent colour {accent_hex}")
