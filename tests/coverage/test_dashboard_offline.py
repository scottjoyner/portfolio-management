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
    with mock.patch.object(ds, "_fetch_candles_batch_http", side_effect=RuntimeError("down")):
        res = ds.api_market_watchlist(limit_pairs=1)

    assert res["offline"] is True
    assert len(res["watchlist"]) == 1
    row = res["watchlist"][0]
    assert row["symbol"] == "BTC-USD"
    assert row["last"] is not None
    assert row["regime"] in ("bull", "bear", "chop", "n/a")


def test_system_truth_contract_mode_precedence_and_fail_closed(tmp_path, monkeypatch):
    paper = tmp_path / "paper.json"
    paper.write_text('{"paper_cash": 10000}')
    heartbeat = tmp_path / ".heartbeat"
    heartbeat.write_text(str(time.time() - 10))
    health = tmp_path / "system-health.json"
    health.write_text('{"services": {"collector": "ok"}}')
    monkeypatch.setattr(ds, "PAPER_TRADER_PATH", str(paper))
    monkeypatch.setattr(ds, "SYSTEM_HEALTH_PATH", str(health))
    monkeypatch.setattr(ds, "DAEMON_HEARTBEAT_PATH", str(heartbeat))
    monkeypatch.setattr(ds, "_probe_trader_health", lambda: {"available": True, "mode": "live", "source": "trader_health"})
    monkeypatch.setattr(ds, "_inspect_feed_cache", lambda: {"status": "unknown", "source": "feed_cache"})

    truth = ds.api_system_truth()

    assert set(truth) == {"generated_at", "trading_mode", "feed", "cache", "services", "exposure", "terminal", "warnings"}
    assert truth["trading_mode"]["value"] == "unknown"
    assert truth["trading_mode"]["source"] == "mode_conflict"
    assert truth["trading_mode"]["status"] == "warn"
    assert truth["feed"]["heartbeat"]["freshness"] == "fresh"
    assert truth["services"]["snapshot"]["freshness"] == "fresh"
    assert truth["cache"]["source"] == "feed_cache"


def test_system_truth_missing_or_stale_evidence_is_unknown_or_warning(tmp_path, monkeypatch):
    heartbeat = tmp_path / ".heartbeat"
    heartbeat.write_text(str(time.time() - ds.FRESHNESS_STALE_SECONDS - 1))
    health = tmp_path / "system-health.json"
    health.write_text('{"status": "ok"}')
    old = time.time() - ds.FRESHNESS_STALE_SECONDS - 1
    os.utime(health, (old, old))
    monkeypatch.setattr(ds, "PAPER_TRADER_PATH", str(tmp_path / "absent-paper.json"))
    monkeypatch.setattr(ds, "SYSTEM_HEALTH_PATH", str(health))
    monkeypatch.setattr(ds, "DAEMON_HEARTBEAT_PATH", str(heartbeat))
    monkeypatch.setattr(ds, "_probe_trader_health", lambda: {"available": False, "source": "trader_health"})
    monkeypatch.setattr(ds, "_inspect_feed_cache", lambda: {"status": "unknown", "source": "feed_cache"})

    truth = ds.api_system_truth()

    assert truth["trading_mode"]["value"] == "unknown"
    assert truth["feed"]["heartbeat"]["freshness"] == "stale"
    assert truth["services"]["snapshot"]["freshness"] == "stale"
    assert truth["warnings"]


def test_system_truth_uses_persisted_paper_state_when_local_health_is_unavailable(tmp_path, monkeypatch):
    paper = tmp_path / "paper.json"
    paper.write_text('{"paper_cash": 10000}')
    monkeypatch.setattr(ds, "PAPER_TRADER_PATH", str(paper))
    monkeypatch.setattr(ds, "urlopen", mock.Mock(side_effect=OSError("local health unavailable")))
    monkeypatch.setattr(ds, "_inspect_feed_cache", lambda: {"status": "ok", "readable": True, "source": "feed_cache"})

    truth = ds.api_system_truth()

    assert truth["trading_mode"] == {"value": "paper", "source": "persisted_paper_state"}


def test_system_truth_reachable_invalid_mode_fails_closed_despite_paper_evidence(tmp_path, monkeypatch):
    paper = tmp_path / "paper.json"
    paper.write_text('{"paper_cash": 10000}')
    monkeypatch.setattr(ds, "PAPER_TRADER_PATH", str(paper))
    monkeypatch.setattr(ds, "_probe_trader_health", lambda: {
        "available": True, "mode": None, "source": "trader_health", "payload": {"mode": "sandbox"},
    })
    monkeypatch.setattr(ds, "_inspect_feed_cache", lambda: {"status": "ok", "readable": True, "source": "feed_cache"})

    truth = ds.api_system_truth()

    assert truth["trading_mode"]["value"] == "unknown"
    assert truth["trading_mode"]["status"] == "warn"
    assert "trader health did not provide a recognized mode" in truth["warnings"]


def test_system_truth_reachable_mode_conflicting_with_paper_evidence_fails_closed(tmp_path, monkeypatch):
    paper = tmp_path / "paper.json"
    paper.write_text('{"paper_cash": 10000}')
    monkeypatch.setattr(ds, "PAPER_TRADER_PATH", str(paper))
    monkeypatch.setattr(ds, "_probe_trader_health", lambda: {
        "available": True, "mode": "live", "source": "trader_health",
    })
    monkeypatch.setattr(ds, "_inspect_feed_cache", lambda: {"status": "ok", "readable": True, "source": "feed_cache"})

    truth = ds.api_system_truth()

    assert truth["trading_mode"]["value"] == "unknown"
    assert truth["trading_mode"]["status"] == "warn"
    assert "trader health mode conflicts with persisted paper state" in truth["warnings"]


def test_system_truth_terminal_url_rejects_unsafe_schemes(monkeypatch):
    monkeypatch.setenv("TRADING_TERMINAL_URL", "javascript:alert(1)")
    monkeypatch.setattr(ds, "_probe_trader_health", lambda: {"available": False, "source": "trader_health"})
    monkeypatch.setattr(ds, "_inspect_feed_cache", lambda: {"status": "ok", "readable": True, "source": "feed_cache"})

    truth = ds.api_system_truth()

    assert truth["terminal"]["url"] == "/dashboard"
    assert truth["terminal"]["status"] == "warn"
    assert any("unsafe terminal URL" in warning for warning in truth["warnings"])


def test_safe_terminal_url_contract():
    assert ds._safe_terminal_url("/terminal") == "/terminal"
    assert ds._safe_terminal_url("https://terminal.example/trade") == "https://terminal.example/trade"
    assert ds._safe_terminal_url("javascript:alert(1)") is None
    assert ds._safe_terminal_url("//terminal.example/trade") is None


def test_trader_health_probe_is_fixed_to_local_loopback(monkeypatch):
    captured = {}

    class Response:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

        def read(self):
            return b'{"mode": "paper"}'

    def fake_urlopen(request, timeout):
        captured["url"] = request.full_url
        captured["timeout"] = timeout
        return Response()

    monkeypatch.setenv("TRADER_HEALTH_URL", "https://remote.example/health")
    monkeypatch.setattr(ds, "urlopen", fake_urlopen)

    result = ds._probe_trader_health()

    assert result["mode"] == "paper"
    assert captured == {"url": "http://127.0.0.1:9090/health", "timeout": 0.25}


def test_dashboard_system_truth_strip_static_contract():
    """The dashboard must expose every server-truth cell and poll its endpoint."""
    page = Path(ds.__file__).with_name("dashboard.html").read_text()

    assert 'api("/system-truth")' in page
    for element_id in (
        "system-truth",
        "truth-mode",
        "truth-trader",
        "truth-feed",
        "truth-cache",
        "truth-services",
        "truth-exposure",
        "truth-terminal",
    ):
        assert f'id="{element_id}"' in page
    assert "function renderSystemTruth" in page
    assert "truth.trading_mode" in page
    assert "loadSystemTruth()," in page
    assert "acc.accounts[0].mode" not in page
    assert "function safeTerminalUrl" in page
    assert "link.href = safeTerminalUrl(terminalHref);" in page


def test_dashboard_chart_uses_default_uplot_path_and_never_overrides_dark_theme():
    """The operator terminal must render its fetched candles on a dark surface."""
    page = Path(ds.__file__).with_name("dashboard.html").read_text()

    assert "paths: u =>" not in page
    assert "@media (prefers-color-scheme: light)" not in page
    assert "--bg:#0a0e14" in page
    assert 'stroke: "#4ea1ff"' in page
    assert "if (!uplot) { initChart(); }" in page
