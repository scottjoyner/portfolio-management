import urllib.error
import urllib.request

import pytest

from trading_system.apps.ui import main


def test_health():
    assert main.health() == {"status": "ok", "service": "trading-ui"}


def test_esc_and_badge():
    assert main.esc("<x>") == "&lt;x&gt;"
    b = main.badge(True)
    assert "good" in b and "OK" in b
    b2 = main.badge(False, truthy_label="Y", falsey_label="N")
    assert "warn" in b2 and "N" in b2
    # bool() edge
    assert "good" in main.badge(1)
    assert "warn" in main.badge(0)


def test_render_kv():
    assert "<tr>" in main.render_kv({"a": 1, "b": "x"})


def test_render_balances_empty_and_full():
    empty = main.render_balances([])
    assert "No balances" in empty
    full = main.render_balances([
        {"currency": "USD", "available": 10, "hold": 0, "name": "c"},
    ])
    assert "USD" in full


def test_render_strategies_empty_and_full():
    assert "No strategies" in main.render_strategies([])
    assert "strat" in main.render_strategies([
        {"name": "strat", "category": "c", "status": "s", "mode": "m"},
    ])
    # fallbacks
    assert "No strategies" not in main.render_strategies([{}])


def test_render_events_empty_and_full():
    assert "No events" in main.render_events([])
    ev = main.render_events([
        {"timestamp": "t", "source": "s", "event_type": "e", "payload": {"k": 1}},
    ])
    assert "k" in ev
    assert "No events" not in main.render_events([{}])


def _fake_response(payload):
    class R:
        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

        def read(self):
            return payload.encode("utf-8")
    return R()


def test_fetch_json_success(monkeypatch):
    monkeypatch.setattr(
        urllib.request, "urlopen", lambda *a, **k: _fake_response('{"connected": true}')
    )
    assert main.fetch_json("/x") == {"connected": True}


def test_fetch_json_error(monkeypatch):
    def boom(*a, **k):
        raise urllib.error.URLError("nope")
    monkeypatch.setattr(urllib.request, "urlopen", boom)
    out = main.fetch_json("/x")
    assert "error" in out


def test_dashboard(monkeypatch):
    def fake_urlopen(url, timeout=10):
        if "balances" in str(url):
            return _fake_response('{"accounts": [{"currency": "USD", "available": 1, "hold": 0, "name": "c"}]}')
        if "catalog" in str(url):
            return _fake_response('{"strategies": [{"name": "s", "category": "c", "status": "ok", "mode": "paper"}]}')
        if "events" in str(url):
            return _fake_response('{"events": [{"timestamp": "t", "source": "s", "event_type": "e", "payload": {}}]}')
        return _fake_response('{"mode": "paper", "live_trading_enabled": true, "coinbase_connected": true, "worker_status": "up", "event_log_status": "avail", "timestamp": "now", "connected": true, "environment": "prod", "account_count": 1, "error": null}')
    monkeypatch.setattr(urllib.request, "urlopen", fake_urlopen)
    resp = main.dashboard()
    assert isinstance(resp, main.HTMLResponse)
    assert "Trading Network Dashboard" in resp.body.decode("utf-8")
