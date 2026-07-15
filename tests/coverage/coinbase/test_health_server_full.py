"""Coverage tests for coinbase/src/health_server.py"""
from __future__ import annotations

import json
import socket
import urllib.request
from types import SimpleNamespace

from coinbase.src import health_server


def _free_port():
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    p = s.getsockname()[1]
    s.close()
    return p


def test_health_server_get_and_options():
    port = _free_port()
    calls = {"n": 0}

    def status_fn():
        calls["n"] += 1
        return {"status": "ok", "value": 1}

    hs = health_server.HealthServer(port, status_fn, name="t")
    try:
        hs.start()
        with urllib.request.urlopen(f"http://127.0.0.1:{port}/", timeout=5) as r:
            body = json.loads(r.read())
        assert body == {"status": "ok", "value": 1}
        assert calls["n"] == 1

        req = urllib.request.Request(f"http://127.0.0.1:{port}/x", method="OPTIONS")
        with urllib.request.urlopen(req, timeout=5) as r:
            assert r.status == 200
    finally:
        hs.stop()


def test_health_server_status_error():
    port = _free_port()

    def bad():
        raise RuntimeError("boom")

    hs = health_server.HealthServer(port, bad, name="t2")
    try:
        hs.start()
        with urllib.request.urlopen(f"http://127.0.0.1:{port}/", timeout=5) as r:
            body = json.loads(r.read())
        assert body["status"] == "error"
        assert "boom" in body["detail"]
    finally:
        hs.stop()


def test_health_server_start_idempotent_and_stop_none():
    hs = health_server.HealthServer(1, lambda: {})
    hs._server = "not none"
    hs.start()  # early return because already started
    hs._server = None
    hs.stop()  # nothing happens, no error


def test_build_optimizer_status_with_feed():
    state = SimpleNamespace(holdings=[1, 2, 3], total_value=123.4)
    feed = SimpleNamespace(running=True, stats=lambda: {"a": 1})
    opt = SimpleNamespace(
        state=state,
        running=True,
        dry_run=False,
        _health_alerts=[],
        _tick_count=7,
        _last_tick_ts=1.1,
        _last_detected_opportunities=[1],
        _feed_mgr=feed,
        _start_ts=0.0,
    )
    out = health_server.build_optimizer_status(opt)
    assert out["status"] == "running"
    assert out["holdings"] == 3
    assert out["total_value"] == 123.4
    assert out["smart_feed_active"] is True
    assert out["smart_feed_stats"] == {"a": 1}


def test_build_optimizer_status_no_feed():
    state = SimpleNamespace(holdings=[], total_value=0.0)
    opt = SimpleNamespace(
        state=state,
        running=False,
        dry_run=True,
        _health_alerts=["x"],
        _tick_count=0,
        _last_tick_ts=0.0,
        _last_detected_opportunities=[],
        _feed_mgr=None,
        _start_ts=1.0,
    )
    out = health_server.build_optimizer_status(opt)
    assert out["status"] == "stopped"
    assert out["health_ok"] is False
    assert out["smart_feed_active"] is False
    assert out["smart_feed_stats"] is None
