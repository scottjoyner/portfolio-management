"""Tests for coinbase/src/health_server.py"""
from __future__ import annotations

from types import SimpleNamespace

import pytest

from coinbase.src.health_server import HealthServer, build_optimizer_status


def test_health_server_start_stop():
    calls = {"n": 0}

    def status_fn():
        calls["n"] += 1
        return {"ok": True}

    srv = HealthServer(port=18080, status_fn=status_fn, name="test")
    srv.start()
    # start is idempotent
    srv.start()
    assert srv._server is not None
    srv.stop()
    assert srv._server is None


def test_health_server_start_bind_fail():
    # Use a port likely unavailable by double-binding
    def status_fn():
        return {}

    srv = HealthServer(port=18081, status_fn=status_fn, name="t")
    srv.start()
    srv2 = HealthServer(port=18081, status_fn=status_fn, name="t2")
    # If first still holds the port, second may warn; just ensure no crash
    srv2.start()
    srv.stop()
    srv2.stop()


def _fake_optimizer():
    state = SimpleNamespace(
        holdings=[1, 2, 3],
        total_value=1234.56,
    )
    feed = SimpleNamespace(running=True, stats=lambda: {"a": 1})
    return SimpleNamespace(
        state=state,
        running=True,
        dry_run=True,
        _health_alerts=[],
        _tick_count=7,
        _last_tick_ts=1.0,
        _last_detected_opportunities=[1, 2],
        _feed_mgr=feed,
        _start_ts=0.0,
    )


def test_build_optimizer_status():
    opt = _fake_optimizer()
    status = build_optimizer_status(opt)
    assert status["status"] == "running"
    assert status["health_ok"] is True
    assert status["holdings"] == 3
    assert status["total_value"] == 1234.56
    assert status["smart_feed_active"] is True
    assert status["smart_feed_stats"] == {"a": 1}


def test_build_optimizer_status_minimal():
    opt = SimpleNamespace(
        state=None, running=False, dry_run=False,
        _health_alerts=["x"], _tick_count=0, _last_tick_ts=0.0,
        _last_detected_opportunities=None, _feed_mgr=None, _start_ts=0.0,
    )
    status = build_optimizer_status(opt)
    assert status["status"] == "stopped"
    assert status["health_ok"] is False
    assert status["holdings"] == 0
