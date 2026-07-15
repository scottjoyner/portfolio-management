"""Tests for trading_system.core.timing (LatencyProfiler, timed, ping, http)."""

import asyncio
import subprocess
import sys
from unittest.mock import MagicMock, patch

import pytest

from trading_system.core import timing
from trading_system.core.timing import (
    timed,
    LatencyProfiler,
    ping_host,
    measure_coinbase_latency,
    http_roundtrip,
)


def test_timed_sync():
    @timed(level=10)
    def add(a, b):
        return a + b

    assert add(2, 3) == 5


def test_timed_async():
    @timed(level=10)
    async def aadd(a, b):
        return a + b

    assert asyncio.run(aadd(2, 3)) == 5


def test_latency_profiler_measure_and_record():
    p = LatencyProfiler()
    with p.measure("stage_a"):
        pass
    p.record("stage_b", 2.5)
    summary = p.summary()
    assert "stage_a" in summary
    assert "stage_b" in summary
    assert summary["stage_a"]["count"] == 1


def test_latency_profiler_record_tick():
    p = LatencyProfiler()
    # first tick has no previous timestamp
    p.record_tick()
    assert p.summary().get("_tick") is None
    # second tick creates tick durations
    p.record_tick()
    assert "_tick" in p.summary()


def test_latency_profiler_stats_empty():
    stats = LatencyProfiler._stats([])
    assert stats == {"count": 0, "min": 0, "max": 0, "avg": 0, "p50": 0, "p95": 0, "p99": 0}


def test_latency_profiler_stats_nonempty():
    vals = [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0]
    stats = LatencyProfiler._stats(vals)
    assert stats["count"] == 10
    assert stats["min"] == 1.0
    assert stats["max"] == 10.0
    assert stats["avg"] == 5.5
    assert stats["p50"] == vals[5]


def test_latency_profiler_exceeds_history():
    p = LatencyProfiler(max_history=2)
    for i in range(5):
        p.record("s", float(i))
    # buffer exceeded max_history -> buf.pop(0) executed
    assert len(p._stages["s"]) == 2
    for _ in range(5):
        p.record_tick()
    # tick duration buffer also capped at max_history
    assert len(p._tick_durations) == 2


def _fake_run(stdout):
    res = MagicMock()
    res.stdout = stdout
    return res


def test_ping_host_standard():
    with patch("trading_system.core.timing.subprocess.run", return_value=_fake_run(
        "rtt min/avg/max/mdev = 1.2/2.3/3.4/0.5 ms\n"
    )):
        r = ping_host("api.coinbase.com")
    assert r["min_ms"] == 1.2
    assert r["avg_ms"] == 2.3
    assert r["max_ms"] == 3.4
    assert r["mdev_ms"] == 0.5


def test_ping_host_busybox():
    with patch("trading_system.core.timing.subprocess.run", return_value=_fake_run(
        "rtt min/avg/max = 1.2/2.3/3.4 ms\n"
    )):
        r = ping_host("h")
    assert r["mdev_ms"] == 3.4


def test_ping_host_macos():
    with patch("trading_system.core.timing.subprocess.run", return_value=_fake_run(
        "round-trip min/avg/max/stddev = 1.2/2.3/3.4/0.5 ms\n"
    )):
        r = ping_host("h")
    assert r["max_ms"] == 3.4


def test_ping_host_too_few_parts_continues():
    with patch("trading_system.core.timing.subprocess.run", return_value=_fake_run(
        "rtt min/avg/max/mdev = 1.2 ms\nno other line\n"
    )):
        r = ping_host("h")
    assert r["error"] == "no ping stats found"


def test_ping_host_no_stats():
    with patch("trading_system.core.timing.subprocess.run", return_value=_fake_run(
        "totally unrelated output\n"
    )):
        r = ping_host("h")
    assert r["error"] == "no ping stats found"


def test_ping_host_exception():
    with patch("trading_system.core.timing.subprocess.run", side_effect=Exception("boom")):
        r = ping_host("h")
    assert r["error"] == "boom"


def test_measure_coinbase_latency():
    with patch("trading_system.core.timing.ping_host", return_value={"host": "h", "min_ms": 1.0}):
        res = measure_coinbase_latency()
    assert len(res) == 3
    assert res[0]["host"] == "h"


def test_http_roundtrip_success():
    resp = MagicMock()
    resp.status = 200
    resp.read.return_value = b"hello"
    with patch("urllib.request.urlopen", return_value=resp):
        r = http_roundtrip("https://example.com")
    assert r["status"] == 200
    assert r["bytes"] == 5
    assert r["rtt_ms"] >= 0


def test_http_roundtrip_error():
    with patch(
        "urllib.request.urlopen",
        side_effect=Exception("net down"),
    ):
        r = http_roundtrip("https://example.com")
    assert r["error"] == "net down"
    assert r["rtt_ms"] >= 0
