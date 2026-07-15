"""Tests for coinbase/src/api_throttle.py"""
from __future__ import annotations

import threading
import time

from coinbase.src import api_throttle
from coinbase.src.api_throttle import api_slot, active_count, available


def test_api_slot_basic():
    before = active_count()
    with api_slot():
        assert active_count() == before + 1
    assert active_count() == before


def test_active_and_available_counts():
    # No slot held
    assert active_count() >= 0
    assert available() >= 0


def test_api_slot_concurrent_threads():
    held = []

    def worker():
        with api_slot():
            held.append(active_count())

    threads = [threading.Thread(target=worker) for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    # At least some concurrency was observed; no crash
    assert held
    assert active_count() == 0
    assert available() == api_throttle.MAX_CONCURRENT


def test_api_slot_within_body_runs():
    # Ensure code inside the context manager executes
    marker = {"ran": False}
    with api_slot():
        marker["ran"] = True
    assert marker["ran"] is True
