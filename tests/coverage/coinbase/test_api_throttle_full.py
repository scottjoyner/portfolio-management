"""Coverage tests for coinbase/src/api_throttle.py"""
from __future__ import annotations

import time

from coinbase.src import api_throttle


def test_api_slot_waits_and_counts(monkeypatch):
    clock = {"t": 0.0}

    class FakeTime:
        def time(self):
            return clock["t"]

        def sleep(self, s):
            clock["t"] += s

    ft = FakeTime()
    monkeypatch.setattr(api_throttle, "time", ft)
    api_throttle._active = 0
    api_throttle._last_req_ts = 0.0

    # First call: gap is 0 so wait>0 -> sleep branch taken.
    with api_throttle.api_slot():
        pass
    assert api_throttle.active_count() == 0

    # Advance clock so the next call has negative wait -> no sleep branch.
    clock["t"] = 100.0
    with api_throttle.api_slot():
        assert api_throttle.active_count() == 1
    assert api_throttle.active_count() == 0
    assert api_throttle.available() == api_throttle.MAX_CONCURRENT


def test_active_and_available():
    before = api_throttle.active_count()
    assert before >= 0
    assert api_throttle.available() == max(0, api_throttle.MAX_CONCURRENT - before)
