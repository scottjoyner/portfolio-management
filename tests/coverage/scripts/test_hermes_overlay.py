from __future__ import annotations

import datetime as dt
import importlib
import json
import sys
from pathlib import Path
from unittest import mock

import pytest

REPO = Path(__file__).resolve().parent.parent.parent.parent
sys.path.insert(0, str(REPO))

import scripts.hermes_overlay as ov


def test_sentiment_bias_buckets():
    assert ov.sentiment_bias({"bucket": "EXTREME_FEAR"}) == 1.2
    assert ov.sentiment_bias({"bucket": "FEAR"}) == 1.05
    assert ov.sentiment_bias({"bucket": "NEUTRAL"}) == 1.0
    assert ov.sentiment_bias({"bucket": "GREED"}) == 0.85
    assert ov.sentiment_bias({"bucket": "EXTREME_GREED"}) == 0.5
    assert ov.sentiment_bias({"bucket": "UNKNOWN"}) == 0.9
    assert ov.sentiment_bias({"bucket": "NOPE"}) == 0.9


def test_sentiment_bias_monotonic_fade():
    vals = [ov.sentiment_bias({"bucket": b})
            for b in ["EXTREME_FEAR", "FEAR", "NEUTRAL", "GREED", "EXTREME_GREED"]]
    assert vals == sorted(vals, reverse=True)


def test_fear_greed_offline_fallback():
    with mock.patch.object(ov.urllib.request, "urlopen",
                           side_effect=OSError("offline")):
        fg = ov.fear_greed()
    assert fg["ok"] is False
    assert fg["bucket"] == "UNKNOWN"
    assert fg["value"] is None


def test_fear_greed_buckets():
    samples = [
        (10, "EXTREME_FEAR"), (20, "EXTREME_FEAR"),
        (30, "FEAR"), (40, "FEAR"),
        (50, "NEUTRAL"), (60, "NEUTRAL"),
        (70, "GREED"), (80, "GREED"),
        (90, "EXTREME_GREED"),
    ]
    for val, expected in samples:
        body = json.dumps({"data": [{"value": str(val),
                                     "value_classification": expected}]}).encode()
        with mock.patch.object(ov.urllib.request, "urlopen") as u:
            u.return_value.__enter__.return_value.read.return_value = body
            fg = ov.fear_greed()
        assert fg["ok"] is True
        assert fg["value"] == val
        assert fg["bucket"] == expected


def test_event_risk_first_friday():
    now = dt.datetime(2026, 2, 6, 14, 0, tzinfo=dt.timezone.utc)  # 1st Friday
    ev = ov.event_risk(now)
    assert ev["elevated"] is True
    assert ev["reason"] == "NFP_Friday"
    assert ev["block_new"] is False


def test_event_risk_fomc_wednesday():
    now = dt.datetime(2026, 2, 4, 14, 0, tzinfo=dt.timezone.utc)  # 1st Wed
    ev = ov.event_risk(now)
    assert ev["elevated"] is True
    assert ev["reason"] == "FOMC_Wednesday"


def test_event_risk_quiet_day():
    now = dt.datetime(2026, 2, 10, 14, 0, tzinfo=dt.timezone.utc)  # Tuesday w2
    ev = ov.event_risk(now)
    assert ev["elevated"] is False
    assert ev["reason"] == "none"


def test_overlay_state_shrinks_on_macro():
    with mock.patch.object(ov, "fear_greed",
                           return_value={"bucket": "NEUTRAL", "ok": True}):
        normal = ov.overlay_state()
    fg_fear = {"bucket": "FEAR", "ok": True}
    ev_first_fri = {"elevated": True, "block_new": False,
                     "reason": "NFP_Friday", "utc_hour": 14}
    with mock.patch.object(ov, "fear_greed", return_value=fg_fear), \
         mock.patch.object(ov, "event_risk", return_value=ev_first_fri):
        elevated = ov.overlay_state()
    assert elevated["size_mult"] < normal["size_mult"]


def test_overlay_state_stand_down_extreme_greed_plus_macro():
    fg = {"bucket": "EXTREME_GREED", "ok": True}
    ev = {"elevated": True, "block_new": False, "reason": "NFP_Friday",
          "utc_hour": 14}
    with mock.patch.object(ov, "fear_greed", return_value=fg), \
         mock.patch.object(ov, "event_risk", return_value=ev):
        st = ov.overlay_state()
    assert st["stand_down_new"] is True
    assert st["size_mult"] <= 0.5 * 0.7 + 1e-9
