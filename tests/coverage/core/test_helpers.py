"""Tests for trading_system.core.utils.helpers."""

import time
from datetime import datetime, timezone
from decimal import Decimal
from unittest.mock import patch

import pytest

from trading_system.core.utils import helpers


def test_now_utc():
    d = helpers.now_utc()
    assert isinstance(d, datetime)
    assert d.tzinfo is timezone.utc


def test_timestamp_ms():
    with patch("trading_system.core.utils.helpers.time.time", return_value=1.5):
        assert helpers.timestamp_ms() == 1500


def test_retry_success_first_try():
    calls = []

    @helpers.retry(max_attempts=3, delay=0.01)
    def ok():
        calls.append(1)
        return "done"

    assert ok() == "done"
    assert len(calls) == 1


def test_retry_success_after_failures():
    calls = []

    @helpers.retry(max_attempts=3, delay=0.001, backoff=2.0)
    def flaky():
        calls.append(1)
        if len(calls) < 2:
            raise ValueError("boom")
        return "recovered"

    with patch("trading_system.core.utils.helpers.time.sleep") as sleep:
        result = flaky()
    assert result == "recovered"
    assert len(calls) == 2
    # sleep called once between the retries, with the current wait (0.001)
    assert sleep.call_count == 1
    sleep.assert_called_with(0.001)


def test_retry_all_failures_raises():
    calls = []

    @helpers.retry(max_attempts=2, delay=0.001, backoff=2.0)
    def always_fail():
        calls.append(1)
        raise RuntimeError("nope")

    with patch("trading_system.core.utils.helpers.time.sleep"):
        with pytest.raises(RuntimeError) as exc:
            always_fail()
    assert "failed after 2 attempts" in str(exc.value)
    assert exc.value.__cause__ is not None
    assert len(calls) == 2


def test_retry_preserves_args():
    @helpers.retry(max_attempts=1)
    def add(a, b):
        return a + b

    assert add(2, 3) == 5


def test_quantize_decimal_default():
    v = Decimal("1.123456789")
    assert helpers.quantize_decimal(v) == Decimal("1.12345679")


def test_quantize_decimal_custom():
    v = Decimal("1.555")
    out = helpers.quantize_decimal(v, Decimal("0.01"))
    assert out == Decimal("1.56")


def test_bps_to_decimal():
    assert helpers.bps_to_decimal(100) == Decimal("0.01")
    assert helpers.bps_to_decimal(1) == Decimal("0.0001")
