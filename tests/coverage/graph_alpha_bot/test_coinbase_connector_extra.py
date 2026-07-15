from datetime import datetime, timedelta
from app.connectors.coinbase_connector import (
    CircuitBreaker, CircuitBreakerConfig, CircuitBreakerState, CoinbaseConnector,
)


def test_record_success_closed_state():
    cb = CircuitBreaker(CircuitBreakerConfig())
    cb.record_success()  # not half-open -> just increments
    assert cb.success_count == 1
    assert cb.state == CircuitBreakerState.CLOSED


def test_execute_order_non_buy_side():
    c = CoinbaseConnector()
    res = c.execute_order("BTC-USD", "SELL", 0.1)
    # No SELL handling: returns filled dict without modifying portfolio
    assert res["status"] == "filled"


def test_record_failure_closed_below_threshold():
    cb = CircuitBreaker(CircuitBreakerConfig(failure_threshold=5))
    cb.record_failure(ValueError("x"))
    assert cb.state == CircuitBreakerState.CLOSED
    assert cb.failure_count == 1


def test_record_success_half_open_below_threshold():
    cb = CircuitBreaker(CircuitBreakerConfig(failure_threshold=1, success_threshold=5))
    cb.record_failure()  # opens (threshold 1)
    cb.opened_at = datetime.utcnow() - timedelta(seconds=1000)
    assert cb.is_closed() is True  # -> half open
    cb.record_success()  # success_count=1 < 5 -> stays half open
    assert cb.state == CircuitBreakerState.HALF_OPEN
    assert cb.success_count == 1


def test_record_success_half_open_closes():
    cb = CircuitBreaker(CircuitBreakerConfig(failure_threshold=1, success_threshold=2))
    cb.record_failure()  # opens
    cb.opened_at = datetime.utcnow() - timedelta(seconds=1000)
    assert cb.is_closed() is True  # half open
    cb.record_success()  # 1 < 2
    cb.record_success()  # 2 >= 2 -> _close
    assert cb.state == CircuitBreakerState.CLOSED


def test_record_failure_half_open_closes():
    cb = CircuitBreaker(CircuitBreakerConfig(failure_threshold=1))
    cb.record_failure()  # opens
    cb.opened_at = datetime.utcnow() - timedelta(seconds=1000)
    assert cb.is_closed() is True  # half open
    cb.record_failure()  # half open -> _close
    assert cb.state == CircuitBreakerState.CLOSED


def test_record_failure_open_state():
    cb = CircuitBreaker(CircuitBreakerConfig(failure_threshold=1))
    cb.record_failure()  # opens (opened_at recent -> stays OPEN)
    cb.record_failure()  # state is OPEN -> neither HALF_OPEN nor CLOSED -> no change
    assert cb.state == CircuitBreakerState.OPEN


def test_execute_order_existing_position():
    c = CoinbaseConnector()
    c.execute_order("BTC-USD", "BUY", 0.1)
    c.execute_order("BTC-USD", "BUY", 0.1)  # position already exists -> skip init
    assert c.positions["BTC-USD"]["quantity"] == 0.7
