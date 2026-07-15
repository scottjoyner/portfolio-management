from datetime import datetime, timedelta
from app.connectors.coinbase_connector import (
    CircuitBreaker, CircuitBreakerConfig, CircuitBreakerState,
    PriceFetcher, CoinbaseConnector,
)


def test_price_fetcher_known():
    pf = PriceFetcher()
    assert pf.get_price("BTC-USD") == 68500.0
    assert pf.get_price("UNKNOWN-USD") is None


def test_circuit_breaker_closed_initially():
    cb = CircuitBreaker(CircuitBreakerConfig())
    assert cb.is_closed() is True


def test_circuit_breaker_opens_after_threshold():
    cb = CircuitBreaker(CircuitBreakerConfig(failure_threshold=2))
    cb.record_failure(ValueError("x"))
    assert cb.is_closed() is True
    cb.record_failure(ValueError("y"))
    assert cb.state == CircuitBreakerState.OPEN
    assert cb.is_closed() is False


def test_circuit_breaker_half_open_after_timeout():
    cb = CircuitBreaker(CircuitBreakerConfig(failure_threshold=2, timeout_seconds=600))
    cb.record_failure()
    cb.record_failure()
    assert cb.state == CircuitBreakerState.OPEN
    # force opened_at into the past so the timeout has elapsed
    cb.opened_at = datetime.utcnow() - timedelta(seconds=1000)
    assert cb.is_closed() is True
    assert cb.state == CircuitBreakerState.HALF_OPEN


def test_circuit_breaker_half_open_closes_on_success():
    cb = CircuitBreaker(CircuitBreakerConfig(timeout_seconds=600, success_threshold=2))
    cb.record_failure()
    cb.record_failure()
    cb.opened_at = datetime.utcnow() - timedelta(seconds=1000)
    cb.is_closed()  # -> half open
    cb.record_success()
    cb.record_success()
    assert cb.state == CircuitBreakerState.CLOSED


def test_circuit_breaker_half_open_closes_on_failure():
    cb = CircuitBreaker(CircuitBreakerConfig(timeout_seconds=600))
    cb.record_failure()
    cb.record_failure()
    cb.opened_at = datetime.utcnow() - timedelta(seconds=1000)
    cb.is_closed()  # half open
    cb.record_failure()
    assert cb.state == CircuitBreakerState.CLOSED


def test_circuit_breaker_reset():
    cb = CircuitBreaker(CircuitBreakerConfig())
    cb.failure_count = 3
    cb.reset()
    assert cb.failure_count == 0
    assert cb.state == CircuitBreakerState.CLOSED


def _open_breaker(c):
    c.circuit_breaker.state = CircuitBreakerState.OPEN
    c.circuit_breaker.opened_at = datetime.utcnow()  # recent -> stays open


def test_connector_portfolio_value_blocked_when_open():
    c = CoinbaseConnector()
    _open_breaker(c)
    assert c.get_portfolio_value() == {"error": "Circuit breaker open"}


def test_connector_portfolio_value():
    c = CoinbaseConnector()
    val = c.get_portfolio_value()
    assert val["total_usd"] > 100000.0  # BTC+ETH holdings valued
    assert val["circuit_breaker_state"] == "closed"


def test_connector_execute_order_blocked():
    c = CoinbaseConnector()
    _open_breaker(c)
    res = c.execute_order("BTC-USD", "BUY", 0.1)
    assert res["status"] == "blocked"


def test_connector_execute_order_buy():
    c = CoinbaseConnector()
    usd_before = c.portfolio["USD"]
    res = c.execute_order("BTC-USD", "BUY", 0.1)
    assert res["status"] == "filled"
    assert c.portfolio["BTC"] == 0.6
    assert c.portfolio["USD"] < usd_before
    assert c.positions["BTC-USD"]["side"] == "BUY"


def test_connector_execute_order_no_price():
    c = CoinbaseConnector()
    res = c.execute_order("NOPE-USD", "BUY", 1.0)
    assert res["status"] == "failed"
    assert c.circuit_breaker.failure_count == 1
