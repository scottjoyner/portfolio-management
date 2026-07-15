from trading_system.execution.queue_model.models import QueueEstimate, SimpleQueueModel


def test_estimate_basic():
    m = SimpleQueueModel()
    est = m.estimate(queue_ahead=10, trade_rate=5, cancel_rate=5)
    assert isinstance(est, QueueEstimate)
    assert 0.0 <= est.fill_probability <= 1.0
    assert est.expected_queue_time_ms > 0
    assert est.expected_fill_size >= 0
    assert est.adverse_selection_bps <= 50.0
    assert 0.0 <= est.stale_quote_decay <= 1.0


def test_estimate_zero_rates_uses_floor():
    m = SimpleQueueModel()
    # with no queue ahead, the 1e-6 pressure floor still yields a valid (high) fill prob
    est = m.estimate(queue_ahead=0, trade_rate=0, cancel_rate=0)
    assert 0.0 < est.fill_probability <= 1.0


def test_estimate_large_queue_low_fill():
    m = SimpleQueueModel()
    # huge queue ahead with near-zero pressure -> fill prob collapses to 0
    est = m.estimate(queue_ahead=1e9, trade_rate=0, cancel_rate=0)
    assert est.fill_probability < 1e-6


def test_estimate_no_queue_full_fill():
    m = SimpleQueueModel()
    est = m.estimate(queue_ahead=0, trade_rate=50, cancel_rate=50)
    assert est.fill_probability > 0.9


def test_estimate_with_quote_age():
    m = SimpleQueueModel()
    est = m.estimate(queue_ahead=1, trade_rate=100, cancel_rate=100, quote_age_ms=5000)
    assert est.stale_quote_decay > 0.0


def test_estimate_defaults():
    m = SimpleQueueModel()
    est = m.estimate(queue_ahead=1, trade_rate=5, cancel_rate=5)
    assert 0.0 < est.fill_probability < 1.0
