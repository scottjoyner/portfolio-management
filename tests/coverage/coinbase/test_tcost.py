import math
import pytest

from coinbase.src import tcost


def test_rust_backend_available():
    assert isinstance(tcost._HAS_RUST_TCOST, bool)


def test_spread_bps_basic():
    assert tcost.estimate_spread_bps(100.0, 102.0) == pytest.approx(20000.0 * 2.0 / 202.0)


def test_impact_bps_zero_notional():
    assert tcost.impact_bps(0.0, 1.5) == 0.0


def test_impact_bps_positive():
    val = tcost.impact_bps(10000.0, 1.5)
    assert val == pytest.approx(1.5 * math.sqrt(1.0))


def test_effective_fill_price_buy():
    price = tcost.effective_fill_price(
        "buy", mid=100.0, bid=99.0, ask=101.0,
        notional_usd=10000.0, taker_fee_bps=8.0, slippage_bps=1.0, impact_coeff=1.5,
    )
    assert price > 100.0


def test_effective_fill_price_sell():
    price = tcost.effective_fill_price(
        "SELL", mid=100.0, bid=99.0, ask=101.0,
        notional_usd=10000.0, taker_fee_bps=8.0, slippage_bps=1.0, impact_coeff=1.5,
    )
    assert price < 100.0


# --- Python fallback path (exercises validation branches) ---
@pytest.fixture
def force_python(monkeypatch):
    monkeypatch.setattr(tcost, "_HAS_RUST_TCOST", False)


def test_spread_bps_py_invalid_bid(force_python):
    with pytest.raises(ValueError):
        tcost.estimate_spread_bps(0.0, 102.0)


def test_spread_bps_py_invalid_ask_lt_bid(force_python):
    with pytest.raises(ValueError):
        tcost.estimate_spread_bps(102.0, 100.0)


def test_spread_bps_py_negative_bid(force_python):
    with pytest.raises(ValueError):
        tcost.estimate_spread_bps(-1.0, 100.0)


def test_impact_bps_py_zero(force_python):
    assert tcost.impact_bps(0.0, 1.5) == 0.0


def test_effective_fill_price_py_buy(force_python):
    price = tcost.effective_fill_price(
        "buy", mid=100.0, bid=99.0, ask=101.0,
        notional_usd=10000.0, taker_fee_bps=8.0, slippage_bps=1.0, impact_coeff=1.5,
    )
    assert price > 100.0


def test_effective_fill_price_py_sell(force_python):
    price = tcost.effective_fill_price(
        "SELL", mid=100.0, bid=99.0, ask=101.0,
        notional_usd=10000.0, taker_fee_bps=8.0, slippage_bps=1.0, impact_coeff=1.5,
    )
    assert price < 100.0


def test_effective_fill_price_py_invalid_mid(force_python):
    with pytest.raises(ValueError):
        tcost.effective_fill_price("buy", mid=0.0, bid=1.0, ask=2.0, notional_usd=10.0)
