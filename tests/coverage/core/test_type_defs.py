import pytest
from datetime import datetime
from trading_system.type_defs import (
    Candle, OHLCV, TradingSignal, Position,
    parse_candle_data, parse_signal_data,
)


def test_candle_repr():
    c = Candle(timestamp=datetime.now(), open=100.5, high=101.2, low=99.8, close=101.0)
    assert "101.00" in repr(c)


def test_ohlcv_default():
    o = OHLCV(timestamp=datetime.now(), open=1, high=2, low=0.5, close=1.5)
    assert o.volume is None


def test_trading_signal_codes():
    assert TradingSignal(type='BUY').signal_code == 1
    assert TradingSignal(type='SELL').signal_code == -1
    assert TradingSignal(type='HOLD').signal_code == 0
    assert TradingSignal(type='OTHER').signal_code == 0


def test_trading_signal_from_code():
    assert TradingSignal.from_code(1).type == 'BUY'
    assert TradingSignal.from_code(0).type == 'HOLD'
    assert TradingSignal.from_code(-1).type == 'SELL'
    assert TradingSignal.from_code(1).strength == 1.0
    assert TradingSignal.from_code(-1).strength == -1.0


def test_position_defaults():
    p = Position(symbol='BTC/USDT', size=0.5, entry_price=45000.0)
    assert p.unrealized_pnl is None
    assert p.roi_percentage is None


def test_parse_candle_data_candle_passthrough():
    c = Candle(timestamp=datetime.now(), open=1, high=2, low=0.5, close=1.5)
    assert parse_candle_data(c) is c


def test_parse_candle_data_dict_datetime_ts():
    ts = datetime(2024, 1, 1)
    c = parse_candle_data({'open': 1, 'high': 2, 'low': 0.5, 'close': 1.5, 'timestamp': ts})
    assert c.timestamp == ts
    assert c.close == 1.5


def test_parse_candle_data_dict_str_ts_valid():
    c = parse_candle_data({'open': 1, 'high': 2, 'low': 0.5, 'close': 1.5,
                           'timestamp': '2024-01-01T00:00:00'})
    assert c.timestamp == datetime(2024, 1, 1)


def test_parse_candle_data_dict_str_ts_invalid():
    c = parse_candle_data({'open': 1, 'high': 2, 'low': 0.5, 'close': 1.5,
                           'timestamp': 'not-a-date'})
    assert isinstance(c.timestamp, datetime)


def test_parse_candle_data_dict_missing_ts():
    c = parse_candle_data({'open': 1, 'high': 2, 'low': 0.5, 'close': 1.5})
    assert c.timestamp is None


def test_parse_candle_data_dict_custom_field():
    ts = datetime(2024, 1, 1)
    c = parse_candle_data({'open': 1, 'high': 2, 'low': 0.5, 'close': 1.5, 'time': ts},
                          timestamp_field='time')
    assert c.timestamp == ts


def test_parse_candle_data_unsupported_type():
    with pytest.raises(ValueError):
        parse_candle_data(123)


def test_parse_signal_data_passthrough():
    s = TradingSignal(type='BUY', strength=0.5)
    assert parse_signal_data(s) is s


def test_parse_signal_data_dict():
    s = parse_signal_data({'type': 'SELL', 'strength': -0.5, 'reason': 'r'})
    assert s.type == 'SELL'
    assert s.strength == -0.5
    assert s.reason == 'r'


def test_parse_signal_data_dict_defaults():
    s = parse_signal_data({})
    assert s.type == 'HOLD'
    assert s.strength == 0.0
    assert s.reason is None


def test_parse_signal_data_dict_signal_key():
    s = parse_signal_data({'signal': 'BUY', 'strength': 1.0})
    assert s.type == 'BUY'


def test_parse_signal_data_unsupported_type():
    with pytest.raises(ValueError):
        parse_signal_data(123)
