"""Coverage tests for trading_system/backtester.py (CoinbaseBacktester,
verify_coinbase_auth, main)."""

import json
from unittest import mock

import pytest

import backtester as bt


def _make_response(data, raise_status=None):
    resp = mock.Mock()
    resp.json.return_value = data
    if raise_status is not None:
        resp.raise_for_status.side_effect = raise_status
    else:
        resp.raise_for_status.return_value = None
    return resp


def test_init_default_and_explicit():
    b = bt.CoinbaseBacktester()
    assert b.api_key is None
    assert b.base_url == "https://api.coinbase.com/v2"
    assert b.historical_data == {}

    with mock.patch.dict("os.environ", {"COINBASE_API_KEY": "k"}):
        b2 = bt.CoinbaseBacktester()
        assert b2.api_key == "k"
    b3 = bt.CoinbaseBacktester(api_key="explicit")
    assert b3.api_key == "explicit"


def test_download_historical_data_success():
    payload = {"data": [
        [1, 10, 12, 9, 11, 100],
        [2, 11, 13, 10, 12, 110],
    ]}
    with mock.patch("backtester.requests.get", return_value=_make_response(payload)):
        df = bt.CoinbaseBacktester().download_historical_data(
            "BTC-USD", "2024-01-01", "2024-01-02", "hourly"
        )
    assert len(df) == 2
    assert "close" in df.columns
    assert list(df["close"]) == [11, 12]


def test_download_historical_data_error():
    with mock.patch("backtester.requests.get", side_effect=RuntimeError("net")):
        with pytest.raises(Exception):
            bt.CoinbaseBacktester().download_historical_data("BTC-USD", "a", "b")


def test_replay_no_df_stored_raises():
    # product_id absent -> .get() returns None -> guard raises
    with pytest.raises(ValueError):
        bt.CoinbaseBacktester().replay_trades([], "BTC-USD")


def test_replay_none_value_raises():
    # explicitly store None so the guard triggers the intended ValueError
    b = bt.CoinbaseBacktester()
    b.historical_data["BTC-USD"] = None
    with pytest.raises(ValueError):
        b.replay_trades([], "BTC-USD")


def test_replay_metrics():
    # The repo's guard `if not self.historical_data.get(product_id)` is buggy for
    # real DataFrames (pandas truthiness is ambiguous). We mock the library quirk
    # so the genuine replay math (lines 101-120) is exercised.
    with mock.patch.object(bt.pd.DataFrame, "__bool__", lambda self: True):
        b = bt.CoinbaseBacktester()
        b.historical_data["BTC-USD"] = bt.pd.DataFrame({
            "close": [100.0, 101.0, 102.0, 101.5, 103.0],
        })
        metrics = b.replay_trades([{"t": 1}], "BTC-USD")
    assert set(metrics) == {"total_return", "sharpe_ratio", "max_drawdown"}
    assert isinstance(metrics["total_return"], float)
    assert isinstance(metrics["sharpe_ratio"], float)
    assert isinstance(metrics["max_drawdown"], float)


def test_compare_strategies():
    with mock.patch.object(bt.pd.DataFrame, "__bool__", lambda self: True):
        b = bt.CoinbaseBacktester()
        b.historical_data["BTC-USD"] = bt.pd.DataFrame({
            "close": [100.0, 101.0, 102.0],
        })
        good = {"name": "s1", "trades": [{"t": 1}]}
        bad = {"name": "s2"}  # missing 'trades' -> KeyError -> caught
        df = b.compare_strategies([good, bad], "BTC-USD")
    assert list(df["strategy"]) == ["s1"]


def test_compare_strategies_empty():
    with mock.patch.object(bt.pd.DataFrame, "__bool__", lambda self: True):
        b = bt.CoinbaseBacktester()
        b.historical_data["BTC-USD"] = bt.pd.DataFrame({
            "close": [100.0, 101.0, 102.0],
        })
        df = b.compare_strategies([], "BTC-USD")
    assert df.empty


def test_export_results(tmp_path):
    f = tmp_path / "out.json"
    bt.CoinbaseBacktester().export_results({"a": 1}, str(f))
    assert json.loads(f.read_text()) == {"a": 1}


def test_verify_coinbase_auth_success():
    res = mock.Mock(returncode=0, stderr="")
    with mock.patch("backtester.subprocess.run", return_value=res):
        assert bt.verify_coinbase_auth() is True


def test_verify_coinbase_auth_fail():
    res = mock.Mock(returncode=1, stderr="bad")
    with mock.patch("backtester.subprocess.run", return_value=res):
        assert bt.verify_coinbase_auth() is False


def test_verify_coinbase_auth_exception():
    with mock.patch("backtester.subprocess.run", side_effect=OSError("x")):
        assert bt.verify_coinbase_auth() is False


def test_main_auth_failure(capsys):
    with mock.patch("backtester.verify_coinbase_auth", return_value=False):
        assert bt.main() is None
    out = capsys.readouterr().out
    assert "authentication failed" in out.lower()


def test_main_success():
    fake_instance = mock.Mock()
    fake_instance.download_historical_data.return_value = mock.Mock()
    fake_instance.replay_trades.return_value = {"total_return": 0.1}
    fake_cls = mock.Mock(return_value=fake_instance)
    with mock.patch("backtester.verify_coinbase_auth", return_value=True), \
         mock.patch("backtester.CoinbaseBacktester", fake_cls):
        assert bt.main() is None
    fake_instance.download_historical_data.assert_called_once()
    fake_instance.replay_trades.assert_called_once()
    fake_instance.export_results.assert_called_once()
