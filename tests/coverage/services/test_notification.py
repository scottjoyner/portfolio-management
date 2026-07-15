"""Coverage tests for notification.TradeNotifier (email building + SMTP)."""

from unittest.mock import MagicMock, patch

import pytest

from notification import TradeNotifier


def make_notifier():
    return TradeNotifier(
        smtp_host="smtp.gmail.com",
        smtp_port=465,
        smtp_user="me@gmail.com",
        smtp_password="apppass",
        from_addr="me@gmail.com",
        to_addr="me@gmail.com",
        approval_base_url="http://localhost:8080",
    )


def test_default_from_to_addr():
    n = TradeNotifier(smtp_user="u@x.com")
    assert n.from_addr == "u@x.com"
    assert n.to_addr == "u@x.com"
    n2 = TradeNotifier(from_addr="a@b.com", to_addr="c@d.com")
    assert n2.from_addr == "a@b.com"
    assert n2.to_addr == "c@d.com"


def test_url_stripping():
    n = TradeNotifier(approval_base_url="http://host:8080/")
    assert n.approval_base_url == "http://host:8080"


def test_build_subject_buy_sell():
    opp_buy = {"side": "BUY", "type": "rebalance", "size_usd": 1000, "currency": "BTC-USD"}
    opp_sell = {"side": "SELL", "type": "rebalance", "size_usd": 1000, "currency": "BTC-USD"}
    sb = make_notifier()._build_subject(opp_buy)
    ss = make_notifier()._build_subject(opp_sell)
    assert "BUY" in sb and "BTC-USD" in sb
    assert "SELL" in ss


def test_build_html_without_verdict():
    n = make_notifier()
    opp = {"side": "BUY", "type": "rebalance", "size_usd": 1000, "currency": "BTC-USD",
           "reason": "good", "expected_fee": 2.0, "priority": 0.5}
    state = {"total_value": 100000, "usdc_balance": 50000}
    html = n._build_html(opp, state, None, token="tok123")
    assert "Approve" in html and "Deny" in html
    assert "tok123" in html
    assert "Risk / Reward" not in html  # no verdict


def test_build_html_with_verdict():
    n = make_notifier()
    opp = {"side": "BUY", "size_usd": 1000, "currency": "BTC-USD",
           "expected_fee": 10.0, "reason": "good", "priority": 0.5}
    verdict = {"win_rate": 0.6, "sharpe_ratio": 1.2, "profit_factor": 1.5,
               "max_drawdown_pct": 5.0, "strategy": "ema_cross", "currency": "BTC-USD"}
    state = {"total_value": 100000, "usdc_balance": 50000}
    html = n._build_html(opp, state, verdict, token="abc")
    assert "Risk / Reward" in html
    assert "Win Rate" in html
    assert "Sharpe" in html


def test_compute_risk_reward():
    n = make_notifier()
    opp = {"size_usd": 1000, "expected_fee": 10.0}
    verdict = {"win_rate": 0.6, "profit_factor": 2.0, "max_drawdown_pct": 5.0}
    rr = n._compute_risk_reward(opp, verdict)
    # fee_pct = 10/1000*100 = 1.0
    assert rr["fee_pct"] == 1.0
    # ev computed
    assert rr["ev"] != 0

    # zero size -> fee_pct 0
    rr2 = n._compute_risk_reward({"size_usd": 0, "expected_fee": 0}, verdict)
    assert rr2["fee_pct"] == 0.0

    # win_rate 0 or profit_factor invalid -> ev 0
    rr3 = n._compute_risk_reward(opp, {"win_rate": 0, "profit_factor": 1.0, "max_drawdown_pct": 5.0})
    assert rr3["ev"] == 0


def test_send_trade_alert_success():
    n = make_notifier()
    opp = {"side": "BUY", "type": "rebalance", "size_usd": 1000, "currency": "BTC-USD",
           "reason": "good", "expected_fee": 2.0, "priority": 0.5}
    state = {"total_value": 100000, "usdc_balance": 50000}
    verdict = {"win_rate": 0.6, "sharpe_ratio": 1.2, "profit_factor": 1.5,
               "max_drawdown_pct": 5.0, "strategy": "ema_cross", "currency": "BTC-USD"}
    fake_server = MagicMock()
    ctx = MagicMock()
    ctx.__enter__ = MagicMock(return_value=fake_server)
    ctx.__exit__ = MagicMock(return_value=False)
    with patch("notification.smtplib.SMTP_SSL", return_value=ctx) as SM:
        with patch("notification.ssl.create_default_context", return_value=MagicMock()):
            assert n.send_trade_alert(opp, state, verdict, token="t") is True
            SM.assert_called_once()
            fake_server.login.assert_called_once()
            fake_server.sendmail.assert_called_once()


def test_send_trade_alert_no_credentials():
    n = TradeNotifier()  # no user/password
    opp = {"side": "BUY", "type": "rebalance", "size_usd": 1000, "currency": "BTC-USD"}
    state = {"total_value": 1, "usdc_balance": 1}
    fake_server = MagicMock()
    ctx = MagicMock()
    ctx.__enter__ = MagicMock(return_value=fake_server)
    ctx.__exit__ = MagicMock(return_value=False)
    with patch("notification.smtplib.SMTP_SSL", return_value=ctx):
        with patch("notification.ssl.create_default_context", return_value=MagicMock()):
            assert n.send_trade_alert(opp, state) is True
            fake_server.login.assert_not_called()


def test_send_failure_returns_false():
    n = make_notifier()
    opp = {"side": "BUY", "type": "rebalance", "size_usd": 1000, "currency": "BTC-USD"}
    state = {"total_value": 1, "usdc_balance": 1}
    ctx = MagicMock()
    ctx.__enter__ = MagicMock(return_value=ctx)
    ctx.__exit__ = MagicMock(return_value=False)
    ctx.login.side_effect = RuntimeError("auth failed")
    with patch("notification.smtplib.SMTP_SSL", return_value=ctx):
        with patch("notification.ssl.create_default_context", return_value=MagicMock()):
            assert n.send_trade_alert(opp, state) is False
