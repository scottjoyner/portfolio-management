from unittest.mock import MagicMock, patch
from app.exec.order_engine import fetch_today_signals, create_orders


def test_fetch_today_signals():
    rows = [{"sym": "BTC-USD", "s": 1.5}, {"sym": "ETH-USD", "s": 0.5}]
    sess = MagicMock()
    sess.run.return_value = rows
    driver = MagicMock()
    driver.session.return_value.__enter__.return_value = sess

    with patch("app.exec.order_engine.GraphDatabase.driver", return_value=driver):
        out = fetch_today_signals(limit=10)

    assert out == [
        {"symbol": "BTC-USD", "score": 1.5},
        {"symbol": "ETH-USD", "score": 0.5},
    ]


def test_create_orders_empty():
    assert create_orders([]) == []


def test_create_orders_all_negative():
    assert create_orders([{"symbol": "X", "score": -1.0}]) == []


def test_create_orders_weights():
    cands = [{"symbol": "A", "score": 3.0}, {"symbol": "B", "score": 1.0}]
    orders = create_orders(cands, cash=400.0)
    assert len(orders) == 2
    a = next(o for o in orders if o["symbol"] == "A")
    b = next(o for o in orders if o["symbol"] == "B")
    # A gets 3/4 of cash, B 1/4
    assert a["alloc"] == 300.0
    assert b["alloc"] == 100.0
    assert all(o["side"] == "buy" for o in orders)
