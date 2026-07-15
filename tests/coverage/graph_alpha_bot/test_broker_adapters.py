import requests
from unittest.mock import MagicMock, patch
from app.exec.broker_adapters import (
    Order, Broker, FidelityViaSnapTrade, MerrillReadOnly,
)


def test_order_is_dict():
    o = Order()
    o["symbol"] = "AAPL"
    assert o["symbol"] == "AAPL"


def test_fidelity_preview():
    broker = FidelityViaSnapTrade()
    resp = MagicMock()
    resp.raise_for_status.return_value = None
    resp.json.return_value = {"preview": True}
    with patch.object(requests, "post", return_value=resp) as m:
        out = broker.preview({"symbol": "AAPL"})
    assert out == {"preview": True}
    assert m.call_args.kwargs["json"]["connectionId"] == broker.connection_id


def test_fidelity_place():
    broker = FidelityViaSnapTrade()
    resp = MagicMock()
    resp.raise_for_status.return_value = None
    resp.json.return_value = {"id": 1}
    with patch.object(requests, "post", return_value=resp):
        out = broker.place({"symbol": "AAPL"})
    assert out == {"id": 1}


def test_fidelity_positions():
    broker = FidelityViaSnapTrade()
    resp = MagicMock()
    resp.raise_for_status.return_value = None
    resp.json.return_value = {"holdings": []}
    with patch.object(requests, "get", return_value=resp):
        out = broker.positions()
    assert out == {"holdings": []}


def test_fidelity_cancel_not_implemented():
    broker = FidelityViaSnapTrade()
    try:
        broker.cancel("x")
        assert False
    except NotImplementedError:
        pass


def test_merrill_preview_not_implemented():
    b = MerrillReadOnly()
    try:
        b.preview({})
        assert False
    except NotImplementedError:
        pass


def test_merrill_place_not_implemented():
    b = MerrillReadOnly()
    try:
        b.place({})
        assert False
    except NotImplementedError:
        pass


def test_merrill_positions_empty():
    assert MerrillReadOnly().positions() == {}


def test_merrill_cancel_not_implemented():
    b = MerrillReadOnly()
    try:
        b.cancel("x")
        assert False
    except NotImplementedError:
        pass
