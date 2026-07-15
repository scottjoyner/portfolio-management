from unittest.mock import MagicMock, patch

import app.exec.rebalance as m
from app.exec.rebalance import main


def test_main_no_orders(monkeypatch, capsys):
    monkeypatch.setattr(m, "fetch_today_signals", lambda limit=20: [])
    monkeypatch.setattr(m, "create_orders", lambda c, cash: [])
    monkeypatch.setattr("sys.argv", ["rebalance", "--broker", "fidelity"])
    main()
    assert "No orders to place" in capsys.readouterr().out


def test_main_dry_run(monkeypatch, capsys):
    monkeypatch.setattr(m, "fetch_today_signals", lambda limit=20: [{"symbol": "BTC", "score": 1.0}])
    monkeypatch.setattr(m, "create_orders", lambda c, cash: [{"symbol": "BTC", "qty": 1}])
    monkeypatch.setattr("sys.argv", ["rebalance", "--broker", "fidelity", "--dry-run"])
    main()
    out = capsys.readouterr().out
    assert "Dry run" in out


def test_main_fidelity(monkeypatch, capsys):
    monkeypatch.setattr(m, "fetch_today_signals", lambda limit=20: [{"symbol": "BTC", "score": 1.0}])
    monkeypatch.setattr(m, "create_orders", lambda c, cash: [{"symbol": "BTC", "qty": 1}])
    placed = []
    broker = MagicMock()
    broker.preview.return_value = {"preview": True}
    broker.place.side_effect = lambda p: placed.append(p) or {"ok": True}
    monkeypatch.setattr(m, "FidelityViaSnapTrade", lambda: broker)
    monkeypatch.setattr("sys.argv", ["rebalance", "--broker", "fidelity"])
    main()
    assert placed


def test_main_merrill(monkeypatch, capsys):
    monkeypatch.setattr(m, "fetch_today_signals", lambda limit=20: [{"symbol": "BTC", "score": 1.0}])
    monkeypatch.setattr(m, "create_orders", lambda c, cash: [{"symbol": "BTC", "qty": 1}])
    monkeypatch.setattr("sys.argv", ["rebalance", "--broker", "merrill"])
    main()
    assert "no public trading API" in capsys.readouterr().out
