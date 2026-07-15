from unittest.mock import MagicMock, patch

import mcp_server_main as m


def test_main_connection_success(monkeypatch, capsys):
    monkeypatch.setattr("sys.argv", ["mcp_server_main", "--uri", "bolt://x:7687", "--user", "neo4j"])

    fake_session = MagicMock()
    fake_session.run.return_value.fetch_all.return_value = [{"test": 1}]
    fake_db = MagicMock()
    fake_db.session.return_value.__enter__.return_value = fake_session

    with patch.object(m, "create_graph_database", return_value=fake_db) as cg:
        m.main()
    cg.assert_called_once()
    assert "connection successful" in capsys.readouterr().out


def test_main_connection_failure(monkeypatch, capsys):
    monkeypatch.setattr("sys.argv", ["mcp_server_main"])

    with patch.object(m, "create_graph_database", side_effect=RuntimeError("cannot connect")):
        m.main()
    out = capsys.readouterr().out
    assert "Could not connect" in out


def test_main_default_args(monkeypatch, capsys):
    monkeypatch.setattr("sys.argv", ["mcp_server_main", "--port", "9999"])
    fake_session = MagicMock()
    fake_session.run.return_value.fetch_all.return_value = []
    fake_db = MagicMock()
    fake_db.session.return_value.__enter__.return_value = fake_session
    with patch.object(m, "create_graph_database", return_value=fake_db):
        m.main()
    assert "HTTP Port: 9999" in capsys.readouterr().out
