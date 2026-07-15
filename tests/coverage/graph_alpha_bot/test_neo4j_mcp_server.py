import json
from unittest.mock import MagicMock, patch

import neo4j_mcp_server as m


# ---------------------------------------------------------------------------
# Neo4jGraphClient
# ---------------------------------------------------------------------------

def _fake_driver():
    driver = MagicMock()
    sess = MagicMock()
    sess.__enter__.return_value = sess
    sess.__exit__.return_value = False
    driver.session.return_value = sess
    return driver, sess


def test_graph_client_execute_query():
    driver, sess = _fake_driver()
    rec = MagicMock()
    rec.data.return_value = {"a": 1}
    sess.run.return_value = [rec]
    with patch.object(m, "GraphDatabase") as GD:
        GD.driver.return_value = driver
        client = m.Neo4jGraphClient("bolt://x", "neo4j", "pw")
        out = client.execute_query("MATCH (n) RETURN n")
    assert out == [{"a": 1}]
    client.close()
    assert driver.close.called


def test_graph_client_execute_query_raises():
    driver, sess = _fake_driver()
    sess.run.side_effect = RuntimeError("boom")
    with patch.object(m, "GraphDatabase") as GD:
        GD.driver.return_value = driver
        client = m.Neo4jGraphClient("bolt://x", "neo4j", "pw")
        try:
            client.execute_query("X")
            assert False
        except RuntimeError:
            pass


def test_graph_client_execute_write_success():
    driver, sess = _fake_driver()
    result = MagicMock()
    result._result_summary = "summary"
    sess.run.return_value = result
    with patch.object(m, "GraphDatabase") as GD:
        GD.driver.return_value = driver
        client = m.Neo4jGraphClient("bolt://x", "neo4j", "pw")
        out = client.execute_write("CREATE ()")
    assert out["success"] is True
    assert "_summary" in out


def test_graph_client_execute_write_no_summary():
    driver, sess = _fake_driver()
    result = MagicMock(spec=[])
    sess.run.return_value = result  # no _result_summary attribute
    with patch.object(m, "GraphDatabase") as GD:
        GD.driver.return_value = driver
        client = m.Neo4jGraphClient("bolt://x", "neo4j", "pw")
        out = client.execute_write("CREATE ()")
    assert out["success"] is True
    assert "_summary" not in out


def test_graph_client_execute_write_error():
    driver, sess = _fake_driver()
    sess.run.side_effect = RuntimeError("bad")
    with patch.object(m, "GraphDatabase") as GD:
        GD.driver.return_value = driver
        client = m.Neo4jGraphClient("bolt://x", "neo4j", "pw")
        out = client.execute_write("CREATE ()")
    assert out["success"] is False


# ---------------------------------------------------------------------------
# Neo4jMCPServer
# ---------------------------------------------------------------------------

def _server(client=None, connected=True):
    srv = m.Neo4jMCPServer(m.Config())
    srv.client = client
    return srv


def test_connect_success():
    client = MagicMock()
    client.execute_query.return_value = [{"test": 1}]
    with patch.object(m, "Neo4jGraphClient", return_value=client):
        srv = m.Neo4jMCPServer(m.Config())
        assert srv.connect() is True
        assert srv.client is client


def test_connect_failure():
    with patch.object(m, "Neo4jGraphClient") as G:
        G.side_effect = RuntimeError("no")
        srv = m.Neo4jMCPServer(m.Config())
        assert srv.connect() is False
        assert srv.client is None


def test_get_graph_stats_not_connected():
    assert _server(None).get_graph_stats()["error"] == "Not connected"


def test_get_graph_stats_empty_result():
    # execute_query returns a falsy (empty) result -> stat key omitted
    client = MagicMock()
    client.execute_query.return_value = []
    srv = _server(client)
    stats = srv.get_graph_stats()
    assert "ticker_count" not in stats["stats"]


def test_get_graph_stats_connected():
    client = MagicMock()
    client.execute_query.return_value = [{"cnt": 5}]
    srv = _server(client)
    stats = srv.get_graph_stats()
    assert stats["stats"]["ticker_count"] == 5
    assert stats["connected"] is True


def test_get_graph_stats_query_fails():
    client = MagicMock()
    client.execute_query.side_effect = RuntimeError("boom")
    srv = _server(client)
    stats = srv.get_graph_stats()
    # topics that raised are absent
    assert "ticker_count" not in stats["stats"]


def test_query_tickers_disconnected():
    assert _server(None).query_tickers() == []


def test_query_tickers_connected():
    client = MagicMock()
    client.execute_query.return_value = [{"symbol": "BTC-USD"}]
    srv = _server(client)
    out = srv.query_tickers(symbols=["BTC-USD"], limit=10)
    assert out == [{"symbol": "BTC-USD"}]
    # No symbols -> empty list param path
    srv.query_tickers()
    assert client.execute_query.call_count == 2


def test_query_price_history_disconnected():
    assert _server(None).query_price_history("BTC-USD") == []


def test_query_price_history_connected():
    client = MagicMock()
    client.execute_query.return_value = [{"date": "2024"}]
    srv = _server(client)
    out = srv.query_price_history("BTC-USD", days=7, limit=5)
    assert out == [{"date": "2024"}]


def test_query_news_disconnected():
    assert _server(None).query_news_for_ticker("BTC-USD") == []


def test_query_news_connected():
    client = MagicMock()
    client.execute_query.return_value = [{"title": "x"}]
    srv = _server(client)
    out = srv.query_news_for_ticker("BTC-USD", days=3, limit=4)
    assert out == [{"title": "x"}]


def test_query_sentiment_disconnected():
    assert _server(None).query_sentiment_trend("BTC-USD") == []


def test_query_sentiment_connected():
    client = MagicMock()
    client.execute_query.return_value = [{"avg_score": 0.5}]
    srv = _server(client)
    out = srv.query_sentiment_trend("BTC-USD", period_days=15)
    assert out == [{"avg_score": 0.5}]


def test_query_correlation_pairs_disconnected():
    assert _server(None).query_correlation_pairs() == []


def test_query_correlation_pairs_connected():
    client = MagicMock()
    client.execute_query.return_value = [{"ticker1": "A", "ticker2": "B"}]
    srv = _server(client)
    out = srv.query_correlation_pairs(symbols=["A", "B"], min_correlation=0.8)
    assert out == [{"ticker1": "A", "ticker2": "B"}]
    srv.query_correlation_pairs()
    assert client.execute_query.call_count == 2


def test_create_signal_disconnected():
    assert _server(None).create_signal("s", "BTC-USD", 0.5)["error"] == "Not connected"


def test_create_signal_connected():
    client = MagicMock()
    client.execute_write.return_value = {"success": True}
    srv = _server(client)
    out = srv.create_signal("s", "BTC-USD", 0.5, direction="long", meta={"a": 1})
    assert out["success"] is True
    assert client.execute_write.called


# ---------------------------------------------------------------------------
# HTTPHandler
# ---------------------------------------------------------------------------

def _handler(client=None):
    srv = _server(client)
    return m.HTTPHandler(srv), srv


def test_handle_options():
    h, _ = _handler()
    code, body = h.handle_request("OPTIONS", "/anything", {}, None)
    assert code == 200 and body["status"] == "ok"


def test_handle_health():
    client = MagicMock()
    client.execute_query.return_value = [{"cnt": 2}]
    h, _ = _handler(client)
    code, body = h.handle_request("GET", "/health", {}, None)
    assert code == 200 and body["stats"]["ticker_count"] == 2


def test_handle_tickers_with_and_without_symbols():
    client = MagicMock()
    client.execute_query.return_value = []
    h, _ = _handler(client)
    code, body = h.handle_request("GET", "/query/tickers", {"symbols": ["BTC-USD"], "limit": ["5"]}, None)
    assert code == 200
    # no symbols path
    h.handle_request("GET", "/query/tickers", {}, None)
    assert client.execute_query.call_count == 2


def test_handle_price_missing_symbol():
    h, _ = _handler(MagicMock())
    code, body = h.handle_request("GET", "/query/price", {}, None)
    assert code == 400 and "Missing symbol" in body["error"]


def test_handle_price_ok():
    client = MagicMock()
    client.execute_query.return_value = [{"date": "x"}]
    h, _ = _handler(client)
    code, body = h.handle_request("GET", "/query/price", {"symbol": ["BTC-USD"], "days": ["3"], "limit": ["2"]}, None)
    assert code == 200


def test_handle_news_missing_symbol():
    h, _ = _handler(MagicMock())
    code, body = h.handle_request("GET", "/query/news", {}, None)
    assert code == 400


def test_handle_news_ok():
    client = MagicMock()
    client.execute_query.return_value = []
    h, _ = _handler(client)
    code, _ = h.handle_request("GET", "/query/news", {"symbol": ["BTC-USD"], "days": ["2"]}, None)
    assert code == 200


def test_handle_sentiment_missing_symbol():
    h, _ = _handler(MagicMock())
    code, body = h.handle_request("GET", "/query/sentiment", {}, None)
    assert code == 400


def test_handle_sentiment_ok():
    client = MagicMock()
    client.execute_query.return_value = []
    h, _ = _handler(client)
    code, _ = h.handle_request("GET", "/query/sentiment", {"symbol": ["BTC-USD"]}, None)
    assert code == 200


def test_handle_correlations():
    client = MagicMock()
    client.execute_query.return_value = []
    h, _ = _handler(client)
    code, _ = h.handle_request("GET", "/query/correlations", {"min_correlation": ["0.9"]}, None)
    assert code == 200


def test_handle_signal_create():
    client = MagicMock()
    client.execute_write.return_value = {"success": True}
    h, _ = _handler(client)
    body = json.dumps({"strategy": "s", "symbol": "BTC-USD", "score": 0.7, "direction": "long", "meta": {}}).encode()
    code, resp = h.handle_request("POST", "/signal/create", {}, body)
    assert code == 201 and resp["success"] is True


def test_handle_not_found():
    h, _ = _handler(MagicMock())
    code, body = h.handle_request("GET", "/nope", {}, None)
    assert code == 404 and "Not found" in body["error"]


def test_handle_exception_returns_500():
    h, srv = _handler(MagicMock())
    srv.query_tickers = MagicMock(side_effect=RuntimeError("boom"))
    code, body = h.handle_request("GET", "/query/tickers", {}, None)
    assert code == 500 and "error" in body


# ---------------------------------------------------------------------------
# main()
# ---------------------------------------------------------------------------

def test_main_test_runs_and_shuts_down(monkeypatch):
    import http.server as hserver

    args = MagicMock()
    args.uri = "bolt://x"
    args.user = "neo4j"
    args.password = "pw"
    args.port = 8080

    inst = MagicMock()
    inst.connect.return_value = True
    inst.get_graph_stats.return_value = {"stats": {"ticker_count": 0}, "connected": True}
    inst.create_signal.return_value = {"success": True}
    inst.query_tickers.return_value = []

    class FakeHTTPServer:
        def __init__(self, addr, handler_cls):
            self.handler_cls = handler_cls
            self.addr = addr

        def serve_forever(self):
            cls = self.handler_cls
            h = cls.__new__(cls)
            h.server = self
            h.connection = MagicMock()
            h.request_version = "HTTP/1.1"
            h.requestline = ""
            h.raw_requestline = ""
            h.rfile = MagicMock()
            h.wfile = MagicMock()
            h.headers = {"Content-Length": "0"}
            # Stub out BaseHTTPRequestHandler I/O so we only exercise our logic
            h.send_response = lambda *a, **k: None
            h.send_header = lambda *a, **k: None
            h.end_headers = lambda *a, **k: None
            h.log_message = lambda *a, **k: None
            # GET /health
            h.path = "/health"
            h.do_GET()
            # POST /signal/create
            h.path = "/signal/create"
            h.headers = {"Content-Length": "2"}
            h.rfile.read.return_value = b"{}"
            h.do_POST()
            # OPTIONS
            h.do_OPTIONS()
            raise KeyboardInterrupt

    with patch("neo4j_mcp_server.argparse.ArgumentParser") as AP:
        AP.return_value.parse_args.return_value = args
        with patch("neo4j_mcp_server.Neo4jMCPServer", return_value=inst):
            with patch.object(hserver, "HTTPServer", FakeHTTPServer):
                m.main()
    inst.connect.assert_called_once()
    inst.client.close.assert_called_once()


def test_main_connect_failure_exits(monkeypatch):
    args = MagicMock()
    args.uri = "bolt://x"
    args.user = "neo4j"
    args.password = "pw"
    args.port = 8080
    with patch("neo4j_mcp_server.argparse.ArgumentParser") as AP:
        AP.return_value.parse_args.return_value = args
        inst = MagicMock()
        inst.connect.return_value = False
        with patch("neo4j_mcp_server.Neo4jMCPServer", return_value=inst):
            try:
                m.main()
            except SystemExit as e:
                assert e.code == 1
