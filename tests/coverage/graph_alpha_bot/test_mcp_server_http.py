import base64
import json
import sys
import urllib.error
from unittest.mock import MagicMock, patch

import mcp_server_http as m


def _response(text=b"{}", status=200):
    resp = MagicMock()
    resp.read.return_value = text
    resp.__enter__.return_value = resp
    return resp


def test_make_request_post_success():
    resp = _response(b'{"results": []}')
    with patch.object(urllib.request, "urlopen", return_value=resp) as urlopen:
        out = m.GraphDatabaseProxy("bolt://x:7687", "neo4j", "pw")._make_request(
            "session/run", {"a": 1}
        )
    assert out == {"results": []}
    req = urlopen.call_args[0][0]
    assert req.get_full_url().endswith("/api/v1/session/run")
    assert req.get_method() == "POST"
    assert req.headers["Authorization"].startswith("Basic ")


def test_make_request_get_success():
    resp = _response(b'{"results": []}')
    with patch.object(urllib.request, "urlopen", return_value=resp) as urlopen:
        out = m.GraphDatabaseProxy("bolt://x:7687", "neo4j", "pw")._make_request(
            "session/run", {"a": "b"}, method="GET"
        )
    assert out == {"results": []}
    req = urlopen.call_args[0][0]
    assert "a=b" in req.get_full_url()


def test_make_request_httperror():
    err = urllib.error.HTTPError("u", 500, "boom", {}, None)
    with patch.object(urllib.request, "urlopen", side_effect=err):
        out = m.GraphDatabaseProxy("bolt://x:7687", "neo4j", "pw")._make_request("x", {})
    assert "error" in out and out["status_code"] == 500


def test_make_request_generic_exception():
    with patch.object(urllib.request, "urlopen", side_effect=RuntimeError("down")):
        out = m.GraphDatabaseProxy("bolt://x:7687", "neo4j", "pw")._make_request("x", {})
    assert "error" in out


def test_session_and_run():
    proxy = m.GraphDatabaseProxy("bolt://x:7687", "neo4j", "pw")
    sess = proxy.session()
    assert isinstance(sess, m.SessionProxy)
    resp = _response(json.dumps({"results": [{"a": 1}], "summary": {}}).encode())
    with patch.object(urllib.request, "urlopen", return_value=resp):
        result = sess.run("RETURN 1", {})
    assert result.fetch_all()[0].get("a") == 1
    assert result.single().get("a") == 1
    assert len(list(iter(result))) == 1


def test_session_run_error_raises():
    proxy = m.GraphDatabaseProxy("bolt://x:7687", "neo4j", "pw")
    sess = proxy.session()
    resp = _response(json.dumps({"error": "bad"}).encode())
    with patch.object(urllib.request, "urlopen", return_value=resp):
        try:
            sess.run("RETURN 1", {})
            assert False
        except Exception as e:
            assert "bad" in str(e)


def test_session_close():
    proxy = m.GraphDatabaseProxy("bolt://x:7687", "neo4j", "pw")
    sess = proxy.session()
    resp = _response(b"{}")
    with patch.object(urllib.request, "urlopen", return_value=resp):
        sess.close()


def test_query_result_empty():
    qr = m.QueryResult([], {})
    assert qr.fetch_all() == []
    assert qr.single() is None


def test_record_get_and_keys():
    rec = m.Record({"a": 1, "b": 2})
    assert rec.get("a") == 1
    assert rec.get("missing", "x") == "x"
    assert set(rec.keys()) == {"a", "b"}


def test_create_graph_database():
    db = m.create_graph_database("bolt://x:7687", "neo4j", "pw")
    assert isinstance(db, m.GraphDatabaseProxy)
    assert db.uri == "https://x:7687"


def test_dotenv_guard_when_missing(monkeypatch):
    """Cover the `except ImportError` branch of the top-level dotenv guard."""
    import importlib

    # Force `import dotenv` to fail by poisoning the module cache.
    monkeypatch.setitem(sys.modules, "dotenv", None)
    sys.modules.pop("mcp_server_http", None)
    try:
        importlib.import_module("mcp_server_http")
    finally:
        sys.modules.pop("mcp_server_http", None)
        importlib.import_module("mcp_server_http")  # restore healthy module
