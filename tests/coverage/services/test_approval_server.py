"""Coverage tests for approval_server (HTTP routes via real server)."""

import json
import os
import sys
import threading
import urllib.error
import urllib.request
from http.server import HTTPServer

import pytest

import approval_server as ap
from approval_server import ApprovalHandler, serve


PENDING = {}


def _write_pending(tmp_path, data):
    p = tmp_path / "pending.json"
    p.write_text(json.dumps(data))
    return str(p)


def _start_server(tmp_path, token="secrettoken"):
    data = {}
    pf = _write_pending(tmp_path, data)
    ApprovalHandler.pending_file = pf
    ApprovalHandler._auth_token = token
    srv = HTTPServer(("127.0.0.1", 0), ApprovalHandler)
    t = threading.Thread(target=srv.serve_forever, daemon=True)
    t.start()
    return srv, srv.server_address[1], pf


def _get(port, path):
    url = f"http://127.0.0.1:{port}{path}"
    try:
        with urllib.request.urlopen(url, timeout=5) as resp:
            return resp.status, resp.read().decode()
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode()


@pytest.fixture
def server(tmp_path):
    srv, port, pf = _start_server(tmp_path)
    yield port, pf
    srv.shutdown()
    srv.server_close()


def test_root_page(server):
    port, pf = server
    status, body = _get(port, "/")
    assert status == 200
    assert "Approval Server Running" in body


def test_approve_flow(server):
    port, pf = server
    token = "tok-approve-1"
    data = {token: {"type": "rebalance", "side": "BUY", "currency": "BTC-USD",
                    "size_usd": 1000, "reason": "good", "bracket": False}}
    with open(pf) as f:
        existing = json.load(f)
    existing.update(data)
    with open(pf, "w") as f:
        json.dump(existing, f)
    status, body = _get(port, f"/approve/{token}")
    assert status == 200
    assert "Approved" in body
    with open(pf) as f:
        saved = json.load(f)
    assert saved[token]["status"] == "approved"
    assert "resolved_at" in saved[token]


def test_approve_invalid_token(server):
    port, pf = server
    status, body = _get(port, "/approve/nonexistent")
    assert status == 404
    assert "Invalid Token" in body


def test_deny_flow(server):
    port, pf = server
    token = "tok-deny-1"
    data = {token: {"type": "rebalance", "side": "SELL", "currency": "ETH-USD",
                    "size_usd": 500, "reason": "bad", "bracket": True,
                    "stop_price": 100.0, "target_price": 200.0}}
    with open(pf) as f:
        existing = json.load(f)
    existing.update(data)
    with open(pf, "w") as f:
        json.dump(existing, f)
    status, body = _get(port, f"/deny/{token}")
    assert status == 200
    assert "Denied" in body
    with open(pf) as f:
        saved = json.load(f)
    assert saved[token]["status"] == "denied"
    # bracket detail html rendered
    assert "100.00" in body and "200.00" in body


def test_deny_invalid_token(server):
    port, pf = server
    status, body = _get(port, "/deny/nope")
    assert status == 404


def test_status_page(server):
    port, pf = server
    token = "tok-status-1"
    data = {token: {"type": "rebalance", "side": "BUY", "currency": "BTC-USD",
                    "size_usd": 1000, "reason": "good", "bracket": True,
                    "stop_price": 50.0, "target_price": 60.0, "status": "pending"}}
    with open(pf) as f:
        existing = json.load(f)
    existing.update(data)
    with open(pf, "w") as f:
        json.dump(existing, f)
    status, body = _get(port, "/status")
    assert status == 200
    assert "Trade Approvals" in body
    assert "BRACKET" in body
    assert "BTC-USD" in body


def test_api_status_auth(server):
    port, pf = server
    token = "tok-api-1"
    data = {token: {"type": "rebalance", "side": "BUY", "currency": "BTC-USD",
                    "size_usd": 1000, "reason": "good", "status": "approved"}}
    with open(pf) as f:
        existing = json.load(f)
    existing.update(data)
    with open(pf, "w") as f:
        json.dump(existing, f)
    # without auth -> 403
    status, _ = _get(port, "/api/status")
    assert status == 403
    # with auth
    url = f"http://127.0.0.1:{port}/api/status"
    req = urllib.request.Request(url, headers={"Authorization": "Bearer secrettoken"})
    with urllib.request.urlopen(req, timeout=5) as resp:
        assert resp.status == 200
        out = json.loads(resp.read().decode())
    assert token in out
    # plain token (non-bearer) also accepted
    req2 = urllib.request.Request(url, headers={"Authorization": "secrettoken"})
    with urllib.request.urlopen(req2, timeout=5) as resp2:
        assert resp2.status == 200


def test_not_found(server):
    port, pf = server
    status, body = _get(port, "/random/path")
    assert status == 404
    assert "Not Found" in body


def test_check_auth_method():
    # Build a minimal fake handler instance to exercise _check_auth branches
    class Fake(ApprovalHandler):
        def __init__(self, auth_header):
            self.headers = {"Authorization": auth_header}
            self._auth_token = "abc"

    assert Fake("Bearer abc")._check_auth() is True
    assert Fake("abc")._check_auth() is True
    assert Fake("wrong")._check_auth() is False
    assert Fake("")._check_auth() is False


def test_serve_creates_pending_file(tmp_path, monkeypatch):
    pf = str(tmp_path / "new_pending.json")
    assert not os.path.exists(pf)

    class FakeServer:
        def __init__(self, addr, handler):
            self.addr = addr
        def serve_forever(self):
            return None
        def shutdown(self):
            return None
        def server_close(self):
            return None

    monkeypatch.setattr(ap, "HTTPServer", FakeServer)
    ap.ApprovalHandler._auth_token = ""
    monkeypatch.setenv("APPROVAL_TOKEN", "")  # force random token branch

    # serve() will create the pending file then call serve_forever (no-op)
    ap.serve(pending_file=pf, port=0, host="127.0.0.1")
    assert os.path.exists(pf)
    with open(pf) as f:
        assert json.load(f) == {}
    # auth token generated (non-empty)
    assert ap.ApprovalHandler._auth_token


def test_read_pending_missing_file(tmp_path):
    class FakeH(ap.ApprovalHandler):
        def __init__(self, pf):
            self.pending_file = pf
            self.headers = {}
    h = FakeH(str(tmp_path / "does_not_exist.json"))
    assert h._read_pending() == {}


def test_parse_token_branches():
    class FakeH(ap.ApprovalHandler):
        def __init__(self):
            self.headers = {}
    h = FakeH()
    assert h._parse_token("/approve/abc", "/approve/") == "abc"
    # non-matching prefix -> empty (else branch)
    assert h._parse_token("/status", "/approve/") == ""


def test_serve_existing_file_and_explicit_token(tmp_path, monkeypatch):
    pf = tmp_path / "existing.json"
    pf.write_text(json.dumps({"t": {"status": "pending"}}))

    class FakeServer:
        def __init__(self, addr, handler):
            self.addr = addr
        def serve_forever(self):
            return None
        def shutdown(self):
            return None
        def server_close(self):
            return None

    monkeypatch.setattr(ap, "HTTPServer", FakeServer)
    monkeypatch.setenv("APPROVAL_TOKEN", "fixedtoken")
    ap.ApprovalHandler._auth_token = ""
    ap.serve(pending_file=str(pf), port=0, host="127.0.0.1")
    assert ap.ApprovalHandler._auth_token == "fixedtoken"
    with open(pf) as f:
        assert json.load(f) == {"t": {"status": "pending"}}


def test_main_invoke(monkeypatch):
    import approval_server as _ap
    called = {}

    def fake_serve(pending_file, port, host):
        called["pending_file"] = pending_file
        called["port"] = port
        called["host"] = host

    monkeypatch.setattr(_ap, "serve", fake_serve)
    monkeypatch.setattr(sys, "argv", ["approval_server", "--port", "9999",
                                       "--host", "127.0.0.1",
                                       "--pending-file", "/tmp/x.json"])
    _ap.main()
    assert called["port"] == 9999
    assert called["host"] == "127.0.0.1"
    assert called["pending_file"] == "/tmp/x.json"
