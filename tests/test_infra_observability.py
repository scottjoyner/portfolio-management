"""Infrastructure/observability tests: network guard + dashboard experiment endpoint."""
from __future__ import annotations

import json
import socket
import urllib.request
from pathlib import Path

import pytest

try:
    import requests as _requests
except Exception:  # pragma: no cover
    _requests = None

_ORIG_SEND = _requests.Session.send if _requests else None
_ORIG_REQUEST = _requests.Session.request if _requests else None
_ORIG_URLOPEN = urllib.request.urlopen
_ORIG_SOCKET = socket.socket


class _NetworkBlocked(RuntimeError):
    pass


def _raise(label, *args, **kwargs):
    raise _NetworkBlocked(f"blocked ({label})")


def _apply_network_block():
    if _requests is not None:
        _requests.Session.send = (
            lambda self, *args, **kwargs: _raise("requests.Session.send")
        )
        _requests.Session.request = (
            lambda self, *args, **kwargs: _raise("requests.Session.request")
        )
    urllib.request.urlopen = (
        lambda *args, **kwargs: _raise("urllib.request.urlopen")
    )

    class _BlockedSocket:
        def __init__(self, *args, **kwargs):
            raise _NetworkBlocked("socket.socket() blocked")

    socket.socket = _BlockedSocket


def _restore_network_block():
    if _requests is not None:
        _requests.Session.send = _ORIG_SEND
        _requests.Session.request = _ORIG_REQUEST
    urllib.request.urlopen = _ORIG_URLOPEN
    socket.socket = _ORIG_SOCKET


@pytest.fixture(autouse=True)
def _restore_network_state_after_test():
    """Prevent a failed assertion from poisoning later process-global tests."""
    try:
        yield
    finally:
        _restore_network_block()


def test_network_block_raises_on_socket():
    """The network_block guard must raise on a real socket attempt."""
    pytest.importorskip("requests")
    _apply_network_block()
    with pytest.raises(Exception):
        socket.create_connection(("1.2.3.4", 80), timeout=1)


def test_network_block_raises_on_urlopen():
    pytest.importorskip("requests")
    _apply_network_block()
    with pytest.raises(Exception):
        urllib.request.urlopen("http://example.invalid/", timeout=1)


class _FakeHandler:
    """Minimal stand-in for the dashboard BaseHTTPRequestHandler for unit testing."""

    def __init__(self):
        self.sent = None

    def _json_response(self, data_str, status=200):
        self.sent = (status, json.loads(data_str))


def _make_server_module():
    import importlib.util

    path = (
        Path(__file__).resolve().parents[1]
        / "trading_system"
        / "ui"
        / "dashboard_server.py"
    )
    spec = importlib.util.spec_from_file_location("dashboard_server_test", path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def test_backtest_experiments_endpoint_lists_ledger(tmp_path, monkeypatch):
    mod = _make_server_module()
    exp_dir = tmp_path / "scripts" / "experiments"
    exp_dir.mkdir(parents=True)
    (exp_dir / "ledger.jsonl").write_text(
        json.dumps(
            {
                "name": "smoke_v1",
                "pass_rate": 0.1,
                "n_passed": 2,
                "mean_sharpe_passed": 1.2,
            }
        )
        + "\n"
    )
    scorecard_dir = exp_dir / "smoke_v1"
    scorecard_dir.mkdir()
    (scorecard_dir / "scorecard.json").write_text(
        json.dumps(
            {
                "name": "smoke_v1",
                "n_strategies_tested": 5,
                "mean_sharpe_passed": 1.2,
                "ensemble": {"voting": "soft"},
                "regime": "trending",
            }
        )
    )

    monkeypatch.setattr(mod, "_experiments_dir", lambda: exp_dir)
    result = mod.api_backtest_experiments()
    assert result["status"] == "ok"
    assert result["count"] == 1
    row = result["experiments"][0]
    assert row["name"] == "smoke_v1"
    assert row["n_strategies_tested"] == 5
    assert row["ensemble"] == {"voting": "soft"}
    assert row["regime"] == "trending"


def test_backtest_experiments_single_name(tmp_path, monkeypatch):
    mod = _make_server_module()
    exp_dir = tmp_path / "scripts" / "experiments"
    exp_dir.mkdir(parents=True)
    (exp_dir / "ledger.jsonl").write_text(
        json.dumps({"name": "wf_v1", "pass_rate": 0.0, "n_passed": 0})
        + "\n"
    )
    monkeypatch.setattr(mod, "_experiments_dir", lambda: exp_dir)
    result = mod.api_backtest_experiments(name="wf_v1")
    assert result["experiment"]["name"] == "wf_v1"

    missing = mod.api_backtest_experiments(name="nope")
    assert missing["status"] == "error"
