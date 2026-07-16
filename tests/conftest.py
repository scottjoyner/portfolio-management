"""Shared pytest fixtures.

Network guard (opt-in):
  Tests decorated with ``@pytest.mark.network_block`` get socket / requests /
  urllib patched so that ANY real network call raises instead of hanging or
  hitting a live API. This makes a forgotten mock fail fast.

  Opt a test into the guard with:
      @pytest.mark.network_block
      def test_something(): ...

  The ``ALLOW_NETWORK=1`` env var disables the block globally (useful for
  local runs against real services).
"""
from __future__ import annotations

import functools
import os
import socket
import sys
import types
import urllib.request

import pytest


def _stub_event_markets():
    """Stub broken/in-progress event_markets submodules so imports succeed.

    NOTE: this stub was added as a temporary workaround while
    event_markets/signal_adapter.py had a transient IndentationError. That
    module now imports cleanly, so the stub is disabled — event_markets tests
    run against the REAL package. Re-enable only if a genuine transient break
    returns in an event_markets submodule.
    """
    return  # Stub disabled: signal_adapter now imports cleanly.
    if "event_markets" in sys.modules:
        return
    pkg = types.ModuleType("event_markets")
    pkg.__path__ = []
    sys.modules["event_markets"] = pkg

    for name in (
        "event_markets.comparison_engine",
        "event_markets.arbitrage",
        "event_markets.unified_client",
        "event_markets.polymarket_client",
        "event_markets.kalshi_client",
        "event_markets.knowledge_gap",
        "event_markets.signal_adapter",
    ):
        m = types.ModuleType(name)
        for attr in (
            "ComparisonEngine", "format_signal", "EventArbitrageScanner",
            "format_arbitrage", "UnifiedPredictionMarketClient", "PolymarketClient",
            "KalshiClient", "KnowledgeGapAnalyzer", "PredictionMarketAdapter",
        ):
            setattr(m, attr, object)
        sys.modules[name] = m

_stub_event_markets()

try:
    import requests as _requests
except Exception:  # pragma: no cover - requests may be absent in some envs
    _requests = None


_REAL_SOCKET = getattr(socket, "socket", None)
_BLOCK_ACTIVE = os.environ.get("ALLOW_NETWORK") != "1"

_ORIG_SEND = _requests.Session.send if _requests else None
_ORIG_REQUEST = _requests.Session.request if _requests else None
_ORIG_URLOPEN = urllib.request.urlopen
_ORIG_URLRETRIEVE = getattr(urllib.request, "urlretrieve", None)


class _NetworkBlocked(RuntimeError):
    pass


def _raise(label, *args, **kwargs):
    raise _NetworkBlocked(
        f"Real network call blocked by network_block fixture ({label}). "
        "Mock the dependency or set ALLOW_NETWORK=1."
    )


def _apply_network_block():
    if _requests is not None:
        _requests.Session.send = functools.wraps(_ORIG_SEND)(lambda self, *a, **k: _raise("requests.Session.send"))
        _requests.Session.request = functools.wraps(_ORIG_REQUEST)(lambda self, *a, **k: _raise("requests.Session.request"))

    urllib.request.urlopen = functools.wraps(_ORIG_URLOPEN)(lambda *a, **k: _raise("urllib.request.urlopen"))
    if _ORIG_URLRETRIEVE is not None:
        urllib.request.urlretrieve = functools.wraps(_ORIG_URLRETRIEVE)(lambda *a, **k: _raise("urllib.request.urlretrieve"))

    class _BlockedSocket:
        def __init__(self, *a, **k):
            raise _NetworkBlocked("socket.socket() blocked by network_block fixture.")

    socket.socket = _BlockedSocket  # type: ignore[assignment]


def _restore_network_block():
    if _requests is not None:
        _requests.Session.send = _ORIG_SEND
        _requests.Session.request = _ORIG_REQUEST
    urllib.request.urlopen = _ORIG_URLOPEN
    if _ORIG_URLRETRIEVE is not None:
        urllib.request.urlretrieve = _ORIG_URLRETRIEVE
    if _REAL_SOCKET is not None:
        socket.socket = _REAL_SOCKET  # type: ignore[assignment]


def pytest_runtest_setup(item):
    """Apply the network guard to any test marked ``network_block``."""
    if item.get_closest_marker("network_block") is None:
        return
    if not _BLOCK_ACTIVE:
        pytest.skip("ALLOW_NETWORK=1 set; network block disabled")
    _apply_network_block()


def pytest_runtest_teardown(item):
    if item.get_closest_marker("network_block") is not None:
        _restore_network_block()


def pytest_configure(config):
    config.addinivalue_line(
        "markers",
        "network_block: activate the socket/requests/urllib network guard for this test",
    )
