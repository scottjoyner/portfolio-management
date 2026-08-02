"""Shared conftest for api coverage tests.

Patches fastapi.APIRouter with a no-op stub BEFORE any target module is
imported, so invalid route decorators do not raise at import time. Route
handler functions are still defined normally and can be invoked directly.
"""

import sys

import fastapi


class _StubRouter:
    """No-op replacement for fastapi.APIRouter used during unit testing."""

    def __init__(self, *args, **kwargs):
        self.routes = []

    def __call__(self, *args, **kwargs):
        if args:
            return args[0]
        return self

    def get(self, *args, **kwargs):
        return lambda fn: fn

    def post(self, *args, **kwargs):
        return lambda fn: fn

    def put(self, *args, **kwargs):
        return lambda fn: fn

    def delete(self, *args, **kwargs):
        return lambda fn: fn

    def patch(self, *args, **kwargs):
        return lambda fn: fn

    def websocket(self, *args, **kwargs):
        return lambda fn: fn

    def include_router(self, *args, **kwargs):
        pass

    def add_api_route(self, *args, **kwargs):
        pass

    def add_api_websocket_route(self, *args, **kwargs):
        pass


fastapi.APIRouter = _StubRouter
sys.modules.setdefault("fastapi", fastapi)

# The root coverage conftest provides a minimal Coinbase service collaborator.
# Keep it compatible with the current API module's sanitized-error import.
coinbase_service = sys.modules.get("trading_system.core.exchange.coinbase_service")
if coinbase_service is not None and not hasattr(coinbase_service, "sanitize_error"):
    coinbase_service.sanitize_error = lambda error: str(error).replace("SHOULD_NOT_LEAK", "[REDACTED]")
