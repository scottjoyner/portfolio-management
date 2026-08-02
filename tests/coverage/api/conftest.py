"""Shared conftest for API generated-coverage tests.

Patches FastAPI routing with a small decorator-compatible stub before target
modules import. The tests invoke route handlers directly and do not need a
fully constructed application router.
"""

import sys

import fastapi


class _StubRouter:
    """No-op replacement for ``fastapi.APIRouter`` used by direct unit tests."""

    def __init__(self, *args, **kwargs):
        self.routes = []

    def _contains_router(self, router):
        return False

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

coinbase_service = sys.modules.get("trading_system.core.exchange.coinbase_service")
if coinbase_service is not None and not hasattr(coinbase_service, "sanitize_error"):
    coinbase_service.sanitize_error = lambda error: str(error).replace(
        "SHOULD_NOT_LEAK", "[REDACTED]"
    )

settings_module = sys.modules.get("core.config.settings")
settings_class = getattr(settings_module, "Settings", None) if settings_module else None
if settings_class is not None and not hasattr(settings_class, "onchain_mode"):
    settings_class.onchain_mode = "paper"
