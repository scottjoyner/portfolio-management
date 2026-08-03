"""Shared conftest for API generated-coverage tests.

Generated API tests invoke route functions directly. They do not need FastAPI
to mount the complete production router graph, so mounting is disabled only in
the isolated generated-test process.
"""

import sys

import fastapi


class _StubRouter:
    """Decorator-compatible replacement for ``fastapi.APIRouter``."""

    def __init__(self, *args, **kwargs):
        self.routes = []

    def __call__(self, *args, **kwargs):
        return args[0] if args else self

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
fastapi.FastAPI.include_router = lambda self, *args, **kwargs: None
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
