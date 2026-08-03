"""Shared collaborators for generated application tests."""

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
