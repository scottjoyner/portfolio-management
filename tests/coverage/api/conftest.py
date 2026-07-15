"""Shared conftest for api coverage tests.

Patches fastapi.APIRouter with a no-op stub BEFORE any target module is
imported, so that invalid route decorators (e.g. class decorators, routes with
non-pydantic dependency params like AsyncSession) do not raise at import time.
Route handler functions are still defined normally and can be invoked directly
from tests.
"""

import sys

import fastapi


class _StubRouter:
    """No-op replacement for fastapi.APIRouter used during unit testing."""

    def __init__(self, *args, **kwargs):
        self.routes = []

    def __call__(self, *args, **kwargs):
        # Support usage as a class decorator: @APIRouter(prefix=...)
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


# Patch at import time of this conftest (runs before any test module import).
fastapi.APIRouter = _StubRouter
sys.modules.setdefault("fastapi", fastapi)
