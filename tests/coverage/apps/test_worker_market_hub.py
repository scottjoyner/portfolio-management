import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from _helpers import install_fakes

install_fakes({
    "core.config.settings": {"Settings": MagicMock()},
})

from trading_system.apps.worker import market_hub as mh


def run(coro):
    return asyncio.run(coro)


@pytest.fixture(autouse=True)
def _reset_market_hub(monkeypatch):
    # Default: MarketHub present (returns a MagicMock hub instance).
    monkeypatch.setattr(mh, "MarketHub", lambda *a, **k: MagicMock())


def test_constructor_present():
    sub = mh.MarketHubSubscriber()
    assert sub._hub is not None


def test_constructor_absent(monkeypatch):
    monkeypatch.setattr(mh, "MarketHub", None)
    sub = mh.MarketHubSubscriber()
    assert sub._hub is None


def test_constructor_init_raises(monkeypatch):
    class RaiseHub:
        def __init__(self, *a, **k):
            raise RuntimeError("cannot connect")
    monkeypatch.setattr(mh, "MarketHub", RaiseHub)
    sub = mh.MarketHubSubscriber()
    assert sub._hub is None


def test_subscribe_no_callback_or_hub():
    sub = mh.MarketHubSubscriber()
    sub._hub = None
    sub.subscribe()
    sub.subscribe(lambda e: None)


def test_subscribe_with_callback_and_hub():
    sub = mh.MarketHubSubscriber()
    sub._hub = MagicMock()
    sub.subscribe(lambda e: None)
    assert len(sub._signal_handlers) == 1


def test_get_next_signal_no_hub():
    sub = mh.MarketHubSubscriber()
    sub._hub = None
    assert run(sub.get_next_signal()) is None


def test_get_next_signal_not_running():
    sub = mh.MarketHubSubscriber()
    sub._hub = MagicMock()
    sub._running = False
    assert run(sub.get_next_signal()) is None


def test_get_next_signal_running_no_last():
    sub = mh.MarketHubSubscriber()
    sub._hub = MagicMock()
    sub._running = True
    assert run(sub.get_next_signal(timeout=0.2)) is None


def test_get_next_signal_with_last():
    sub = mh.MarketHubSubscriber()
    sub._hub = MagicMock()
    sub._running = True
    sub._last_signal = {"a": 1}
    assert run(sub.get_next_signal(timeout=0.2)) == {"a": 1}


def test_on_market_event_sync_handler():
    sub = mh.MarketHubSubscriber()
    sub._hub = MagicMock()
    seen = []
    sub.subscribe(lambda e: seen.append(e))
    run(sub.on_market_event({"x": 1}))
    assert seen == [{"x": 1}]


def test_on_market_event_async_handler():
    sub = mh.MarketHubSubscriber()
    sub._hub = MagicMock()
    seen = []

    async def h(e):
        seen.append(e)
    sub.subscribe(h)
    run(sub.on_market_event({"y": 2}))
    assert seen == [{"y": 2}]


def test_on_market_event_handler_raises():
    sub = mh.MarketHubSubscriber()
    sub._hub = MagicMock()

    def boom(e):
        raise ValueError("x")
    sub.subscribe(boom)
    run(sub.on_market_event({"z": 3}))


def test_run_no_hub_returns():
    sub = mh.MarketHubSubscriber()
    sub._hub = None
    assert run(sub.run()) is None


def test_run_with_hub(monkeypatch):
    sub = mh.MarketHubSubscriber()
    sub._hub = MagicMock()
    sub._running = True
    calls = {"n": 0}

    async def fake_wait_for(*a, **k):
        calls["n"] += 1
        if calls["n"] == 1:
            raise asyncio.TimeoutError()
        sub._running = False
        return None

    monkeypatch.setattr(mh, "asyncio", SimpleNamespace(wait_for=fake_wait_for, sleep=AsyncMock(), TimeoutError=asyncio.TimeoutError))
    run(sub.run())
    assert sub._running is False
    assert calls["n"] == 2


def test_stop():
    sub = mh.MarketHubSubscriber()
    sub._hub = AsyncMock()
    run(sub.stop())
    assert sub._running is False
    sub._hub.stop.assert_awaited_once()


def test_constructor_no_redis_url(monkeypatch):
    import sys
    from types import SimpleNamespace
    fake_cs = sys.modules["core.config.settings"]
    fake_cs.Settings.from_env.return_value = SimpleNamespace()  # no redis_url
    monkeypatch.setattr(mh, "MarketHub", lambda *a, **k: MagicMock())
    sub = mh.MarketHubSubscriber()
    assert sub._hub is not None


def test_get_next_signal_exception():
    sub = mh.MarketHubSubscriber()
    sub._hub = MagicMock()
    sub._running = True
    last = MagicMock()
    last.copy.side_effect = RuntimeError("boom")
    sub._last_signal = last
    assert run(sub.get_next_signal(timeout=0.01)) is None


def test_get_next_signal_timeout_zero():
    # timeout=0 -> range(0) empty -> loop body skipped -> returns None (120->127)
    sub = mh.MarketHubSubscriber()
    sub._hub = MagicMock()
    sub._running = True
    sub._last_signal = None
    assert run(sub.get_next_signal(timeout=0.0)) is None


def test_constructor_redis_url_exception(monkeypatch):
    # AppSettings.from_env() raises -> except -> redis_url=None (72-73)
    import sys
    fake_cs = sys.modules["core.config.settings"]
    monkeypatch.setattr(fake_cs.Settings, "from_env", lambda: (_ for _ in ()).throw(RuntimeError("no settings")))
    monkeypatch.setattr(mh, "MarketHub", lambda *a, **k: MagicMock())
    sub = mh.MarketHubSubscriber()
    assert sub._hub is not None


def test_run_loop_continues_on_timeout(monkeypatch):
    # wait_for raises TimeoutError while still running -> continue (171)
    sub = mh.MarketHubSubscriber()
    sub._hub = MagicMock()
    sub._running = True
    calls = {"n": 0}

    async def fake_wait_for(*a, **k):
        calls["n"] += 1
        if calls["n"] == 1:
            raise asyncio.TimeoutError()
        sub._running = False
        return None

    monkeypatch.setattr(mh, "asyncio", SimpleNamespace(wait_for=fake_wait_for, sleep=AsyncMock(), TimeoutError=asyncio.TimeoutError))
    run(sub.run())
    assert sub._running is False
    assert calls["n"] == 2


def test_stop_no_hub():
    sub = mh.MarketHubSubscriber()
    sub._hub = None
    run(sub.stop())
    assert sub._running is False


def test_create_market_hub_subscriber_redis_ping_fails(monkeypatch):
    import redis
    monkeypatch.setattr(mh, "MarketHub", lambda *a, **k: MagicMock())
    failing = MagicMock()
    failing.ping.side_effect = RuntimeError("redis down")
    monkeypatch.setattr(redis.Redis, "from_url", lambda *a, **k: failing)
    sub = run(mh.create_market_hub_subscriber())
    assert sub is not None


def test_create_market_hub_subscriber_no_redis_url(monkeypatch):
    import sys
    fake_cs = sys.modules["core.config.settings"]
    monkeypatch.setattr(fake_cs.Settings, "from_env", lambda: SimpleNamespace())  # no redis_url
    monkeypatch.setattr(mh, "MarketHub", lambda *a, **k: MagicMock())
    # redis import may be absent; ensure from_url not called
    sub = run(mh.create_market_hub_subscriber())
    assert sub is not None


def test_create_market_hub_subscriber_from_env_raises(monkeypatch):
    import sys
    fake_cs = sys.modules["core.config.settings"]
    monkeypatch.setattr(fake_cs.Settings, "from_env", lambda: (_ for _ in ()).throw(RuntimeError("boom")))
    monkeypatch.setattr(mh, "MarketHub", lambda *a, **k: MagicMock())
    sub = run(mh.create_market_hub_subscriber())
    assert sub is not None


def test_import_fallback_none():
    # Force both hub import paths to fail -> MarketHub becomes None.
    import importlib
    import importlib.abc
    import sys

    blocked = {"trading_system.hub.pubsub", "hub.pubsub"}

    class _Blocker(importlib.abc.MetaPathFinder):
        def find_spec(self, name, path, target=None):
            if name in blocked:
                raise ImportError("blocked for test")
            return None

    blocker = _Blocker()
    sys.meta_path.insert(0, blocker)
    saved = {}
    for name in ("trading_system.hub.pubsub", "hub.pubsub", "hub"):
        if name in sys.modules:
            saved[name] = sys.modules.pop(name)
    try:
        importlib.reload(mh)
        assert mh.MarketHub is None
    finally:
        sys.meta_path.remove(blocker)
        for name, mod in saved.items():
            sys.modules[name] = mod


def test_run_dispatches_event(monkeypatch):
    # Cover the internal on_market_update body (line 158) by invoking the
    # callback that run() registers with the hub.
    sub = mh.MarketHubSubscriber()
    hub = MagicMock()
    captured = {}

    def fake_subscribe(topic, cb):
        captured["cb"] = cb

    hub.subscribe.side_effect = fake_subscribe
    sub._hub = hub
    sub._running = True

    async def fake_wait_for(*a, **k):
        sub._running = False
        return None

    monkeypatch.setattr(
        mh, "asyncio",
        SimpleNamespace(wait_for=fake_wait_for, sleep=AsyncMock(), TimeoutError=asyncio.TimeoutError),
    )
    run(sub.run())
    run(captured["cb"]({"topic": "x", "data": {"price": 1}}))
    assert sub._last_signal is not None


def test_create_market_hub_subscriber_redis_ok(monkeypatch):
    import redis
    import sys
    fake_cs = sys.modules["core.config.settings"]
    monkeypatch.setattr(fake_cs.Settings, "from_env", lambda: SimpleNamespace(redis_url="redis://localhost:6379"))
    monkeypatch.setattr(mh, "MarketHub", lambda *a, **k: MagicMock())
    monkeypatch.setattr(redis.Redis, "from_url", lambda *a, **k: MagicMock(ping=MagicMock()))
    sub = run(mh.create_market_hub_subscriber())
    assert sub is not None


