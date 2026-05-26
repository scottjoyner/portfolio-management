from core.events.pubsub import EventBus, Event
from core.security.utils import sha256_hex, generate_salt
from core.utils.helpers import retry, bps_to_decimal, timestamp_ms
from decimal import Decimal


def test_eventbus():
    bus = EventBus()
    received = []

    def handler(event: Event) -> None:
        received.append(event)

    bus.subscribe("test.event", handler)
    bus.publish(Event(type="test.event", data={"key": "value"}))
    assert len(received) == 1
    assert received[0].type == "test.event"
    assert received[0].data["key"] == "value"


def test_eventbus_unsubscribe():
    bus = EventBus()
    received = []

    def handler(event: Event) -> None:
        received.append(event)

    bus.subscribe("test.event", handler)
    bus.unsubscribe("test.event", handler)
    bus.publish(Event(type="test.event"))
    assert len(received) == 0


def test_sha256_hex():
    result = sha256_hex(b"hello")
    assert len(result) == 64
    assert result == sha256_hex("hello")


def test_generate_salt():
    s1 = generate_salt()
    s2 = generate_salt()
    assert len(s1) == 32
    assert s1 != s2


def test_bps_to_decimal():
    result = bps_to_decimal(50)
    assert result == Decimal("0.005")


def test_timestamp_ms():
    ts = timestamp_ms()
    assert ts > 1700000000000


def test_retry_success():
    call_count = 0

    @retry(max_attempts=3, delay=0)
    def succeeds() -> str:
        nonlocal call_count
        call_count += 1
        return "ok"

    result = succeeds()
    assert result == "ok"
    assert call_count == 1


def test_retry_failure():
    call_count = 0

    @retry(max_attempts=3, delay=0)
    def always_fails() -> str:
        nonlocal call_count
        call_count += 1
        raise ValueError("fail")

    try:
        always_fails()
        assert False
    except RuntimeError:
        assert call_count == 3
