from core.events.pubsub import Event, EventBus


def test_subscribe_and_publish():
    bus = EventBus()
    received = []
    bus.subscribe("price", lambda e: received.append(e))
    bus.publish(Event(type="price", data={"x": 1}))
    assert len(received) == 1
    assert received[0].data == {"x": 1}


def test_publish_no_subscribers():
    bus = EventBus()
    bus.publish(Event(type="nope", data={}))


def test_multiple_handlers_same_type():
    bus = EventBus()
    a, b = [], []
    bus.subscribe("t", lambda e: a.append(e))
    bus.subscribe("t", lambda e: b.append(e))
    bus.publish(Event(type="t"))
    assert len(a) == 1 and len(b) == 1


def test_unsubscribe_removes():
    bus = EventBus()
    received = []
    handler = lambda e: received.append(e)
    bus.subscribe("t", handler)
    bus.unsubscribe("t", handler)
    bus.publish(Event(type="t"))
    assert received == []


def test_unsubscribe_keeps_others():
    bus = EventBus()
    a, b = [], []
    ha = lambda e: a.append(e)
    hb = lambda e: b.append(e)
    bus.subscribe("t", ha)
    bus.subscribe("t", hb)
    bus.unsubscribe("t", ha)
    bus.publish(Event(type="t"))
    assert a == [] and len(b) == 1


def test_event_defaults():
    e = Event(type="x")
    assert e.data == {}
