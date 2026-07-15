from decimal import Decimal

from trading_system.execution.router.service import (
    ExecutionRouter,
    Venue,
    VenueType,
    RoutingDecision,
)


def _venue(name, vtype=VenueType.COINBASE, priority=0, enabled=True, products=None):
    return Venue(name=name, venue_type=vtype, priority=priority, enabled=enabled,
                 supported_products=products if products is not None else set())


def test_register_venue_sorts_by_priority():
    r = ExecutionRouter()
    r.register_venue(_venue("b", priority=2))
    r.register_venue(_venue("a", priority=1))
    assert [v.name for v in r.venues] == ["a", "b"]


def test_remove_venue():
    r = ExecutionRouter()
    r.register_venue(_venue("a"))
    r.register_venue(_venue("b"))
    r.remove_venue("a")
    assert [v.name for v in r.venues] == ["b"]
    # remove non-existent -> no drop
    r.remove_venue("zzz")
    assert [v.name for v in r.venues] == ["b"]


def test_route_empty():
    r = ExecutionRouter()
    d = r.route("BTC-USD", "buy", Decimal("1"), "market")
    assert d.venue is None
    assert d.reason == "no suitable venue found"


def test_route_disabled_venue():
    r = ExecutionRouter()
    r.register_venue(_venue("a", enabled=False))
    d = r.route("BTC-USD", "buy", Decimal("1"), "market")
    assert d.venue is None


def test_route_enabled_but_unsupported():
    r = ExecutionRouter()
    r.register_venue(_venue("a", products={"ETH-USD"}))
    d = r.route("BTC-USD", "buy", Decimal("1"), "market")
    assert d.venue is None


def test_route_empty_supported_routes():
    r = ExecutionRouter()
    r.register_venue(_venue("a", products=set()))
    d = r.route("BTC-USD", "buy", Decimal("1"), "market")
    assert d.venue is not None
    assert d.venue.name == "a"
    assert d.reason == "routed to a"


def test_route_supported():
    r = ExecutionRouter()
    r.register_venue(_venue("a", products={"BTC-USD"}))
    d = r.route("BTC-USD", "buy", Decimal("1"), "market")
    assert isinstance(d, RoutingDecision)
    assert d.venue.name == "a"


def test_route_picks_highest_priority():
    r = ExecutionRouter()
    r.register_venue(_venue("low", priority=5, products={"BTC-USD"}))
    r.register_venue(_venue("high", priority=1, products={"BTC-USD"}))
    d = r.route("BTC-USD", "buy", Decimal("1"), "market")
    assert d.venue.name == "high"
