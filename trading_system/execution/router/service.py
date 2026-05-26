from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
from enum import Enum


class VenueType(Enum):
    COINBASE = "coinbase"
    PAPER = "paper"


@dataclass
class Venue:
    name: str
    venue_type: VenueType
    priority: int = 0
    enabled: bool = True
    supported_products: set[str] = field(default_factory=set)


@dataclass
class RoutingDecision:
    venue: Venue | None
    reason: str = ""


@dataclass
class ExecutionRouter:
    venues: list[Venue] = field(default_factory=list)

    def register_venue(self, venue: Venue) -> None:
        self.venues.append(venue)
        self.venues.sort(key=lambda v: v.priority)

    def remove_venue(self, name: str) -> None:
        self.venues = [v for v in self.venues if v.name != name]

    def route(self, product_id: str, side: str, size: Decimal, order_type: str) -> RoutingDecision:
        for venue in self.venues:
            if not venue.enabled:
                continue
            if venue.supported_products and product_id not in venue.supported_products:
                continue
            return RoutingDecision(venue=venue, reason=f"routed to {venue.name}")

        return RoutingDecision(venue=None, reason="no suitable venue found")
