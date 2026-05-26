from __future__ import annotations

from collections import defaultdict
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any


@dataclass
class Event:
    type: str
    data: dict[str, Any] = field(default_factory=dict)


Handler = Callable[[Event], None]


@dataclass
class EventBus:
    _handlers: dict[str, list[Handler]] = field(default_factory=lambda: defaultdict(list))

    def subscribe(self, event_type: str, handler: Handler) -> None:
        self._handlers[event_type].append(handler)

    def unsubscribe(self, event_type: str, handler: Handler) -> None:
        self._handlers[event_type] = [h for h in self._handlers[event_type] if h is not handler]

    def publish(self, event: Event) -> None:
        for handler in self._handlers.get(event.type, []):
            handler(event)
