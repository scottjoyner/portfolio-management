from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any

log = logging.getLogger(__name__)


@dataclass
class WebhookEndpoint:
    url: str
    secret: str = ""
    headers: dict[str, str] = field(default_factory=dict)
    enabled: bool = True


@dataclass
class WebhookService:
    endpoints: list[WebhookEndpoint] = field(default_factory=list)

    def add_endpoint(self, endpoint: WebhookEndpoint) -> None:
        self.endpoints.append(endpoint)

    def remove_endpoint(self, url: str) -> None:
        self.endpoints = [e for e in self.endpoints if e.url != url]

    def dispatch(self, event_type: str, payload: dict[str, Any]) -> list[bool]:
        results: list[bool] = []
        for ep in self.endpoints:
            if not ep.enabled:
                continue
            log.info("webhook_dispatch url=%s event=%s", ep.url, event_type)
            results.append(True)
        return results
