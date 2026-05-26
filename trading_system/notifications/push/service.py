from __future__ import annotations

import logging
from dataclasses import dataclass, field

log = logging.getLogger(__name__)


@dataclass
class PushNotification:
    title: str
    body: str
    data: dict = field(default_factory=dict)
    device_tokens: list[str] = field(default_factory=list)


@dataclass
class PushService:
    def send(self, notification: PushNotification) -> bool:
        log.info("push_sent title=%s tokens=%d", notification.title, len(notification.device_tokens))
        return True
