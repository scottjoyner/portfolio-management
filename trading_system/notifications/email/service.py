from __future__ import annotations

import logging
from dataclasses import dataclass

log = logging.getLogger(__name__)


@dataclass
class EmailAlert:
    to: list[str]
    subject: str
    body: str
    html: str = ""


@dataclass
class EmailService:
    smtp_host: str = "localhost"
    smtp_port: int = 25
    from_address: str = "trading@localhost"
    use_tls: bool = False

    def send(self, alert: EmailAlert) -> bool:
        log.info("email_sent to=%s subject=%s", alert.to, alert.subject)
        return True
