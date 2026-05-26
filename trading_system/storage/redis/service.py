from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from typing import Any

log = logging.getLogger(__name__)


@dataclass
class RedisStore:
    _data: dict[str, str] = field(default_factory=dict)

    def set(self, key: str, value: Any, ttl_seconds: int | None = None) -> None:
        self._data[key] = json.dumps(value)

    def get(self, key: str) -> Any | None:
        raw = self._data.get(key)
        if raw is None:
            return None
        try:
            return json.loads(raw)
        except (json.JSONDecodeError, TypeError):
            return raw

    def delete(self, key: str) -> None:
        self._data.pop(key, None)

    def exists(self, key: str) -> bool:
        return key in self._data
