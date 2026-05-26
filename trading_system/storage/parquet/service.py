from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any

log = logging.getLogger(__name__)


@dataclass
class ParquetStore:
    base_path: str = "/tmp/parquet_data"
    _data: dict[str, list[dict[str, Any]]] = field(default_factory=dict)

    def append(self, table: str, row: dict[str, Any]) -> None:
        self._data.setdefault(table, []).append(row)

    def read(self, table: str) -> list[dict[str, Any]]:
        return self._data.get(table, [])

    def flush(self, table: str) -> None:
        self._data.pop(table, None)
