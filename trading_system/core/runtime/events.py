from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, Dict, List, Optional

from trading_system.core.runtime.models import TradingEvent

DEFAULT_EVENTS_PATH = Path("runtime/events/trading-events.jsonl")


class EventRecorder:
    """Append-only JSONL event recorder with simple tail/filter support."""

    def __init__(self, path: Optional[Path | str] = None) -> None:
        configured = path or os.getenv("TRADING_EVENTS_PATH") or DEFAULT_EVENTS_PATH
        self.path = Path(configured)
        self.path.parent.mkdir(parents=True, exist_ok=True)

    def record(self, event: TradingEvent | Dict[str, Any]) -> Dict[str, Any]:
        data = event.to_dict() if hasattr(event, "to_dict") else dict(event)
        with self.path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(data, sort_keys=True) + "\n")
        return data

    def tail(
        self,
        limit: int = 100,
        strategy_id: Optional[str] = None,
        source: Optional[str] = None,
        event_type: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        if limit <= 0:
            return []
        if not self.path.exists():
            return []

        events: List[Dict[str, Any]] = []
        with self.path.open("r", encoding="utf-8") as handle:
            for line in handle:
                line = line.strip()
                if not line:
                    continue
                try:
                    event = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if strategy_id is not None and event.get("strategy_id") != strategy_id:
                    continue
                if source is not None and event.get("source") != source:
                    continue
                if event_type is not None and event.get("event_type") != event_type:
                    continue
                events.append(event)
        return events[-limit:]

    def status(self) -> Dict[str, Any]:
        return {
            "path": str(self.path),
            "exists": self.path.exists(),
            "event_count": len(self.tail(limit=10_000)) if self.path.exists() else 0,
        }
