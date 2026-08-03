#!/usr/bin/env python3
"""Append-only, hash-chained lineage for model decisions and learning changes."""
from __future__ import annotations

import hashlib
import json
import os
import tempfile
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

ROOT = Path(__file__).resolve().parent.parent
DEFAULT_PATH = ROOT / "data" / "learning_lineage.jsonl"
EVENT_TYPES = {
    "model_request", "signal", "trade", "outcome", "proposal",
    "evaluation", "promotion", "rollback", "budget_block", "error",
}
SENSITIVE_KEYS = {"api_key", "authorization", "password", "private_key", "secret"}


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _sanitize(value: Any) -> Any:
    if isinstance(value, dict):
        return {
            str(key): ("[REDACTED]" if str(key).lower() in SENSITIVE_KEYS else _sanitize(item))
            for key, item in value.items()
        }
    if isinstance(value, list):
        return [_sanitize(item) for item in value]
    if isinstance(value, tuple):
        return [_sanitize(item) for item in value]
    return value


def _canonical(event: dict[str, Any]) -> bytes:
    body = {key: value for key, value in event.items() if key != "event_hash"}
    return json.dumps(body, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")


def _hash(event: dict[str, Any]) -> str:
    return hashlib.sha256(_canonical(event)).hexdigest()


class LineageStore:
    def __init__(self, path: Path | str = DEFAULT_PATH):
        self.path = Path(path)

    def events(self) -> list[dict[str, Any]]:
        if not self.path.exists():
            return []
        rows: list[dict[str, Any]] = []
        for line in self.path.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            row = json.loads(line)
            if not isinstance(row, dict):
                raise ValueError("lineage row is not an object")
            rows.append(row)
        return rows

    def append(
        self,
        event_type: str,
        payload: dict[str, Any],
        *,
        actor: str = "system",
        parents: Iterable[str] = (),
        event_id: str | None = None,
        occurred_at: str | None = None,
    ) -> dict[str, Any]:
        if event_type not in EVENT_TYPES:
            raise ValueError(f"unsupported lineage event type: {event_type}")
        rows = self.events()
        known = {row.get("id") for row in rows}
        parent_ids = list(dict.fromkeys(str(parent) for parent in parents if parent))
        missing = [parent for parent in parent_ids if parent not in known]
        if missing:
            raise ValueError(f"unknown lineage parents: {missing}")
        event = {
            "schema_version": 1,
            "sequence": len(rows) + 1,
            "id": event_id or f"lin-{uuid.uuid4().hex}",
            "type": event_type,
            "actor": actor,
            "occurred_at": occurred_at or _utc_now(),
            "parents": parent_ids,
            "previous_hash": rows[-1].get("event_hash") if rows else None,
            "payload": _sanitize(payload),
        }
        event["event_hash"] = _hash(event)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        fd, temp_name = tempfile.mkstemp(prefix=f".{self.path.name}.", suffix=".tmp", dir=self.path.parent)
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as handle:
                for row in rows:
                    handle.write(json.dumps(row, sort_keys=True, separators=(",", ":")) + "\n")
                handle.write(json.dumps(event, sort_keys=True, separators=(",", ":")) + "\n")
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temp_name, self.path)
        finally:
            try:
                os.unlink(temp_name)
            except FileNotFoundError:
                pass
        return event

    def verify(self) -> dict[str, Any]:
        rows = self.events()
        known: set[str] = set()
        previous = None
        errors: list[str] = []
        for expected_sequence, row in enumerate(rows, start=1):
            event_id = row.get("id")
            if row.get("sequence") != expected_sequence:
                errors.append(f"sequence:{event_id}")
            if row.get("previous_hash") != previous:
                errors.append(f"previous_hash:{event_id}")
            if row.get("event_hash") != _hash(row):
                errors.append(f"event_hash:{event_id}")
            for parent in row.get("parents", []):
                if parent not in known:
                    errors.append(f"parent_order:{event_id}:{parent}")
            if event_id in known:
                errors.append(f"duplicate_id:{event_id}")
            known.add(event_id)
            previous = row.get("event_hash")
        return {
            "ok": not errors,
            "events": len(rows),
            "head_hash": previous,
            "errors": errors,
            "verified_at": _utc_now(),
        }

    def descendants(self, event_id: str) -> list[dict[str, Any]]:
        rows = self.events()
        frontier = {event_id}
        output: list[dict[str, Any]] = []
        for row in rows:
            if frontier.intersection(row.get("parents", [])):
                output.append(row)
                frontier.add(row["id"])
        return output


def record_model_request(store: LineageStore, **payload: Any) -> dict[str, Any]:
    return store.append("model_request", payload, actor=str(payload.get("agent_id", "openrouter-agent")))


def record_signal(store: LineageStore, model_request_id: str, **payload: Any) -> dict[str, Any]:
    return store.append("signal", payload, actor="openrouter-agent", parents=[model_request_id])


def record_trade(store: LineageStore, signal_id: str, **payload: Any) -> dict[str, Any]:
    return store.append("trade", payload, actor="paper-execution", parents=[signal_id])


def record_outcome(store: LineageStore, trade_id: str, **payload: Any) -> dict[str, Any]:
    return store.append("outcome", payload, actor="outcome-reconciler", parents=[trade_id])
