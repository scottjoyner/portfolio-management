from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
import json


@dataclass
class ExperimentRun:
    run_id: str
    strategy_id: str
    config_hash: str
    metrics: dict


class ExperimentTracker:
    def __init__(self, manifest_path: str = "artifacts/experiments.jsonl") -> None:
        self.path = Path(manifest_path)
        self.path.parent.mkdir(parents=True, exist_ok=True)

    def record(self, run: ExperimentRun) -> None:
        payload = asdict(run) | {"recorded_at": datetime.now(timezone.utc).isoformat()}
        with self.path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(payload) + "\n")
