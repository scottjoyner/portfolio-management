from __future__ import annotations

import json
from pathlib import Path
from typing import Iterable

from .ranking import RANKING_STATE_PATH, StrategyRanking
from .sidecar_adapter import SidecarResearchRecord, research_record_from_manifest


def load_manifest(path: str | Path) -> dict:
    with Path(path).open("r", encoding="utf-8") as handle:
        return json.load(handle)


def add_record(record: SidecarResearchRecord, ranking: StrategyRanking) -> None:
    if record.num_trades <= 0:
        return
    count = max(1, min(record.num_trades, 25))
    wins = int(round(count * max(0.0, min(record.win_rate_pct, 100.0)) / 100.0))
    losses = max(0, count - wins)
    positive_value = abs(record.total_return_pct / 100.0) / max(wins, 1) if wins else 0.0
    negative_value = -abs(record.max_drawdown_pct / 100.0) / max(losses, 1) if losses else -0.001
    confidence = max(0.05, min(0.95, record.research_score))
    for _ in range(wins):
        ranking.record_trade(record.strategy_name, positive_value, confidence)
    for _ in range(losses):
        ranking.record_trade(record.strategy_name, negative_value, confidence)
    ranking.rank_all()


def import_manifest_paths(paths: Iterable[str | Path], ranking_path: str | Path = RANKING_STATE_PATH) -> list[SidecarResearchRecord]:
    ranking = StrategyRanking()
    ranking.load(str(ranking_path))
    records: list[SidecarResearchRecord] = []
    for path in paths:
        record = research_record_from_manifest(load_manifest(path), manifest_path=str(path))
        if record.product_id and record.is_coinbase_product:
            add_record(record, ranking)
            records.append(record)
    ranking.save(str(ranking_path))
    return records
