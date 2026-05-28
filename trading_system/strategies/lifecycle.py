from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from sqlalchemy.orm import Session

from storage.postgres.models import StrategyConfig, StrategyRun
from strategies.registry.registry import load_strategies

log = logging.getLogger(__name__)


class StrategyLifecycleError(Exception):
    pass


@dataclass
class StrategyLifecycleManager:
    repo: Any

    def start(self, strategy_id: str, mode: str = "paper") -> StrategyRun:
        config = self.repo.db.query(StrategyConfig).filter(StrategyConfig.strategy_id == strategy_id).first()
        if not config:
            config = StrategyConfig(strategy_id=strategy_id, strategy_type="unknown", status="implemented", enabled=True)
            self.repo.db.add(config)
            self.repo.db.commit()
        if not config.enabled:
            raise StrategyLifecycleError(f"strategy {strategy_id} is disabled")

        run = StrategyRun(
            task_id=f"{strategy_id}-{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}",
            strategy_id=strategy_id,
            status="running",
            mode=mode,
            started_at=datetime.now(timezone.utc),
        )
        self.repo.create_strategy_run(run)
        log.info("strategy_started id=%s mode=%s", strategy_id, mode)
        return run

    def stop(self, task_id: str) -> StrategyRun | None:
        run = self.repo.get_strategy_run(task_id)
        if run:
            self.repo.update_strategy_run(task_id, status="stopped", completed_at=datetime.now(timezone.utc))
            log.info("strategy_stopped task=%s", task_id)
        return run

    def pause(self, task_id: str) -> StrategyRun | None:
        return self.repo.update_strategy_run(task_id, status="paused")

    def resume(self, task_id: str) -> StrategyRun | None:
        return self.repo.update_strategy_run(task_id, status="running", started_at=datetime.now(timezone.utc))

    def running_strategies(self) -> list[StrategyRun]:
        return self.repo.db.query(StrategyRun).filter(StrategyRun.status == "running").all()

    def enable(self, strategy_id: str) -> StrategyConfig | None:
        config = self.repo.db.query(StrategyConfig).filter(StrategyConfig.strategy_id == strategy_id).first()
        if config:
            config.enabled = True
            self.repo.db.commit()
        return config

    def disable(self, strategy_id: str) -> StrategyConfig | None:
        config = self.repo.db.query(StrategyConfig).filter(StrategyConfig.strategy_id == strategy_id).first()
        if config:
            config.enabled = False
            self.repo.db.commit()
        return config

    def disabled_ids(self) -> set[str]:
        return {
            row[0] for row in
            self.repo.db.query(StrategyConfig.strategy_id).filter(StrategyConfig.enabled.is_(False)).all()
        }


def sync_catalog_to_db(db: Session) -> list[StrategyConfig]:
    """Sync strategy catalog entries into strategy_configs table."""
    repo = __import__("storage.postgres.repository", fromlist=["OpsRepository"]).OpsRepository(db)
    strategies = load_strategies()
    synced: list[StrategyConfig] = []
    for s in strategies:
        meta = s.metadata()
        config = repo.db.query(StrategyConfig).filter(StrategyConfig.strategy_id == s.strategy_id).first()
        if not config:
            config = StrategyConfig(
                strategy_id=s.strategy_id,
                strategy_type=meta.get("strategy_type", "unknown"),
                status="implemented",
                paper_mode=meta.get("paper_mode", True),
                live_supported=meta.get("live_supported", False),
                enabled=meta.get("enabled", True),
                config_json=str(meta.get("config", {})),
            )
            repo.db.add(config)
            synced.append(config)
        else:
            config.strategy_type = meta.get("strategy_type", config.strategy_type)
            config.config_json = str(meta.get("config", {}))
            synced.append(config)
    repo.db.commit()
    log.info("synced_strategy_configs count=%d", len(synced))
    return synced
