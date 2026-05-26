from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from storage.postgres.models import StrategyConfig, StrategyRun

log = logging.getLogger(__name__)


class StrategyLifecycleError(Exception):
    pass


@dataclass
class StrategyLifecycleManager:
    repo: Any

    def start(self, strategy_id: str, mode: str = "paper") -> StrategyRun:
        config = self.repo.db.query(StrategyConfig).filter(StrategyConfig.strategy_id == strategy_id).first()
        if not config:
            raise StrategyLifecycleError(f"strategy {strategy_id} not found")
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
