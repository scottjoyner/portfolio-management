from __future__ import annotations

from dataclasses import dataclass
from time import monotonic

from strategies.base.interfaces import Strategy, StrategyConfig, StrategyMetadata, StrategySignal


@dataclass(frozen=True)
class SimpleSignalModel:
    score_key: str = "score"
    positive_is_long: bool = True


class BaseSignalStrategy(Strategy):
    """Common implementation for threshold-driven strategies used in paper/replay scaffolding."""

    metadata_model: StrategyMetadata
    config: StrategyConfig
    signal_model: SimpleSignalModel

    def __init__(self, metadata: StrategyMetadata, config: StrategyConfig, signal_model: SimpleSignalModel | None = None) -> None:
        self.strategy_id = metadata.strategy_id
        self.metadata_model = metadata
        self.config = config
        self.signal_model = signal_model or SimpleSignalModel()
        self._last_emit_ts = 0.0

    def metadata(self) -> dict:
        payload = self.metadata_model.model_dump()
        payload["config"] = self.config.model_dump()
        return payload

    def required_inputs(self) -> set[str]:
        return set(self.metadata_model.data_requirements)

    def supports_mode(self, mode: str) -> bool:
        mode_lower = mode.lower()
        supported = {
            "live": self.metadata_model.live_supported,
            "paper": self.metadata_model.paper_mode,
            "replay": self.metadata_model.replay_supported,
            "backtest": self.metadata_model.backtest_supported,
        }
        return supported.get(mode_lower, False)

    def is_disabled(self, market_state: dict) -> tuple[bool, str]:
        if not self.config.enabled:
            return True, "strategy disabled by config"
        if abs(float(market_state.get(self.signal_model.score_key, 0.0))) > self.config.max_abs_score:
            return True, "input score breached safety ceiling"
        return False, "enabled"

    def generate_signal(self, market_state: dict) -> StrategySignal | None:
        disabled, _ = self.is_disabled(market_state)
        if disabled:
            return None
        if self.in_cooldown():
            return None

        missing = self.required_inputs() - set(market_state)
        if missing:
            return None

        score = float(market_state.get(self.signal_model.score_key, 0.0))
        if score <= self.config.threshold:
            return None

        self._last_emit_ts = monotonic()
        confidence = min(1.0, max(0.0, abs(score)))
        return StrategySignal(
            strategy_id=self.strategy_id,
            product_id=str(market_state.get("product_id", "BTC-USD")),
            score=score,
            reason=f"score={score:.3f} above threshold={self.config.threshold:.3f}",
            confidence=confidence,
            warmup_passed=bool(market_state.get("warmup_complete", True)),
            tags=[self.metadata_model.strategy_type, self.metadata_model.status],
            features={
                "score_key": self.signal_model.score_key,
                "cooldown_seconds": self.config.cooldown_seconds,
            },
        )

    def in_cooldown(self) -> bool:
        if self.config.cooldown_seconds <= 0:
            return False
        if self._last_emit_ts <= 0:
            return False
        return monotonic() - self._last_emit_ts < self.config.cooldown_seconds

    def explain_trade(self, signal: StrategySignal) -> str:
        return (
            f"{self.strategy_id} {signal.product_id} score={signal.score:.3f} "
            f"confidence={signal.confidence:.2f} reason={signal.reason}"
        )
