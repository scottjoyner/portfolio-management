"""Smart rebalance strategy driven by the shared Coinbase rebalancer.

This :class:`ExchangeBotStrategy` wraps :class:`RebalanceEngine` (which in turn
delegates to the Rust rebalancer) and emits one :class:`StrategySignal` per
drifted asset on each ``generate_signal`` call.  Multiple orders are queued and
drained across successive calls so the parent loop sees each asset as its own
signal without losing any rebalance leg.
"""
from __future__ import annotations

from typing import Optional

from pydantic import Field

from coinbase.src.rebalance_engine import RebalanceEngine

from .base import BotConfig, ExchangeBotStrategy
from ..base.interfaces import StrategySignal


class SmartRebalanceConfig(BotConfig):
    """Configurable parameters for the smart rebalance strategy."""

    preset: str = "core_balanced"
    targets: Optional[dict[str, float]] = None
    drift_threshold: float = 0.05
    profit_take_pct: float = 1.0
    min_trade_notional: float = 1.0


class SmartRebalanceStrategy(ExchangeBotStrategy):
    """Rebalance a portfolio toward a target allocation (preset or explicit)."""

    config_model = SmartRebalanceConfig
    bot_config: SmartRebalanceConfig

    def __init__(
        self,
        strategy_id: str,
        strategy_type: str,
        bot_config: Optional[SmartRebalanceConfig] = None,
        metadata=None,
        config=None,
    ) -> None:
        super().__init__(
            strategy_id=strategy_id,
            strategy_type=strategy_type,
            bot_config=bot_config or self.config_model(),
            metadata=metadata,
            config=config,
        )
        self._pending: list[tuple[str, str, float]] = []
        self._broken: bool = False
        cfg = self.bot_config
        try:
            if cfg.targets:
                self._engine = RebalanceEngine(
                    cfg.targets,
                    drift_threshold=cfg.drift_threshold,
                    profit_take_pct=cfg.profit_take_pct,
                    min_trade_notional=cfg.min_trade_notional,
                )
            else:
                self._engine = RebalanceEngine.from_preset(
                    cfg.preset,
                    drift_threshold=cfg.drift_threshold,
                    profit_take_pct=cfg.profit_take_pct,
                    min_trade_notional=cfg.min_trade_notional,
                )
        except KeyError:
            self._broken = True
            self._engine = None  # type: ignore[assignment]

    # ------------------------------------------------------------------ signal
    def generate_signal(self, market_state: dict) -> Optional[StrategySignal]:
        if not market_state.get("warmup_complete", True):
            return None
        if self.is_disabled(market_state)[0] or getattr(self, "_broken", False):
            return None

        holdings = (
            market_state.get("holdings")
            or market_state.get("portfolio")
            or {}
        )
        if not holdings:
            return None
        total = sum(float(v) for v in holdings.values())
        if total <= 0:
            return None

        if not self._pending:
            rec = self._engine.compute(holdings, total=None)
            for order in rec.orders:
                self._pending.append((order.asset, order.side, order.notional))
        if not self._pending:
            return None

        asset, side, notional = self._pending.pop(0)

        product_id = str(market_state.get("product_id", "BTC-USD"))
        price = market_state.get("price") if asset == product_id else None
        if price is not None and asset == product_id:
            price_f = float(price)
            size = notional / price_f if price_f else notional
        else:
            price_f = 0.0
            size = notional

        self._record(asset, side, price_f, size)

        score = 0.8 if side == "BUY" else -0.8
        reason = f"rebalance {side} {asset} notional={notional:.2f}"
        return StrategySignal(
            strategy_id=self.strategy_id,
            product_id=asset,
            score=score,
            reason=reason,
            confidence=0.8,
            warmup_passed=bool(market_state.get("warmup_complete", True)),
            tags=[self.metadata_model.strategy_type, "rebalance"],
        )
