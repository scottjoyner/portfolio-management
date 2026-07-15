"""Spot grid trading bot (exchange-bot template).

Places staggered buy/sell orders across an evenly spaced price grid. As price
drops through an unfilled grid line the bot buys a slice of the allocated
notional; as price rises back through a previously filled line it sells that
slice. State is tracked per product so a single instance can serve many
products independently.
"""
from __future__ import annotations

from .base import BotConfig, ExchangeBotStrategy


class SpotGridConfig(BotConfig):
    lower: float = 0.0
    upper: float = 0.0
    grids: int = 2
    investment: float = 0.0
    side: str = "long"
    trigger: str = "both"

    def model_post_init(self, __context) -> None:
        if self.trigger not in ("both", "buy", "sell"):
            self.trigger = "both"


class SpotGridStrategy(ExchangeBotStrategy):
    config_model = SpotGridConfig

    def __init__(
        self,
        strategy_id: str = "spot_grid",
        strategy_type: str = "spot_grid",
        bot_config: SpotGridConfig | None = None,
        metadata=None,
        config=None,
    ) -> None:
        super().__init__(
            strategy_id=strategy_id,
            strategy_type=strategy_type,
            bot_config=bot_config,
            metadata=metadata,
            config=config,
        )
        self._filled: dict[str, set[int]] = {}
        cfg = self.bot_config
        if cfg.grids >= 2:
            step = (cfg.upper - cfg.lower) / (cfg.grids - 1)
            self._lines = [cfg.lower + i * step for i in range(cfg.grids)]
        else:
            self._lines = []

    def generate_signal(self, market_state: dict):
        if not market_state.get("warmup_complete", True):
            return None
        if self.is_disabled(market_state)[0]:
            return None

        cfg = self.bot_config
        if cfg.upper <= cfg.lower:
            return None
        if cfg.grids < 2:
            return None
        if cfg.investment <= 0:
            return None

        product_id = str(market_state.get("product_id", "BTC-USD"))
        filled = self._filled.setdefault(product_id, set())
        p = self._price(market_state)
        if p <= 0:
            return None

        lines = self._lines
        per_grid = cfg.investment / cfg.grids
        size = per_grid / p

        if cfg.trigger in ("both", "buy"):
            buy_cands = [
                i
                for i in range(len(lines))
                if lines[i] <= p and i not in filled and p <= cfg.upper
            ]
            if buy_cands:
                i = min(buy_cands)
                filled.add(i)
                self._record(product_id, "BUY", lines[i], size)
                return self._emit(
                    "BUY", f"grid buy @ {lines[i]:.4f}", market_state
                )

        if cfg.trigger in ("both", "sell"):
            sell_cands = [
                i for i in range(len(lines)) if lines[i] <= p and i in filled
            ]
            if sell_cands:
                i = max(sell_cands)
                filled.discard(i)
                self._record(product_id, "SELL", lines[i], size)
                return self._emit(
                    "SELL", f"grid sell @ {lines[i]:.4f}", market_state
                )

        return None
