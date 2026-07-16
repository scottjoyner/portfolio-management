from strategies.base import BaseSignalStrategy, StrategyConfig, StrategyMetadata


class RollMicropriceBiasStrategy(BaseSignalStrategy):
    """Roll implied-spread / microprice bias as informed-trader pressure.

    When ``best_bid`` / ``best_ask`` / ``mid_price`` are available, the
    microprice (volume-weighted midpoint) is compared against the naive mid.
    A persistent bias indicates informed-trader pressure in that direction.
    Falls back to the (high - low) / close range as a proxy when no book
    quotes are supplied.
    """

    def __init__(self, window: int = 20, bias_threshold: float = 0.0005) -> None:
        super().__init__(
            metadata=StrategyMetadata(
                strategy_id="RollMicropriceBiasStrategy",
                strategy_type="microstructure",
                data_requirements=[
                    "product_id",
                    "best_bid",
                    "best_ask",
                    "mid_price",
                    "highs",
                    "lows",
                    "close",
                    "warmup_complete",
                ],
                risk_mode_hint="NORMAL",
                capital_bucket="ACTIVE_TRADING",
            ),
            config=StrategyConfig(threshold=bias_threshold, cooldown_seconds=15.0, warmup_period=window),
        )
        self.window = window
        self.bias_threshold = bias_threshold

    def generate_signal(self, market_state: dict):
        from trading_system.strategies.base.interfaces import StrategySignal

        disabled, _ = self.is_disabled(market_state)
        if disabled:
            return None
        if self.in_cooldown():
            return None

        missing = self.required_inputs() - set(market_state)
        if missing:
            return None

        best_bid = market_state.get("best_bid")
        best_ask = market_state.get("best_ask")
        mid_price = market_state.get("mid_price")

        if best_bid is not None and best_ask is not None and mid_price is not None:
            bid = float(best_bid)
            ask = float(best_ask)
            mid = float(mid_price)
            spread = ask - bid
            if spread <= 0.0:
                return None
            microprice = (bid * ask + ask * bid) / (2.0 * (bid + ask)) if (bid + ask) > 0 else mid
            bias = (microprice - mid) / mid if mid > 0 else 0.0
            bias_series = [bias]
            source = "microprice"
        else:
            highs = list(market_state.get("highs", []))
            lows = list(market_state.get("lows", []))
            close = float(market_state.get("close", 0.0))
            if len(highs) < self.window or len(lows) < self.window or close <= 0.0:
                return None
            bias_series = []
            for h, l in zip(highs[-self.window:], lows[-self.window:]):
                rng = (float(h) - float(l)) / close
                bias_series.append(rng)
            source = "range"

        if not bias_series:
            return None

        avg_bias = sum(bias_series) / len(bias_series)
        if abs(avg_bias) <= self.bias_threshold:
            return None

        score = max(-1.0, min(1.0, avg_bias / (self.bias_threshold * 4.0)))
        confidence = min(1.0, abs(avg_bias) / (self.bias_threshold * 4.0))
        self._last_emit_ts = self._now()
        return StrategySignal(
            strategy_id=self.strategy_id,
            product_id=str(market_state.get("product_id", "BTC-USD")),
            score=score,
            reason=f"{source} bias={avg_bias:.5f} informed pressure signal",
            confidence=confidence,
            warmup_passed=bool(market_state.get("warmup_complete", True)),
            tags=[self.metadata_model.strategy_type, self.metadata_model.status],
            features={"avg_bias": avg_bias, "source": source},
        )

    def _now(self) -> float:
        from time import monotonic

        return monotonic()
