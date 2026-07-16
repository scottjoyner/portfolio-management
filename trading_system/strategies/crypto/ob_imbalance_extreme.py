"""
Order-book-imbalance extreme reversion (crypto microstructure).

When `best_bid` / `best_ask` / `mid_price` / `spread_bps` are supplied by the
caller, compute the order-book imbalance and fade extreme reads (a lopsided book
that has run to an extreme tends to mean-revert as liquidity refreshes).

When those fields are absent, fall back to an (high - low) range-extremity proxy
derived from the most recent candle: price closing near the top of its range
after an expansion is faded short; near the bottom is faded long.

Pure Python, deterministic. Returns None gracefully when neither the book fields
nor the OHLCV fallback fields are present, so it never crashes in the harness.
"""
from __future__ import annotations

from time import monotonic

from strategies.base import BaseSignalStrategy, StrategyConfig, StrategyMetadata, StrategySignal


class OBImbalanceExtremeReversionStrategy(BaseSignalStrategy):
    def __init__(
        self,
        imbalance_threshold: float = 0.7,
        range_extreme: float = 0.85,
    ) -> None:
        self._imbalance_threshold = imbalance_threshold
        self._range_extreme = range_extreme
        super().__init__(
            metadata=StrategyMetadata(
                strategy_id="OBImbalanceExtremeStrategy",
                strategy_type="crypto_microstructure",
                live_supported=False,
                replay_supported=True,
                backtest_supported=True,
                data_requirements=["product_id", "closes", "highs", "lows", "warmup_complete"],
                risk_mode_hint="NORMAL",
                capital_bucket="ACTIVE_TRADING",
            ),
            config=StrategyConfig(threshold=0.1, cooldown_seconds=60, warmup_period=0),
        )

    def generate_signal(self, market_state: dict) -> StrategySignal | None:
        disabled, _ = self.is_disabled(market_state)
        if disabled:
            return None
        if self.in_cooldown():
            return None

        product_id = str(market_state.get("product_id", "BTC-USD"))

        bid = market_state.get("best_bid")
        ask = market_state.get("best_ask")
        mid = market_state.get("mid_price")
        has_book = None not in (bid, ask, mid) and ask > 0 and ask >= bid

        raw = 0.0
        score = 0.0
        reason = ""
        if has_book:
            spread = ask - bid
            imbalance = (mid - (bid + ask) / 2.0) / spread if spread > 0 else 0.0
            norm = min(1.0, abs(imbalance) / (self._imbalance_threshold if self._imbalance_threshold > 0 else 1.0))
            if norm >= self._imbalance_threshold:
                # Book skewed to bids (mid below mid-quote) -> faded long; skewed to asks -> short.
                raw = 1.0 if imbalance < 0 else -1.0
                score = norm
                reason = f"book imbalance extreme ({imbalance:.3f}) -> fade"
        else:
            # Fallback: candle range-extremity proxy.
            closes = market_state.get("closes")
            highs = market_state.get("highs")
            lows = market_state.get("lows")
            if not (closes and highs and lows) or len(closes) < 1:
                return None
            c = closes[-1]
            h = highs[-1]
            l = lows[-1]
            rng = h - l
            if rng <= 0:
                return None
            pos = (c - l) / rng  # 1.0 == closed at the top of the range
            if pos >= self._range_extreme:
                raw = -1.0
                score = (pos - self._range_extreme) / (1.0 - self._range_extreme)
                reason = f"range-top close (pos={pos:.2f}) -> fade short"
            elif pos <= (1.0 - self._range_extreme):
                raw = 1.0
                score = ((1.0 - self._range_extreme) - pos) / (1.0 - self._range_extreme)
                reason = f"range-bottom close (pos={pos:.2f}) -> fade long"

        if raw == 0.0:
            return None

        self._last_emit_ts = monotonic()
        return StrategySignal(
            strategy_id=self.strategy_id,
            product_id=product_id,
            score=raw * min(1.0, max(0.0, score)),
            reason=reason,
            confidence=min(1.0, max(0.0, score)),
            warmup_passed=bool(market_state.get("warmup_complete", True)),
            tags=[self.metadata_model.strategy_type, self.metadata_model.status],
            features={"imbalance_threshold": self._imbalance_threshold, "range_extreme": self._range_extreme},
        )
