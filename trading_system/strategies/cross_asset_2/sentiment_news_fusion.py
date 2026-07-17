"""
Sentiment-news fusion strategy (cross-asset-2).

Combines a `news_score` field in [-1, 1] (aggregated news/attention sentiment)
with short-horizon price momentum.  We only trade when news conviction *aligns*
with the price trend (bullish news + positive momentum => long; bearish news +
negative momentum => short).  When news and momentum disagree, or news is
negligible, we stay flat (return None).

Pure Python, deterministic, math only. Returns None gracefully when `news_score`
or `closes` are absent.
"""
from __future__ import annotations

from time import monotonic

from strategies.base import BaseSignalStrategy, StrategyConfig, StrategyMetadata, StrategySignal


class SentimentNewsFusionStrategy(BaseSignalStrategy):
    def __init__(
        self,
        lookback: int = 20,
        min_news_conviction: float = 0.25,
        min_momentum: float = 0.01,
        news_weight: float = 0.6,
    ) -> None:
        self._lookback = lookback
        self._min_news_conviction = min_news_conviction
        self._min_momentum = min_momentum
        self._news_weight = news_weight
        super().__init__(
            metadata=StrategyMetadata(
                strategy_id="SentimentNewsFusionStrategy",
                strategy_type="cross_asset_2",
                live_supported=False,
                replay_supported=True,
                backtest_supported=True,
                data_requirements=["product_id", "closes", "news_score", "warmup_complete"],
                risk_mode_hint="NORMAL",
                capital_bucket="ACTIVE_TRADING",
            ),
            config=StrategyConfig(threshold=0.1, cooldown_seconds=120, warmup_period=lookback),
        )

    def generate_signal(self, market_state: dict) -> StrategySignal | None:
        disabled, _ = self.is_disabled(market_state)
        if disabled:
            return None
        if self.in_cooldown():
            return None

        news = market_state.get("news_score")
        if news is None:
            return None
        news = float(news)
        if abs(news) < self._min_news_conviction:
            return None

        closes = market_state.get("closes")
        if not closes or len(closes) < self._lookback + 1:
            return None

        window = closes[-(self._lookback + 1):]
        mom = (window[-1] - window[0]) / window[0] if window[0] > 0 else 0.0
        if abs(mom) < self._min_momentum:
            return None

        # Alignment gate: news and momentum must share sign.
        aligned = (news > 0 and mom > 0) or (news < 0 and mom < 0)
        if not aligned:
            return None

        # Blended signed score: news conviction dominates, momentum confirms.
        momentum_weight = 1.0 - self._news_weight
        raw = self._news_weight * news + momentum_weight * max(-1.0, min(1.0, mom / max(self._min_momentum * 10, 1e-9)))
        raw = max(-1.0, min(1.0, raw * (abs(news) + abs(mom))))
        score = raw / max(0.5, (abs(news) + abs(mom)))

        if abs(score) < self.config.threshold:
            return None

        product_id = str(market_state.get("product_id", "BTC-USD"))
        self._last_emit_ts = monotonic()
        side = "LONG" if score > 0 else "SHORT"
        return StrategySignal(
            strategy_id=self.strategy_id,
            product_id=product_id,
            score=score,
            reason=f"news={news:.3f} momentum={mom:.4f} aligned {side}",
            confidence=min(1.0, abs(score)),
            warmup_passed=bool(market_state.get("warmup_complete", True)),
            tags=[self.metadata_model.strategy_type, self.metadata_model.status],
            features={"news_score": news, "momentum": mom},
        )
