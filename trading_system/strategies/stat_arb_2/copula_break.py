"""
Copula / Dependence-Break Reversion Strategy (stat_arb_2).

Pure-Python rolling Pearson correlation between this asset and a peer
series (`peer_closes`). When dependence (|corr|) abruptly drops while the
normalized spread widens, the relationship is "breaking" and expected to
snap back: we fade the spread. Returns None when `peer_closes` absent.
"""
from __future__ import annotations

from math import sqrt

from strategies.base import BaseSignalStrategy, StrategyConfig, StrategyMetadata

from trading_system.strategies.base.interfaces import StrategySignal


def _pearson(a: list[float], b: list[float]) -> float | None:
    n = min(len(a), len(b))
    if n < 3:
        return None
    a = a[-n:]
    b = b[-n:]
    ma = sum(a) / n
    mb = sum(b) / n
    num = sum((a[i] - ma) * (b[i] - mb) for i in range(n))
    da = sum((x - ma) ** 2 for x in a)
    db = sum((x - mb) ** 2 for x in b)
    if da <= 0 or db <= 0:
        return None
    return num / sqrt(da * db)


class CopulaDependenceBreakReversion(BaseSignalStrategy):
    """Trade reversion when rolling correlation drops while spread widens."""

    def __init__(self) -> None:
        super().__init__(
            metadata=StrategyMetadata(
                strategy_id="CopulaDependenceBreakReversion",
                strategy_type="stat_arb",
                status="implemented",
                live_supported=False,
                replay_supported=True,
                backtest_supported=True,
                products=["BTC-USD"],
                data_requirements=["product_id", "score", "closes", "peer_closes"],
                risk_mode_hint="NORMAL",
                capital_bucket="ACTIVE_TRADING",
            ),
            config=StrategyConfig(threshold=0.1, cooldown_seconds=30, warmup_period=40),
        )
        self._corr_window = 30
        self._prev_corr: float | None = None

    def generate_signal(self, market_state: dict) -> StrategySignal | None:
        disabled, _ = self.is_disabled(market_state)
        if disabled:
            return None
        if self.in_cooldown():
            return None

        closes = market_state.get("closes") or []
        peer = market_state.get("peer_closes")
        if peer is None:
            return None
        if len(closes) < self._corr_window or len(peer) < self._corr_window:
            return None

        win = self._corr_window
        cur = _pearson(closes[-win:], peer[-win:])
        if cur is None:
            return None

        if self._prev_corr is not None:
            drop = self._prev_corr - cur
            spread_now = abs(closes[-1] / peer[-1] - closes[-win] / peer[-win])
            spread_mean = abs(sum(closes[-win:][i] / peer[-win:][i] for i in range(win)) / win
                              - sum(closes[-win:][i] / peer[-win:][i] for i in range(win)) / win)
            if drop > 0.15 and spread_now > 1.2 * (spread_mean + 1e-9):
                score = -drop * min(1.0, spread_now)
                if abs(score) > self.config.threshold:
                    self._last_emit_ts = self._now()
                    reason = (f"dependence_break corr {self._prev_corr:.3f}->{cur:.3f} "
                              f"drop={drop:.3f} spread_wide={spread_now:.4f}")
                    return StrategySignal(
                        strategy_id=self.strategy_id,
                        product_id=str(market_state.get("product_id", self.metadata_model.products[0])),
                        score=max(-1.0, min(1.0, score)),
                        reason=reason,
                        confidence=min(1.0, abs(score)),
                        warmup_passed=bool(market_state.get("warmup_complete", True)),
                        tags=[self.metadata_model.strategy_type, self.metadata_model.status],
                        features={"corr": cur, "prev_corr": self._prev_corr, "spread": spread_now},
                    )
        self._prev_corr = cur
        return None

    @staticmethod
    def _now() -> float:
        from time import monotonic

        return monotonic()
