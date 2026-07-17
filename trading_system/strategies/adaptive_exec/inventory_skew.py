from __future__ import annotations

from time import monotonic

from strategies.base import BaseSignalStrategy, StrategyConfig, StrategyMetadata
from trading_system.strategies.base.interfaces import StrategySignal


class InventorySkewSignalStrategy(BaseSignalStrategy):
    """Market-making inventory-skew directional signal (Avellaneda-style tilt).

    Novel angle: this converts a market maker's *inventory risk* into a
    directional alpha signal. A MM holding inventory `q` away from its target
    must skew quotes to unwind risk; the fair-value shift it applies is
    proportional to inventory, risk aversion (gamma), instantaneous variance,
    and remaining horizon (Avellaneda-Stoikov reservation price shift
    r = mid - q * gamma * sigma^2 * (T - t)).

    We flip that into a taker signal: when the aggregate/desk inventory is LONG
    and risk-adjusted skew is large, the reservation price sits BELOW mid ->
    expect downward pressure as inventory is worked off -> SELL (negative
    score). When inventory is SHORT -> BUY. The score is the normalised
    reservation-price shift relative to mid, gated by realised variance.

    Inputs: `inventory` (signed units, or fraction of `inventory_limit`),
    `closes` (for realised variance / mid). Optional `gamma`, `horizon`.
    """

    def __init__(
        self,
        window: int = 30,
        gamma: float = 0.5,
        horizon: float = 1.0,
        inventory_limit: float = 1.0,
    ) -> None:
        super().__init__(
            metadata=StrategyMetadata(
                strategy_id="MarketMakerInventorySkew",
                strategy_type="adaptive_exec",
                live_supported=True,
                data_requirements=[
                    "product_id",
                    "closes",
                    "inventory",
                    "warmup_complete",
                ],
                risk_mode_hint="LAB_HFT",
                capital_bucket="ACTIVE_TRADING",
            ),
            config=StrategyConfig(threshold=0.1, cooldown_seconds=15, warmup_period=window),
        )
        self.window = int(window)
        self.gamma = float(gamma)
        self.horizon = float(horizon)
        self.inventory_limit = float(inventory_limit) if inventory_limit else 1.0

    def generate_signal(self, market_state: dict) -> StrategySignal | None:
        disabled, _ = self.is_disabled(market_state)
        if disabled or self.in_cooldown():
            return None

        closes = market_state.get("closes") or []
        if len(closes) < self.window:
            return None
        if "inventory" not in market_state:
            return None

        c = closes[-self.window:]
        mid = float(c[-1])
        if mid <= 0:
            return None

        # Realised variance of simple returns over the window.
        rets = []
        for i in range(1, len(c)):
            if c[i - 1] > 0:
                rets.append((c[i] - c[i - 1]) / c[i - 1])
        if len(rets) < 2:
            return None
        rmean = sum(rets) / len(rets)
        sigma2 = sum((r - rmean) ** 2 for r in rets) / (len(rets) - 1)
        if sigma2 <= 0:
            return None

        # Normalise inventory to fraction of limit -> q in roughly [-1, 1].
        q = float(market_state["inventory"]) / self.inventory_limit
        gamma = float(market_state.get("gamma", self.gamma))
        horizon = float(market_state.get("horizon", self.horizon))

        # Avellaneda-Stoikov reservation-price shift relative to mid.
        # r - mid = -q * gamma * sigma^2 * horizon  (in return-space units).
        skew = -q * gamma * sigma2 * horizon
        # Express as a bounded score. Long inventory (q>0) -> negative skew
        # -> SELL. Short inventory (q<0) -> positive skew -> BUY.
        # Scale by 1/sigma so the score is dimensionally normalised.
        sigma = sigma2 ** 0.5
        score = max(-1.0, min(1.0, skew / (sigma if sigma > 0 else 1.0)))
        if abs(score) <= self.config.threshold:
            return None

        self._last_emit_ts = monotonic()
        return StrategySignal(
            strategy_id=self.strategy_id,
            product_id=str(market_state.get("product_id", "BTC-USD")),
            score=score,
            reason=f"q={q:+.3f} sigma2={sigma2:.6f} skew={skew:+.6f} score={score:+.3f}",
            confidence=min(1.0, abs(score)),
            warmup_passed=bool(market_state.get("warmup_complete", True)),
            tags=[self.metadata_model.strategy_type, self.metadata_model.status],
            features={
                "inventory_norm": round(q, 4),
                "sigma2": round(sigma2, 8),
                "reservation_skew": round(skew, 8),
                "gamma": gamma,
            },
        )
