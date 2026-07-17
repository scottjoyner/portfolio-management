from __future__ import annotations

from time import monotonic

from strategies.base import BaseSignalStrategy, StrategyConfig, StrategyMetadata
from trading_system.strategies.base.interfaces import StrategySignal


class OrderFlowTurbulenceStrategy(BaseSignalStrategy):
    """Signed-volume autocorrelation / order-flow turbulence continuation.

    Novel angle: rather than looking at price momentum or raw order-flow
    imbalance, this measures the *persistence* of signed volume through the
    lag-1 autocorrelation of the signed-volume series (tick-rule sign of each
    bar's return times its volume). High positive autocorrelation means order
    flow is "sticky" / trending (a directional participant is working an order
    or momentum ignition is under way); near-zero or negative autocorrelation
    means the flow is choppy / turbulent and reverting.

    When flow is persistent (autocorr high) we trade WITH the net signed-volume
    direction (continuation). When flow is turbulent (autocorr low/negative) we
    suppress the signal. The magnitude scales by both the autocorrelation and
    the normalised net flow -- a compact "trend-quality x direction" score.
    """

    def __init__(self, window: int = 30, ac_floor: float = 0.1) -> None:
        super().__init__(
            metadata=StrategyMetadata(
                strategy_id="OrderFlowTurbulenceAutocorr",
                strategy_type="adaptive_exec",
                live_supported=True,
                data_requirements=[
                    "product_id",
                    "closes",
                    "volumes",
                    "warmup_complete",
                ],
                risk_mode_hint="LAB_HFT",
                capital_bucket="ACTIVE_TRADING",
            ),
            config=StrategyConfig(threshold=0.12, cooldown_seconds=10, warmup_period=window),
        )
        self.window = int(window)
        self.ac_floor = float(ac_floor)

    def generate_signal(self, market_state: dict) -> StrategySignal | None:
        disabled, _ = self.is_disabled(market_state)
        if disabled or self.in_cooldown():
            return None

        closes = market_state.get("closes") or []
        volumes = market_state.get("volumes") or []
        if len(closes) < self.window + 1 or len(volumes) < self.window:
            return None

        c = closes[-(self.window + 1):]
        v = volumes[-self.window:]

        # Signed volume via tick rule: sign of bar return * bar volume.
        signed = []
        for i in range(self.window):
            ret = c[i + 1] - c[i]
            sgn = 1.0 if ret > 0 else (-1.0 if ret < 0 else 0.0)
            signed.append(sgn * max(0.0, float(v[i])))

        m = len(signed)
        mean = sum(signed) / m
        # Variance / lag-1 autocovariance of signed volume.
        var = sum((x - mean) ** 2 for x in signed) / m
        if var <= 0:
            return None
        cov1 = 0.0
        for i in range(1, m):
            cov1 += (signed[i] - mean) * (signed[i - 1] - mean)
        cov1 /= (m - 1)
        autocorr = max(-1.0, min(1.0, cov1 / var))

        # Turbulence gate: only act when flow is persistent enough.
        if autocorr < self.ac_floor:
            return None

        # Net normalised flow direction over window.
        total_vol = sum(max(0.0, float(x)) for x in v)
        if total_vol <= 0:
            return None
        net_flow = sum(signed) / total_vol  # in [-1, 1]

        # Continuation score = flow-quality (autocorr) x direction (net_flow).
        score = max(-1.0, min(1.0, autocorr * net_flow))
        if abs(score) <= self.config.threshold:
            return None

        self._last_emit_ts = monotonic()
        return StrategySignal(
            strategy_id=self.strategy_id,
            product_id=str(market_state.get("product_id", "BTC-USD")),
            score=score,
            reason=f"autocorr={autocorr:+.3f} net_flow={net_flow:+.3f} score={score:+.3f}",
            confidence=min(1.0, abs(score)),
            warmup_passed=bool(market_state.get("warmup_complete", True)),
            tags=[self.metadata_model.strategy_type, self.metadata_model.status],
            features={
                "autocorr": round(autocorr, 4),
                "net_flow": round(net_flow, 4),
                "signed_var": round(var, 4),
            },
        )
