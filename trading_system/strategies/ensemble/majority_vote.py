from __future__ import annotations

from strategies.base import BaseSignalStrategy, StrategyConfig, StrategyMetadata
from trading_system.strategies.base.interfaces import StrategySignal


def _ema(values: list[float], period: int) -> float:
    if not values:
        return 0.0
    k = 2.0 / (period + 1)
    ema = values[0]
    for v in values[1:]:
        ema = v * k + ema * (1 - k)
    return ema


def _sma(values: list[float]) -> float:
    if not values:
        return 0.0
    return sum(values) / len(values)


def _rsi(closes: list[float], period: int = 14) -> float:
    if len(closes) < period + 1:
        return 50.0
    gains = 0.0
    losses = 0.0
    for i in range(-period, 0):
        d = closes[i] - closes[i - 1]
        if d >= 0:
            gains += d
        else:
            losses -= d
    if losses == 0:
        return 100.0
    rs = (gains / period) / (losses / period)
    return 100.0 - (100.0 / (1.0 + rs))


def _bollinger_pos(closes: list[float], period: int = 20, num_std: float = 2.0) -> float:
    if len(closes) < period:
        return 0.5
    win = closes[-period:]
    mid = _sma(win)
    var = sum((c - mid) ** 2 for c in win) / period
    sd = var ** 0.5 + 1e-9
    last = closes[-1]
    return (last - (mid - num_std * sd)) / (2 * num_std * sd)


def _vol_trend(closes: list[float], volumes: list[float]) -> float:
    if len(volumes) < 2 or len(closes) < 2:
        return 0.0
    v_avg = _sma(volumes[-10:])
    if v_avg <= 0:
        return 0.0
    vol_ratio = volumes[-1] / v_avg
    ret = (closes[-1] - closes[-2]) / (closes[-2] + 1e-9)
    return max(-1.0, min(1.0, ret * 50.0 * min(2.0, vol_ratio)))


class MajorityVoteEnsembleStrategy(BaseSignalStrategy):
    """Weighted majority vote over 4 simple sub-signals; emits net agreement."""

    def __init__(self, warmup_period: int = 30) -> None:
        super().__init__(
            metadata=StrategyMetadata(
                strategy_id="MajorityVoteEnsemble",
                strategy_type="ensemble_vote",
                data_requirements=["product_id", "closes", "volumes", "warmup_complete"],
                risk_mode_hint="NORMAL",
                capital_bucket="ACTIVE_TRADING",
            ),
            config=StrategyConfig(threshold=0.2, cooldown_seconds=20, warmup_period=warmup_period),
        )
        self.warmup_period = warmup_period

    def _subsignals(self, closes: list[float], volumes: list[float]) -> list[tuple[float, float]]:
        sub: list[tuple[float, float]] = []
        if len(closes) >= 22:
            ema_fast = _ema(closes[-22:], 9)
            ema_slow = _ema(closes[-22:], 21)
            w = 1.0
            s = 1.0 if ema_fast > ema_slow else -1.0
            sub.append((s, w))
        rsi = _rsi(closes)
        if rsi < 30:
            sub.append((1.0, 1.0))
        elif rsi > 70:
            sub.append((-1.0, 1.0))
        else:
            sub.append((0.0, 0.0))
        bp = _bollinger_pos(closes)
        if bp > 0.8:
            sub.append((-1.0, 1.0))
        elif bp < 0.2:
            sub.append((1.0, 1.0))
        else:
            sub.append((0.0, 0.0))
        vt = _vol_trend(closes, volumes)
        sub.append((vt, min(1.0, abs(vt))))
        return sub

    def generate_signal(self, market_state: dict) -> StrategySignal | None:
        disabled, _ = self.is_disabled(market_state)
        if disabled:
            return None
        if self.in_cooldown():
            return None

        closes = market_state.get("closes") or []
        volumes = market_state.get("volumes") or []
        if len(closes) < self.warmup_period:
            return None

        sub = self._subsignals(closes, volumes)
        total_w = sum(w for _, w in sub) + 1e-9
        net = sum(s * w for s, w in sub) / total_w

        if abs(net) <= self.config.threshold:
            return None

        self._last_emit_ts = __import__("time").monotonic()
        return StrategySignal(
            strategy_id=self.strategy_id,
            product_id=str(market_state.get("product_id", "BTC-USD")),
            score=max(-1.0, min(1.0, net)),
            reason=f"majority-vote net agreement={net:.3f}",
            confidence=min(1.0, abs(net)),
            warmup_passed=bool(market_state.get("warmup_complete", True)),
            tags=[self.metadata_model.strategy_type, self.metadata_model.status],
            features={"n_sub": len(sub), "net_agreement": round(net, 4)},
        )
