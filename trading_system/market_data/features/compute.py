from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass
class FeatureSet:
    product_id: str
    mid_price: float = 0.0
    spread_bps: float = 0.0
    microprice: float = 0.0
    imbalance: float = 0.0
    volume_1m: float = 0.0
    volatility_1m_bps: float = 0.0
    trade_count_1m: int = 0
    buy_ratio_1m: float = 0.5
    toxic_flow: float = 0.0

    def to_dict(self) -> dict[str, Any]:
        return {k: float(v) if not isinstance(v, str) else v for k, v in self.__dict__.items()}


class FeatureComputer:
    def __init__(self, product_id: str) -> None:
        self.product_id = product_id
        self._prices: list[float] = []
        self._volumes: list[float] = []
        self._buys: list[float] = []
        self._sells: list[float] = []

    def ingest_trade(self, price: float, size: float, side: str) -> None:
        self._prices.append(price)
        self._volumes.append(size)
        if side.upper() == "BUY":
            self._buys.append(size)
        else:
            self._sells.append(size)
        max_samples = 500
        if len(self._prices) > max_samples:
            self._prices = self._prices[-max_samples:]
            self._volumes = self._volumes[-max_samples:]
            self._buys = self._buys[-max_samples:]
            self._sells = self._sells[-max_samples:]

    def compute(self, bid: float = 0.0, ask: float = 0.0) -> FeatureSet:
        mid = (bid + ask) / 2.0 if bid and ask else (self._prices[-1] if self._prices else 0.0)
        spread = ask - bid if bid and ask else 0.0
        spread_bps = (spread / mid * 10000) if mid > 0 else 0.0
        volume_1m = sum(self._volumes[-100:])
        total_buy = sum(self._buys[-100:])
        total_sell = sum(self._sells[-100:])
        total_vol = total_buy + total_sell
        buy_ratio = total_buy / total_vol if total_vol > 0 else 0.5

        recent = self._prices[-20:] if len(self._prices) >= 20 else self._prices
        if len(recent) >= 2:
            returns = [(recent[i] - recent[i - 1]) / recent[i - 1] for i in range(1, len(recent))]
            vol_bps = (sum(r * r for r in returns) / len(returns)) ** 0.5 * 10000 if returns else 0.0
        else:
            vol_bps = 0.0

        return FeatureSet(
            product_id=self.product_id,
            mid_price=mid,
            spread_bps=spread_bps,
            microprice=(ask * total_sell + bid * total_buy) / total_vol if total_vol > 0 and bid and ask else mid,
            imbalance=(total_buy - total_sell) / total_vol if total_vol > 0 else 0.0,
            volume_1m=volume_1m,
            volatility_1m_bps=vol_bps,
            trade_count_1m=len(self._prices[-100:]),
            buy_ratio_1m=buy_ratio,
        )
