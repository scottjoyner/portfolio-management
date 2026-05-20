from __future__ import annotations

from dataclasses import dataclass


@dataclass(slots=True)
class TopOfBook:
    bid_px: float
    bid_sz: float
    ask_px: float
    ask_sz: float


@dataclass(slots=True)
class TradePrint:
    side: str
    size: float
    price: float


class MicrostructureFeatureBuilder:
    """Fast-path microstructure feature extraction for maker decisioning."""

    @staticmethod
    def microprice(book: TopOfBook) -> float:
        denom = book.bid_sz + book.ask_sz
        if denom <= 0:
            return (book.bid_px + book.ask_px) / 2.0
        return (book.ask_px * book.bid_sz + book.bid_px * book.ask_sz) / denom

    @staticmethod
    def imbalance(book: TopOfBook) -> float:
        denom = book.bid_sz + book.ask_sz
        return (book.bid_sz - book.ask_sz) / denom if denom > 0 else 0.0


class ToxicFlowEstimator:
    """VPIN-like rolling toxic flow proxy from aggressive trade prints."""

    def __init__(self, bucket_volume: float = 5.0) -> None:
        self.bucket_volume = max(bucket_volume, 1e-6)
        self.buy_vol = 0.0
        self.sell_vol = 0.0

    def update(self, trade: TradePrint) -> float:
        if trade.side.upper() == "BUY":
            self.buy_vol += trade.size
        else:
            self.sell_vol += trade.size
        if self.buy_vol + self.sell_vol >= self.bucket_volume:
            total = self.buy_vol + self.sell_vol
            toxic = abs(self.buy_vol - self.sell_vol) / total if total > 0 else 0.0
            self.buy_vol = 0.0
            self.sell_vol = 0.0
            return toxic
        return 0.0
