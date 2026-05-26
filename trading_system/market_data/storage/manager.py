from __future__ import annotations

from datetime import datetime
from typing import Any

from market_data.candles.aggregator import CandleAggregator
from market_data.features.compute import FeatureComputer
from market_data.indicators.technical import TechnicalIndicatorSet
from market_data.orderbook.book import OrderBook
from market_data.trades.recorder import TradeRecorder


class MarketDataStore:
    def __init__(self, product_ids: list[str]) -> None:
        self.candle_agg = {pid: CandleAggregator(product_id=pid) for pid in product_ids}
        self.features = {pid: FeatureComputer(product_id=pid) for pid in product_ids}
        self.indicators = {pid: TechnicalIndicatorSet() for pid in product_ids}
        self.orderbooks = {pid: OrderBook(product_id=pid) for pid in product_ids}
        self.trades = TradeRecorder()

    def ingest_trade(self, product_id: str, price: float, size: float, side: str, timestamp: datetime | None = None) -> None:
        self.candle_agg[product_id].ingest_trade(price, size, timestamp)
        self.features[product_id].ingest_trade(price, size, side)
        self.indicators[product_id].ingest(price, size)
        self.trades.record(product_id, side, price, size)

    def ingest_orderbook_snapshot(self, product_id: str, bids: list[list[float]], asks: list[list[float]]) -> None:
        self.orderbooks[product_id].snapshot(bids, asks)

    def ingest_orderbook_update(self, product_id: str, side: str, price: float, size: float) -> None:
        self.orderbooks[product_id].update(side, price, size)

    def features_for(self, product_id: str) -> dict[str, Any]:
        ob = self.orderbooks[product_id].top_of_book()
        fs = self.features[product_id].compute(bid=ob["bid_px"], ask=ob["ask_px"])
        ind = self.indicators[product_id]
        result = fs.to_dict()
        result["sma_20"] = ind.sma(20)
        result["ema_20"] = ind.ema(20)
        result["rsi_14"] = ind.rsi()
        result["zscore_20"] = ind.zscore()
        result["bb_upper"], result["bb_mid"], result["bb_lower"] = ind.bollinger_bands().values()
        return result
