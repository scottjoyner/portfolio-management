from __future__ import annotations
import json
import time
import logging
import threading
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Callable, Set
from enum import Enum

log = logging.getLogger(__name__)


class FeedSource(Enum):
    COINBASE_ADVANCED = "coinbase_advanced"
    COINBASE_PUBLIC = "coinbase_public"
    COINBASE_INTX = "coinbase_intx"
    SYNTHETIC = "synthetic"


@dataclass
class Ticker:
    product_id: str
    price: float
    bid: float
    ask: float
    volume_24h: float
    timestamp: float
    source: FeedSource = FeedSource.COINBASE_PUBLIC


@dataclass
class Candle:
    product_id: str
    timestamp: float
    open: float
    high: float
    low: float
    close: float
    volume: float


@dataclass
class OrderBookLevel:
    price: float
    size: float


@dataclass
class OrderBookUpdate:
    product_id: str
    bids: List[OrderBookLevel]
    asks: List[OrderBookLevel]
    timestamp: float


class TickerCache:
    def __init__(self, ttl: float = 1.0):
        self._lock = threading.Lock()
        self._tickers: Dict[str, Ticker] = {}
        self._candles: Dict[str, List[Candle]] = {}
        self._orderbooks: Dict[str, OrderBookUpdate] = {}
        self._ttl = ttl
        self._listeners: Dict[str, List[Callable]] = {}

    def on(self, event: str, fn: Callable):
        with self._lock:
            self._listeners.setdefault(event, []).append(fn)

    def _emit(self, event: str, data: Any):
        fns = []
        with self._lock:
            fns = list(self._listeners.get(event, []))
        for fn in fns:
            try:
                fn(data)
            except Exception:
                pass

    def update_ticker(self, ticker: Ticker):
        with self._lock:
            self._tickers[ticker.product_id] = ticker
        self._emit("ticker", ticker)

    def update_candle(self, candle: Candle):
        with self._lock:
            self._candles.setdefault(candle.product_id, []).append(candle)
            max_len = 500
            if len(self._candles[candle.product_id]) > max_len:
                self._candles[candle.product_id] = self._candles[candle.product_id][-max_len:]
        self._emit("candle", candle)

    def update_orderbook(self, update: OrderBookUpdate):
        with self._lock:
            self._orderbooks[update.product_id] = update
        self._emit("orderbook", update)

    def get_ticker(self, product_id: str) -> Optional[Ticker]:
        with self._lock:
            t = self._tickers.get(product_id)
            if t and time.time() - t.timestamp < self._ttl:
                return t
            return None

    def get_candles(self, product_id: str, n: int = 100) -> List[Candle]:
        with self._lock:
            candles = list(self._candles.get(product_id, []))
        return candles[-n:]

    def get_orderbook(self, product_id: str) -> Optional[OrderBookUpdate]:
        with self._lock:
            return self._orderbooks.get(product_id)

    def all_prices(self) -> Dict[str, float]:
        with self._lock:
            cutoff = time.time() - self._ttl * 10
            return {pid: t.price for pid, t in self._tickers.items() if t.timestamp > cutoff}


class PollingFeed:
    def __init__(self, cb_client: Any, cache: TickerCache,
                 poll_interval: float = 1.0):
        self.cb = cb_client
        self.cache = cache
        self.poll_interval = poll_interval
        self._products: Set[str] = set()
        self._running = False
        self._thread: Optional[threading.Thread] = None

    def subscribe(self, product_ids: List[str]):
        self._products.update(product_ids)

    def start(self):
        if self._running:
            return
        self._running = True
        self._thread = threading.Thread(target=self._poll_loop, daemon=True)
        self._thread.start()
        log.info(f"[FEED] Started polling feed for {len(self._products)} products")

    def stop(self):
        self._running = False

    def _poll_loop(self):
        while self._running:
            try:
                self._poll_once()
            except Exception as e:
                log.debug(f"[FEED] Poll error: {e}")
            time.sleep(self.poll_interval)

    def _poll_once(self):
        if not self._products or self.cb is None:
            return
        pids = list(self._products)
        try:
            best = self.cb.best_bid_ask(pids)
            pricebooks = best.get("pricebooks", [])
            for pb in pricebooks:
                pid = pb.get("product_id")
                if not pid:
                    continue
                bids = pb.get("bids", [])
                asks = pb.get("asks", [])
                bid = float(bids[0]["price"]) if bids else 0.0
                ask = float(asks[0]["price"]) if asks else 0.0
                price = (bid + ask) / 2 if bid and ask else max(bid, ask)
                if price > 0:
                    self.cache.update_ticker(Ticker(
                        product_id=pid,
                        price=price,
                        bid=bid,
                        ask=ask,
                        volume_24h=0.0,
                        timestamp=time.time(),
                    ))
        except Exception:
            pass


class WebSocketFeed:
    def __init__(self, cache: TickerCache,
                 ws_url: str = "wss://advanced-trade-ws.coinbase.com"):
        self.cache = cache
        self.ws_url = ws_url
        self._products: Set[str] = set()
        self._running = False
        self._thread: Optional[threading.Thread] = None

    def subscribe(self, product_ids: List[str]):
        self._products.update(product_ids)

    def start(self):
        import importlib.util
        if importlib.util.find_spec("websocket") is None:
            log.warning("[WS] websocket-client not installed, falling back to polling")
            return False
        self._running = True
        self._thread = threading.Thread(target=self._ws_loop, daemon=True)
        self._thread.start()
        log.info(f"[WS] Started WebSocket feed for {len(self._products)} products")
        return True

    def stop(self):
        self._running = False

    def _ws_loop(self):
        import websocket
        import json
        while self._running:
            try:
                ws = websocket.WebSocketApp(
                    self.ws_url,
                    on_message=self._on_message,
                    on_error=lambda ws, e: log.debug(f"[WS] Error: {e}"),
                    on_close=lambda ws, *a: None,
                )
                ws.on_open = lambda ws: self._subscribe(ws)
                ws.run_forever(ping_interval=30, ping_timeout=10)
            except Exception:
                pass
            time.sleep(5)

    def _subscribe(self, ws):
        if not self._products:
            return
        msg = {
            "type": "subscribe",
            "product_ids": list(self._products),
            "channel": "ticker",
        }
        ws.send(json.dumps(msg))

    def _on_message(self, ws, message):
        try:
            data = json.loads(message)
            if data.get("type") != "ticker":
                return
            pid = data.get("product_id")
            price = float(data.get("price", 0))
            bid = float(data.get("best_bid", 0))
            ask = float(data.get("best_ask", 0))
            vol = float(data.get("volume_24_h", 0))
            if price > 0:
                self.cache.update_ticker(Ticker(
                    product_id=pid, price=price, bid=bid, ask=ask,
                    volume_24h=vol, timestamp=time.time(),
                    source=FeedSource.COINBASE_ADVANCED,
                ))
        except Exception:
            pass
