from __future__ import annotations
import json
import time
import logging
import threading
import hmac
import hashlib
import base64
import os
import jwt
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Callable, Set, Any
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
        # Prime the cache before the main loop starts so the first trader tick
        # does not run empty if the background thread has not polled yet.
        for _ in range(3):
            self._poll_once()
            if self.cache.all_prices():
                break
            time.sleep(min(self.poll_interval, 1.0))
        self._thread = threading.Thread(target=self._poll_loop, daemon=True)
        self._thread.start()
        log.info(f"[FEED] Started polling feed for {len(self._products)} products")

    def stop(self):
        self._running = False

    def poll_once(self):
        """Public method to trigger a single poll of the feed."""
        self._poll_once()

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
                 ws_url: str = "wss://ws-feed.exchange.coinbase.com",
                 use_advanced: bool = False):
        self.cache = cache
        self.ws_url = ws_url
        self._use_advanced = use_advanced
        self._products: Set[str] = set()
        self._running = False
        self._thread: Optional[threading.Thread] = None
        self.reconnect_count: int = 0
        self._reconnect_delay: float = 1.0

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
        log.info(f"[WS] Started WebSocket feed for {len(self._products)} products at {self.ws_url}")
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
                ws.on_open = lambda ws: (setattr(self, '_reconnect_delay', 1.0), self._subscribe(ws))
                ws.run_forever(ping_interval=30, ping_timeout=10)
            except Exception:
                pass
            if not self._running:
                break
            self.reconnect_count += 1
            log.info("[WS] Disconnected — reconnecting in %.0fs (attempt #%d)", self._reconnect_delay, self.reconnect_count)
            time.sleep(self._reconnect_delay)
            self._reconnect_delay = min(30.0, self._reconnect_delay * 2.0)

    def _subscribe(self, ws):
        if not self._products:
            return
        if self._use_advanced:
            # Advanced Trade API format (authenticated)
            msg = {
                "type": "subscribe",
                "product_ids": list(self._products),
                "channel": "ticker",
            }
        else:
            # Public Exchange API format (no auth)
            msg = {
                "type": "subscribe",
                "channels": [{"name": "ticker", "product_ids": list(self._products)}],
            }
        ws.send(json.dumps(msg))

    def _on_message(self, ws, message):
        try:
            data = json.loads(message)
            msg_type = data.get("type", "")
            if msg_type not in ("ticker", "snapshot", "l2update"):
                return
            pid = data.get("product_id")
            price = float(data.get("price", 0))
            bid = float(data.get("best_bid", data.get("bid", 0)))
            ask = float(data.get("best_ask", data.get("ask", 0)))
            vol = float(data.get("volume_24_h", data.get("volume", 0)))
            if price > 0:
                self.cache.update_ticker(Ticker(
                    product_id=pid, price=price, bid=bid, ask=ask,
                    volume_24h=vol, timestamp=time.time(),
                    source=FeedSource.COINBASE_PUBLIC,
                ))
        except Exception:
            pass


class AdvancedTradeWebSocket:
    """Authenticated WebSocket for Coinbase Advanced Trade user data.
    
    Handles: order fills, account updates, order status changes.
    Uses JWT authentication per Coinbase Advanced Trade spec.
    """
    
    def __init__(self, api_key: str, api_secret: str, cache: TickerCache,
                 ws_url: str = "wss://advanced-trade-ws.coin.coinbase.com",
                 on_fill: Optional[Callable] = None,
                 on_order: Optional[Callable] = None,
                 on_account: Optional[Callable] = None):
        self.api_key = api_key
        self.api_secret = api_secret
        self.cache = cache
        self.ws_url = ws_url
        self.on_fill = on_fill
        self.on_order = on_order
        self.on_account = on_account
        self._running = False
        self._thread: Optional[threading.Thread] = None
        self.reconnect_count: int = 0
        self._reconnect_delay: float = 1.0
        self._ws = None
        
    def _generate_jwt(self) -> str:
        """Generate JWT token for Coinbase Advanced Trade WS auth."""
        now = int(time.time())
        payload = {
            "sub": self.api_key,
            "iss": "coinbase-cloud",
            "nbf": now,
            "exp": now + 120,  # 2 min expiry
            "uri": self.ws_url,
        }
        # api_secret is base64-encoded Ed25519 private key
        signing_key = base64.b64decode(self.api_secret)
        token = jwt.encode(payload, signing_key, algorithm="EdDSA")
        return token
    
    def start(self):
        import importlib.util
        if importlib.util.find_spec("websocket") is None:
            log.warning("[ADV-WS] websocket-client not installed")
            return False
        if importlib.util.find_spec("jwt") is None:
            log.warning("[ADV-WS] pyjwt not installed")
            return False
        self._running = True
        self._thread = threading.Thread(target=self._ws_loop, daemon=True)
        self._thread.start()
        log.info(f"[ADV-WS] Started authenticated WebSocket for user data")
        return True
    
    def stop(self):
        self._running = False
        if self._ws:
            self._ws.close()
    
    def _ws_loop(self):
        import websocket
        while self._running:
            try:
                jwt_token = self._generate_jwt()
                headers = {"Authorization": f"Bearer {jwt_token}"}
                self._ws = websocket.WebSocketApp(
                    self.ws_url,
                    header=headers,
                    on_message=self._on_message,
                    on_error=lambda ws, e: log.debug(f"[ADV-WS] Error: {e}"),
                    on_close=lambda ws, *a: log.info("[ADV-WS] Connection closed"),
                )
                self._ws.on_open = lambda ws: self._subscribe(ws)
                self._ws.run_forever(ping_interval=30, ping_timeout=10)
            except Exception as e:
                log.debug(f"[ADV-WS] Loop error: {e}")
            if not self._running:
                break
            self.reconnect_count += 1
            log.info("[ADV-WS] Disconnected — reconnecting in %.0fs (attempt #%d)", 
                     self._reconnect_delay, self.reconnect_count)
            time.sleep(self._reconnect_delay)
            self._reconnect_delay = min(30.0, self._reconnect_delay * 2.0)
    
    def _subscribe(self, ws):
        # Subscribe to user channels: orders, fills, accounts
        msg = {
            "type": "subscribe",
            "channels": [
                {"name": "orders"},
                {"name": "fills"},
                {"name": "accounts"},
            ]
        }
        ws.send(json.dumps(msg))
        log.info("[ADV-WS] Subscribed to user channels: orders, fills, accounts")
    
    def _on_message(self, ws, message):
        try:
            data = json.loads(message)
            channel = data.get("channel", "")
            events = data.get("events", [])
            
            for event in events:
                event_type = event.get("type", "")
                
                if channel == "fills" and self.on_fill:
                    self.on_fill(event)
                elif channel == "orders" and self.on_order:
                    self.on_order(event)
                elif channel == "accounts" and self.on_account:
                    self.on_account(event)
                    
        except Exception as e:
            log.debug(f"[ADV-WS] Message parse error: {e}")
