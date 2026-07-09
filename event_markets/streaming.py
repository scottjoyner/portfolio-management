"""Real-time price streaming for prediction markets (Polymarket + Kalshi).

This module lays the groundwork for websocket-driven pricing to replace REST
polling in latency-sensitive paths (arbitrage detection, order-book depth).

Design:
  - ``PriceUpdate`` is the normalized event both venues emit.
  - ``PolymarketStream`` subscribes to the CLOB *market* channel by token id.
  - ``KalshiStream`` subscribes to the *ticker* channel with API-key auth
    (reuses the same RSA-PSS request signing as the REST client).
  - ``PredictionMarketStreamManager`` runs both, maintains a thread-safe cache of
    the latest price per (platform, market_id), and degrades gracefully: if a
    socket can't connect (or ``websocket-client`` is missing) it simply reports
    ``connected == False`` so callers can fall back to REST polling.

Connections run in background daemon threads with exponential-backoff reconnect.
"""

from __future__ import annotations

import base64
import json
import logging
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, Dict, List, Optional

logger = logging.getLogger("event_streaming")

try:  # optional dependency; module still imports without it
    import websocket as _ws  # websocket-client
except Exception:  # pragma: no cover
    _ws = None

try:
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.asymmetric import padding
except Exception:  # pragma: no cover
    hashes = serialization = padding = None

POLYMARKET_WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
KALSHI_WS_PATH = "/trade-api/ws/v2"
KALSHI_WS_URL_PROD = "wss://api.elections.kalshi.com" + KALSHI_WS_PATH
KALSHI_WS_URL_DEMO = "wss://demo-api.kalshi.co" + KALSHI_WS_PATH


@dataclass
class PriceUpdate:
    platform: str
    market_id: str          # token_id (polymarket) or ticker (kalshi)
    yes_price: float = 0.0
    best_bid: float = 0.0
    best_ask: float = 0.0
    ts: float = field(default_factory=time.time)


OnUpdate = Callable[[PriceUpdate], None]


class _BaseStream(threading.Thread):
    """Common reconnect/lifecycle machinery for a single websocket."""

    url: str = ""

    def __init__(self, on_update: OnUpdate, name: str = "stream"):
        super().__init__(daemon=True, name=name)
        self._on_update = on_update
        self._stop = threading.Event()
        self._app = None
        self.connected = False
        self.last_msg_ts = 0.0
        self._backoff = 1.0

    # ---- to be provided by subclasses ----
    def _subscribe_messages(self) -> List[dict]:
        return []

    def _headers(self) -> List[str]:
        return []

    def _handle(self, data: dict) -> None:  # pragma: no cover - overridden
        raise NotImplementedError

    # ---- lifecycle ----
    def available(self) -> bool:
        return _ws is not None

    def stop(self) -> None:
        self._stop.set()
        try:
            if self._app is not None:
                self._app.close()
        except Exception:
            pass

    def _emit(self, upd: PriceUpdate) -> None:
        self.last_msg_ts = time.time()
        try:
            self._on_update(upd)
        except Exception as e:  # never let a callback kill the socket
            logger.debug("on_update callback error: %s", e)

    def _on_open(self, app) -> None:
        self.connected = True
        self._backoff = 1.0
        for msg in self._subscribe_messages():
            try:
                app.send(json.dumps(msg))
            except Exception as e:
                logger.debug("subscribe send failed: %s", e)
        logger.info("%s connected", self.name)

    def _on_message(self, app, message) -> None:
        try:
            data = json.loads(message)
        except (ValueError, TypeError):
            return
        # Some venues batch updates as a list
        if isinstance(data, list):
            for item in data:
                if isinstance(item, dict):
                    self._handle(item)
        elif isinstance(data, dict):
            self._handle(data)

    def _on_error(self, app, error) -> None:
        logger.debug("%s ws error: %s", self.name, error)

    def _on_close(self, app, *_a) -> None:
        self.connected = False

    def run(self) -> None:
        if _ws is None:
            logger.warning("%s: websocket-client not installed — streaming disabled", self.name)
            return
        while not self._stop.is_set():
            try:
                self._app = _ws.WebSocketApp(
                    self.url,
                    header=self._headers(),
                    on_open=self._on_open,
                    on_message=self._on_message,
                    on_error=self._on_error,
                    on_close=self._on_close,
                )
                self._app.run_forever(ping_interval=20, ping_timeout=10)
            except Exception as e:  # pragma: no cover - network
                logger.debug("%s run_forever crashed: %s", self.name, e)
            self.connected = False
            if self._stop.is_set():
                break
            time.sleep(self._backoff)
            self._backoff = min(self._backoff * 2, 60.0)


class PolymarketStream(_BaseStream):
    """Subscribe to Polymarket CLOB market channel for a set of token ids."""

    url = POLYMARKET_WS_URL

    def __init__(self, asset_ids: List[str], on_update: OnUpdate):
        super().__init__(on_update, name="polymarket-ws")
        self.asset_ids = [a for a in asset_ids if a]

    def _subscribe_messages(self) -> List[dict]:
        if not self.asset_ids:
            return []
        return [{"type": "market", "assets_ids": self.asset_ids}]

    def _handle(self, data: dict) -> None:
        etype = data.get("event_type") or data.get("type")
        asset = data.get("asset_id") or data.get("market") or ""
        if etype == "book":
            bids = data.get("bids") or data.get("buys") or []
            asks = data.get("asks") or data.get("sells") or []
            best_bid = max((float(b.get("price", 0)) for b in bids), default=0.0)
            best_ask = min((float(a.get("price", 0)) for a in asks), default=0.0)
            mid = (best_bid + best_ask) / 2.0 if best_bid and best_ask else (best_bid or best_ask)
            self._emit(PriceUpdate("polymarket", asset, mid, best_bid, best_ask))
        elif etype in ("price_change", "last_trade_price"):
            price = float(data.get("price", 0) or 0)
            if price:
                self._emit(PriceUpdate("polymarket", asset, price, price, price))


class KalshiStream(_BaseStream):
    """Subscribe to Kalshi ticker channel with API-key (RSA-PSS) auth."""

    def __init__(
        self,
        tickers: List[str],
        on_update: OnUpdate,
        api_key_id: str = "",
        private_key_path: str = "",
        env: str = "prod",
    ):
        super().__init__(on_update, name="kalshi-ws")
        self.tickers = [t for t in tickers if t]
        self.api_key_id = api_key_id
        self.private_key_path = private_key_path
        self.url = KALSHI_WS_URL_DEMO if env == "demo" else KALSHI_WS_URL_PROD
        self._private_key = None

    def available(self) -> bool:
        return _ws is not None and bool(self.api_key_id and self.private_key_path)

    def _load_key(self):
        if self._private_key is not None:
            return self._private_key
        if serialization is None or not self.private_key_path:
            return None
        raw = Path(self.private_key_path).read_bytes()
        self._private_key = serialization.load_pem_private_key(raw, password=None)
        return self._private_key

    def _headers(self) -> List[str]:
        key = self._load_key()
        if key is None or not self.api_key_id:
            return []
        ts = str(int(time.time() * 1000))
        message = f"{ts}GET{KALSHI_WS_PATH}".encode("utf-8")
        sig = base64.b64encode(
            key.sign(
                message,
                padding.PSS(mgf=padding.MGF1(hashes.SHA256()), salt_length=padding.PSS.DIGEST_LENGTH),
                hashes.SHA256(),
            )
        ).decode("utf-8")
        return [
            f"KALSHI-ACCESS-KEY: {self.api_key_id}",
            f"KALSHI-ACCESS-SIGNATURE: {sig}",
            f"KALSHI-ACCESS-TIMESTAMP: {ts}",
        ]

    def _subscribe_messages(self) -> List[dict]:
        if not self.tickers:
            return []
        return [{
            "id": 1,
            "cmd": "subscribe",
            "params": {"channels": ["ticker"], "market_tickers": self.tickers},
        }]

    def _handle(self, data: dict) -> None:
        if data.get("type") != "ticker":
            return
        msg = data.get("msg") or {}
        ticker = msg.get("market_ticker") or msg.get("ticker") or ""
        # Kalshi prices are in cents (0-100); normalize to 0-1 probability.
        bid = float(msg.get("yes_bid", 0) or 0) / 100.0
        ask = float(msg.get("yes_ask", 0) or 0) / 100.0
        price = float(msg.get("price", 0) or 0) / 100.0 or ((bid + ask) / 2.0 if bid and ask else (bid or ask))
        if ticker:
            self._emit(PriceUpdate("kalshi", ticker, price, bid, ask))


class PredictionMarketStreamManager:
    """Runs venue streams and keeps a thread-safe cache of latest prices."""

    def __init__(
        self,
        polymarket_asset_ids: Optional[List[str]] = None,
        kalshi_tickers: Optional[List[str]] = None,
        kalshi_api_key_id: str = "",
        kalshi_private_key_path: str = "",
        kalshi_env: str = "prod",
    ):
        self._lock = threading.Lock()
        self._cache: Dict[str, PriceUpdate] = {}
        self._streams: List[_BaseStream] = []
        if polymarket_asset_ids:
            self._streams.append(PolymarketStream(polymarket_asset_ids, self._on_update))
        if kalshi_tickers:
            self._streams.append(KalshiStream(
                kalshi_tickers, self._on_update,
                api_key_id=kalshi_api_key_id,
                private_key_path=kalshi_private_key_path,
                env=kalshi_env,
            ))

    def _key(self, platform: str, market_id: str) -> str:
        return f"{platform}:{market_id}"

    def _on_update(self, upd: PriceUpdate) -> None:
        with self._lock:
            self._cache[self._key(upd.platform, upd.market_id)] = upd

    def start(self) -> None:
        for s in self._streams:
            if s.available():
                s.start()
            else:
                logger.info("%s unavailable — will rely on REST polling", s.name)

    def stop(self) -> None:
        for s in self._streams:
            s.stop()

    def latest(self, platform: str, market_id: str, max_age_s: float = 30.0) -> Optional[PriceUpdate]:
        with self._lock:
            upd = self._cache.get(self._key(platform, market_id))
        if upd is None:
            return None
        if max_age_s and (time.time() - upd.ts) > max_age_s:
            return None
        return upd

    def any_connected(self) -> bool:
        return any(s.connected for s in self._streams)

    def status(self) -> Dict[str, object]:
        return {
            "streams": [{"name": s.name, "connected": s.connected,
                         "available": s.available(), "last_msg_age_s":
                         round(time.time() - s.last_msg_ts, 1) if s.last_msg_ts else None}
                        for s in self._streams],
            "cached_markets": len(self._cache),
        }
