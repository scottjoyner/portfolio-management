from __future__ import annotations

import importlib.metadata as importlib_metadata
import logging
import os
import sys
import threading
import uuid
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Any, Dict, Optional

log = logging.getLogger(__name__)

_SDK_IMPORT_LOCK = threading.Lock()


def _repo_root() -> str:
    return os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))


@contextmanager
def _sdk_import_context():
    with _SDK_IMPORT_LOCK:
        repo_root = _repo_root()
        dist_root = str(importlib_metadata.distribution("coinbase-advanced-py").locate_file(""))
        saved_path = list(sys.path)
        saved_modules = {
            k: sys.modules[k]
            for k in list(sys.modules)
            if k == "coinbase" or k.startswith("coinbase.")
        }
        try:
            sys.path = [p for p in sys.path if os.path.abspath(p or ".") != repo_root]
            if dist_root not in sys.path:
                sys.path.insert(0, dist_root)
            for key in list(sys.modules):
                if key == "coinbase" or key.startswith("coinbase."):
                    del sys.modules[key]
            yield
        finally:
            sys.path = saved_path
            for key in list(sys.modules):
                if key == "coinbase" or key.startswith("coinbase."):
                    del sys.modules[key]
            sys.modules.update(saved_modules)


def _as_dict(obj: Any) -> Dict[str, Any]:
    if obj is None:
        return {}
    for meth in ("model_dump", "to_dict", "dict"):
        fn = getattr(obj, meth, None)
        if callable(fn):
            try:
                out = fn()
                if isinstance(out, dict):
                    return out
            except Exception:
                pass
    if isinstance(obj, dict):
        return obj
    data = getattr(obj, "__dict__", None)
    return data if isinstance(data, dict) else {"value": obj}


def _order_id(resp: Any) -> str:
    data = _as_dict(resp)
    return str(data.get("order_id") or data.get("id") or data.get("client_order_id") or "")


@dataclass
class FuturesOrderResult:
    success: bool
    order_id: str = ""
    client_order_id: str = ""
    raw: Dict[str, Any] = field(default_factory=dict)
    error: str = ""


class CoinbaseFuturesExecutor:
    def __init__(
        self,
        api_key: str,
        api_secret: str,
        *,
        base_url: str = "api.coinbase.com",
        timeout: int = 30,
        portfolio_uuid: str = "",
        margin_type: str = "CROSS",
        default_leverage: float = 2.0,
    ):
        if not api_key or not api_secret:
            raise ValueError("Coinbase API key/secret are required for futures mode")
        self.api_key = api_key
        self.api_secret = api_secret
        self.base_url = base_url
        self.timeout = timeout
        self.portfolio_uuid = portfolio_uuid
        self.margin_type = margin_type.upper()
        self.default_leverage = max(1.0, float(default_leverage))
        self._perp_products: Dict[str, str] = {}
        with _sdk_import_context():
            from coinbase.rest import RESTClient
        self._RESTClient = RESTClient
        self.client = RESTClient(
            api_key=api_key,
            api_secret=api_secret,
            base_url=base_url,
            timeout=timeout,
        )
        self._load_perp_products()

    def _load_perp_products(self):
        try:
            products = self.client.get_products(product_type="future")
        except Exception as e:
            log.warning("Futures product discovery failed: %s", e)
            products = []
        items = _as_dict(products).get("products", products if isinstance(products, list) else [])
        if not isinstance(items, list):
            items = []
        mapping: Dict[str, str] = {}
        for p in items:
            if not isinstance(p, dict):
                continue
            if str(p.get("contract_expiry_type", "")).upper() != "PERPETUAL":
                continue
            pid = str(p.get("product_id") or p.get("id") or "").strip()
            if not pid:
                continue
            base = pid.split("-")[0].upper()
            mapping[base] = pid
        self._perp_products = mapping

    def validate(self) -> Dict[str, Any]:
        summary: Dict[str, Any] = {}
        if self.portfolio_uuid:
            try:
                summary = _as_dict(self.client.get_perps_portfolio_summary(self.portfolio_uuid))
            except Exception as e:
                raise RuntimeError(f"Futures portfolio validation failed: {e}") from e
        else:
            try:
                summary = _as_dict(self.client.get_futures_balance_summary())
            except Exception as e:
                raise RuntimeError(f"Futures balance validation failed: {e}") from e
        return summary

    def discover_product_id(self, symbol: str) -> Optional[str]:
        base = symbol.upper().replace("-INTX", "").replace("-PERP", "").replace("-USD", "")
        if base in self._perp_products:
            return self._perp_products[base]
        return None

    def summary(self) -> Dict[str, Any]:
        if self.portfolio_uuid:
            try:
                return _as_dict(self.client.get_perps_portfolio_summary(self.portfolio_uuid))
            except Exception:
                return {}
        try:
            return _as_dict(self.client.get_futures_balance_summary())
        except Exception:
            return {}

    def positions(self) -> Dict[str, Any]:
        if not self.portfolio_uuid:
            return {}
        try:
            return _as_dict(self.client.list_perps_positions(self.portfolio_uuid))
        except Exception:
            return {}

    def _order_kwargs(self, leverage: float) -> Dict[str, Any]:
        kw = {
            "leverage": str(max(1.0, float(leverage or self.default_leverage))),
            "margin_type": self.margin_type,
        }
        if self.portfolio_uuid:
            kw["retail_portfolio_id"] = self.portfolio_uuid
        return kw

    def _market_entry(self, product_id: str, side: str, base_size: str, leverage: float) -> Any:
        side_u = side.upper()
        kwargs = self._order_kwargs(leverage)
        client_order_id = str(uuid.uuid4())
        if side_u == "BUY":
            return self.client.market_order_buy(
                client_order_id,
                product_id,
                base_size=base_size,
                **kwargs,
            )
        return self.client.market_order_sell(
            client_order_id,
            product_id,
            base_size=base_size,
            **kwargs,
        )

    def _stop_exit(self, product_id: str, side: str, base_size: str, stop_price: str, leverage: float) -> Any:
        side_u = side.upper()
        kwargs = self._order_kwargs(leverage)
        client_order_id = str(uuid.uuid4())
        stop_direction = "stop_direction_stop_down" if side_u == "SELL" else "stop_direction_stop_up"
        stop_val = float(stop_price)
        limit_price = f"{(stop_val * 0.999):.2f}" if side_u == "SELL" else f"{(stop_val * 1.001):.2f}"
        if side_u == "SELL":
            return self.client.stop_limit_order_gtc_sell(
                client_order_id,
                product_id,
                base_size=base_size,
                limit_price=limit_price,
                stop_price=stop_price,
                stop_direction=stop_direction,
                **kwargs,
            )
        return self.client.stop_limit_order_gtc_buy(
            client_order_id,
            product_id,
            base_size=base_size,
            limit_price=limit_price,
            stop_price=stop_price,
            stop_direction=stop_direction,
            **kwargs,
        )

    def _target_exit(self, product_id: str, side: str, base_size: str, target_price: str, leverage: float) -> Any:
        side_u = side.upper()
        kwargs = self._order_kwargs(leverage)
        client_order_id = str(uuid.uuid4())
        if side_u == "SELL":
            return self.client.limit_order_gtc_sell(
                client_order_id,
                product_id,
                base_size=base_size,
                limit_price=target_price,
                **kwargs,
            )
        return self.client.limit_order_gtc_buy(
            client_order_id,
            product_id,
            base_size=base_size,
            limit_price=target_price,
            **kwargs,
        )

    def place_bracket(
        self,
        *,
        symbol: str,
        side: str,
        base_size: float,
        stop_price: float,
        target_price: float,
        leverage: float,
    ) -> FuturesOrderResult:
        product_id = self.discover_product_id(symbol)
        base_size_s = f"{float(base_size):.8f}".rstrip("0").rstrip(".") or "0"
        stop_s = f"{float(stop_price):.2f}"
        target_s = f"{float(target_price):.2f}"
        try:
            entry = self._market_entry(product_id, side, base_size_s, leverage)
            stop_side = "SELL" if side.upper() == "BUY" else "BUY"
            stop = self._stop_exit(product_id, stop_side, base_size_s, stop_s, leverage)
            target = self._target_exit(product_id, stop_side, base_size_s, target_s, leverage)
            return FuturesOrderResult(
                success=True,
                order_id=_order_id(entry),
                client_order_id=str(_as_dict(entry).get("client_order_id") or ""),
                raw={
                    "entry": _as_dict(entry),
                    "stop": _as_dict(stop),
                    "target": _as_dict(target),
                    "product_id": product_id,
                    "leverage": leverage,
                },
            )
        except Exception as e:
            return FuturesOrderResult(success=False, error=str(e), raw={"product_id": product_id})

    def close_position(self, symbol: str, size: Optional[float] = None) -> FuturesOrderResult:
        product_id = self.discover_product_id(symbol)
        try:
            if size is None:
                resp = self.client.close_position(str(uuid.uuid4()), product_id)
            else:
                resp = self.client.close_position(str(uuid.uuid4()), product_id, size=f"{float(size):.8f}")
            return FuturesOrderResult(success=True, order_id=_order_id(resp), raw=_as_dict(resp))
        except Exception as e:
            return FuturesOrderResult(success=False, error=str(e), raw={"product_id": product_id})
