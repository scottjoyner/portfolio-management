"""Brokers-coverage conftest.

The four target modules (``trading_system/brokers/*.py``) import two
collaborators that do not exist in this repository checkout:

* ``coinbase.py`` imports ``exchange.coinbase.rest.client.CoinbaseRestClient``
  (the ``exchange`` package is absent) — a no-op placeholder is provided as a
  real stub package (``exchange/coinbase/rest/client.py``) because tests inject
  a fake client and never instantiate the real one.
* ``paper.py`` imports ``apps.paper_exchange.engine.PaperExchangeEngine`` (the
  ``apps/paper_exchange`` directory does not exist in this repo) — a faithful
  in-memory fake (``apps/paper_exchange/engine.py``) provides the exact
  interface the adapter uses so it can be exercised end-to-end.

This conftest re-installs the same stubs defensively (no-op if already present)
so the broker modules import regardless of how the test session is bootstrapped.
"""


from __future__ import annotations

import importlib
import sys
import types
from datetime import datetime
from decimal import Decimal


# ---------------------------------------------------------------------------
# Stub: exchange.coinbase.rest.client.CoinbaseRestClient
# ---------------------------------------------------------------------------
def _install_exchange_stub() -> None:
    try:
        importlib.import_module("exchange.coinbase.rest.client")
        return
    except Exception:
        pass
    _stub_pkg("exchange")
    _stub_pkg("exchange.coinbase")
    _stub_pkg("exchange.coinbase.rest")

    client_mod = types.ModuleType("exchange.coinbase.rest.client")
    client_mod.__path__ = ["exchange/coinbase/rest/client"]

    class CoinbaseRestClient:
        def __init__(self, *args, **kwargs):
            pass

    client_mod.CoinbaseRestClient = CoinbaseRestClient
    sys.modules["exchange.coinbase.rest.client"] = client_mod


def _stub_pkg(name: str) -> None:
    if name in sys.modules and getattr(sys.modules[name], "__path__", None):
        return
    mod = types.ModuleType(name)
    mod.__path__ = [name.replace(".", "/")]
    sys.modules[name] = mod


# ---------------------------------------------------------------------------
# Stub: apps.paper_exchange.engine.{PaperExchangeEngine, PaperOrder, PaperFill,
#        PaperPosition}
# ---------------------------------------------------------------------------
class PaperOrder:
    def __init__(self, order_id, strategy_id, portfolio_id, product_id, side,
                 order_type, size, price, status, created_at=None,
                 filled_size=None, remaining_size=None, fee=None):
        self.order_id = order_id
        self.strategy_id = strategy_id
        self.portfolio_id = portfolio_id
        self.product_id = product_id
        self.side = side
        self.order_type = order_type
        self.size = size
        self.price = price
        self.status = status
        self.created_at = created_at if created_at is not None else datetime.utcnow()
        self.filled_size = filled_size if filled_size is not None else size
        self.remaining_size = remaining_size if remaining_size is not None else Decimal("0")
        self.fee = fee if fee is not None else Decimal("0")


class PaperFill:
    def __init__(self, fill_id, order_id, product_id, side, size, price, fee=None):
        self.fill_id = fill_id
        self.order_id = order_id
        self.product_id = product_id
        self.side = side
        self.size = size
        self.price = price
        self.fee = fee if fee is not None else Decimal("0")


class PaperPosition:
    def __init__(self, product_id, side, size, cost_basis):
        self.product_id = product_id
        self.side = side
        self.size = size
        self.cost_basis = cost_basis
        self.unrealized_pnl = Decimal("0")
        self.realized_pnl = Decimal("0")


class PaperExchangeEngine:
    def __init__(self, starting_cash=Decimal("0"), products=None):
        self.cash = Decimal(str(starting_cash))
        self.products = list(products or [])
        self.orders = {}
        self.fills = []
        self.positions = {}
        self.mid_prices = {}
        self._oid = 0

    def set_market_price(self, product_id, price):
        self.mid_prices[product_id] = Decimal(str(price))
        if product_id not in self.products:
            self.products.append(product_id)

    def place_order(self, strategy_id, portfolio_id, product_id, side,
                    order_type, size, limit_price=None):
        self._oid += 1
        oid = f"po{self._oid}"
        created_at = datetime.utcnow()
        size = Decimal(str(size))
        ref_price = self.mid_prices.get(product_id, Decimal(str(limit_price or "0")))
        status = "open"
        if order_type == "market" and product_id in self.mid_prices:
            status = "filled"
            self._execute_fill(oid, strategy_id, portfolio_id, product_id,
                               side, size, ref_price)
        order = PaperOrder(
            order_id=oid, strategy_id=strategy_id, portfolio_id=portfolio_id,
            product_id=product_id, side=side, order_type=order_type,
            size=size, price=ref_price if order_type != "market" else
            (Decimal(str(limit_price)) if limit_price is not None else ref_price),
            status=status, created_at=created_at,
        )
        self.orders[oid] = order
        return order

    def _execute_fill(self, oid, strategy_id, portfolio_id, product_id, side,
                      size, price):
        self.cash -= size * price
        self.fills.append(PaperFill(
            fill_id=f"f{len(self.fills) + 1}", order_id=oid, product_id=product_id,
            side=side, size=size, price=price, fee=Decimal("0"),
        ))
        pos = self.positions.get(product_id)
        if pos is None:
            pos = PaperPosition(product_id, "long", Decimal("0"), Decimal("0"))
            self.positions[product_id] = pos
        pos.side = "long" if side == "buy" else "short"
        pos.size = pos.size + (size if side == "buy" else -size)
        pos.cost_basis = price
        mid = self.mid_prices.get(product_id, price)
        pos.unrealized_pnl = (mid - price) * pos.size

    def cancel_order(self, order_id):
        order = self.orders.get(order_id)
        if not order:
            return False
        order.status = "cancelled"
        return True


def _install_paper_engine_stub() -> None:
    try:
        importlib.import_module("apps.paper_exchange.engine")
        return
    except Exception:
        pass
    _stub_pkg("apps")
    _stub_pkg("apps.paper_exchange")
    eng_mod = types.ModuleType("apps.paper_exchange.engine")
    eng_mod.__path__ = ["apps/paper_exchange/engine"]
    eng_mod.PaperExchangeEngine = PaperExchangeEngine
    eng_mod.PaperOrder = PaperOrder
    eng_mod.PaperFill = PaperFill
    eng_mod.PaperPosition = PaperPosition
    sys.modules["apps.paper_exchange.engine"] = eng_mod


_install_exchange_stub()
_install_paper_engine_stub()
