"""Polymarket (CLOB) order-execution client.

Wraps `py_clob_client` to place real on-chain-settled orders on Polymarket's
central limit order book. Kept separate from the read-only `PolymarketClient`
so that market-data scanning never needs a wallet.

Configuration (env):
  POLYMARKET_PRIVATE_KEY     wallet private key (0x...) that signs orders
  POLYMARKET_FUNDER          address holding USDC (defaults to signer address
                             for signature_type 0 / EOA). For Polymarket UI
                             (email/magic) accounts this is the proxy address.
  POLYMARKET_SIGNATURE_TYPE  0=EOA (default), 1=email/magic proxy, 2=Gnosis safe
  POLYMARKET_CLOB_HOST       default https://clob.polymarket.com
  POLYMARKET_CHAIN_ID        default 137 (Polygon mainnet)

Nothing here places an order unless a private key is configured AND the caller
explicitly requests it. Order placement requires that the wallet has USDC and
the necessary CTF/exchange allowances set (a one-time on-chain approval done
outside this module, e.g. via the Polymarket UI or update_balance_allowance).
"""
from __future__ import annotations

import logging
import os
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

DEFAULT_HOST = "https://clob.polymarket.com"
DEFAULT_CHAIN_ID = 137


class PolymarketExecutionClient:
    def __init__(self,
                 private_key: str = "",
                 funder: str = "",
                 signature_type: Optional[int] = None,
                 host: str = "",
                 chain_id: Optional[int] = None,
                 data_client=None):
        self.private_key = private_key or os.environ.get("POLYMARKET_PRIVATE_KEY", "")
        self.funder = funder or os.environ.get("POLYMARKET_FUNDER", "")
        st = signature_type if signature_type is not None else os.environ.get("POLYMARKET_SIGNATURE_TYPE", "0")
        try:
            self.signature_type = int(st)
        except (TypeError, ValueError):
            self.signature_type = 0
        self.host = host or os.environ.get("POLYMARKET_CLOB_HOST", DEFAULT_HOST)
        self.chain_id = int(chain_id or os.environ.get("POLYMARKET_CHAIN_ID", DEFAULT_CHAIN_ID) or DEFAULT_CHAIN_ID)
        self._client = None
        self._creds = None
        # Read-only PolymarketClient (Gamma) used to resolve condition_id -> token_ids.
        self._data = data_client

    # ── capability ──────────────────────────────────────────────────
    @staticmethod
    def _lib_available() -> bool:
        try:
            import py_clob_client  # noqa: F401
            return True
        except Exception:
            return False

    def is_configured(self) -> tuple[bool, str]:
        if not self.private_key:
            return False, "no POLYMARKET_PRIVATE_KEY set"
        if not self._lib_available():
            return False, "py_clob_client not installed"
        return True, ""

    # ── client bootstrap ────────────────────────────────────────────
    def _get_client(self):
        if self._client is not None:
            return self._client
        ok, why = self.is_configured()
        if not ok:
            raise RuntimeError(f"Polymarket execution not configured: {why}")
        from py_clob_client.client import ClobClient
        from py_clob_client.clob_types import ApiCreds

        funder = self.funder or None
        client = ClobClient(
            self.host,
            chain_id=self.chain_id,
            key=self.private_key,
            signature_type=self.signature_type,
            funder=funder,
        )
        # Derive L2 API credentials from the wallet (deterministic) and attach.
        creds = client.create_or_derive_api_creds()
        client.set_api_creds(creds)
        self._creds = creds
        self._client = client
        return client

    def _data_client(self):
        if self._data is not None:
            return self._data
        try:
            from event_markets.polymarket_client import PolymarketClient
            self._data = PolymarketClient()
        except Exception as e:
            logger.debug("polymarket data client init failed: %s", e)
        return self._data

    # ── token resolution ────────────────────────────────────────────
    def resolve_token_id(self, market_id: str, outcome: str) -> str:
        """Resolve a token_id for an outcome ("yes"/"no" or label) from a
        condition_id. If `market_id` already looks like a numeric token_id
        (Polymarket token ids are large integers), return it unchanged.
        """
        if market_id and market_id.isdigit():
            return market_id
        want = outcome.lower()
        # Primary: the CLOB market endpoint returns tokens keyed by condition_id.
        tokens = self._clob_market_tokens(market_id)
        if tokens:
            for o, tid in tokens:
                ol = o.lower()
                if ol == want or (want == "yes" and ol in ("yes", "true")) or (want == "no" and ol in ("no", "false")):
                    return str(tid)
            # positional fallback: 0=yes, 1=no
            idx = 0 if want == "yes" else 1
            if idx < len(tokens) and tokens[idx][1]:
                return str(tokens[idx][1])
        # Fallback: Gamma data client (works when market_id is a numeric gamma id).
        data = self._data_client()
        if data:
            detail = data.fetch_market_detail(market_id)
            if detail and getattr(detail, "tokens", None):
                outcomes = [str(o).lower() for o in (getattr(detail, "outcomes", []) or [])]
                toks = [t.get("token_id") for t in detail.tokens]
                idx = None
                for i, o in enumerate(outcomes):
                    if o == want or (want == "yes" and o in ("yes", "true")) or (want == "no" and o in ("no", "false")):
                        idx = i
                        break
                if idx is None:
                    idx = 0 if want == "yes" else 1
                if idx < len(toks) and toks[idx]:
                    return str(toks[idx])
        raise RuntimeError(f"could not resolve token_id for {market_id} / {outcome}")

    @staticmethod
    def _clob_market_tokens(condition_id: str) -> List[tuple]:
        """Return [(outcome, token_id), ...] from the public CLOB market endpoint."""
        import urllib.request
        import json as _json
        host = os.environ.get("POLYMARKET_CLOB_HOST", DEFAULT_HOST)
        try:
            req = urllib.request.Request(
                f"{host}/markets/{condition_id}",
                headers={"User-Agent": "Mozilla/5.0", "Accept": "application/json"},
            )
            with urllib.request.urlopen(req, timeout=10) as r:
                d = _json.loads(r.read().decode())
            return [(t.get("outcome", ""), t.get("token_id", "")) for t in d.get("tokens", [])]
        except Exception as e:
            logger.debug("clob market tokens failed for %s: %s", condition_id, e)
            return []

    # ── orders ──────────────────────────────────────────────────────
    def place_order(self, market_id: str, side: str, price: float, size: float,
                    outcome: str = "yes", order_type: str = "GTC",
                    token_id: str = "") -> Dict[str, Any]:
        """Place a CLOB order.

        side: "buy"|"sell"; price: dollars [0,1]; size: number of shares.
        Either pass an explicit token_id, or a condition_id (market_id) + outcome.
        order_type: GTC | FOK | GTD | FAK.
        """
        from py_clob_client.clob_types import OrderArgs, OrderType
        from py_clob_client.order_builder.constants import BUY, SELL

        client = self._get_client()
        tid = token_id or self.resolve_token_id(market_id, outcome)
        args = OrderArgs(
            token_id=tid,
            price=round(float(price), 4),
            size=float(size),
            side=BUY if side.lower() == "buy" else SELL,
        )
        ot = getattr(OrderType, order_type, OrderType.GTC)
        resp = client.create_and_post_order(args, options=None) if order_type == "GTC" else None
        if resp is None:
            signed = client.create_order(args)
            resp = client.post_order(signed, orderType=ot)
        return resp if isinstance(resp, dict) else {"raw": resp}

    def cancel(self, order_id: str) -> Dict[str, Any]:
        client = self._get_client()
        return client.cancel(order_id)

    def get_orders(self) -> List[Dict[str, Any]]:
        try:
            client = self._get_client()
            return client.get_orders() or []
        except Exception as e:
            logger.debug("polymarket get_orders failed: %s", e)
            return []

    # ── balances ────────────────────────────────────────────────────
    # Cloudflare in front of clob.polymarket.com rejects the default
    # py_clob_client User-Agent on the /balance-allowance/update endpoint
    # (HTTP 403, CF error 1010). Sending browser-style headers avoids it.
    _BROWSER_HEADERS = {
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                      "(KHTML, like Gecko) Chrome/124.0 Safari/537.36",
        "Accept": "application/json, text/plain, */*",
        "Origin": "https://polymarket.com",
        "Referer": "https://polymarket.com/",
    }

    def _balance_allowance_request(self, endpoint: str, asset_type=None,
                                   token_id: str = "") -> Optional[dict]:
        """Call a CLOB balance-allowance endpoint with browser headers.

        `endpoint` is GET_BALANCE_ALLOWANCE or UPDATE_BALANCE_ALLOWANCE.
        Returns parsed JSON (or {} for empty-body success), None on failure.
        """
        import json
        import urllib.request
        import urllib.error
        from py_clob_client.clob_types import BalanceAllowanceParams, AssetType, RequestArgs
        from py_clob_client.headers.headers import create_level_2_headers
        from py_clob_client.http_helpers.helpers import add_balance_allowance_params_to_url

        client = self._get_client()
        if asset_type is None:
            asset_type = AssetType.COLLATERAL
        params = BalanceAllowanceParams(
            asset_type=asset_type,
            token_id=token_id or "",
            signature_type=self.signature_type,
        )
        request_args = RequestArgs(method="GET", request_path=endpoint)
        headers = {**create_level_2_headers(client.signer, client.creds, request_args),
                   **self._BROWSER_HEADERS}
        url = add_balance_allowance_params_to_url(f"{client.host}{endpoint}", params)
        try:
            with urllib.request.urlopen(urllib.request.Request(url, headers=headers), timeout=25) as r:
                body = r.read().decode()
            return json.loads(body) if body.strip() else {}
        except urllib.error.HTTPError as e:
            logger.debug("balance-allowance %s failed: %s %s", endpoint, e.code, e.read()[:200])
            return None
        except Exception as e:
            logger.debug("balance-allowance %s error: %s", endpoint, e)
            return None

    def refresh_balance_allowance(self, asset_type=None, token_id: str = "") -> bool:
        """Force Polymarket's backend to re-scan on-chain balance/allowance.

        Required after funding the wallet or setting on-chain allowances so the
        CLOB's cached view reflects reality. Returns True on HTTP success.
        """
        from py_clob_client.endpoints import UPDATE_BALANCE_ALLOWANCE
        return self._balance_allowance_request(UPDATE_BALANCE_ALLOWANCE, asset_type, token_id) is not None

    def get_balance_allowance(self, asset_type=None, token_id: str = "") -> Optional[dict]:
        """Return the CLOB's view of {balance, allowances} (browser-header path)."""
        from py_clob_client.endpoints import GET_BALANCE_ALLOWANCE
        return self._balance_allowance_request(GET_BALANCE_ALLOWANCE, asset_type, token_id)

    def get_usdc_balance(self) -> Optional[float]:
        """Return USDC (collateral) balance in dollars, or None if unavailable."""
        try:
            res = self.get_balance_allowance()
            if res is None:
                # Fall back to the library method (no browser headers).
                from py_clob_client.clob_types import BalanceAllowanceParams, AssetType
                res = self._get_client().get_balance_allowance(
                    BalanceAllowanceParams(asset_type=AssetType.COLLATERAL))
            bal = res.get("balance") if isinstance(res, dict) else None
            if bal is None:
                return None
            # Polymarket returns USDC balance in 6-decimal base units.
            return round(float(bal) / 1_000_000.0, 2)
        except Exception as e:
            logger.debug("polymarket balance failed: %s", e)
            return None

    def address(self) -> str:
        try:
            return self._get_client().get_address()
        except Exception:
            return ""
