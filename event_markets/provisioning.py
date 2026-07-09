"""Polymarket wallet provisioning & on-chain setup (Polygon).

Everything needed to take a Polymarket execution wallet from nothing to
trade-ready, as reusable functions plus a CLI. All operations are standard
self-custody wallet actions on Polygon (chainId 137):

  1. generate a fresh EOA wallet (private key + address)
  2. inspect balances (MATIC gas, native USDC, USDC.e collateral) + allowances
  3. swap native USDC -> USDC.e (Polymarket collateral) via Uniswap v3
  4. set the on-chain ERC20/ERC1155 allowances Polymarket's exchange needs
  5. ask Polymarket's CLOB to re-scan the wallet (refresh cached balance)

IMPORTANT — jurisdiction: provisioning the wallet works anywhere, but
Polymarket *trading* (order placement) is geoblocked by region under
Polymarket's CFTC settlement (e.g. US IPs are blocked). This module does NOT
and will NOT circumvent that restriction. The operator is responsible for
running execution only from a permitted jurisdiction and complying with all
applicable law.

CLI:
    python -m event_markets.provisioning generate  --keyfile PATH
    python -m event_markets.provisioning status     [--key 0x.. | reads .env]
    python -m event_markets.provisioning swap       [--amount N] [--yes]
    python -m event_markets.provisioning allowances [--yes]
    python -m event_markets.provisioning refresh
"""
from __future__ import annotations

import logging
import os
import secrets
import stat
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

logger = logging.getLogger("polymarket.provisioning")

# ── Polygon mainnet addresses ────────────────────────────────────────────
CHAIN_ID = 137
USDC_NATIVE = "0x3c499c542cEF5E3811e1192ce70d8cc03d5c3359"   # native USDC
USDCE = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174"         # USDC.e = Polymarket collateral
CTF = "0x4D97DCd97eC945f40cF65F87097ACe5EA0476045"           # Conditional Tokens (ERC1155)
UNISWAP_V3_ROUTER = "0x68b3465833fb72A70ecDF485E0e4C7bD8665Fc45"  # SwapRouter02
UNISWAP_V3_FEE_TIER = 100                                     # 0.01% (deep USDC/USDC.e pool)

# The exchange operator contracts Polymarket requires spending approval for.
# These are exactly the spenders reported by the CLOB balance-allowance API.
POLYMARKET_SPENDERS = [
    "0xE111180000d2663C0091e4f400237545B87B996B",  # CTF Exchange
    "0xd91E80cF2E7be2e162c6513ceD06f1dD0dA35296",  # Neg Risk Adapter
    "0xe2222d279d744050d28e00520010520000310F59",  # Neg Risk CTF Exchange
]

POLYGON_RPCS = [
    "https://polygon-bor-rpc.publicnode.com",
    "https://polygon.llamarpc.com",
    "https://1rpc.io/matic",
    "https://polygon-rpc.com",
]

MAX_UINT256 = 2**256 - 1

_ERC20_ABI = [
    {"inputs": [{"type": "address"}], "name": "balanceOf", "outputs": [{"type": "uint256"}], "stateMutability": "view", "type": "function"},
    {"inputs": [{"type": "address"}, {"type": "address"}], "name": "allowance", "outputs": [{"type": "uint256"}], "stateMutability": "view", "type": "function"},
    {"inputs": [{"type": "address"}, {"type": "uint256"}], "name": "approve", "outputs": [{"type": "bool"}], "stateMutability": "nonpayable", "type": "function"},
    {"inputs": [], "name": "symbol", "outputs": [{"type": "string"}], "stateMutability": "view", "type": "function"},
]
_ERC1155_ABI = [
    {"inputs": [{"type": "address"}, {"type": "address"}], "name": "isApprovedForAll", "outputs": [{"type": "bool"}], "stateMutability": "view", "type": "function"},
    {"inputs": [{"type": "address"}, {"type": "bool"}], "name": "setApprovalForAll", "outputs": [], "stateMutability": "nonpayable", "type": "function"},
]
_ROUTER_ABI = [{"inputs": [{"components": [
    {"name": "tokenIn", "type": "address"}, {"name": "tokenOut", "type": "address"},
    {"name": "fee", "type": "uint24"}, {"name": "recipient", "type": "address"},
    {"name": "amountIn", "type": "uint256"}, {"name": "amountOutMinimum", "type": "uint256"},
    {"name": "sqrtPriceLimitX96", "type": "uint160"}], "name": "params", "type": "tuple"}],
    "name": "exactInputSingle", "outputs": [{"name": "amountOut", "type": "uint256"}],
    "stateMutability": "payable", "type": "function"}]


# ── wallet generation ────────────────────────────────────────────────────
def generate_wallet(keyfile_path: str) -> Dict[str, str]:
    """Generate a fresh Polygon EOA wallet and save it to a 0600 key file.

    Returns {"address", "private_key", "keyfile"}. The private key is written
    to disk with owner-only permissions; back it up securely — losing it means
    losing any funds sent to the address.
    """
    from eth_account import Account

    priv = "0x" + secrets.token_hex(32)
    acct = Account.from_key(priv)
    stamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    with open(keyfile_path, "w") as f:
        f.write(f"# Polymarket self-custody wallet (EOA) generated {stamp}\n")
        f.write(f"# Chain: Polygon (chainId {CHAIN_ID}). Fund with USDC.e + MATIC.\n")
        f.write(f"address: {acct.address}\n")
        f.write(f"private_key: {priv}\n")
    os.chmod(keyfile_path, stat.S_IRUSR | stat.S_IWUSR)  # 0600
    logger.info("generated wallet %s -> %s", acct.address, keyfile_path)
    return {"address": acct.address, "private_key": priv, "keyfile": keyfile_path}


# ── web3 helpers ─────────────────────────────────────────────────────────
def get_web3(rpcs: Optional[List[str]] = None):
    """Return a connected web3 instance (Polygon, POA middleware injected)."""
    from web3 import Web3
    from web3.middleware import ExtraDataToPOAMiddleware

    last = None
    for url in (rpcs or POLYGON_RPCS):
        try:
            w3 = Web3(Web3.HTTPProvider(url, request_kwargs={"timeout": 25}))
            w3.middleware_onion.inject(ExtraDataToPOAMiddleware, layer=0)
            if w3.is_connected() and w3.eth.block_number > 0:
                logger.debug("connected to %s", url)
                return w3
        except Exception as e:  # noqa: BLE001
            last = e
    raise RuntimeError(f"could not connect to any Polygon RPC: {last}")


def _eip1559_fees(w3) -> Dict[str, int]:
    base = w3.eth.get_block("latest").get("baseFeePerGas", w3.to_wei(30, "gwei"))
    tip = w3.to_wei(35, "gwei")  # Polygon minimum priority fee
    return {"maxFeePerGas": base * 2 + tip, "maxPriorityFeePerGas": tip}


def _send(w3, acct, fn, gas: Optional[int] = None) -> Dict[str, Any]:
    """Build, sign, send a contract call; wait for the receipt; assert success."""
    if gas is None:
        gas = int(fn.estimate_gas({"from": acct.address}) * 1.5)
    tx = fn.build_transaction({
        "from": acct.address,
        "nonce": w3.eth.get_transaction_count(acct.address, "pending"),
        "chainId": CHAIN_ID, "gas": gas, **_eip1559_fees(w3),
    })
    h = w3.eth.send_raw_transaction(acct.sign_transaction(tx).raw_transaction)
    r = w3.eth.wait_for_transaction_receipt(h, timeout=240)
    if r.status != 1:
        raise RuntimeError(f"tx reverted: {r.transactionHash.hex()}")
    return {"tx": r.transactionHash.hex(), "gas_used": r.gasUsed}


def _resolve_key(private_key: str = "") -> str:
    key = private_key or os.environ.get("POLYMARKET_PRIVATE_KEY", "")
    if not (key.startswith("0x") and len(key) == 66):
        raise ValueError("no valid POLYMARKET_PRIVATE_KEY (0x + 64 hex) provided")
    return key


# ── status ───────────────────────────────────────────────────────────────
def wallet_status(private_key: str = "") -> Dict[str, Any]:
    """Return balances (MATIC/native-USDC/USDC.e) and Polymarket allowances."""
    from web3 import Web3

    key = _resolve_key(private_key)
    w3 = get_web3()
    acct = w3.eth.account.from_key(key)
    me = acct.address
    usdc = w3.eth.contract(address=Web3.to_checksum_address(USDC_NATIVE), abi=_ERC20_ABI)
    usdce = w3.eth.contract(address=Web3.to_checksum_address(USDCE), abi=_ERC20_ABI)
    ctf = w3.eth.contract(address=Web3.to_checksum_address(CTF), abi=_ERC1155_ABI)

    allowances = {}
    for sp in POLYMARKET_SPENDERS:
        spx = Web3.to_checksum_address(sp)
        allowances[sp] = {
            "usdce_approved": usdce.functions.allowance(me, spx).call() > 0,
            "ctf_approved": ctf.functions.isApprovedForAll(me, spx).call(),
        }
    return {
        "address": me,
        "matic": w3.eth.get_balance(me) / 1e18,
        "usdc_native": usdc.functions.balanceOf(me).call() / 1e6,
        "usdce": usdce.functions.balanceOf(me).call() / 1e6,
        "allowances": allowances,
        "all_allowances_set": all(
            a["usdce_approved"] and a["ctf_approved"] for a in allowances.values()
        ),
    }


# ── swap native USDC -> USDC.e ────────────────────────────────────────────
def swap_usdc_to_usdce(private_key: str = "", amount_usdc: Optional[float] = None,
                       slippage: float = 0.005) -> Dict[str, Any]:
    """Swap native USDC -> USDC.e on Uniswap v3 (0.01% pool). amount_usdc=None
    swaps the entire native-USDC balance. Approves the router first if needed.
    """
    from web3 import Web3

    key = _resolve_key(private_key)
    w3 = get_web3()
    acct = w3.eth.account.from_key(key)
    me = acct.address
    usdc = w3.eth.contract(address=Web3.to_checksum_address(USDC_NATIVE), abi=_ERC20_ABI)
    router = w3.eth.contract(address=Web3.to_checksum_address(UNISWAP_V3_ROUTER), abi=_ROUTER_ABI)

    bal = usdc.functions.balanceOf(me).call()
    amount_in = bal if amount_usdc is None else int(amount_usdc * 1e6)
    if amount_in <= 0 or amount_in > bal:
        raise ValueError(f"invalid swap amount: have {bal/1e6} native USDC")
    min_out = int(amount_in * (1 - slippage))

    out: Dict[str, Any] = {"amount_in": amount_in / 1e6}
    if usdc.functions.allowance(me, Web3.to_checksum_address(UNISWAP_V3_ROUTER)).call() < amount_in:
        out["approve"] = _send(w3, acct, usdc.functions.approve(
            Web3.to_checksum_address(UNISWAP_V3_ROUTER), amount_in))
    params = (Web3.to_checksum_address(USDC_NATIVE), Web3.to_checksum_address(USDCE),
              UNISWAP_V3_FEE_TIER, me, amount_in, min_out, 0)
    out["swap"] = _send(w3, acct, router.functions.exactInputSingle(params))
    usdce = w3.eth.contract(address=Web3.to_checksum_address(USDCE), abi=_ERC20_ABI)
    out["usdce_balance"] = usdce.functions.balanceOf(me).call() / 1e6
    return out


# ── on-chain allowances ───────────────────────────────────────────────────
def set_polymarket_allowances(private_key: str = "") -> Dict[str, Any]:
    """Approve USDC.e (ERC20) and CTF (ERC1155) for all Polymarket operators.

    Idempotent: skips any spender already approved.
    """
    from web3 import Web3

    key = _resolve_key(private_key)
    w3 = get_web3()
    acct = w3.eth.account.from_key(key)
    me = acct.address
    usdce = w3.eth.contract(address=Web3.to_checksum_address(USDCE), abi=_ERC20_ABI)
    ctf = w3.eth.contract(address=Web3.to_checksum_address(CTF), abi=_ERC1155_ABI)

    results: Dict[str, Any] = {}
    for sp in POLYMARKET_SPENDERS:
        spx = Web3.to_checksum_address(sp)
        r: Dict[str, Any] = {}
        if usdce.functions.allowance(me, spx).call() < MAX_UINT256 // 2:
            r["usdce_approve"] = _send(w3, acct, usdce.functions.approve(spx, MAX_UINT256))
        else:
            r["usdce_approve"] = "already approved"
        if not ctf.functions.isApprovedForAll(me, spx).call():
            r["ctf_approve"] = _send(w3, acct, ctf.functions.setApprovalForAll(spx, True))
        else:
            r["ctf_approve"] = "already approved"
        results[sp] = r
    return results


def refresh_clob_cache(private_key: str = "") -> bool:
    """Ask Polymarket's CLOB to re-scan on-chain balance/allowance for the wallet.

    NOTE: this authenticated CLOB call is subject to Polymarket's regional
    restrictions; from a geoblocked region it will fail. That is expected and
    is not something this module works around.
    """
    _resolve_key(private_key)
    from event_markets.polymarket_executor import PolymarketExecutionClient
    pm = PolymarketExecutionClient(private_key=private_key or None)
    return pm.refresh_balance_allowance()


# ── CLI ────────────────────────────────────────────────────────────────────
def _main() -> None:
    import argparse
    import json

    try:
        from dotenv import load_dotenv
        load_dotenv()
    except ImportError:
        pass

    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    p = argparse.ArgumentParser(description="Polymarket wallet provisioning")
    sub = p.add_subparsers(dest="cmd", required=True)
    g = sub.add_parser("generate"); g.add_argument("--keyfile", required=True)
    sub.add_parser("status").add_argument("--key", default="")
    sw = sub.add_parser("swap"); sw.add_argument("--key", default=""); sw.add_argument("--amount", type=float); sw.add_argument("--yes", action="store_true")
    al = sub.add_parser("allowances"); al.add_argument("--key", default=""); al.add_argument("--yes", action="store_true")
    sub.add_parser("refresh").add_argument("--key", default="")
    args = p.parse_args()

    if args.cmd == "generate":
        print(json.dumps({k: v for k, v in generate_wallet(args.keyfile).items() if k != "private_key"}, indent=2))
        print("private key written to", args.keyfile, "(0600) — back it up securely")
    elif args.cmd == "status":
        print(json.dumps(wallet_status(args.key), indent=2))
    elif args.cmd == "swap":
        if not args.yes:
            print("This sends a REAL on-chain swap. Re-run with --yes to confirm."); return
        print(json.dumps(swap_usdc_to_usdce(args.key, args.amount), indent=2, default=str))
    elif args.cmd == "allowances":
        if not args.yes:
            print("This sends REAL on-chain approvals. Re-run with --yes to confirm."); return
        print(json.dumps(set_polymarket_allowances(args.key), indent=2, default=str))
    elif args.cmd == "refresh":
        print("CLOB refresh ok:", refresh_clob_cache(args.key))


if __name__ == "__main__":
    _main()
