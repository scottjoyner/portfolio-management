"""Validate every prediction-market / venue config reachable from this host.

Read-only: performs no trades and no on-chain writes. Reports what is
configured, what is reachable, and (for Polymarket) whether trading is
geoblocked from the current region.

    python scripts/validate_venue_config.py
"""
from __future__ import annotations

import json
import os
import sys
import urllib.request

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

try:
    from dotenv import load_dotenv
    load_dotenv(os.path.join(ROOT, ".env"))
except ImportError:
    pass

OK, WARN, FAIL, INFO = "PASS", "WARN", "FAIL", "INFO"
rows: list[tuple[str, str, str]] = []


def check(name: str, status: str, detail: str = "") -> None:
    rows.append((name, status, detail))


# ── env presence ──────────────────────────────────────────────────────────
def check_env() -> None:
    required = {
        "KALSHI_API_KEY_ID": "Kalshi API key id",
        "KALSHI_PRIVATE_KEY_PATH": "Kalshi RSA private key path",
    }
    optional = {
        "POLYMARKET_PRIVATE_KEY": "Polymarket wallet key",
        "POLYMARKET_FUNDER": "Polymarket funder address",
        "KALSHI_API_BASE_URL": "Kalshi base url",
    }
    for k, desc in required.items():
        v = os.environ.get(k, "")
        check(f"env {k}", OK if v else FAIL, desc if v else f"MISSING ({desc})")
    for k, desc in optional.items():
        v = os.environ.get(k, "")
        check(f"env {k}", OK if v else WARN, desc if v else f"unset ({desc})")
    kp = os.environ.get("KALSHI_PRIVATE_KEY_PATH", "")
    if kp:
        check("Kalshi key file exists", OK if os.path.exists(kp) else FAIL, kp)


# ── Kalshi ──────────────────────────────────────────────────────────────────
def check_kalshi() -> None:
    try:
        from event_markets.kalshi_client import KalshiClient
        kc = KalshiClient(
            api_key_id=os.environ.get("KALSHI_API_KEY_ID", ""),
            private_key_path=os.environ.get("KALSHI_PRIVATE_KEY_PATH", ""),
            base_url=os.environ.get("KALSHI_API_BASE_URL", ""),
        )
    except Exception as e:  # noqa: BLE001
        check("Kalshi client init", FAIL, str(e)[:120]); return
    try:
        bal = kc.get_balance() or {}
        dollars = bal.get("balance_dollars")
        if dollars is None and bal.get("balance") is not None:
            dollars = float(bal["balance"]) / 100.0
        check("Kalshi auth + balance", OK, f"${dollars}")
    except Exception as e:  # noqa: BLE001
        check("Kalshi auth + balance", FAIL, str(e)[:120])
    for meth in ("get_positions", "get_orders", "get_fills"):
        try:
            getattr(kc, meth)()
            check(f"Kalshi {meth}", OK)
        except Exception as e:  # noqa: BLE001
            check(f"Kalshi {meth}", WARN, str(e)[:80])


# ── Coinbase ─────────────────────────────────────────────────────────────────
def check_coinbase() -> None:
    try:
        from portfolio_optimizer import CoinbaseCLI
        cb = CoinbaseCLI()
    except Exception as e:  # noqa: BLE001
        check("Coinbase client init", WARN, str(e)[:120]); return
    try:
        p = cb.get_price("BTC-USD")
        px = float(p.get("price", p.get("bid", 0)) or 0)
        check("Coinbase spot price", OK if px > 0 else WARN, f"BTC-USD=${px}")
    except Exception as e:  # noqa: BLE001
        check("Coinbase spot price", WARN, str(e)[:120])


# ── Polymarket ───────────────────────────────────────────────────────────────
def check_polymarket() -> None:
    key = os.environ.get("POLYMARKET_PRIVATE_KEY", "")
    if not key:
        check("Polymarket wallet", WARN, "no POLYMARKET_PRIVATE_KEY set"); return
    try:
        from event_markets.provisioning import wallet_status
        st = wallet_status(key)
        check("Polymarket wallet on-chain", OK, st["address"])
        check("  MATIC (gas)", OK if st["matic"] > 0.05 else WARN, f"{st['matic']:.3f}")
        check("  USDC.e (collateral)", OK if st["usdce"] > 0 else WARN, f"{st['usdce']:.4f}")
        if st["usdc_native"] > 0:
            check("  native USDC (needs swap)", WARN, f"{st['usdc_native']:.4f} — run provisioning swap")
        check("  on-chain allowances", OK if st["all_allowances_set"] else WARN,
              "all set" if st["all_allowances_set"] else "incomplete — run provisioning allowances")
    except Exception as e:  # noqa: BLE001
        check("Polymarket wallet on-chain", FAIL, str(e)[:150])

    # Geoblock status (informational — trading is region-restricted).
    try:
        req = urllib.request.Request("https://polymarket.com/api/geoblock",
                                     headers={"User-Agent": "Mozilla/5.0"})
        d = json.loads(urllib.request.urlopen(req, timeout=15).read().decode())
        blocked = d.get("blocked")
        check("Polymarket trading region", INFO if not blocked else WARN,
              f"blocked={blocked} country={d.get('country')} region={d.get('region')}"
              + ("" if not blocked else " — execution NOT permitted from here"))
    except Exception as e:  # noqa: BLE001
        check("Polymarket trading region", INFO, str(e)[:100])


def main() -> int:
    check_env()
    check_kalshi()
    check_coinbase()
    check_polymarket()

    width = max(len(n) for n, _, _ in rows)
    print("\n=== Venue / config validation ===\n")
    for name, status, detail in rows:
        print(f"  [{status:4}] {name.ljust(width)}  {detail}")
    fails = sum(1 for _, s, _ in rows if s == FAIL)
    warns = sum(1 for _, s, _ in rows if s == WARN)
    print(f"\n{len(rows)} checks — {fails} FAIL, {warns} WARN\n")
    return 1 if fails else 0


if __name__ == "__main__":
    raise SystemExit(main())
