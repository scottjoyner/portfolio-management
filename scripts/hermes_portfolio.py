#!/usr/bin/env python3
"""
hermes_portfolio.py — Phase 11: CORRELATION / CONCENTRATION RISK.

The bot opens mean-reversion trades on ~13 assets with no regard for the fact
that most alts are >0.7 correlated to BTC — so a single BTC move can wreck 8
positions at once. This module:

  * correlation_to_btc(asset, client) -> Pearson r of 120h log-returns vs BTC.
  * concentration(led) -> how much open notional is BTC-correlated (long or
    short) right now.
  * exposure_ok(led, asset, side, cap) -> False if adding this trade would push
    BTC-correlated exposure past `cap` (default $20 = 2x the $10/trade max, i.e.
    at most ~2 correlated positions of max size, or more smaller ones).

The loop calls exposure_ok BEFORE opening a new position. OPEN positions are
never force-closed by this — it only gates NEW entries (like every other gate).

Read-only candles; no state writes.
"""
from __future__ import annotations

import sys as _sys
from pathlib import Path as _P

# Bootstrap so `from scripts...` works whether imported by the loop (cwd=repo)
# or run directly (python scripts/hermes_portfolio.py).
_REPO = str(_P(__file__).resolve().parent.parent)
if _REPO not in _sys.path:
    _sys.path.insert(0, _REPO)

import datetime as dt

from scripts.hermes_agent_loop import _candles
from scripts.hermes_agent_trader import load_ledger, MAX_NOTIONAL

# Max BTC-correlated open notional (long or short) the agent allows at once.
# 40% of the $10k starting book ($4000) — lets the agent run a diversified book
# of ~15-20 correlated positions while still containing a single BTC shock to
# ~40% of equity. Uncorrelated alts (|r|<0.5) are never capped, so the agent can
# deploy freely across independent flow shots.
CORRELATED_CAP_USD = 4000.0
# Assets with r >= this to BTC count as "BTC-correlated" for the cap.
CORR_THRESHOLD = 0.5


def _log_returns(candles: list) -> list:
    closes = [float(c["close"]) for c in candles if c.get("close")]
    out = []
    for i in range(1, len(closes)):
        if closes[i - 1] > 0:
            out.append((closes[i] - closes[i - 1]) / closes[i - 1])
    return out


def correlation_to_btc(asset: str, client, window_h: int = 120) -> float:
    """Pearson correlation of `asset` 1h log-returns vs BTC over `window_h`."""
    a = _log_returns(_candles(client, asset, window_h))
    b = _log_returns(_candles(client, "BTC-USD", window_h))
    n = min(len(a), len(b))
    if n < 10:
        return 0.0
    a, b = a[-n:], b[-n:]
    ma = sum(a) / n
    mb = sum(b) / n
    cov = sum((x - ma) * (y - mb) for x, y in zip(a, b))
    va = sum((x - ma) ** 2 for x in a)
    vb = sum((y - mb) ** 2 for y in b)
    if va <= 0 or vb <= 0:
        return 0.0
    return cov / (va ** 0.5 * vb ** 0.5)


def concentration(led: dict | None = None, corr_cache: dict | None = None) -> dict:
    """Open notional that is BTC-correlated (per cached r>=threshold), by side."""
    if led is None:
        led = load_ledger()
    if corr_cache is None:
        corr_cache = {}
    long_cor = 0.0
    short_cor = 0.0
    for pid, pos in led.get("positions", {}).items():
        base = pos.get("base", 0.0)
        if base <= 1e-12:
            continue
        is_short = pid.startswith("SHORT:")
        asset = pid.replace("SHORT:", "")
        r = corr_cache.get(asset)
        if r is None:
            r = corr_cache.get(asset, 0.0)  # caller should pre-fill; default 0
        if abs(r) < CORR_THRESHOLD:
            continue  # not BTC-correlated -> outside this cap
        # approximate notional from cost_basis (long) or entry_price*base (short)
        if is_short:
            notional = abs(pos.get("entry_price", 0.0) * base)
            short_cor += notional
        else:
            notional = abs(pos.get("cost_basis", 0.0))
            long_cor += notional
    return {"long_correlated": round(long_cor, 2),
            "short_correlated": round(short_cor, 2),
            "total_correlated": round(long_cor + short_cor, 2)}


def exposure_ok(asset: str, side: str, notional: float,
                corr_cache: dict, led: dict | None = None,
                cap: float = CORRELATED_CAP_USD) -> tuple[bool, str]:
    """Gate a NEW entry: False if it would push BTC-correlated exposure past cap.
    `side` is BUY (long) or SHORT_OPEN/SELL (short). `corr_cache` maps asset->r."""
    r = corr_cache.get(asset, 0.0)
    if abs(r) < CORR_THRESHOLD:
        return True, "not_btc_correlated"
    if led is None:
        led = load_ledger()
    conc = concentration(led, corr_cache)
    side_bucket = conc["long_correlated"] if side in ("BUY", "LONG") else conc["short_correlated"]
    projected = side_bucket + notional
    if projected > cap:
        return False, f"corr_cap:{side_bucket:.0f}+{notional:.0f}>{cap:.0f}"
    return True, "ok"


if __name__ == "__main__":
    import sys as _sys
    from pathlib import Path as _P
    _sys.path.insert(0, str(_P(__file__).resolve().parent.parent))
    import sys
    sys.path.insert(0, ".")
    from coinbase.src.cb_client import CBClient
    c = CBClient(dry_run_cli=True)
    print("=== Phase 11 BTC-correlation (120h log-returns) ===")
    cache = {}
    for a in ["ETH-USD", "SOL-USD", "LINK-USD", "ZEC-USD", "IOTX-USD", "GNO-USD"]:
        r = correlation_to_btc(a, c)
        cache[a] = r
        print(f"  {a:12} r(BTC)={r:+.2f}")
    print("\n=== concentration of OPEN book ===")
    print("  ", concentration(load_ledger(), cache))
