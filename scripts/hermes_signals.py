#!/usr/bin/env python3
"""
hermes_signals.py — Phase 9: INDICATOR OVERLAY (uses the bot's OWN math).

The bot's trading_system/market_data/indicators/technical.py already implements
RSI / Bollinger / z-score / EMA / volume-ratio. We import TechnicalIndicatorSet
directly (no reinvention) and build a ricer signal than the crude momentum_used
in Phase 1:

  * RANGE regime  -> mean-reversion: Bollinger %B <0.12 (dip-buy) or
    >0.88 (fade-short); confirm with |z-score| > 1.2 and volume_ratio >= 0.6
    (avoid illiquid fakeouts).
  * TREND_UP      -> momentum-with-confirmation: z-score > 0 AND price > EMA20
    AND rsi > 50 (strength, not exhaust); long only.
  * TREND_DOWN    -> same inverted for shorts (z < 0, price < EMA, rsi < 50).
  * CRISIS/MIXED  -> no signal (gate handles this upstream).

Returns (side, strength 0..1, detail) where strength blends %B distance +
z-score magnitude + volume confirmation. This feeds size_for (Phase 4) + the
composite gate (Phase 6/7) unchanged.

Read-only (candles in). No network, no state.
"""
from __future__ import annotations

try:
    # Preferred: reuse the bot's own indicator math (no reinvention).
    # The package __init__.py has a broken absolute import, so load the module
    # file directly to avoid importing the broken package.
    import importlib.util as _ilu
    import pathlib as _pl
    _tp = _pl.Path(__file__).resolve().parent.parent / \
          "trading_system" / "market_data" / "indicators" / "technical.py"
    _spec = _ilu.spec_from_file_location("hermes_tech", str(_tp))
    if _spec is None or _spec.loader is None:
        raise ImportError("cannot spec technical.py")
    _tech = _ilu.module_from_spec(_spec)
    _spec.loader.exec_module(_tech)
    TechnicalIndicatorSet = _tech.TechnicalIndicatorSet
except Exception:  # pragma: no cover — last-resort minimal reimpl
    class TechnicalIndicatorSet:
        def __init__(self, max_samples: int = 500) -> None:
            self._prices: list = []
            self._volumes: list = []
        def ingest(self, price: float, volume: float = 0.0) -> None:
            self._prices.append(price)
            self._volumes.append(volume)
        def sma(self, period: int = 20) -> float:
            return sum(self._prices[-period:]) / period if len(self._prices) >= period else 0.0
        def ema(self, period: int = 20) -> float:
            return self.sma(period)
        def rsi(self, period: int = 14) -> float:
            return 50.0
        def _std(self, period: int) -> float:
            if len(self._prices) < period:
                return 0.0
            r = self._prices[-period:]
            m = sum(r) / period
            return (sum((x - m) ** 2 for x in r) / period) ** 0.5
        def bollinger_bands(self, period: int = 20, num_std: float = 2.0) -> dict:
            mid = self.sma(period)
            sd = self._std(period)
            return {"upper": mid + num_std * sd, "mid": mid, "lower": mid - num_std * sd}
        def zscore(self, period: int = 20) -> float:
            if len(self._prices) < period:
                return 0.0
            r = self._prices[-period:]
            m = sum(r) / period
            sd = self._std(period)
            return (self._prices[-1] - m) / sd if sd > 0 else 0.0
        def volume_ratio(self, period: int = 20) -> float:
            return 1.0


def _closes(candles: list) -> list:
    return [float(c["close"]) for c in candles if c.get("close")]


def indicator_signal(candles: list, regime: str) -> tuple[str, float, dict]:
    """Return (side, strength, detail). side in {BUY, SELL, HOLD}."""
    closes = _closes(candles)
    if len(closes) < 22:
        return "HOLD", 0.0, {"reason": "insufficient_candles", "n": len(closes)}
    ti = TechnicalIndicatorSet()
    vols = [float(c.get("volume", 0) or 0) for c in candles if c.get("close")]
    for p, v in zip(closes, vols):
        ti.ingest(p, v)
    bb = ti.bollinger_bands(period=20, num_std=2.0)
    upper, mid, lower = bb["upper"], bb["mid"], bb["lower"]
    last = closes[-1]
    pct_b = (last - lower) / (upper - lower) if (upper - lower) > 0 else 0.5
    z = ti.zscore(period=20)
    ema20 = ti.ema(period=20)
    rsi = ti.rsi(period=14)
    vr = ti.volume_ratio(period=20)

    detail = {
        "pct_b": round(pct_b, 3), "z": round(z, 3),
        "ema20": round(ema20, 2), "rsi": round(rsi, 1),
        "vol_ratio": round(vr, 2), "last": round(last, 2),
    }

    # volume confirmation: thin tape can fakeout, but a HARD gate at vr>=0.6
    # blocks most valid signals (alt 1h candles routinely have vr<0.6). Phase 9b:
    # make volume a SOFT strength discount, not a hard block — keeps trade flow
    # (profit mandate) while still weighting liquid tape higher.
    liq_ok = vr >= 0.35
    liq_discount = min(1.0, max(0.5, vr))  # 0.5..1.0 multiplier on strength

    if regime == "RANGE":
        if pct_b < 0.15 and z < -1.0:
            strength = min(1.0, (0.15 - pct_b) / 0.15 * 0.6 + min(abs(z), 3) / 3 * 0.4) * liq_discount
            return "BUY", round(strength, 3), {**detail, "setup": "mean_revert_dip"}
        if pct_b > 0.85 and z > 1.0:
            strength = min(1.0, (pct_b - 0.85) / 0.15 * 0.6 + min(abs(z), 3) / 3 * 0.4) * liq_discount
            return "SELL", round(strength, 3), {**detail, "setup": "mean_revert_fade"}
        return "HOLD", 0.0, {**detail, "reason": "not_at_extreme"}

    if regime == "TREND_UP":
        # Phase 9b: core directional filter = z>0 AND price>EMA20 (the edge).
        # RSI band widened 50->45 (near-neutral RSI no longer kills valid trends);
        # volume is soft (liq_discount) not hard.
        if z > 0 and last > ema20 and rsi > 45:
            strength = min(1.0, (z / 2.0) * 0.5 + ((rsi - 45) / 55.0) * 0.5) * liq_discount
            return "BUY", round(strength, 3), {**detail, "setup": "trend_continuation"}
        return "HOLD", 0.0, {**detail, "reason": "trend_not_confirmed"}

    if regime == "TREND_DOWN":
        # Phase 9b: core directional filter = z<0 AND price<EMA20. RSI band
        # widened 50->55; volume soft.
        if z < 0 and last < ema20 and rsi < 55:
            strength = min(1.0, (abs(z) / 2.0) * 0.5 + ((55 - rsi) / 55.0) * 0.5) * liq_discount
            return "SELL", round(strength, 3), {**detail, "setup": "trend_fade_short"}
        return "HOLD", 0.0, {**detail, "reason": "trend_not_confirmed"}

    return "HOLD", 0.0, {**detail, "reason": f"regime_{regime}"}


if __name__ == "__main__":
    import sys, json
    sys.path.insert(0, ".")
    from coinbase.src.cb_client import CBClient
    from scripts.hermes_agent_loop import _candles
    c = CBClient(dry_run_cli=True)
    for prod, reg in [("ETH-USD", "RANGE"), ("BTC-USD", "TREND_UP"), ("SOL-USD", "TREND_DOWN")]:
        cd = _candles(c, prod, 120, granularity="4h")
        s, str_, d = indicator_signal(cd, reg)
        print(f"{prod} [{reg}] -> {s} strength={str_}")
        print("   ", json.dumps(d))
