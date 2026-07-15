"""Shared helpers for strategy coverage tests.

Provides synthetic OHLCV generation and a resilient generic driver that
attempts to instantiate each strategy class and feed it synthetic price
series (rising / falling / volatile / flat) to exercise entry and exit
branches.  Network/SDK access is avoided entirely.
"""
from __future__ import annotations

import inspect
import math
from typing import Any, Callable, Optional, Sequence


# ---------------------------------------------------------------------------
# Synthetic data
# ---------------------------------------------------------------------------

def make_bar(close: float, volume: float = 1000.0, spread: float = 0.01,
             trend: int = 1, i: int = 0) -> dict:
    """Build a synthetic OHLCV bar enriched with a wide set of synthetic
    indicator fields so that strategies which expect pre-computed indicators
    (atr, atr_trend, rsi, macd, bands, vwap, etc.) find plausible values.

    ``trend`` selects the regime bias used to set the indicator values:
      +1 -> bullish (rising), -1 -> bearish (falling), 0 -> neutral.
    """
    if trend > 0:
        rsi = 72.0          # overbought (sell trigger for mean-rev)
        atr_trend = 1
        macd = 0.5
        macd_signal = 0.2
        fast_ema = close * 1.002
        slow_ema = close * 0.998
        upper = close * 1.01
        lower = close * 0.99
        middle = close
        stoch_k = 85.0
        stoch_d = 80.0
        zscore = 2.2
    elif trend < 0:
        rsi = 28.0          # oversold (buy trigger for mean-rev)
        atr_trend = -1
        macd = -0.5
        macd_signal = -0.2
        fast_ema = close * 0.998
        slow_ema = close * 1.002
        upper = close * 1.01
        lower = close * 0.99
        middle = close
        stoch_k = 15.0
        stoch_d = 20.0
        zscore = -2.2
    else:
        rsi = 50.0
        atr_trend = 0
        macd = 0.0
        macd_signal = 0.0
        fast_ema = close
        slow_ema = close
        upper = close * 1.01
        lower = close * 0.99
        middle = close
        stoch_k = 50.0
        stoch_d = 50.0
        zscore = 0.0

    return {
        "open": round(close * (1 - spread / 2), 8),
        "high": round(close * (1 + spread), 8),
        "low": round(close * (1 - spread), 8),
        "close": round(close, 8),
        "price": round(close, 8),
        "volume": volume,
        "timestamp": i,
        "bars_since_start": i + 200,   # satisfy warmup checks
        # Indicator fields many strategies read from the bar/market_state
        "atr": round(close * 0.06, 8),
        "atr_value": round(close * 0.06, 8),
        "atr_trend": atr_trend,
        "rsi": rsi,
        "rsi_value": rsi,
        "macd": macd,
        "macd_line": macd,
        "macd_signal": macd_signal,
        "signal_line": macd_signal,
        "histogram": macd - macd_signal,
        "fast_ema": fast_ema,
        "slow_ema": slow_ema,
        "upper": upper,
        "upper_band": upper,
        "lower": lower,
        "lower_band": lower,
        "middle": middle,
        "middle_band": middle,
        "vwap": round(close, 8),
        "stoch_k": stoch_k,
        "stoch_d": stoch_d,
        "zscore": zscore,
        "trend": trend,
        "trend_strength": 0.6 if trend != 0 else 0.1,
        "sma_fast": fast_ema,
        "sma_slow": slow_ema,
        "plus_di": 25.0 if trend > 0 else 15.0,
        "minus_di": 15.0 if trend > 0 else 25.0,
        "adx": 30.0 if trend != 0 else 10.0,
        "williams_r": -15.0 if trend > 0 else -85.0,
        "signal": macd - macd_signal,
    }


def price_series(n: int, kind: str = "rising", start: float = 100.0) -> list[float]:
    prices: list[float] = []
    p = start
    for i in range(n):
        if kind == "rising":
            p *= 1.01
        elif kind == "falling":
            p *= 0.99
        elif kind == "volatile":
            p *= 1.0 + ((i % 2) * 2 - 1) * 0.04
        elif kind == "flathigh":
            # flat then a strong up move (breakout)
            if i < n // 2:
                p = start
            else:
                p *= 1.03
        elif kind == "flatlow":
            if i < n // 2:
                p = start
            else:
                p *= 0.97
        # flat: do nothing
        prices.append(round(p, 8))
    return prices


def bars(n: int, kind: str = "rising", start: float = 100.0,
         volume: float = 1000.0) -> list[dict]:
    trend = 1
    if kind in ("falling", "flatlow"):
        trend = -1
    elif kind == "flat":
        trend = 0
    return [make_bar(p, volume=volume + i * 5, trend=trend, i=i)
            for i, p in enumerate(price_series(n, kind, start))]


# ---------------------------------------------------------------------------
# Generic discovery / driver
# ---------------------------------------------------------------------------

_CLASS_SUFFIXES = ("Strategy", "Bot", "StrategyV2", "Trader", "Algo")


# Names that are clearly not strategies (configs, positions, state, results).
_NON_STRATEGY = {
    "Signal", "StrategyConfig", "StrategyBase", "BaseStrategy", "OHLCVBar",
    "StrategySignal", "StrategyMetadata", "BacktestResult", "Strategy",
    "Config", "Position", "State", "Result", "Params", "Settings",
}


def discover_strategy_classes(module) -> list[type]:
    """Return classes defined in *module* that look like strategies.

    A class qualifies if it (a) has an ``on_bar`` method, or (b) is a
    (non-abstract) subclass of StrategyBase, or (c) its name ends in a
    recognised strategy suffix.  Config/position/state dataclasses are
    excluded.
    """
    out: list[type] = []
    for name, obj in inspect.getmembers(module, inspect.isclass):
        if obj.__module__ != module.__name__:
            continue
        if name.startswith("_") or name in _NON_STRATEGY:
            continue
        # Exclude dataclasses that only carry config/state (no on_bar)
        has_on_bar = hasattr(obj, "on_bar") and callable(getattr(obj, "on_bar"))
        bases = {b.__name__ for b in inspect.getmro(obj)}
        is_base_sub = "StrategyBase" in bases
        is_suffix = any(name.endswith(s) for s in _CLASS_SUFFIXES)
        # Exclude abstract base classes (they have abstractmethods)
        is_abstract = bool(getattr(obj, "__abstractmethods__", None))
        if is_abstract:
            continue
        if has_on_bar or is_base_sub or is_suffix:
            out.append(obj)
    return out


def _instantiate(cls) -> Optional[Any]:
    """Best-effort instantiation returning an instance or None."""
    # 1. No-arg
    try:
        return cls()
    except Exception:
        pass
    # 2. config=None (factory-style)
    try:
        return cls(config=None)
    except Exception:
        pass
    # 3. First positional config-like: try with a dummy object
    try:
        sig = inspect.signature(cls.__init__)
        params = [p for p in sig.parameters.values()
                  if p.name not in ("self", "args", "kwargs")]
        if params and params[0].default is inspect._empty:
            # requires an arg -> try a bare object
            class _Cfg:
                pass
            try:
                return cls(_Cfg())
            except Exception:
                pass
    except (ValueError, TypeError):
        pass
    return None


def _feed_init(instance, bar_list: list[dict]) -> bool:
    """Call init() best-effort. Return True if it ran without raising."""
    init = getattr(instance, "init", None)
    if init is None:
        return False
    sig = inspect.signature(init)
    pos = [p for p in sig.parameters.values()
           if p.name not in ("self",)]
    # If init wants a single arg, it may accept a list or a dict
    try:
        init(bar_list)
        return True
    except Exception:
        pass
    try:
        init({"bars": bar_list, "data": bar_list, "prices": [b["close"] for b in bar_list]})
        return True
    except Exception:
        pass
    try:
        init(bar_list[0] if bar_list else {})
        return True
    except Exception:
        pass
    return False


def roundtrip_series(n: int = 80, start: float = 100.0) -> list[dict]:
    """Long rise followed by a long fall - triggers entry then exit."""
    prices: list[float] = []
    p = start
    half = n // 2
    for i in range(n):
        if i < half:
            p *= 1.015
            trend = 1
        else:
            p *= 0.985
            trend = -1
        prices.append(round(p, 8))
    return [make_bar(p, volume=1000.0 + i * 5, trend=trend, i=i)
            for i, p in enumerate(prices)]


def drive_class(cls, scenarios: Sequence[str] = ("rising", "falling", "volatile",
                                                  "flathigh", "flatlow", "flat",
                                                  "roundtrip"),
                n: int = 90) -> dict:
    """Instantiate and feed several synthetic scenarios.

    Returns a dict summarizing what happened (for debugging), and captures
    any signal-like return values.
    """
    result: dict[str, Any] = {
        "instantiated": False,
        "init_ok": False,
        "signals": 0,
        "errors": [],
        "returns": [],
    }
    inst = _instantiate(cls)
    if inst is None:
        result["errors"].append("instantiation_failed")
        return result
    result["instantiated"] = True

    # Seed init with a long history so indicators warm up.
    hist = bars(n, "rising", 100.0)
    init_ok = _feed_init(inst, hist)
    result["init_ok"] = init_ok

    on_bar = getattr(inst, "on_bar", None)
    gen_sig = getattr(inst, "generate_signal", None)

    if on_bar is None and gen_sig is None:
        result["errors"].append("no_on_bar")
        return result

    market_states = []
    if on_bar is not None:
        for scenario in scenarios:
            if scenario == "roundtrip":
                blist = roundtrip_series(n)
            else:
                blist = bars(n, scenario, 100.0)
            for bar in blist:
                try:
                    sig = on_bar(bar)
                    if sig is not None:
                        result["signals"] += 1
                        result["returns"].append(sig)
                except Exception as e:  # keep going through the scenario
                    result["errors"].append(f"{scenario}:{type(e).__name__}:{e}")
                    break
            market_states.append(blist[-1])
        # Try finalize if present
        try:
            fin = getattr(inst, "finalize", None)
            if fin is not None:
                fin()
        except Exception:
            pass

    # Drive the generate_signal() interface (Strategy ABC style) too.
    if gen_sig is not None and market_states:
        for ms in market_states:
            try:
                out = gen_sig(ms)
                if out is not None:
                    result["signals"] += 1
            except Exception as e:
                result["errors"].append(f"gensig:{type(e).__name__}:{e}")
                break
        # Also exercise metadata / explain_trade / sizing_hints if present
        for aux in ("metadata", "explain_trade", "sizing_hints",
                    "risk_hints", "required_inputs", "supports_mode",
                    "order_intents", "analytics_tags"):
            fn = getattr(inst, aux, None)
            if callable(fn):
                try:
                    if aux in ("explain_trade", "order_intents"):
                        fn(object())
                    else:
                        fn(ms) if aux in ("sizing_hints", "risk_hints") else fn()
                except Exception:
                    pass
    return result
