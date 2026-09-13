#!/usr/bin/env python3
"""Scan Coinbase strategy signals on live 30-day data.

Outputs JSON with only the signals that pass the 30-day paper-test gate.
The score incorporates market direction sentiment so the signal is weighted
with the current trend before it is promoted to an opportunity.
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import os
import subprocess
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from pathlib import Path
from statistics import mean, pstdev
from typing import Any
from urllib.error import URLError, HTTPError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from strategy_engine import run_strategies, backtest_strategy  # noqa: E402
from trading_system.signal_confidence import ConfidenceEngine  # noqa: E402


DEFAULT_PRODUCTS = ["BTC-USD", "ETH-USD", "SOL-USD", "ADA-USD", "DOT-USD", "MATIC-USD", "AVAX-USD", "LINK-USD"]
SAFE_BASES = {"BTC", "ETH"}
COINGECKO_IDS = {
    "BTC": "bitcoin",
    "ETH": "ethereum",
    "SOL": "solana",
    "ADA": "cardano",
    "DOT": "polkadot",
    "MATIC": "polygon-pos",
    "POL": "polygon-pos",
    "AVAX": "avalanche-2",
    "LINK": "chainlink",
    "XRP": "ripple",
    "DOGE": "dogecoin",
    "LTC": "litecoin",
    "BCH": "bitcoin-cash",
    "ATOM": "cosmos",
    "UNI": "uniswap",
    "ETC": "ethereum-classic",
    "NEAR": "near",
    "SUI": "sui",
    "APT": "aptos",
    "OP": "optimism",
    "ARB": "arbitrum",
    "AVA": "avalanche-2",
}
COINGECKO_HOST = "https://api.coingecko.com/api/v3"
COINBASE_PRODUCTS_URL = "https://api.exchange.coinbase.com/products"
CACHE_FILE = ROOT / "data" / "strategy_signal_cache.json"
_CACHE_LOCK = threading.Lock()
_CACHE_STATE: dict[str, Any] | None = None


@dataclass
class CandleSeries:
    product_id: str
    source: str
    candles: list[dict[str, float]]


def _clamp(value: float, lo: float = -1.0, hi: float = 1.0) -> float:
    return max(lo, min(hi, value))


def _base_symbol(product_id: str) -> str:
    return product_id.split("-")[0].upper()


def _quote_symbol(product_id: str) -> str:
    parts = product_id.split("-")
    return parts[1].upper() if len(parts) > 1 else ""


def _asset_class(product_id: str) -> str:
    base = _base_symbol(product_id)
    if base in SAFE_BASES:
        return "safe"
    return "growth"


def _load_historical_csv(product_id: str) -> list[dict[str, float]]:
    candidates = [
        ROOT / "data" / "historical" / f"{product_id}_daily.csv",
        ROOT / "data" / f"{product_id.lower().replace('-', '_')}.csv",
    ]
    for path in candidates:
        if not path.exists():
            continue
        rows: list[dict[str, float]] = []
        with path.open(newline="") as fh:
            reader = csv.DictReader(fh)
            for row in reader:
                try:
                    rows.append({
                        "open": float(row.get("open", 0) or 0),
                        "high": float(row.get("high", 0) or 0),
                        "low": float(row.get("low", 0) or 0),
                        "close": float(row.get("close", 0) or 0),
                        "volume": float(row.get("volume", 0) or 0),
                    })
                except Exception:
                    continue
        if rows:
            return rows
    return []


def _load_cache() -> dict[str, Any]:
    global _CACHE_STATE
    with _CACHE_LOCK:
        if _CACHE_STATE is not None:
            return _CACHE_STATE
        if not CACHE_FILE.exists():
            _CACHE_STATE = {"products": {"saved_at": 0, "value": []}, "candles": {}}
            return _CACHE_STATE
        try:
            _CACHE_STATE = json.loads(CACHE_FILE.read_text())
        except Exception:
            _CACHE_STATE = {"products": {"saved_at": 0, "value": []}, "candles": {}}
        _CACHE_STATE.setdefault("products", {"saved_at": 0, "value": []})
        _CACHE_STATE.setdefault("candles", {})
        return _CACHE_STATE


def _save_cache() -> None:
    with _CACHE_LOCK:
        CACHE_FILE.parent.mkdir(parents=True, exist_ok=True)
        state = _CACHE_STATE or {"products": {"saved_at": 0, "value": []}, "candles": {}}
        CACHE_FILE.write_text(json.dumps(state, indent=2, sort_keys=True))


def _cache_get(section: str, key: str, ttl_seconds: int) -> Any | None:
    state = _load_cache()
    bucket = state.get(section, {})
    item = bucket.get(key)
    if not item:
        return None
    if time.time() - float(item.get("saved_at", 0)) > ttl_seconds:
        return None
    return item.get("value")


def _cache_put(section: str, key: str, value: Any) -> None:
    state = _load_cache()
    bucket = state.setdefault(section, {})
    bucket[key] = {"saved_at": time.time(), "value": value}
    _save_cache()


def _load_coinbase_products(cache_ttl_seconds: int = 6 * 3600, refresh: bool = False) -> list[str]:
    if not refresh:
        cached = _cache_get("products", "coinbase_products", cache_ttl_seconds)
        if isinstance(cached, list) and cached:
            return [str(item).upper() for item in cached if item]
    try:
        req = Request(COINBASE_PRODUCTS_URL, headers={"User-Agent": "PortfolioOptimizer/1.0", "Accept": "application/json"})
        with urlopen(req, timeout=30) as resp:
            payload = json.loads(resp.read().decode())
    except Exception:
        return DEFAULT_PRODUCTS.copy()

    products: list[str] = []
    for row in payload if isinstance(payload, list) else []:
        if not isinstance(row, dict):
            continue
        product_id = str(row.get("id") or row.get("product_id") or "").strip().upper()
        if not product_id:
            continue
        if row.get("trading_disabled") or str(row.get("status") or "").lower() not in {"", "online", "active"}:
            continue
        if row.get("quote_currency") and str(row.get("quote_currency")).upper() not in {"USD", "USDC", "USDT", "DAI", "BTC", "ETH"}:
            continue
        products.append(product_id)
    result = sorted(set(products)) or DEFAULT_PRODUCTS.copy()
    _cache_put("products", "coinbase_products", result)
    return result


def _filter_by_quote_currencies(product_ids: list[str], quote_currencies: list[str] | None) -> list[str]:
    if not quote_currencies:
        return product_ids
    allowed = {str(quote).upper() for quote in quote_currencies if str(quote).strip()}
    if not allowed:
        return product_ids
    return [product_id for product_id in product_ids if _quote_symbol(product_id) in allowed]


def _strategy_family(strategy_name: str) -> str:
    name = str(strategy_name or "").lower()
    if any(token in name for token in ("rsi", "zscore", "williams", "scci")):
        return "mean_reversion"
    if any(token in name for token in ("ema", "hma", "psar", "aroon", "price_eff")):
        return "trend"
    if any(token in name for token in ("boll", "donchian", "keltner", "range_exp")):
        return "breakout"
    if any(token in name for token in ("vol_mom", "force_idx", "vpt", "obv", "cmf", "volume")):
        return "momentum"
    return "other"


def _family_profile(strategy_name: str) -> dict[str, float | str]:
    family = _strategy_family(strategy_name)
    profiles = {
        "mean_reversion": {"stop_pct": 0.035, "take_profit_r": 1.55, "trail_pct": 0.0, "holding_bias": "short"},
        "breakout": {"stop_pct": 0.045, "take_profit_r": 2.25, "trail_pct": 0.02, "holding_bias": "medium"},
        "trend": {"stop_pct": 0.05, "take_profit_r": 2.75, "trail_pct": 0.025, "holding_bias": "long"},
        "momentum": {"stop_pct": 0.04, "take_profit_r": 2.0, "trail_pct": 0.015, "holding_bias": "medium"},
        "other": {"stop_pct": 0.04, "take_profit_r": 1.8, "trail_pct": 0.0, "holding_bias": "medium"},
    }
    profile = profiles[family].copy()
    profile["family"] = family
    return profile


def _volatility_floor(closes: list[float], highs: list[float], lows: list[float], current_price: float) -> float:
    window = min(20, len(closes), len(highs), len(lows))
    if window < 3 or current_price <= 0:
        return 0.04
    range_pct = (max(highs[-window:]) - min(lows[-window:])) / current_price
    return max(0.01, min(0.12, range_pct * 0.5))


def _build_trade_plan(signal: Any, verdict: Any, current_price: float, closes: list[float], highs: list[float], lows: list[float], sentiment_score: float) -> dict[str, Any]:
    profile = _family_profile(str(getattr(signal, "strategy", "")))
    action = str(getattr(signal, "action", "BUY")).upper()
    family = str(profile["family"])
    base_stop_pct = max(float(profile["stop_pct"]), _volatility_floor(closes, highs, lows, current_price))
    target_r = float(profile["take_profit_r"])

    if getattr(verdict, "win_rate", 0.0) >= 0.65:
        target_r += 0.25
    if getattr(verdict, "profit_factor", 0.0) >= 1.5:
        target_r += 0.15
    if getattr(verdict, "sharpe_ratio", 0.0) < 0:
        target_r = max(1.1, target_r - 0.2)

    entry_price = float(current_price)
    if action == "SELL":
        stop_loss_price = round(entry_price * (1 + base_stop_pct), 10)
        take_profit_price = round(entry_price * (1 - base_stop_pct * target_r), 10)
        plan_type = "exit"
        position_side = "long"
        execution_purpose = "take_profit_exit"
    else:
        stop_loss_price = round(entry_price * (1 - base_stop_pct), 10)
        take_profit_price = round(entry_price * (1 + base_stop_pct * target_r), 10)
        plan_type = "entry"
        position_side = "long"
        execution_purpose = "open_long"

    reward = abs(take_profit_price - entry_price)
    risk = max(1e-9, abs(entry_price - stop_loss_price))

    return {
        "family": family,
        "plan_type": plan_type,
        "position_side": position_side,
        "execution_purpose": execution_purpose,
        "entry_price": round(entry_price, 10),
        "stop_loss_price": stop_loss_price,
        "take_profit_price": take_profit_price,
        "stop_loss_pct": round(base_stop_pct, 4),
        "take_profit_pct": round(abs(take_profit_price - entry_price) / max(entry_price, 1e-9), 4),
        "risk_reward_ratio": round(reward / risk, 4),
        "holding_bias": profile["holding_bias"],
        "trail_pct": round(float(profile["trail_pct"]), 4),
        "sentiment_bias": round(float(sentiment_score), 4),
    }


def _fetch_coingecko_history(base_symbol: str, days_back: int, cache_ttl_seconds: int = 900, refresh: bool = False) -> CandleSeries:
    cg_id = COINGECKO_IDS.get(base_symbol.upper())
    if not cg_id:
        return CandleSeries(product_id=f"{base_symbol}-USD", source="coingecko_unavailable", candles=[])
    cache_key = f"cg:{cg_id}:{days_back}"
    if not refresh:
        cached = _cache_get("candles", cache_key, cache_ttl_seconds)
        if isinstance(cached, dict) and cached.get("candles"):
            return CandleSeries(product_id=str(cached.get("product_id") or f"{base_symbol}-USD"), source=str(cached.get("source") or "coingecko"), candles=cached.get("candles", []))
    to_ts = int(datetime.now(timezone.utc).timestamp())
    from_ts = int((datetime.now(timezone.utc) - timedelta(days=days_back)).timestamp())
    query = urlencode({"vs_currency": "usd", "from": from_ts, "to": to_ts})
    url = f"{COINGECKO_HOST}/coins/{cg_id}/market_chart/range?{query}"
    try:
        req = Request(url, headers={"User-Agent": "PortfolioOptimizer/1.0", "Accept": "application/json"})
        with urlopen(req, timeout=30) as resp:
            payload = json.loads(resp.read().decode())
    except (HTTPError, URLError, TimeoutError, ValueError):
        return CandleSeries(product_id=f"{base_symbol}-USD", source="coingecko_unavailable", candles=[])

    prices = payload.get("prices", []) if isinstance(payload, dict) else []
    if not prices:
        return CandleSeries(product_id=f"{base_symbol}-USD", source="coingecko_unavailable", candles=[])

    candles: list[dict[str, float]] = []
    window = max(1, min(12, len(prices) // 120 or 1))
    for idx in range(0, len(prices), window):
        chunk = prices[idx: idx + window]
        if not chunk:
            continue
        closes = [float(row[1]) for row in chunk if len(row) >= 2]
        if not closes:
            continue
        open_p = closes[0]
        close_p = closes[-1]
        high_p = max(closes)
        low_p = min(closes)
        avg_vol = 0.0
        candles.append({
            "open": open_p,
            "high": high_p,
            "low": low_p,
            "close": close_p,
            "volume": avg_vol,
        })
    series = CandleSeries(product_id=f"{base_symbol}-USD", source="coingecko", candles=candles)
    _cache_put("candles", cache_key, {"product_id": series.product_id, "source": series.source, "candles": series.candles})
    return series


def _fetch_live_candles(product_id: str, granularity: str, days_back: int, cache_ttl_seconds: int = 900, refresh: bool = False) -> CandleSeries:
    cache_key = f"cb:{product_id}:{granularity}:{days_back}"
    if not refresh:
        cached = _cache_get("candles", cache_key, cache_ttl_seconds)
        if isinstance(cached, dict) and cached.get("candles"):
            return CandleSeries(product_id=str(cached.get("product_id") or product_id), source=str(cached.get("source") or "cache"), candles=cached.get("candles", []))
    cli_path = os.environ.get("COINBASE_CLI_PATH", "coinbase")
    start = (datetime.now(timezone.utc) - timedelta(days=days_back)).strftime("%Y-%m-%dT%H:%M:%SZ") if days_back > 0 else None
    result = subprocess.run(
        [
            cli_path,
            "products",
            "candles",
            product_id,
            f"granularity=={granularity}",
            f"start=={start}",
        ],
        capture_output=True,
        text=True,
        timeout=60,
    )
    if result.returncode != 0:
        base = _base_symbol(product_id)
        cg_series = _fetch_coingecko_history(base, days_back, cache_ttl_seconds=cache_ttl_seconds, refresh=refresh)
        return cg_series if cg_series.candles else CandleSeries(product_id=product_id, source="historical_fallback", candles=_load_historical_csv(product_id))

    try:
        payload = json.loads(result.stdout)
    except Exception:
        base = _base_symbol(product_id)
        cg_series = _fetch_coingecko_history(base, days_back, cache_ttl_seconds=cache_ttl_seconds, refresh=refresh)
        return cg_series if cg_series.candles else CandleSeries(product_id=product_id, source="historical_fallback", candles=_load_historical_csv(product_id))

    raw = payload.get("candles", []) if isinstance(payload, dict) else payload
    candles: list[dict[str, float]] = []
    for row in raw:
        try:
            candles.append({
                "open": float(row.get("open", 0) or 0),
                "high": float(row.get("high", 0) or 0),
                "low": float(row.get("low", 0) or 0),
                "close": float(row.get("close", 0) or 0),
                "volume": float(row.get("volume", 0) or 0),
            })
        except Exception:
            continue
    if candles:
        series = CandleSeries(product_id=product_id, source="live_cli", candles=candles)
        _cache_put("candles", cache_key, {"product_id": series.product_id, "source": series.source, "candles": series.candles})
        return series
    base = _base_symbol(product_id)
    cg_series = _fetch_coingecko_history(base, days_back, cache_ttl_seconds=cache_ttl_seconds, refresh=refresh)
    return cg_series if cg_series.candles else CandleSeries(product_id=product_id, source="historical_fallback", candles=_load_historical_csv(product_id))


def _market_sentiment(closes: list[float]) -> float:
    if len(closes) < 2:
        return 0.0
    lookback = min(len(closes) - 1, 24 * 7)
    start = closes[-lookback - 1]
    if start <= 0:
        return 0.0
    trend = (closes[-1] - start) / start
    return _clamp(trend * 3.0)


def _global_consensus(signals: list[Any]) -> float:
    if not signals:
        return 0.0
    buys = sum(1 for sig in signals if getattr(sig, "action", "") == "BUY")
    sells = sum(1 for sig in signals if getattr(sig, "action", "") == "SELL")
    return max(buys, sells) / len(signals)


def _regime(closes: list[float]) -> str:
    if len(closes) < 10:
        return "neutral"
    returns = []
    for idx in range(1, len(closes)):
        prev = closes[idx - 1]
        if prev > 0:
            returns.append((closes[idx] - prev) / prev)
    if not returns:
        return "neutral"
    vol = pstdev(returns[-min(48, len(returns)):]) if len(returns) > 1 else 0.0
    trend = _market_sentiment(closes)
    if vol > 0.03:
        return "volatile"
    if abs(trend) > 0.18:
        return "trending"
    return "neutral"


def scan_product(product_id: str, granularity: str, days_back: int, min_win_rate: float, min_weighted_confidence: float, cache_ttl_seconds: int = 900, refresh: bool = False) -> list[dict[str, Any]]:
    series = _fetch_live_candles(product_id, granularity, days_back, cache_ttl_seconds=cache_ttl_seconds, refresh=refresh)
    closes = [row["close"] for row in series.candles if row.get("close", 0) > 0]
    volumes = [row.get("volume", 0.0) for row in series.candles]
    highs = [row.get("high", 0.0) for row in series.candles]
    lows = [row.get("low", 0.0) for row in series.candles]
    if len(closes) < 40:
        return []

    current_price = closes[-1]
    asset_class = _asset_class(product_id)
    all_signals = run_strategies(product_id, asset_class, closes, volumes, current_price, highs=highs, lows=lows)
    consensus = _global_consensus(all_signals)
    sentiment_score = _market_sentiment(closes)
    regime = _regime(closes)
    engine = ConfidenceEngine(regime_caps={"volatile": 0.5, "neutral": 0.8, "trending": 1.0})

    results: list[dict[str, Any]] = []

    def evaluate(signal: Any) -> dict[str, Any] | None:
        verdict = backtest_strategy(signal.strategy, product_id, closes, volumes, highs=highs, lows=lows)
        if verdict.total_trades < 3 or verdict.win_rate < min_win_rate:
            return None

        strength = max(0.05, min(1.0, float(getattr(signal, "confidence", 0.5))))
        trade_intent = "exit" if str(getattr(signal, "action", "BUY")).upper() == "SELL" else "entry"
        stub = type("SignalStub", (), {})()
        stub.symbol = product_id
        stub.action = signal.action
        stub.strategy = signal.strategy
        stub.strength = strength
        mod = engine.apply_modifiers(
            stub,
            {"spread": 0.0, "volume_24h": sum(volumes[-24:]) if volumes else 0.0, "change_pct": sentiment_score},
            regime=regime,
            market_leaders=["BTC-USD"] if product_id != "BTC-USD" else None,
            sentiment_score=sentiment_score,
            global_consensus=consensus,
        )
        weighted_confidence = float(mod.modified_confidence)
        if weighted_confidence < min_weighted_confidence:
            return None
        score = weighted_confidence * verdict.win_rate
        profit_score = max(0.0, float(verdict.total_return_pct)) * weighted_confidence * max(0.01, verdict.win_rate) / 100.0
        trade_plan = _build_trade_plan(signal, verdict, current_price, closes, highs, lows, sentiment_score)
        return {
            "product_id": product_id,
            "symbol": product_id,
            "base_symbol": _base_symbol(product_id),
            "strategy": signal.strategy,
            "action": signal.action,
            "trade_intent": trade_intent,
            "trade_plan": trade_plan,
            "take_profit_price": trade_plan["take_profit_price"],
            "stop_loss_price": trade_plan["stop_loss_price"],
            "entry_price": trade_plan["entry_price"],
            "execution_purpose": trade_plan["execution_purpose"],
            "price": current_price,
            "win_rate": round(verdict.win_rate, 4),
            "total_trades": int(verdict.total_trades),
            "sentiment_score": round(sentiment_score, 4),
            "consensus": round(consensus, 4),
            "regime": regime,
            "raw_confidence": round(strength, 4),
            "weighted_confidence": round(weighted_confidence, 4),
            "score": round(score, 4),
            "profit_score": round(profit_score, 6),
            "source": series.source,
            "reason": signal.reason,
            "backtest_reason": verdict.reason,
            "backtest_total_return_pct": round(verdict.total_return_pct, 4),
            "backtest_sharpe": round(verdict.sharpe_ratio, 4),
            "backtest_profit_factor": round(verdict.profit_factor, 4),
            "backtest_max_drawdown_pct": round(verdict.max_drawdown_pct, 4),
            "candles": len(closes),
            "market_direction": "bullish" if sentiment_score >= 0 else "bearish",
        }

    if all_signals:
        with ThreadPoolExecutor(max_workers=min(8, len(all_signals))) as pool:
            futures = [pool.submit(evaluate, signal) for signal in all_signals]
            for fut in as_completed(futures):
                candidate = fut.result()
                if candidate is not None:
                    results.append(candidate)

    results.sort(key=lambda item: (item["score"], item["win_rate"], item["weighted_confidence"]), reverse=True)
    return results


def main() -> None:
    parser = argparse.ArgumentParser(description="Scan live Coinbase strategy signals")
    parser.add_argument("--products", default="", help="Comma-separated Coinbase products; empty means discover all Coinbase pairs")
    parser.add_argument("--discover", action="store_true", help="Discover all Coinbase products via public API")
    parser.add_argument("--quote-currencies", default="USD,BTC", help="Comma-separated quote currencies to keep after discovery")
    parser.add_argument("--granularity", default="ONE_HOUR", help="Coinbase candle granularity")
    parser.add_argument("--days-back", type=int, default=30, help="Lookback window in days")
    parser.add_argument("--min-win-rate", type=float, default=0.55, help="Minimum 30d win rate")
    parser.add_argument("--min-weighted-confidence", type=float, default=0.55, help="Minimum sentiment-weighted confidence")
    parser.add_argument("--limit", type=int, default=10, help="Max signals to return")
    parser.add_argument("--cache-ttl", type=int, default=900, help="Cache TTL in seconds for candle data")
    parser.add_argument("--products-cache-ttl", type=int, default=21600, help="Cache TTL in seconds for product discovery")
    parser.add_argument("--refresh", action="store_true", help="Bypass caches and refetch remote data")
    args = parser.parse_args()

    started = time.time()
    candidates: list[dict[str, Any]] = []
    product_ids = [p.strip().upper() for p in args.products.split(",") if p.strip()]
    if args.discover or not product_ids:
        discovered = _load_coinbase_products(cache_ttl_seconds=args.products_cache_ttl, refresh=args.refresh)
        if product_ids:
            product_ids = sorted(set(product_ids + discovered))
        else:
            product_ids = discovered
    quote_currencies = [q.strip().upper() for q in args.quote_currencies.split(",") if q.strip()]
    product_ids = _filter_by_quote_currencies(product_ids, quote_currencies)
    if product_ids:
        cpu_workers = max(8, (os.cpu_count() or 4) * 4)
        with ThreadPoolExecutor(max_workers=min(cpu_workers, len(product_ids))) as pool:
            futures = {
                pool.submit(scan_product, product_id, args.granularity, args.days_back, args.min_win_rate, args.min_weighted_confidence, args.cache_ttl, args.refresh): product_id
                for product_id in product_ids
            }
            for fut in as_completed(futures):
                product_id = futures[fut]
                try:
                    candidates.extend(fut.result())
                except Exception as exc:
                    candidates.append({"product_id": product_id, "error": str(exc)})

    eligible = [c for c in candidates if "error" not in c]
    entry_signals = [item for item in eligible if item.get("trade_intent") == "entry"]
    exit_signals = [item for item in eligible if item.get("trade_intent") == "exit"]
    entry_signals.sort(key=lambda item: (item["profit_score"], item["score"], item["win_rate"], item["weighted_confidence"]), reverse=True)
    exit_signals.sort(key=lambda item: (item["profit_score"], item["score"], item["win_rate"], item["weighted_confidence"]), reverse=True)
    eligible = sorted(eligible, key=lambda item: (item["profit_score"], item["score"], item["win_rate"], item["weighted_confidence"]), reverse=True)
    payload = {
        "ok": True,
        "elapsed_seconds": round(time.time() - started, 3),
        "workers": min(cpu_workers, len(product_ids)) if product_ids else 0,
        "products_scanned": len(product_ids),
        "window_days": args.days_back,
        "granularity": args.granularity,
        "min_win_rate": args.min_win_rate,
        "min_weighted_confidence": args.min_weighted_confidence,
        "entry_count": len(entry_signals),
        "exit_count": len(exit_signals),
        "count": len(eligible[: args.limit]),
        "signals": eligible[: args.limit],
        "entry_signals": entry_signals[: args.limit],
        "exit_signals": exit_signals[: args.limit],
        "errors": [c for c in candidates if "error" in c],
    }
    print(json.dumps(payload, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
