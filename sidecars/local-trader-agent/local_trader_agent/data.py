from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Optional

import pandas as pd


REQUIRED_COLUMNS = ["Open", "High", "Low", "Close"]
OPTIONAL_COLUMNS = ["Adj Close", "Volume"]


def _drop_single_ticker_level(df: pd.DataFrame, ticker: str) -> pd.DataFrame:
    if not isinstance(df.columns, pd.MultiIndex):
        return df
    columns = df.columns
    for level in range(columns.nlevels):
        vals = [str(v).upper() for v in columns.get_level_values(level)]
        unique_vals = set(vals)
        if unique_vals == {ticker.upper()} or unique_vals == {ticker.replace(".", "-").upper()}:
            return df.droplevel(level, axis=1)
    for level in range(columns.nlevels):
        if len(set(columns.get_level_values(level))) == 1:
            return df.droplevel(level, axis=1)
    out = df.copy()
    out.columns = ["_".join(str(part) for part in tup if str(part)) for tup in out.columns]
    return out


def normalize_ohlcv(df: pd.DataFrame, ticker: str = "") -> pd.DataFrame:
    if df.empty:
        raise ValueError("No price data returned")
    df = _drop_single_ticker_level(df, ticker) if ticker else df.copy()
    df = df.copy()
    df.rename(columns={col: str(col).strip() for col in df.columns}, inplace=True)
    title_map = {str(col).lower().replace(" ", ""): col for col in df.columns}
    canonical = {}
    for wanted in REQUIRED_COLUMNS + OPTIONAL_COLUMNS:
        key = wanted.lower().replace(" ", "")
        if key in title_map:
            canonical[title_map[key]] = wanted
    df.rename(columns=canonical, inplace=True)
    missing = [c for c in REQUIRED_COLUMNS if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required OHLC columns: {missing}. Columns={list(df.columns)}")
    keep = [c for c in REQUIRED_COLUMNS + OPTIONAL_COLUMNS if c in df.columns]
    df = df[keep]
    for col in keep:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df.dropna(subset=REQUIRED_COLUMNS, inplace=True)
    df.sort_index(inplace=True)
    return df


def _cache_path(cache_dir: str | Path, ticker: str, start: str, end: str, interval: str, auto_adjust: bool) -> Path:
    safe_ticker = ticker.replace("/", "_").replace(".", "-").upper()
    return Path(cache_dir) / safe_ticker / f"{safe_ticker}_{interval}_{start}_{end}_adjusted-{int(auto_adjust)}.csv"


def fetch_ohlcv(
    ticker: str,
    start: str,
    end: Optional[str] = None,
    interval: str = "1d",
    *,
    auto_adjust: bool = False,
    cache: bool = True,
    refresh_cache: bool = False,
    cache_dir: str | Path = "workspace/cache/yfinance",
) -> pd.DataFrame:
    end_value = end or date.today().isoformat()
    path = _cache_path(cache_dir, ticker, start, end_value, interval, auto_adjust)
    if cache and path.exists() and not refresh_cache:
        return normalize_ohlcv(pd.read_csv(path, index_col=0, parse_dates=True), ticker=ticker)

    try:
        import yfinance as yf
    except ImportError as exc:
        raise RuntimeError("yfinance is not installed. Run: pip install yfinance") from exc

    kwargs = {
        "tickers": ticker,
        "start": start,
        "end": end_value,
        "interval": interval,
        "progress": False,
        "auto_adjust": auto_adjust,
        "multi_level_index": False,
    }
    fetcher = getattr(yf, "download")
    try:
        df = fetcher(**kwargs)
    except TypeError:
        kwargs.pop("multi_level_index", None)
        df = fetcher(**kwargs)
    normalized = normalize_ohlcv(df, ticker=ticker)
    if cache:
        path.parent.mkdir(parents=True, exist_ok=True)
        normalized.to_csv(path)
    return normalized
