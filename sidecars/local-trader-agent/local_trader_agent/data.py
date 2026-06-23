from __future__ import annotations

from datetime import date
from typing import Optional

import pandas as pd


REQUIRED_COLUMNS = ["Open", "High", "Low", "Close"]
OPTIONAL_COLUMNS = ["Adj Close", "Volume"]


def _drop_single_ticker_level(df: pd.DataFrame, ticker: str) -> pd.DataFrame:
    """Flatten common yfinance MultiIndex layouts for single-ticker downloads."""
    if not isinstance(df.columns, pd.MultiIndex):
        return df

    columns = df.columns
    # yfinance commonly returns (Price, Ticker), but historical versions and
    # multi-ticker calls can return (Ticker, Price). Handle both.
    for level in range(columns.nlevels):
        vals = [str(v).upper() for v in columns.get_level_values(level)]
        unique_vals = set(vals)
        if unique_vals == {ticker.upper()} or unique_vals == {ticker.replace(".", "-").upper()}:
            return df.droplevel(level, axis=1)

    # If there is only one unique value in any level, drop it as a defensive
    # flattening step. This avoids many single-ticker MultiIndex quirks.
    for level in range(columns.nlevels):
        if len(set(columns.get_level_values(level))) == 1:
            return df.droplevel(level, axis=1)

    # Last resort: join levels into a flat name so the error message is clear.
    out = df.copy()
    out.columns = ["_".join(str(part) for part in tup if str(part)) for tup in out.columns]
    return out


def normalize_ohlcv(df: pd.DataFrame, ticker: str = "") -> pd.DataFrame:
    """Normalize an OHLCV dataframe into Open/High/Low/Close/Volume columns."""
    if df.empty:
        raise ValueError("No price data returned")
    df = _drop_single_ticker_level(df, ticker) if ticker else df.copy()
    df = df.copy()

    # Strip whitespace and normalize case without losing canonical labels.
    rename = {col: str(col).strip() for col in df.columns}
    df.rename(columns=rename, inplace=True)

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


def fetch_ohlcv(ticker: str, start: str, end: Optional[str] = None, interval: str = "1d") -> pd.DataFrame:
    """Fetch OHLCV data from yfinance and normalize it.

    yfinance is intended for research/personal use. For production finance work,
    replace this provider with a licensed market-data source.
    """
    try:
        import yfinance as yf
    except ImportError as exc:
        raise RuntimeError("yfinance is not installed. Run: pip install yfinance") from exc

    kwargs = {
        "tickers": ticker,
        "start": start,
        "end": end or date.today().isoformat(),
        "interval": interval,
        "progress": False,
        "auto_adjust": False,
        "multi_level_index": False,
    }
    try:
        df = yf.download(**kwargs)
    except TypeError:
        # Older yfinance versions do not accept multi_level_index.
        kwargs.pop("multi_level_index", None)
        df = yf.download(**kwargs)
    return normalize_ohlcv(df, ticker=ticker)
