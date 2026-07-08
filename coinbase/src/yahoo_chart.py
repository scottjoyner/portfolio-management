"""Fetch Yahoo Finance chart data via direct REST API (no yfinance dependency).

Bypasses yfinance's broken cookie/crumb mechanism by using the public
chart API with a proper User-Agent header.
"""

from typing import List, Optional
import time
import requests

_SESSION: Optional[requests.Session] = None
_LAST_FETCH: float = 0.0
_MIN_INTERVAL: float = 0.5  # 500ms between requests to avoid rate limiting


def _session() -> requests.Session:
    global _SESSION
    if _SESSION is None:
        _SESSION = requests.Session()
        _SESSION.headers.update({
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            ),
        })
        _SESSION.headers.update({
            "Accept": "application/json",
            "Accept-Language": "en-US,en;q=0.9",
        })
    return _SESSION


def fetch_closes(
    symbol: str,
    period: str = "5d",
    interval: str = "1d",
) -> List[float]:
    """Fetch close prices for a Yahoo Finance ticker via the chart API.

    Args:
        symbol: Yahoo Finance ticker (e.g. 'SPY', '^VIX', 'BTC-USD')
        period: Time range ('1d', '5d', '1mo', '3mo', '6mo', '1y', '2y', '5y', '10y', 'ytd', 'max')
        interval: Bar interval ('1m', '2m', '5m', '15m', '30m', '60m', '1d', '5d', '1wk', '1mo', '3mo')

    Returns:
        List of close prices, newest last. Empty list on failure.
    """
    global _LAST_FETCH

    now = time.time()
    since_last = now - _LAST_FETCH
    if since_last < _MIN_INTERVAL:
        time.sleep(_MIN_INTERVAL - since_last)

    url = (
        f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"
        f"?range={period}&interval={interval}"
    )

    try:
        resp = _session().get(url, timeout=15)
        _LAST_FETCH = time.time()

        if resp.status_code != 200:
            return []

        data = resp.json()
        result = data.get("chart", {}).get("result")
        if not result:
            return []

        quotes = result[0].get("indicators", {}).get("quote", [])
        if not quotes:
            return []

        closes = [c for c in quotes[0].get("close", []) if c is not None]
        return closes
    except (requests.RequestException, ValueError, KeyError, IndexError):
        return []


def fetch_multiple(
    symbols: List[str],
    period: str = "5d",
    interval: str = "1d",
) -> dict:
    """Fetch close prices for multiple symbols (sequential, rate-limited)."""
    out = {}
    for sym in symbols:
        closes = fetch_closes(sym, period, interval)
        if closes:
            out[sym] = closes
    return out
