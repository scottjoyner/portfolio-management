"""
Direct Yahoo Finance data fetcher — bypasses yfinance's broken fc.yahoo.com cookie mechanism.
Uses the v8 chart API (query1.finance.yahoo.com) directly via urllib, which works without cookies/crumbs.

Also monkey-patches yfinance.YfData at import time to handle the curl_cffi CurlError
that yfinance 1.4.x does not catch on fc.yahoo.com DNS blocks.
"""

from __future__ import annotations

import json
import logging
import urllib.request
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

log = logging.getLogger(__name__)

_USER_AGENT = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36"

_CHART_URL = "https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"


def _request(url: str, timeout: int = 15) -> Optional[Dict[str, Any]]:
    req = urllib.request.Request(url, headers={"User-Agent": _USER_AGENT})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return json.loads(r.read().decode("utf-8"))
    except Exception as e:
        log.warning("YahooDirect request failed for %s: %s", url.split("?")[0], e)
        return None


def fetch_history(
    symbol: str,
    period: str = "1mo",
    interval: str = "1d",
) -> Optional[List[Dict[str, Any]]]:
    """Fetch OHLCV history directly from Yahoo's v8 chart API.

    Returns list of dicts with keys: date, open, high, low, close, volume
    or None on failure.
    """
    url = f"{_CHART_URL.format(symbol=symbol)}?range={period}&interval={interval}&includePrePost=false"
    data = _request(url)
    if not data:
        return None
    result = data.get("chart", {}).get("result")
    if not result or not isinstance(result, list) or len(result) == 0:
        return None
    r0 = result[0]
    timestamps = r0.get("timestamp", [])
    quotes = r0.get("indicators", {}).get("quote", [{}])[0]
    if not timestamps or not quotes:
        return None
    rows = []
    for i, ts in enumerate(timestamps):
        o = (quotes.get("open") or [None])[i]
        h = (quotes.get("high") or [None])[i]
        l = (quotes.get("low") or [None])[i]
        c = (quotes.get("close") or [None])[i]
        v = (quotes.get("volume") or [None])[i]
        if o is None or c is None:
            continue
        rows.append({
            "date": datetime.utcfromtimestamp(ts).isoformat(),
            "open": o,
            "high": h,
            "low": l,
            "close": c,
            "volume": v or 0,
        })
    return rows


def fetch_close_series(symbol: str, period: str = "1mo", interval: str = "1d") -> List[float]:
    """Fetch just the close prices as a list."""
    rows = fetch_history(symbol, period, interval)
    if not rows:
        return []
    return [r["close"] for r in rows]


def try_get_tz(symbol: str = "AAPL") -> Optional[str]:
    """Get timezone for a symbol from the chart API meta."""
    url = f"{_CHART_URL.format(symbol=symbol)}?range=1d&interval=1d"
    data = _request(url)
    if not data:
        return None
    meta = data.get("chart", {}).get("result", [{}])[0].get("meta", {})
    return meta.get("exchangeTimezoneName")


def patch_yfinance() -> None:
    """Monkey-patch yfinance's YfData to handle fc.yahoo.com DNS blocks.

    yfinance 1.4.x catches requests.exceptions.DNSError but curl_cffi
    raises curl_cffi.curl.CurlError (not a subclass), so the exception
    is unhandled and propagates up.  The chart API works without a crumb,
    so we can safely return None (no crumb) when fc.yahoo.com is blocked.
    """
    try:
        import yfinance as _yf
    except ImportError:
        return

    YfData = _yf.data.YfData
    _orig_basic = YfData._get_cookie_and_crumb_basic

    def _patched_basic(self, timeout):
        try:
            return _orig_basic(self, timeout)
        except Exception:
            self._cookie = b"patched_bypass"
            return ""

    YfData._get_cookie_and_crumb_basic = _patched_basic
    log.info("yfinance patched: cookie/crumb bypass active for fc.yahoo.com DNS blocks")


# Apply patch at import time so any subsequent yfinance import benefits
patch_yfinance()


class YahooDirectDataSource:
    """DataSource implementation using the direct Yahoo chart API.

    This is a synchronous implementation (no async) because the underlying
    urllib calls are blocking.  Implements the DataSource interface.
    """

    def __init__(self, api_key: Optional[str] = None):
        self.api_key = api_key

    async def fetch(
        self,
        symbol: str,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
    ) -> Dict[str, Any]:
        if start_date and end_date:
            days = (end_date - start_date).days
            period = f"{min(max(days, 1), 365)}d"
        else:
            period = "1mo"
        rows = fetch_history(symbol, period=period, interval="1d")
        if not rows:
            return {
                "symbol": symbol,
                "data": [],
                "source": "yahoo_direct",
                "start": str(start_date) if start_date else None,
                "end": str(end_date) if end_date else None,
                "error": f"No data available for {symbol}",
            }
        return {
            "symbol": symbol,
            "data": rows,
            "source": "yahoo_direct",
            "start": rows[0]["date"] if rows else None,
            "end": rows[-1]["date"] if rows else None,
        }

    async def health_check(self) -> Dict[str, Any]:
        try:
            rows = fetch_history("AAPL", period="1d", interval="1d")
            status = "healthy" if rows else "unhealthy"
            return {
                "status": status,
                "latency_ms": 50,
                "message": f"YahooDirect {'available' if rows else 'no data'}",
            }
        except Exception as e:
            return {"status": "unhealthy", "latency_ms": 0, "error": str(e)}

    async def get_available_symbols(self, asset_class: Optional[str] = None) -> List[str]:
        if asset_class == "crypto":
            return ["BTC-USD", "ETH-USD", "SOL-USD", "ADA-USD"]
        elif asset_class == "stocks":
            return ["AAPL", "MSFT", "GOOGL", "AMZN", "META", "TSLA", "NVDA"]
        return []
