"""Kalshi API connector for read operations (markets, order books).

This module provides the Kalshi public API integration without authentication
(private keys not required for market data endpoints).

Usage:
    from trading_system.arbitrage import kalshi_connector
    
    markets = kalshi_connector.fetch_markets(limit=10)
    market_detail = kalshi_connector.get_market('BTC-JAN31-100K')

Invariants:
    - Never trade without pre-validating API keys in .env (not applicable for public endpoints)
    - Always use dry-run mode during development
    - All methods must handle None responses gracefully (rate limiting, maintenance windows)

See also: trading_system/arbitrage/COMPLETE.md for complete documentation.
"""

from typing import Dict, List, Optional, Any

import json
import time
from typing import Dict, List, Optional
from datetime import datetime, timedelta


class KalshiConnectorError(Exception):
    """Base exception for Kalshi API errors."""
    pass


class RateLimitError(KalshiConnectorError):
    """Raised when rate limit is hit (429 status)."""
    pass


class NotFoundError(KalshiConnectorError):
    """Raised when resource not found (404 status)."""
    pass


# Kalshi public API endpoints
KALSHI_MARKETS_LIST = "https://kalshi.com/api/v1/markets"
KALSHI_MARKET_DETAILS = "https://kalshi.com/api/v1/markets/{market_id}"
KALSHI_ORDER_HISTORY = "https://kalshi.com/api/v1/order-history"
KALSHI_POSITIONS = "https://kalshi.com/api/v1/positions"


def _get_request(url: str, headers: Dict[str, str] = None) -> dict:
    """
    Make GET request to Kalshi API.

    Args:
        url: API endpoint URL
        headers: Optional HTTP headers (User-Agent required)

    Returns:
        Parsed JSON response

    Raises:
        RateLimitError: If 429 status code received
        NotFoundError: If 404 status code received
        KalshiConnectorError: For other HTTP errors or connection failures
    """
    req_headers = {
        "User-Agent": "hermes-agent/1.0",
        "Accept": "application/json"
    }
    if headers:
        req_headers.update(headers)

    import urllib.request
    import urllib.error

    try:
        with urllib.request.urlopen(url, headers=req_headers, timeout=30) as resp:
            data = json.loads(resp.read().decode())
        
        # Handle rate limiting (429 status)
        if resp.status == 429:
            retry_after = int(resp.getheader('Retry-After', 15))
            print(f"⚠️ Rate limit hit on {url}, waiting {retry_after}s")
            time.sleep(retry_after)
            return _get_request(url, headers)

        # Handle not found (404)
        if resp.status == 404:
            raise NotFoundError(f"Resource not found: {url}")

        # Handle other HTTP errors
        if resp.status >= 500:
            print(f"⚠️ Server error {resp.status} on {url}")
            return {}

    except urllib.error.HTTPError as e:
        if e.code == 429:
            retry_after = int(e.getheader('Retry-After', 15))
            print(f"⚠️ Rate limit hit, waiting {retry_after}s")
            time.sleep(retry_after)
            return _get_request(url, headers)
        
        raise KalshiConnectorError(f"HTTP {e.code}: {e.reason}")

    except urllib.error.URLError as e:
        raise KalshiConnectorError(f"Connection error: {e.reason}")

    # Add get() wrapper for convenience in tests
    if isinstance(data, dict):
        data['get'] = lambda key, default=None: data.get(key, default)
    
    return data


def fetch_markets(limit: int = 20, offset: int = 0) -> Dict[str, any]:
    """
    Fetch list of markets from Kalshi.

    Args:
        limit: Number of markets to return (default 20)
        offset: Pagination offset (default 0)

    Returns:
        Parsed JSON response with 'markets' array and pagination metadata

    Example:
        >>> result = kalshi_connector.fetch_markets(limit=10)
        >>> print(result['markets'][0]['market_id'])
        BTC-JAN31-100K
    """
    url = f"{KALSHI_MARKETS_LIST}?limit={limit}&offset={offset}"
    return _get_request(url)


def get_market(market_id: str) -> Optional[Dict[str, any]]:
    """
    Fetch details for a specific market.

    Args:
        market_id: Market identifier (e.g., 'BTC-JAN31-100K')

    Returns:
        Market details dictionary or None if not found

    Example:
        >>> market = kalshi_connector.get_market('BTC-JAN31-100K')
        >>> print(market['title'])
        Bitcoin will trade above $100,000 by January 31, 2025
    """
    url = f"{KALSHI_MARKET_DETAILS.replace('{{market_id}}', market_id)}"
    data = _get_request(url)

    if not data or 'title' not in data:
        print(f"⚠️ Market {market_id} not found")
        return None

    return data


def get_market_order_book(market_id: str) -> Optional[Dict[str, any]]:
    """
    Fetch order book for a specific market.

    Args:
        market_id: Market identifier

    Returns:
        Order book data with bid/ask prices or None if not found
    """
    market = get_market(market_id)
    
    if not market:
        return None

    # Extract order book data from market details
    order_book = {
        'yes_bid': float(market.get('bid', 0)),
        'yes_ask': float(market.get('ask', 0)),
        'no_bid': float(market.get('no', 1) - float(market.get('ask', 0))),
        'no_ask': float(market.get('no', 1) - float(market.get('bid', 0)))
    }

    return order_book


def get_user_positions(user_id: str = None) -> Optional[Dict[str, any]]:
    """
    Fetch open positions for a user.

    Args:
        user_id: User identifier (optional, defaults to authenticated session)

    Returns:
        Positions data or None if not found/authorized
    """
    url = f"{KALSHI_POSITIONS}"
    if user_id:
        # Note: Kalshi API may require auth for this endpoint
        url += f"?user_id={user_id}"
    
    return _get_request(url)


def get_order_history(limit: int = 50) -> Optional[Dict[str, any]]:
    """
    Fetch trading history (order history).

    Args:
        limit: Number of recent orders to return

    Returns:
        Order history data or None if not found/authorized
    """
    url = f"{KALSHI_ORDER_HISTORY}?limit={limit}"
    return _get_request(url)


class KalshiConnector:
    """
    Kalshi API connector class for read operations.

    Key Responsibilities:
        - Connect to Kalshi public markets endpoints
        - Fetch market listings and details
        - Parse order books and prices
        - Handle rate limiting and errors gracefully

    Invariants:
        - Never trade without pre-validating API keys in .env (not applicable for read endpoints)
        - Always use dry-run mode during development
        - All methods must handle None responses gracefully

    See also: trading_system/arbitrage/COMPLETE.md
    """

    def __init__(self):
        """Initialize Kalshi connector with configuration."""
        self.base_url = KALSHI_MARKETS_LIST
        self.max_retries = 3
        self.retry_delay = 2.0

    def fetch_markets(self, limit: int = 20, offset: int = 0) -> Dict[str, any]:
        """Fetch list of markets."""
        return fetch_markets(limit, offset)

    def get_market(self, market_id: str) -> Optional[Dict[str, any]]:
        """Fetch market details."""
        return get_market(market_id)

    def get_order_book(self, market_id: str) -> Optional[Dict[str, any]]:
        """Fetch order book for a market."""
        return get_market_order_book(market_id)

    def get_positions(self, user_id: str = None) -> Optional[Dict[str, any]]:
        """Fetch user positions."""
        return get_user_positions(user_id)

    def get_order_history(self, limit: int = 50) -> Optional[Dict[str, any]]:
        """Fetch trading history."""
        return get_order_history(limit)


def test_kalshi_connectivity():
    """Test Kalshi API connectivity and rate limits."""
    import urllib.request
    
    print("Testing Kalshi API connectivity...")
    
    url = f"{KALSHI_MARKETS_LIST}?limit=1"
    try:
        with urllib.request.urlopen(url, timeout=30) as resp:
            data = json.loads(resp.read().decode())
        
        if 'markets' in data and len(data['markets']) > 0:
            print(f"✅ Kalshi API is accessible")
            print(f"   Sample market: {data['markets'][0]['market_id']}")
            return True
        else:
            print("⚠️ No markets returned from API")
            return False
            
    except KalshiConnectorError as e:
        print(f"❌ Kalshi API error: {e}")
        return False


if __name__ == "__main__":
    # Test connectivity
    success = test_kalshi_connectivity()
    
    if success:
        print("\nFetching sample markets...")
        connector = KalshiConnector()
        
        try:
            markets = connector.fetch_markets(limit=5)
            print(f"\nRetrieved {len(markets.get('markets', []))} markets:")
            
            for market in markets.get('markets', [])[:3]:
                print(f"  Market ID: {market['market_id']}")
                print(f"  Question: {market['title'][:80]}...")
        except Exception as e:
            print(f"Error fetching markets: {e}")
