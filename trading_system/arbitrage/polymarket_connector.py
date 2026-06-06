"""Polymarket API connector for read operations (markets, order books).

This module provides the Polymarket public API integration without authentication.
For trading execution, Gamma wallet credentials are required.

Usage:
    from trading_system.arbitrage import polymarket_connector
    
    markets = polymarket_connector.search("bitcoin")
    market_detail = polymarket_connector.get_market('us-pres-24-biden-trump')

Invariants:
    - Never trade without pre-validating API keys in .env (Gamma wallet for orders)
    - Always use dry-run mode during development
    - All methods must handle None responses gracefully (rate limiting, maintenance)

See also: trading_system/arbitrage/COMPLETE.md for complete documentation.
"""

from typing import Dict, List, Optional, Any

import json
import time
from typing import Dict, List, Optional, Any
import urllib.request
import urllib.error
import urllib.parse


class PolymarketConnectorError(Exception):
    """Base exception for Polymarket API errors."""
    pass


class RateLimitError(PolymarketConnectorError):
    """Raised when rate limit is hit (429 status)."""
    pass


class NotFoundError(PolymarketConnectorError):
    """Raised when resource not found (404 status)."""
    pass


# Polymarket public API endpoints
GAMMA_API = "https://gamma-api.polymarket.com"
CLOB = "https://clob.polymarket.com"
DATA_API = "https://data-api.polymarket.com"


def _get_request(url: str, headers: Dict[str, str] = None) -> dict | list:
    """
    Make GET request to Polymarket API.

    Args:
        url: API endpoint URL
        headers: Optional HTTP headers (User-Agent required)

    Returns:
        Parsed JSON response

    Raises:
        RateLimitError: If 429 status code received
        NotFoundError: If 404 status code received
        PolymarketConnectorError: For other HTTP errors or connection failures
    """
    req_headers = {
        "User-Agent": "hermes-agent/1.0",
        "Accept": "application/json"
    }
    if headers:
        req_headers.update(headers)

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

    except urllib.error.HTTPError as e:
        if e.code == 429:
            retry_after = int(e.getheader('Retry-After', 15))
            print(f"⚠️ Rate limit hit, waiting {retry_after}s")
            time.sleep(retry_after)
            return _get_request(url, headers)
        
        raise PolymarketConnectorError(f"HTTP {e.code}: {e.reason}")

    except urllib.error.URLError as e:
        raise PolymarketConnectorError(f"Connection error: {e.reason}")

    return data


def fetch_trending_markets(limit: int = 10) -> List[Dict[str, Any]]:
    """
    Fetch trending markets from Polymarket.

    Args:
        limit: Number of markets to return (default 10)

    Returns:
        List of trending market dictionaries

    Example:
        >>> markets = polymarket_connector.fetch_trending_markets(limit=5)
        >>> for m in markets[:3]:
        ...     print(m['slug'])
        us-pres-24-biden-trump
        btc-jan-100k
        ...
    """
    url = f"{GAMMA_API}/markets/trending?limit={limit}"
    data = _get_request(url)
    
    if isinstance(data, dict):
        markets = data.get('marketSummaries', [])
    else:
        markets = data
    
    return markets


def search_markets(query: str) -> Dict[str, Any]:
    """
    Search markets by keyword.

    Args:
        query: Search query (e.g., 'bitcoin', 'election')

    Returns:
        Search results with events array containing matching markets

    Example:
        >>> data = polymarket_connector.search_markets("bitcoin")
        >>> print(f"Found {len(data['events'])} bitcoin-related markets")
        Found 45 bitcoin-related markets
    """
    encoded_query = urllib.parse.quote(query)
    url = f"{GAMMA_API}/public-search?q={encoded_query}"
    data = _get_request(url)
    
    return {
        'query': query,
        'events': data.get('events', [])
    }


def get_market(market_slug: str) -> Optional[Dict[str, Any]]:
    """
    Fetch details for a specific market.

    Args:
        market_slug: Market slug (e.g., 'us-pres-24-biden-trump')

    Returns:
        Market details dictionary or None if not found

    Example:
        >>> market = polymarket_connector.get_market('us-pres-24-biden-trump')
        >>> print(market['question'])
        Will Biden win the 2024 US Presidential election?
    """
    url = f"{GAMMA_API}/market/{market_slug}"
    data = _get_request(url)

    if not isinstance(data, dict):
        print(f"⚠️ Market {market_slug} not found")
        return None

    return data


def get_market_order_book(market_slug: str) -> Optional[Dict[str, Any]]:
    """
    Fetch order book for a specific market.

    Args:
        market_slug: Market slug

    Returns:
        Order book with outcome prices or None if not found

    Example:
        >>> ob = polymarket_connector.get_market_order_book('bitcoin-100k')
        >>> print(ob['outcomePrices'])
        [{'outcome': 'Yes', 'price': 46.2}, ...]
    """
    market = get_market(market_slug)
    
    if not market:
        return None

    outcome_prices = _parse_outcome_prices(market.get('outcomePrices', []))
    outcomes = _parse_outcomes(market.get('outcomes', []))

    # Clean up for API calls (remove trailing .0 from floats, keep quotes on strings)
    outcome_prices_clean = [
        f'"{p["outcome"]}": {int(float(p["price"]) * 100)}'
        for p in outcome_prices
    ]
    outcomes_clean = ' '.join(outcomes)

    return {
        'market_slug': market_slug,
        'question': market.get('question', ''),
        'volume': market.get('volume', 0),
        'open': not market.get('closed', False),
        'outcomePricesClean': outcome_prices_clean,
        'outcomesClean': outcomes_clean
    }


def get_condition_orderbook(token_id: str) -> Dict[str, Any]:
    """
    Fetch order book for a condition (token).

    Args:
        token_id: Condition token ID (e.g., CONDITION_ID from market)

    Returns:
        Order book data or error message

    Example:
        >>> ob = polymarket_connector.get_condition_orderbook('0x...')
        >>> print(ob['bids'])
        [{'price': 45.2, 'size': 150}, ...]
    """
    url = f"{CLOB}/clobTokens/{token_id}?includeOutcomes=false"
    
    try:
        data = _get_request(url)
        return {
            'token_id': token_id,
            'bids': data.get('bids', []),
            'asks': data.get('asks', [])
        }
    except PolymarketConnectorError as e:
        print(f"⚠️ Error fetching condition orderbook for {token_id}: {e}")
        return {}


def search_conditions(query: str) -> List[Dict[str, Any]]:
    """
    Search conditions by keyword.

    Args:
        query: Search query

    Returns:
        List of matching conditions

    Example:
        >>> conditions = polymarket_connector.search_conditions("bitcoin")
        >>> print(f"Found {len(conditions)} bitcoin conditions")
        Found 23 bitcoin conditions
    """
    encoded_query = urllib.parse.quote(query)
    url = f"{CLOB}/clobTokens?query={encoded_query}"
    
    try:
        data = _get_request(url)
        return data.get('items', []) if isinstance(data, dict) else []
    except PolymarketConnectorError as e:
        print(f"⚠️ Error searching conditions for '{query}': {e}")
        return []


def fetch_history(condition_id: str, interval: str = 'all') -> Dict[str, Any]:
    """
    Fetch trading history for a condition.

    Args:
        condition_id: Condition token ID
        interval: History interval (default 'all', also accepts 'daily', 'weekly')

    Returns:
        Trading history data or error message

    Example:
        >>> history = polymarket_connector.fetch_history(
        ...     '0x...CONTRACT_ADDRESS...',
        ...     interval='daily'
        ... )
        >>> print(len(history['trades']))
        15847
    """
    url = f"{DATA_API}/trading-history?token={condition_id}&interval={interval}"
    
    try:
        data = _get_request(url)
        return {
            'condition_id': condition_id,
            'interval': interval,
            'total_trades': len(data.get('trades', [])),
            'recent_trades': data.get('trades', [])[:5]  # Return recent trades
        }
    except PolymarketConnectorError as e:
        print(f"⚠️ Error fetching history for {condition_id}: {e}")
        return {}


def fetch_trades(market_slug: str = None, limit: int = 10) -> Dict[str, Any]:
    """
    Fetch recent trades from CLOB.

    Args:
        market_slug: Optional market slug filter (e.g., 'us-pres-24-biden-trump')
        limit: Number of trades to return (default 10)

    Returns:
        Trading history or empty response

    Example:
        >>> trades = polymarket_connector.fetch_trades('us-pres-24-biden-trump', limit=5)
        >>> for trade in trades.get('trades', []):
        ...     print(f"  {trade['conditionId'][:8]}...: ${trade['total']} @ ${trade['price']}")
    """
    if market_slug:
        url = f"{CLOB}/clobTokens?filter={urllib.parse.quote(market_slug)}&limit={limit}"
    else:
        # Fetch all recent trades
        url = f"{CLOB}/trading-history?limit={limit}"
    
    try:
        data = _get_request(url)
        return {
            'total_trades': len(data.get('items', [])),
            'trades': data.get('items', [])[:limit]
        }
    except PolymarketConnectorError as e:
        print(f"⚠️ Error fetching trades: {e}")
        return {}


def _parse_outcome_prices(outcomes: List[Dict]) -> List[str]:
    """
    Parse and format outcome prices for API calls.

    Args:
        outcomes: List of price objects from market details

    Returns:
        Clean string representation with quotes around outcome names
    
    Example:
        >>> _parse_outcome_prices([{'outcome': 'Yes', 'price': 0.462}])
        ['"Yes": 46']
    """
    cleaned = []
    for p in outcomes:
        price = int(float(p['price']) * 100)
        outcome = p.get('outcome', str(price))
        cleaned.append(f'"{outcome}": {price}')
    return cleaned


def _parse_outcomes(outcomes_field):
    """
    Parse outcomes field from market data.

    Args:
        outcomes_field: Outcomes array or string from API response

    Returns:
        Clean list of outcome labels
    
    Example:
        >>> _parse_outcomes('[{"outcome": "Yes"}, {"outcome": "No"}]')
        ['Yes', 'No']
    """
    if isinstance(outcomes_field, list):
        return [o.get('outcome', str(o)) for o in outcomes_field]
    elif isinstance(outcomes_field, str):
        # Try parsing JSON string
        try:
            parsed = json.loads(outcomes_field)
            return [o.get('outcome', str(o)) for o in parsed]
        except json.JSONDecodeError:
            # Return as-is if can't parse
            return [outcomes_field]
    return []


class PolymarketConnector:
    """
    Polymarket API connector class for read operations.

    Key Responsibilities:
        - Connect to Polymarket public markets endpoints
        - Fetch market listings and order books
        - Search and filter markets by query
        - Handle rate limiting and errors gracefully

    Invariants:
        - Never trade without pre-validating Gamma wallet credentials in .env
        - Always use dry-run mode during development
        - All methods must handle None responses gracefully

    See also: trading_system/arbitrage/COMPLETE.md
    """

    def __init__(self):
        """Initialize Polymarket connector with configuration."""
        self.base_url = GAMMA_API
        self.max_retries = 3
        self.retry_delay = 2.0

    def fetch_trending(self, limit: int = 10) -> List[Dict[str, Any]]:
        """Fetch trending markets."""
        return fetch_trending_markets(limit)

    def search(self, query: str) -> Dict[str, Any]:
        """Search markets by keyword."""
        return search_markets(query)

    def get_market(self, market_slug: str) -> Optional[Dict[str, Any]]:
        """Fetch market details."""
        return get_market(market_slug)

    def get_order_book(self, market_slug: str) -> Optional[Dict[str, Any]]:
        """Fetch order book for a market."""
        return get_market_order_book(market_slug)

    def get_condition_orderbook(self, token_id: str) -> Dict[str, Any]:
        """Fetch condition-specific order book."""
        return get_condition_orderbook(token_id)

    def search_conditions(self, query: str) -> List[Dict[str, Any]]:
        """Search conditions by keyword."""
        return search_conditions(query)

    def fetch_history(self, condition_id: str, interval: str = 'all') -> Dict[str, Any]:
        """Fetch trading history for a condition."""
        return fetch_history(condition_id, interval)

    def fetch_trades(self, market_slug: str = None, limit: int = 10) -> Dict[str, Any]:
        """Fetch recent trades from CLOB."""
        return fetch_trades(market_slug, limit)


def test_polymarket_connectivity():
    """Test Polymarket API connectivity and rate limits."""
    print("Testing Polymarket API connectivity...")
    
    url = f"{GAMMA_API}/markets/trending?limit=1"
    try:
        with urllib.request.urlopen(url, timeout=30) as resp:
            data = json.loads(resp.read().decode())
        
        if 'marketSummaries' in data and len(data['marketSummaries']) > 0:
            print(f"✅ Polymarket API is accessible")
            sample = data['marketSummaries'][0]
            print(f"   Sample market: {sample.get('slug')}")
            return True
        else:
            print("⚠️ No markets returned from API")
            return False
            
    except PolymarketConnectorError as e:
        print(f"❌ Polymarket API error: {e}")
        return False


if __name__ == "__main__":
    # Test connectivity
    success = test_polymarket_connectivity()
    
    if success:
        print("\nFetching sample trending markets...")
        connector = PolymarketConnector()
        
        try:
            trending = connector.fetch_trending(limit=3)
            print(f"\nRetrieved {len(trending)} trending markets:\n")
            
            for market in trending:
                slug = market.get('slug', 'N/A')
                question = market.get('question', 'N/A')[:60] + "..." if len(market.get('question', '')) > 60 else market.get('question', 'N/A')
                
                print(f"Market: {slug}")
                print(f"  Question: {question}")
                print(f"  Volume: ${market.get('volume', 0):,.2f}")
        except Exception as e:
            print(f"Error fetching trending markets: {e}")
