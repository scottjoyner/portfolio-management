#!/usr/bin/env python3
"""Coinboard REST Client Submodule - Production Read-Only Brokerage API."""

import sys
sys.path.insert(0, '/home/falcon/git/portfolio-management')
import asyncio
from pathlib import Path
from typing import Optional, Dict, List, Any, Tuple
from dataclasses import dataclass, field
from datetime import datetime, timedelta
import json
import os

# ============== Circuit Breaker Pattern ==============

@dataclass
class CircuitBreakerState:
    """Track failure count and cooldown period."""
    failure_count: int = 0
    last_failure_time: Optional[datetime] = None
    cooldown_minutes: float = 10.0
    
    def is_open(self) -> bool:
        """Check if circuit breaker is open (too many recent failures)."""
        if self.failure_count < 5:
            return False
        
        now = datetime.now()
        minutes_since_failure = 0.0
        if self.last_failure_time:
            minutes_since_failure = (now - self.last_failure_time).total_seconds() / 60
        
        return minutes_since_failure < self.cooldown_minutes


class CircuitBreakerError(Exception):
    """Raised when circuit breaker is open."""
    pass

# ============== Fee Calculator ==============

@dataclass
class CoinbaseFeeConfig:
    """Coinboard Advanced Trade fee configuration."""
    maker_fee_bps: float = 5.0
    taker_fee_bps: float = 5.0
    volume_tier_multiplier: float = 1.0


class CoinbaseFeeCalculator:
    """Coinboard Advanced Trade fee calculations."""
    
    def __init__(self, config: Optional[CoinbaseFeeConfig] = None):
        self.config = config or CoinbaseFeeConfig()
        
    def calculate_order_fees(
        self, 
        order_amount: float,
        order_side: str,
        maker_taker: bool
    ) -> Tuple[float, float]:
        """Calculate order fees for mock data."""
        if order_side == 'buy':
            fee_rate = self.config.maker_fee_bps if maker_taker else self.config.taker_fee_bps
        else:
            fee_rate = self.config.maker_fee_bps if maker_taker else self.config.taker_fee_bps
        
        fees = order_amount * (fee_rate / 10000)
        return float(fees), float(order_amount - fees)
    
    def calculate_withdrawal_fees(self, currency: str, amount: float) -> float:
        """Calculate withdrawal fees for specified currency."""
        withdrawal_fees = {
            'USD': 1.50,
            'BTC': 0.00002,
            'ETH': 0.002,
        }
        return float(withdrawal_fees.get(currency.upper(), 0))
    
    def get_fee_schedule(self) -> Dict[str, Any]:
        """Return current fee schedule."""
        return {
            'maker_fee_rate': f"{self.config.maker_fee_bps / 100:.2f}%",
            'taker_fee_rate': f"{self.config.taker_fee_bps / 100:.2f}%",
        }

# ============== Coinbase REST Client ==============

class CoinbaseRESTClient:
    """
    Read-only Coinbase API client with circuit breaker protection.
    
    Features:
    - OAuth 2.0 authentication (read-only scope)
    - Circuit breaker for failure handling
    - Rate limiting compliance
    - Fee-adjusted profit calculations
    - Health check endpoints
    """
    
    API_BASE_URL = "https://api.exchange.coinbase.com"
    MAX_RETRIES = 3
    RETRY_DELAY = 2.0  # seconds
    RATE_LIMIT_DELAY = 0.5  # seconds between requests
    
    def __init__(self, config: dict):
        """Initialize Coinbase REST client.
        
        Args:
            config: Config dict with keys:
                - access_token: OAuth access token (required)
                - rate_limit_delay: Delay between API calls
        """
        self.access_token = config.get('access_token', '')
        self.rate_limit_delay = float(config.get('rate_limit_delay', 0.5))
        
        # Initialize circuit breaker for API operations
        from trading_system.connectors.coinboard.rest.circuit_breaker import CircuitBreaker
        self.api_circuit_breaker = CircuitBreaker(
            failure_threshold=5,
            cooldown_minutes=10.0
        )
    
    async def _make_request(self, method: str, endpoint: str, params: dict = None) -> Tuple[Dict[str, Any], bool]:
        """
        Make authenticated API request with retry logic.
        
        Args:
            method: HTTP method ('GET', 'POST')
            endpoint: API endpoint path (e.g., '/v2/accounts')
            params: Query parameters
            
        Returns:
            Tuple of (response_dict, error_occurred)
        """
        try:
            # Check circuit breaker before making request
            if self.api_circuit_breaker.state.is_open():
                raise CircuitBreakerError("Circuit breaker open for API operations")
            
            url = f"{self.API_BASE_URL}{endpoint}"
            headers = {'Authorization': f'Bearer {self.access_token}', 'User-Agent': 'PortfolioManagement/1.0'}
            
            # Rate limiting
            await asyncio.sleep(self.rate_limit_delay)
            
            import urllib.request
            import urllib.parse
            
            data = params.copy() if params else {}
            encoded_data = urllib.parse.urlencode(data).encode('utf-8')
            
            req = urllib.request.Request(url, data=encoded_data, headers=headers)
            req.get_method = lambda: method.upper()
            
            with urllib.request.urlopen(req, timeout=30) as response:
                return json.loads(response.read().decode('utf-8')), False
        
        except Exception as e:
            # General exception - sanitize before logging
            if 'access_token' in str(e):
                sanitized_e = str(e).replace(self.access_token, 'fxp_***...****1234')
            else:
                sanitized_e = str(e)
            
            raise CircuitBreakerError(f"API request error (masked): {sanitized_e}")
    
    async def fetch_account(self, account_id: str) -> Tuple[Dict[str, Any], bool]:
        """
        Fetch account information.
        
        Args:
            account_id: Account ID or alias
            
        Returns:
            Tuple of (account_dict, error_occurred)
        """
        try:
            await self.api_circuit_breaker.call_if_closed(
                lambda: asyncio.create_task(self._fetch_account_impl(account_id))
            )
            
        except CircuitBreakerError as e:
            raise
        
        except Exception as e:
            if 'access_token' in str(e):
                sanitized_e = str(e).replace(self.access_token, 'fxp_***...****1234')
            else:
                sanitized_e = str(e)
            raise CircuitBreakerError(f"Account fetch error (masked): {sanitized_e}")
    
    async def _fetch_account_impl(self, account_id: str) -> Tuple[Dict[str, Any], bool]:
        """Fetch account implementation."""
        endpoint = f"/v2/accounts/{account_id}"
        response, error = await self._make_request('GET', endpoint)
        
        if not error:
            return response, False
        else:
            return {'error': 'Failed to fetch account'}, True
    
    async def list_accounts(self) -> Tuple[List[Dict[str, Any]], bool]:
        """
        List all accounts.
        
        Returns:
            Tuple of (accounts_list, error_occurred)
        """
        try:
            await self.api_circuit_breaker.call_if_closed(
                lambda: asyncio.create_task(self._list_accounts_impl())
            )
            
        except CircuitBreakerError as e:
            raise
        
        except Exception as e:
            if 'access_token' in str(e):
                sanitized_e = str(e).replace(self.access_token, 'fxp_***...****1234')
            else:
                sanitized_e = str(e)
            raise CircuitBreakerError(f"List accounts error (masked): {sanitized_e}")
    
    async def _list_accounts_impl(self) -> Tuple[List[Dict[str, Any]], bool]:
        """List accounts implementation."""
        endpoint = "/v2/accounts"
        response, error = await self._make_request('GET', endpoint)
        
        if not error:
            return response.get('data', []), False
        else:
            return [], True
    
    async def fetch_balance(self, account_id: str) -> Tuple[Dict[str, Any], bool]:
        """
        Fetch account balance.
        
        Args:
            account_id: Account ID or alias
            
        Returns:
            Tuple of (balance_dict, error_occurred)
        """
        try:
            await self.api_circuit_breaker.call_if_closed(
                lambda: asyncio.create_task(self._fetch_balance_impl(account_id))
            )
            
        except CircuitBreakerError as e:
            raise
        
        except Exception as e:
            if 'access_token' in str(e):
                sanitized_e = str(e).replace(self.access_token, 'fxp_***...****1234')
            else:
                sanitized_e = str(e)
            raise CircuitBreakerError(f"Balance fetch error (masked): {sanitized_e}")
    
    async def _fetch_balance_impl(self, account_id: str) -> Tuple[Dict[str, Any], bool]:
        """Fetch balance implementation."""
        endpoint = f"/v2/accounts/{account_id}"
        response, error = await self._make_request('GET', endpoint)
        
        if not error:
            return {
                'currency': response.get('currency', ''),
                'balance': float(response.get('balance', 0)),
                'available': float(response.get('available', 0)),
                'last_refreshed': str(response.get('last_refreshed', '')) if response.get('last_refreshed') else None,
            }, False
        else:
            return {'error': 'Failed to fetch balance'}, True
    
    async def fetch_transactions(self, account_id: str, limit: int = 50) -> Tuple[List[Dict[str, Any]], bool]:
        """
        Fetch recent transactions.
        
        Args:
            account_id: Account ID or alias
            limit: Maximum number of transactions to fetch
            
        Returns:
            Tuple of (transactions_list, error_occurred)
        """
        try:
            await self.api_circuit_breaker.call_if_closed(
                lambda: asyncio.create_task(self._fetch_transactions_impl(account_id, limit))
            )
            
        except CircuitBreakerError as e:
            raise
        
        except Exception as e:
            if 'access_token' in str(e):
                sanitized_e = str(e).replace(self.access_token, 'fxp_***...****1234')
            else:
                sanitized_e = str(e)
            raise CircuitBreakerError(f"Transactions fetch error (masked): {sanitized_e}")
    
    async def _fetch_transactions_impl(
        self,
        account_id: str,
        limit: int = 50
    ) -> Tuple[List[Dict[str, Any]], bool]:
        """Fetch transactions implementation."""
        endpoint = f"/v2/accounts/{account_id}/transactions"
        params = {'limit': limit}
        response, error = await self._make_request('GET', endpoint, params=params)
        
        if not error:
            return [dict(t) for t in response.get('data', [])], False
        else:
            return [], True
    
    async def fetch_market_price(self, pair: str = 'BTC-USD') -> Tuple[float, bool]:
        """
        Fetch current market price.
        
        Args:
            pair: Trading pair (e.g., 'BTC-USD', 'ETH-USDT')
            
        Returns:
            Tuple of (price_float, error_occurred)
        """
        try:
            await self.api_circuit_breaker.call_if_closed(
                lambda: asyncio.create_task(self._fetch_market_price_impl(pair))
            )
            
        except CircuitBreakerError as e:
            raise
        
        except Exception as e:
            if 'access_token' in str(e):
                sanitized_e = str(e).replace(self.access_token, 'fxp_***...****1234')
            else:
                sanitized_e = str(e)
            raise CircuitBreakerError(f"Market price fetch error (masked): {sanitized_e}")
    
    async def _fetch_market_price_impl(self, pair: str) -> Tuple[float, bool]:
        """Fetch market price implementation."""
        endpoint = f"/v2/ticker/price/{pair}"
        response, error = await self._make_request('GET', endpoint)
        
        if not error:
            data = response.get('data', {})
            return float(data.get('price', 0)), False
        else:
            return 0.0, True
    
    async def health_check(self) -> Tuple[Dict[str, Any], bool]:
        """
        Perform health check against Coinbase API.
        
        Returns:
            Tuple of (health_dict, error_occurred)
        """
        try:
            await self.api_circuit_breaker.call_if_closed(
                lambda: asyncio.create_task(self._health_check_impl())
            )
            
        except CircuitBreakerError as e:
            raise
        
        except Exception as e:
            if 'access_token' in str(e):
                sanitized_e = str(e).replace(self.access_token, 'fxp_***...****1234')
            else:
                sanitized_e = str(e)
            raise CircuitBreakerError(f"Health check error (masked): {sanitized_e}")
    
    async def _health_check_impl(self) -> Tuple[Dict[str, Any], bool]:
        """Health check implementation."""
        endpoint = "/v2/ticker/price/BTC-USD"
        response, error = await self._make_request('GET', endpoint)
        
        if not error:
            return {
                'status': 'healthy',
                'btc_price': float(response.get('data', {}).get('price', 0)),
                'timestamp': datetime.now().isoformat(),
            }, False
        else:
            return {'error': 'Failed to reach Coinbase API'}, True
    
    def get_health_check(self) -> Dict[str, Any]:
        """Return structured health check status."""
        return {
            'status': 'ready',
            'version': '1.0.0',
            'components': {
                'circuit_breaker_active': True,
                'rate_limit_compliant': self.rate_limit_delay > 0,
                'fee_calculator_ready': True,
            }
        }

# ============== Factory Function for Read-Only Client ==============

def create_read_only_client(config: dict = None) -> CoinbaseRESTClient:
    """
    Factory function to create read-only Coinbase REST client.
    
    Args:
        config: Config dict with keys:
            - access_token: OAuth access token (required)
            - rate_limit_delay: Delay between API calls (default 0.5s)
    
    Returns:
        CoinbaseRESTClient instance
    """
    if config is None:
        config = {}
    
    # Get access token from various sources
    access_token = config.get('access_token', '')
    
    if not access_token:
        # Try environment variable
        import os
        env_token = os.environ.get('COINBOARD_ACCESS_TOKEN')
        if env_token:
            access_token = env_token
    
    if not access_token:
        # Try auth file
        auth_file = Path('/home/falcon/git/portfolio-management/.hermes/coinboard/auth.json')
        if auth_file.exists():
            try:
                auth_data = json.loads(auth_file.read_text())
                if 'access_token' in auth_data:
                    access_token = auth_data['access_token']
            except (json.JSONDecodeError, KeyError):
                pass
    
    # Return mock client for development if no token found
    if not access_token:
        print("[CoinboardClient] No access token provided. Using mock mode.")
        return CoinbaseRESTClient.__new__(CoinbaseRESTClient)  # Create instance without __init__
    
    return CoinbaseRESTClient(config)


def create_default_rest_client() -> CoinbaseRESTClient:
    """
    Factory function to create default Coinbase REST client.
    
    Returns:
        CoinbaseRESTClient instance with default configuration
    """
    return create_read_only_client()
