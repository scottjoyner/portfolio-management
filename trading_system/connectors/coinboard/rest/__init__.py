"""Coinboard REST Client Package - Production Read-Only Brokerage API.

This package provides production-ready Coinbase brokerage API access with full safety features.

Usage:
    from trading_system.connectors.coinboard.rest import (
        create_read_only_client,
        create_default_rest_client,
    )
    
    client = await create_read_only_client()
    accounts = await client.list_accounts()

Features:
- OAuth 2.0 token management with PKCE support
- Real-time balance fetching from Coinbase brokerage API
- Account information and transaction history
- Circuit breaker pattern (opens after 5 failures, 10-min cooldown)
- Input validation with sanitized logging (API keys masked)
- Rate limiting compliance with exponential backoff
- Health check endpoints for monitoring systems
"""

from trading_system.connectors.coinboard.rest.client import (
    CoinbaseRESTClient,
    CoinbaseFeeCalculator,
    create_default_rest_client,
    create_read_only_client,
)

from trading_system.connectors.coinboard.rest.oauth import (
    CoinbaseOAuthManager,
)

__all__ = [
    'CoinbaseRESTClient',
    'CoinbaseFeeCalculator',
    'create_default_rest_client',
    'create_read_only_client',
    'CoinbaseOAuthManager',
]
