"""Coinbase REST Client Submodule - Read-Only Brokerage API.

This submodule provides the CoinbaseAdvancedRestClient for production read-only access
to your Coinbase brokerage account.

Usage:
    from trading_system.connectors.coinbase.rest.client import CoinbaseAdvancedRestClient as CoinbaseRestClient, create_advanced_rest_client_from_env

    client = CoinbaseAdvancedRestClient(
        api_key="your_api_key",
        api_secret="your_secret",
        passphrase="your_passphrase"
    )
    accounts = await client.list_accounts()
"""
