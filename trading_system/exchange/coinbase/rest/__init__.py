try:
    from .client import CoinbaseRestClient
except ImportError:
    from exchange.coinbase.rest.client import CoinbaseRestClient

__all__ = ["CoinbaseRestClient"]
