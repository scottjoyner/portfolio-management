try:
    from .client import CoinbaseWebSocketClient
except ImportError:
    CoinbaseWebSocketClient = None

__all__ = ["CoinbaseWebSocketClient"]
