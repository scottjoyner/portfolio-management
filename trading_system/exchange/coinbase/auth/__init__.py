try:
    from .jwt import build_jwt_token
except ImportError:
    from exchange.coinbase.auth.jwt import build_jwt_token

__all__ = ["build_jwt_token"]
