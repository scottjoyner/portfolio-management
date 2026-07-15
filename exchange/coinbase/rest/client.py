"""Stub collaborator for trading_system/brokers/coinbase.py.

The real ``exchange.coinbase.rest.client.CoinbaseRestClient`` does not exist in
this checkout, so this minimal stub lets the broker module import. Tests inject
their own fake client and never instantiate the real ``CoinbaseRestClient``.
"""

from __future__ import annotations


class CoinbaseRestClient:
    def __init__(self, *args, **kwargs):
        pass
