#!/usr/bin/env python3
import json
import time

class MockCoinbaseClient:
    def health_check(self):
        return {'status': 'healthy', 'environment': 'mainnet', 'mock_mode': True}
    
    def get_wallet_balance(self, wallet_id):
        return {
            'success': True,
            'data': {
                'BTC': 0.0245,
                'ETH': 1.23, 
                'USD': 1847.56
            },
            'wallet_id': wallet_id,
            'mock': True
        }
    
    def list_pots(self):
        return [
            {'id': 'BTC-USD', 'name': 'Bitcoin USD', 'base_asset_id': '1', 'quote_asset_id': '2', 'mock': True},
            {'id': 'ETH-USD', 'name': 'Etheer USD', 'base_asset_id': '3', 'quote_asset_id': '2', 'mock': True}
        ]

client = MockCoinbaseClient()
print(f"=== Coinbase v3 Trading API Client ===")
print(f"Environment: mainnet (mainnet or testnet)")
print(f"Mock Mode: True\n")

print("=== Health Check ===")
health = client.health_check()
print(json.dumps(health, indent=2))

print("\n=== Mock Balance Check ===")
balance = client.get_wallet_balance('wallet_test_123')
print(json.dumps(balance, indent=2))

print("\n=== Markets List ===")
markets = client.list_pots()
for market in markets:
    print(f"  - {market.get('name', 'Unknown')} ({market.get('id')})")
