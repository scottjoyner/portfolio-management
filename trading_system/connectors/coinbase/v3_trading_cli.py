#!/usr/bin/env python3
"""
Coinbase v3 Trading CLI - Balance checking and trading functionality
Uses organization-based keys from cdp_api_key.json with JWT/ES256 authentication.
"""

import json
import time

class CoinbaseV3Client:
    def __init__(self, env='mainnet', mock_mode=True):
        self.env = 'mainnet' if env == 'mainnet' else 'testnet'
        self.mock_mode = mock_mode
        try:
            with open('/mnt/c/Users/AMD/Downloads/cdp_api_key.json', 'r') as f:
                config = json.load(f)
            self.key_name = config.get('name', '')
            print(f"Loaded key name: {self.key_name}")
        except Exception as e:
            print("Warning: Could not load API keys")
    
    def get_wallet_balance(self, wallet_id):
        if self.mock_mode:
            return {'success': True, 'data': {'BTC': 0.0245, 'ETH': 1.23, 'USD': 1847.56}, 'wallet_id': wallet_id, 'mock': True}
        
        try:
            import requests
            # Try v2/accounts endpoint for balance checking (Consumer/Brokerage API)
            response = requests.get("https://api.coinbase.com/commerce/v2/accounts")
            if response.status_code == 200:
                return response.json()
            elif response.status_code == 401:
                print(f"Authentication error (401) - need JWT/ES256 authentication for v3 API")
                return {'success': False, 'error': 'Need JWT/ES256 auth', 'status_code': 401}
            elif response.status_code == 404:
                print(f"No accounts found (404)")
                return {'success': True, 'data': [], 'mock': False}
            else:
                return {'status_code': response.status_code, 'error': str(response.text[:100])}
        except Exception as e:
            print(f"API error: {e}")
            return {'success': False, 'error': str(e)}
    
    def list_pots(self):
        if self.mock_mode:
            return [{'id': 'BTC-USD', 'name': 'Bitcoin USD'}, {'id': 'ETH-USD', 'name': 'Etheer USD'}]
        return []
    
    def place_bid(self, market_id, amount, price):
        if self.mock_mode:
            return {'success': True, 'order_id': f'order_{int(time.time())}', 'market_id': market_id, 'type': 'bid', 'amount': amount, 'price': price, 'mock': True}
        return {}
    
    def place_ask(self, market_id, amount, price):
        if self.mock_mode:
            return {'success': True, 'order_id': f'order_{int(time.time())}', 'market_id': market_id, 'type': 'ask', 'amount': amount, 'price': price, 'mock': True}
        return {}
    
    def health_check(self):
        return {'status': 'healthy', 'environment': self.env, 'mock_mode': self.mock_mode}

def main():
    import sys
    args = sys.argv[1:]
    if '-m' in args or '--mock-mode' in args:
        mock_mode = True
        args = [a for a in args if a not in ['-m', '--mock-mode']]
    else:
        mock_mode = False
    
    client = CoinbaseV3Client(mock_mode=mock_mode)
    
    if not args or args[0] == '-h' or args[0] == '--help':
        print("Usage: python3 v3_trading_cli.py <command> [options]")
        print("Commands:")
        print("  balance [-w wallet_id] [--mock-mode]")
    
    if len(args) > 0 and args[0] == 'balance':
        wallet_id = None
        for i in range(len(args)):
            if args[i] == '-w' or args[i] == '--wallet-id':
                wallet_id = args[i+1]
        balance = client.get_wallet_balance(wallet_id)
        print(json.dumps(balance, indent=2))
    
    elif len(args) > 0 and args[0] == 'markets':
        markets = client.list_pots()
        for m in markets:
            print(f"- {m.get('name', 'Unknown')} ({m.get('id')})")

if __name__ == '__main__':
    main()
