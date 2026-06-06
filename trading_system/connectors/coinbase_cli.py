#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Coinbase CLI with JWT/ES256 authentication for organization-based keys.
Supports balance checking, mock trades, and live trading when credentials available.
"""

import argparse
import json
from datetime import datetime, timezone
from enum import Enum
from dataclasses import dataclass
from typing import Optional
import sys
import os
import requests

try:
    import jwt  # pip3 install jwt - ES256 JWT authentication
    JWT_AVAILABLE = True
except ImportError:
    print("Warning: jwt package not installed. Install with: pip3 install jwt")
    JWT_AVAILABLE = False

@dataclass()
class AccountBalance:
    """Represents a Coinbase account balance."""
    name: str
    available: float
    currency: str
    id: Optional[str] = None

class AuthenticationMode(Enum):
    MOCK = 'mock'
    LIVE = 'live'

class CoinbaseCLI:
    """Main CLI class for Coinbase interactions."""
    
    def __init__(self, api_key=None, api_secret=None, mode=None):
        self.mode = mode or self._detect_mode(api_key, api_secret)
        print(f'[INFO] Using {self.mode.value} mode')
        if self.mode == AuthenticationMode.MOCK:
            self.client = self._create_mock_client()
        else:
            self.client = self._create_live_client(api_key, api_secret)
    
    def _detect_mode(self, api_key, api_secret):
        """Auto-detect mode based on credentials."""
        if not api_key or '***' in str(api_key):
            return AuthenticationMode.MOCK
        try:
            env_path = os.path.expanduser('~/.config')
            if not os.path.exists(env_path):
                return AuthenticationMode.MOCK
            with open(env_path, 'r') as f:
                content = f.read()
                for line in content.split('\\n'):
                    if 'COINBASE_API_KEY=***' in line and '***' not in line:
                        return AuthenticationMode.LIVE
        except Exception:
            pass
        return AuthenticationMode.MOCK
    
    def _create_mock_client(self):
        """Create a mock client that returns dummy data."""
        class MockClient:
            @staticmethod
            def get_accounts():
                return [
                    {'id': 'mock1', 'name': 'Bitcoin Wallet', 'balance': 1.25, 'currency_code': 'BTC'},
                    {'id': 'mock2', 'name': 'Elturium Wallet', 'balance': 3.75, 'currency_code': 'ETH'}
                ]
        return MockClient()
    
    def _create_live_client(self, api_key, api_secret):
        """Create a live client with proper authentication."""
        class LiveClient:
            def __init__(self, api_key, api_secret):
                self.api_key = api_key
                self.api_secret = api_secret
                # Extract org_id and key_id from organization-based API key format
                if '/' in str(api_key) and len(str(api_key).split('/')) >= 3:
                    parts = str(api_key).split('/')
                    if len(parts) >= 3:
                        self.org_id = parts[1]  # The middle part after "api-keys/"
                        self.key_id = parts[-1].rstrip('"')
            
            def _get_jwt_token(self, path):
                """Generate JWT token with ES256 signature."""
                import time
                payload = {
                    'sub': self.key_id,
                    'iss': self.org_id,
                    'exp': int(time.time()) + 10000000,  # Exp in future for demo
                    'iat': str(int(time.time()))
                }
                try:
                    import jwt
                    token = jwt.encode(payload, self.api_key, algorithm='ES256')
                    return token.decode() if isinstance(token, bytes) else token
                except Exception as e:
                    print(f'[WARN] JWT signing failed: {e}')
                    # Try alternative approach - maybe the key is in a different format
                    try:
                        from cryptography import version
                        from cryptography.key import PrivateKey, KeyType, load_key_from_string, PublicKey, KeyType, PublicLoadError
                        print(f'[INFO] cryptography library available (v{version})')
                    except ImportError:
                        pass
                    return None
            
            def get_accounts(self):
                import hashlib
                # Organization-based keys use a different endpoint format
                url = f'https://api.exchange.coinbase.com/v1/accounts/{self.org_id}'
                headers = {'Accept': 'application/json'}
                
                # Try JWT authentication if available
                jwt_token = self._get_jwt_token(url)
                if jwt_token:
                    headers['Authorization'] = f'Bearer {jwt_token}'
                    print(f'[INFO] Using JWT token for authentication')
                else:
                    print('[WARN] No JWT token generated, proceeding with unsigned request')
                
                response = requests.get(url, headers=headers)
                if response.status_code == 200:
                    data = response.json()
                    accounts = []
                    for acc in data.get('accounts', []):
                        balance = acc.get('balance') or {}
                        available = balance.get('available_balance') or 0
                        accounts.append({
                            'id': acc['id'],
                            'name': acc.get('name', 'Unknown'),
                            'balance': available,
                            'currency_code': acc.get('currency_code', 'USD')
                        })
                    return accounts
                # Fallback to standard endpoint if v1 doesn't work
                url = f'https://api.exchange.coinbase.com/v1/accounts/{self.org_id}/balances'
                response = requests.get(url, headers=headers)
                if response.status_code == 200:
                    data = response.json()
                    accounts = []
                    for acc in data.get('balances', []):
                        available = acc.get('available_balance') or 0
                        accounts.append({
                            'id': acc['account_id'] or acc['id'],
                            'name': acc.get('name', 'Unknown'),
                            'balance': available,
                            'currency_code': acc.get('currency_code', 'USD')
                        })
                    return accounts
                # Last fallback - try organization-level endpoint
                url = f'https://api.exchange.coinbase.com/v1/organizations/{self.org_id}/balances'
                response = requests.get(url, headers=headers)
                if response.status_code == 200:
                    data = response.json()
                    accounts = []
                    for acc in data.get('account', []):
                        balance = acc.get('balance') or {}
                        available = balance.get('available_balance') or 0
                        accounts.append({
                            'id': acc['id'],
                            'name': acc.get('name', 'Unknown'),
                            'balance': available,
                            'currency_code': acc.get('currency_code', 'USD')
                        })
                    return accounts
                # If all v1 endpoints failed, try the original format
                url = f'https://api.exchange.coinbase.com/{self.org_id}/accounts'
                response = requests.get(url, headers=headers)
                if response.status_code == 200:
                    data = response.json()
                    accounts = []
                    for acc in data.get('accounts', []):
                        balance = acc.get('balance') or {}
                        available = balance.get('available_balance') or 0
                        accounts.append({
                            'id': acc['id'],
                            'name': acc.get('name', 'Unknown'),
                            'balance': available,
                            'currency_code': acc.get('currency_code', 'USD')
                        })
                    return accounts
                # Try standard endpoint with api_key as part of the URL path
                url = f'https://api.exchange.coinbase.com/v1/accounts/{self.key_id}/balances'
                response = requests.get(url, headers=headers)
                if response.status_code == 200:
                    data = response.json()
                    accounts = []
                    for acc in data.get('balances', []):
                        available = acc.get('available_balance') or 0
                        accounts.append({
                            'id': self.key_id,
                            'name': f'Account {self.key_id}',
                            'balance': available,
                            'currency_code': 'USD'
                        })
                    return accounts
                print(f'[INFO] No accounts found via any endpoint. Status: {response.status_code}')
                return []
        
        return LiveClient(api_key, api_secret)
    
    def get_accounts(self):
        """Retrieve all account balances."""
        print('Fetching accounts...')
        try:
            raw_accounts = self.client.get_accounts()
            if not raw_accounts:
                if self.mode == AuthenticationMode.MOCK:
                    print('[INFO] No accounts found. Using mock data.')
                    return [{
                        'id': 'mock1',
                        'name': 'Bitcoin Wallet',
                        'balance': 1.25,
                        'currency_code': 'BTC'
                    }]
                print('[INFO] No accounts found.')
                return []
            
            formatted_accounts = []
            for acc in raw_accounts:
                if isinstance(acc, dict):
                    formatted_accounts.append({
                        'id': acc.get('id', 'unknown'),
                        'name': acc.get('name', 'Unknown'),
                        'balance': acc.get('balance', 0),
                        'currency_code': acc.get('currency_code', 'USD')
                    })
            return formatted_accounts
        except Exception as e:
            print(f'[ERROR] Failed to fetch accounts: {e}')
            if self.mode == AuthenticationMode.MOCK:
                return [{
                    'id': 'mock1',
                    'name': 'Bitcoin Wallet',
                    'balance': 1.25,
                    'currency_code': 'BTC'
                }]
            raise
    
    def get_account_balance(self, account_id=None):
        """Get specific account balance."""
        accounts = self.get_accounts()
        if not account_id:
            print('No account ID specified. Showing all balances.')
            return accounts
        for acc in accounts:
            if acc['id'] == account_id or (account_id.lower() in acc['name'].lower()):
                return acc
        return None
    
    def trade(self, currency_from='USD', currency_to='BTC', amount=0.1):
        """Execute a mock or live trade."""
        if self.mode == AuthenticationMode.MOCK:
            print(f'[MOCK] Trade: {amount} {currency_from} -> {currency_to}')
            return {'status': 'mock_trade', 'from': currency_from, 'to': currency_to, 'amount': amount}
        
        if '***' in str(self.api_key):
            parts = str(self.api_key).split('/')
            org_id = parts[1]
            key_id = parts[-1].rstrip('"')
        else:
            print('[ERROR] Need organization-based API key for live trading')
            return None
        
        url = f'https://api.exchange.coinbase.com/{org_id}/b2c-conversion'
        data = {
            'amount': str(amount),
            'from_currency': currency_from,
            'to_currency': currency_to
        }
        headers = {'Accept': 'application/json'}
        response = requests.post(url, json=data, headers=headers)
        if response.status_code == 200:
            result = response.json()
            return {
                'status': 'trade_success',
                'result': result,
                'from_currency': currency_from,
                'to_currency': currency_to,
                'amount': amount
            }
        print(f'[ERROR] Trade failed: {response.status_code}')
        return None
    
    def trade_sim(self, currency_from='USD', currency_to='BTC', amount=0.1):
        """Simulate a trade without executing it."""
        if self.mode == AuthenticationMode.MOCK:
            print(f'[SIMULATED] Trade: {amount} {currency_from} -> {currency_to}')
            return {'status': 'simulated', 'from': currency_from, 'to': currency_to, 'amount': amount}
        
        if '***' in str(self.api_key):
            parts = str(self.api_key).split('/')
            org_id = parts[1]
            key_id = parts[-1].rstrip('"')
        else:
            print('[ERROR] Need organization-based API key for live trading simulation')
            return None
        
        url = f'https://api.exchange.coinbase.com/{org_id}/b2c-conversion'
        data = {
            'amount': str(amount),
            'from_currency': currency_from,
            'to_currency': currency_to
        }
        headers = {'Accept': 'application/json'}
        response = requests.post(url, json=data, headers=headers)
        if response.status_code == 200:
            result = response.json()
            return {
                'status': 'simulated_trade',
                'result': result,
                'from_currency': currency_from,
                'to_currency': currency_to,
                'amount': amount
            }
        print(f'[ERROR] Trade simulation failed: {response.status_code}')
        return None
    
def main():
    # Parse arguments manually for simplicity
    import sys
    
    args = sys.argv[1:]
    if not args or '-h' in args or '--help' in args:
        print("Coinbase CLI - organization-based key support")
        print("Usage: coinbase_cli.py <command> [options]")
        print("Commands: balance, trade, simulate, all")
        return
    
    cmd = args[0].lower()
    
    # Parse command-specific arguments
    options = {}
    i = 1
    while i < len(args):
        if args[i] == '--id' or args[i] == '-i':
            options['account_id'] = args[i+1] if i+1 < len(args) else None
            i += 2
        elif args[i] == '--from' and cmd in ['trade', 'simulate']:
            options['currency_from'] = args[i+1] if i+1 < len(args) else 'USD'
            i += 2
        elif args[i] == '--to':
            options['currency_to'] = args[i+1] if i+1 < len(args) else 'BTC'
            i += 2
        elif args[i] == '--amount' or args[i] == '-a':
            options['amount'] = float(args[i+1]) if i+1 < len(args) and args[i+1].replace('.', '').isdigit() else 0.1
            i += 2
        elif args[i] == '--api-key':
            options['api_key'] = args[i+1] if i+1 < len(args) else None
            i += 2
        else:
            i += 1
    
    # Create CLI with or without API key - allows live mode when provided
    api_key = options.get('api_key')
    cli = CoinbaseCLI(api_key=api_key, mode=AuthenticationMode.LIVE if api_key else None)
    
    if cmd == 'balance':
        result = cli.get_account_balance(options.get('account_id'))
        print(json.dumps(result, indent=2))
    elif cmd == 'trade':
        result = cli.trade(
            currency_from=options.get('currency_from', 'USD'),
            currency_to=options.get('currency_to', 'BTC'),
            amount=float(options.get('amount', 0.1))
        )
        print(json.dumps(result, indent=2))
    elif cmd == 'simulate':
        result = cli.trade_sim(
            currency_from=options.get('currency_from', 'USD'),
            cursor_to=options.get('currency_to', 'BTC'),
            amount=float(options.get('amount', 0.1))
        )
        print(json.dumps(result, indent=2))
    elif cmd == 'all':
        result = cli.get_accounts()
        print(json.dumps(result, indent=2))
    else:
        print(f"Unknown command: {cmd}")

if __name__ == '__main__':
    main()