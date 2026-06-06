#!/usr/bin/env python3
"""Direct balance check - fetches actual Coinbase account balances"""
import os
from pathlib import Path
import dotenv
import requests

# Load environment
dotenv.load_dotenv('.env')

print('='*60)
print('=== COINBASE ACCOUNT BALANCE CHECK ===')
print('='*60 + '\n')

# Get credentials from environment
api_key = os.environ.get('COINBASE_API_KEY', '')
api_secret = os.environ.get('COINBASE_API_SECRET', '')

print(f'Using API Key: {api_key}')
print(f'Using Secret: {api_secret[:20]}...')
print()

# Make authenticated request to Coinbase v1 Accounts endpoint
auth_string = f"{api_key}:{api_secret}"
import base64 as b
credentials = b.b64encode(auth_string.encode()).decode()

headers = {
    'Authorization': f'Basic {credentials}',
    'Content-Type': 'application/json',
}

print('Fetching accounts from https://api.coinbase.com/v1/accounts...\n')
try:
    response = requests.get(
        'https://api.coinbase.com/v1/accounts',
        headers=headers,
        timeout=30
    )
    
    if response.status_code == 200:
        accounts = response.json().get('data', [])
        print(f'Connected successfully!')
        print(f'Result: {len(accounts)} account(s)\n')
        
        for i, account in enumerate(accounts, 1):
            currency_id = account.get('currency', '').upper()
            balance_str = account.get('balance', '0')
            
            try:
                balance_float = float(balance_str)
                if abs(balance_float) > 0.00001:
                    print(f'Account {i}: {currency_id}')
                    print(f'   Balance: {balance_float:.8f} {currency_id}')
            except ValueError:
                pass
    else:
        print(f'Status code: {response.status_code}')
except Exception as e:
    print(f'Error: {e}')

print('\n=== CHECK COMPLETE ===')