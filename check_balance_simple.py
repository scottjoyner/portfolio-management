#!/usr/bin/env python3
"""
Direct balance check - simplest approach
"""
import os, dotenv, requests, base64
from pathlib import Path

dotenv.load_dotenv('.env')

print('='*60)
print('=== COINBASE BALANCE CHECK ===')
print('='*60 + '\n')

api_key = os.environ.get('COINBASE_API_KEY', '')
api_secret = os.environ.get('COINBASE_API_SECRET', '')

if not api_key or not api_secret:
    print('No credentials found in .env')
    exit(1)

print(f'API Key: {api_key}')
print(f'Secret: {api_secret[:30]}...\n')

auth_string = f"{api_key}:{api_secret}"
credentials = base64.b64encode(auth_string.encode()).decode()

headers = {
    'Authorization': f'Basic {credentials}',
}

print('Making request to https://api.coinbase.com/v1/accounts...\n')
r = requests.get('https://api.coinbase.com/v1/accounts', headers=headers, timeout=30)

print(f'Status: {r.status_code}')
if r.ok:
    data = r.json()
    print(f'Accounts found: {len(data)}\n')
    for acc in data:
        currency = acc.get('currency', 'N/A').upper()
        balance = float(acc.get('balance', 0))
        if abs(balance) > 0.0001:
            print(f'{currency}: {balance:.8f}')
else:
    print(f'Reason: {r.text[:200]}')