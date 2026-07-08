#!/usr/bin/env python3
import os, dotenv, requests
from pathlib import Path

dotenv.load_dotenv('.env')
api_key = os.environ.get('COINBASE_API_KEY', '')
api_secret = os.environ.get('COINBASE_API_SECRET', '')

print('='*60)
print('=== COINBASE BALANCE CHECK ===')
print('='*60 + '\n')
print(f'API Key: {api_key}')
print(f'Secret: {api_secret[:30]}...\n')

auth_string = f"{api_key}:{api_secret}"
import base64 as b
credentials = b.b64encode(auth_string.encode()).decode()

headers = {'Authorization': f'Basic {credentials}'}
print('Making request to v2/accounts...\n')
r = requests.get('https://api.coinbase.com/v2/accounts', headers=headers, timeout=30)

print(f'Status: {r.status_code}')
if r.ok:
    d = r.json()
    print(f'Accounts found: {len(d)}\n')
    for acc in d:
        currency = acc.get('currency', 'N/A').upper()
        balance = float(acc.get('balance', 0))
        if abs(balance) > 0.0001:
            print(f'{currency}: {balance:.8f}')
else:
    print(f'Reason: {r.text[:200]}')