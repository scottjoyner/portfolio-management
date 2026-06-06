#!/usr/bin/env python3
"""
Coinbase Balance Check using Exchange API v3 with HMAC auth
"""
import os, dotenv, requests, hmac, hashlib, time
from pathlib import Path

dotenv.load_dotenv('.env')
api_key = os.environ.get('COINBASE_API_KEY', '')
api_secret = os.environ.get('COINBASE_API_SECRET', '')

print('='*60)
print('=== COINBASE BALANCE CHECK (Exchange v3) ===')
print('='*60 + '\n')

if not api_key or not api_secret:
    print('No credentials found in .env')
    exit(1)

print(f'API Key: {api_key}')
print(f'Secret: {api_secret[:30]}...\n')

# Get current timestamp from API endpoint first
timestamp = str(int(time.time()))
endpoint = '/v2/accounts'
method = 'GET'
body = ''  # Empty for GET

# Construct the signature string exactly as required:
sign_string = f"{timestamp}:{method}:{endpoint}{body}"
print(f'Signature string: {sign_string}\n')

import base64
secret_bytes = base64.b64decode(api_secret.encode())
signature = hmac.new(secret_bytes, sign_string.encode(), hashlib.sha256).hexdigest()

headers = {
    'CB-ACCESS-KEY': api_key,
    'CB-ACCESS-SIGN': signature,
    'CB-ACCESS-TIMESTAMP': timestamp,
}

print(f'Sending to https://api.coinbase.com{endpoint}\n')
r = requests.get(f'https://api.coinbase.com{endpoint}', headers=headers, timeout=30)

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
    print(f'Reason: {r.text[:500]}')