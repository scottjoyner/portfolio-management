#!/usr/bin/env python3
import os, dotenv, requests, base64
from pathlib import Path

dotenv.load_dotenv('.env')
api_key = os.environ.get('COINBASE_API_KEY', '')
api_secret = os.environ.get('COINBASE_API_SECRET', '')

print('='*60)
print('=== TRYING VARIATION OF COINBIDE ENPOINTS ===')
print('='*60 + '\n')

auth_string = f"{api_key}:{api_secret}"
credentials = base64.b64encode(auth_string.encode()).decode()
headers = {'Authorization': f'Basic {credentials}'}

endpoints = [
    ('https://api.coinbase.com/v1/accounts', 'v1 Accounts'),
    ('https://api.coinbase.com/v2/accounts', 'v2 Accounts'),
    ('https://api.coinbase.com/commerce/v1/accounts', 'Commerce v1'),
    ('https://api.coinbase.com/commerce/v2/accounts', 'Commerce v2'),
    ('https://api.exchange.coinbase.com/accounts', 'Exchange Accounts'),
    ('https://api.exchange.coinbase.com/v3/accounts', 'Exchange v3 Accounts'),
]

for url, name in endpoints:
    print(f'Trying {name}: {url}\n')
    r = requests.get(url, headers=headers, timeout=10)
    print(f'Status: {r.status_code}')
    if r.ok and len(r.text) > 0:
        try:
            d = r.json()
            data = d if isinstance(d, list) else d.get('data', [])
            print(f'Result: {len(data)} items')
            for item in (data[:3] if isinstance(d, dict) else d[:3]):
                if isinstance(item, dict):
                    currency = item.get('currency', '')
                    for k,v in item.items():
                        if 'balance' in k.lower() or currency:
                            print(f'  {k}: {v}')
        except:
            pass
    print()

print('\n=== DONE ===')