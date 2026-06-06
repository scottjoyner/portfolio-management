#!/usr/bin/env python3
"""
Coinbase v3 Balance Check using JWT/ES256 authentication
Requires: pip install jwt cryptography
"""
import os, dotenv, requests, json, time
from pathlib import Path

dotenv.load_dotenv('.env')
api_key = os.environ.get('COINBASE_API_KEY', '')
orc_id = os.environ.get('ORGANIZATION_ID', '')
key_path = os.environ.get('COINBASE_PRIVATE_KEY_PATH', '')

print('='*60)
print('=== COINBASE v3 BALANCE CHECK (JWT/ES256) ===')
print('='*60 + '\n')

if not api_key or not key_path:
    print('API key or private key path not configured in .env')
    exit(1)

print(f'Using v3 API credentials:')
print(f'  Organization: {orc_id}')
print(f'  API Key ID: {api_key}')
print(f'  Private Key: {key_path}\n')

# Load private key (redacted in file, so we'll use a placeholder for demo)
private_key = 'YOUR_PRIVATE_KEY_HERE'  # Would need actual private key content

if not private_key or private_key == 'YOUR_PRIVATE_KEY_HERE':
    print('Private key is redacted in the JSON file. You\ll need to extract it manually.')
    print('The v3 API requires a real private key for JWT signing.')
    exit(1)

# Since we can\'t sign without the real key, let\'s just show what endpoint will be called
endpoint = '/api/v3/brokerage/accounts'
method = 'GET'

print(f'This would send a request to https://api.coinbase.com{endpoint}')
print('The request would use JWT authentication with ES256 signature.')
print('You\'ll need the actual private key content from your downloaded file.')