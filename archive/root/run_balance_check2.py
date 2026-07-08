#!/usr/bin/env python3
"""
Direct balance check using Coinbase Commerce API v2 endpoint
"""
import os, dotenv, subprocess
from pathlib import Path

dotenv.load_dotenv('.env')

print('='*60)
print('=== COINBASE BALANCE CHECK ===')
print('='*60 + '\n')

api_key = os.environ.get('COINBASE_API_KEY', '')
api_secret = os.environ.get('COINBASE_API_SECRET', '')

print(f'Using credentials from .env...\n')

# Run balance checker via subprocess and capture output
result = subprocess.run(
    ['/home/falcon/git/portfolio-management/.venv/bin/python', '/home/falcon/git/portfolio-management/check_coinbase_balances.py'],
    capture_output=True, text=True, timeout=90
)

print(result.stdout)
if result.stderr:
    print('Stderr:', result.stderr[:500])