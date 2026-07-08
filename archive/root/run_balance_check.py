#!/usr/bin/env python3
"""Simple Coinbase balance check using existing code pattern"""
import os
from pathlib import Path
import dotenv

# Load .env file
env_file = '/home/falcon/git/portfolio-management/.env'
dotenv.load_dotenv(str(env_file))

print('='*60)
print('=== COINBASE BALANCE CHECK ===')
print('='*60 + '\n')

api_key = os.environ.get('COINBASE_API_KEY', '')
api_secret = os.environ.get('COINBASE_API_SECRET', '')

print(f'API Key: {api_key}')
print(f'Secret: {api_secret[:30]}...')
print()

# Run the balance script via subprocess and capture output
import subprocess
result = subprocess.run(
    ['/home/falcon/git/portfolio-management/.venv/bin/python', '/home/falcon/git/portfolio-management/check_coinbase_balances.py'],
    capture_output=True, text=True, timeout=90
)

if result.stdout:
    print(result.stdout)
if result.stderr and result.returncode != 0:
    print('Stderr:')
    print(result.stderr[:1500])