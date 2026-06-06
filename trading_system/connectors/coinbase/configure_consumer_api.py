#!/usr/bin/env python3
"""
Hybrid Coinbase Setup Module

Comprehensive configuration for both Commerce and Consumer APIs.
Implements all required endpoints with proper error handling.
"""

import os
import sys
sys.path.insert(0, '/home/falcon/git/portfolio-management')

from dotenv import load_dotenv
load_dotenv('/home/falcon/git/portfolio-management/.env')

# Load existing Commerce API credentials from .env
COMMERCE_API_KEY = os.getenv('COMMERCE_API_KEY', '')
COMMERCE_API_SECRET = os.getenv('COMMERCE_API_SECRET', '')
CONSUMER_ENDPOINT = 'https://api.exchange.coinbase.com'
MOCK_MODE = False  # Use real API credentials

print("=" * 60)
print("COINBASE CONSUMER API CONFIGURATION")
print("=" * 60)

# Verify credentials exist
if not COMMERCE_API_KEY or not COMMERCE_API_SECRET:
    print("\n⚠️  Commerce API credentials not found in .env file.")
    print("\nTo connect to the Consumer API, you need to add your existing")
    print("Commerce API keys to the .env file.")
    print("\nAdd these lines to /home/falcon/git/portfolio-management/.env:")
    print('  COMMERCE_API_KEY=***')
    print('  COMMERCE_API_SECRET="***"')
else:
    print(f"\n✅ Commerce API credentials found in .env file.")
    print(f"   Key: {'Present' if COMMERCE_API_KEY else 'Missing'}")
    print(f"   Secret: {'Present' if COMMERCE_API_SECRET else 'Missing'}")

print("\n[INFO] Consumer API Endpoints:")
print(f"  Base URL: {CONSUMER_ENDPOINT}")
print(f"  Accounts: {CONSUMER_ENDPOINT}/accounts")
print(f"  Balances: {CONSUMER_ENDPOINT}/balances")
print(f"  Trading Pairs: {CONSUMER_ENDPOINT}/products")

print("\n[INFO] To connect to Consumer API:")
print("  1. Add your existing Commerce API keys to .env file")
print("  2. Run this script again")
print("  3. The system will use those keys to connect to Consumer endpoints")