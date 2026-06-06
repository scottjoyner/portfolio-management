#!/usr/bin/env python3
"""
Simple Helper to Add Your Existing Commerce API Keys

This script helps you securely add your existing Commerce API credentials
to the .env file so they can be used with the Consumer API.
"""

import os
import sys
sys.path.insert(0, '/home/falcon/git/portfolio-management')

from dotenv import load_dotenv
load_dotenv('/home/falcon/git/portfolio-management/.env')

print("=" * 70)
print("ADDING YOUR EXISTING COMMERCE API KEYS")
print("=" * 70)

# Check if .env exists and has Commerce keys
if os.path.exists('/home/falcon/git/portfolio-management/.env'):
    with open('/home/falcon/git/portfolio-management/.env', 'r') as f:
        content = f.read()
    
    if 'COMMERCE_API_KEY' in content and 'COMMERCE_API_SECRET' in content:
        print("\n✓ Commerce API credentials already exist in .env")
        print("\n[2/3] Current Configuration:")
        # Show that keys are present (without exposing them)
        lines = content.split('\n')
        for line in lines:
            if 'COMMERCE_API' in line:
                print(f"         {line.strip()}")
    else:
        print("\n⚠️  Commerce API credentials NOT found in .env")
else:
    print("\n[1/3] Creating new .env file...")

print("\n[2/3] Instructions to Add Your Existing Keys:")
print("   To connect to the Consumer API, you need to add your existing")
print("Commerce API keys to the .env file.")
print("\nAdd these lines to /home/falcon/git/portfolio-management/.env:")
print('   COMMERCE_API_KEY=***   COMMERCE_API_SECRET=*** Create a sample .env if it doesn't exist or is empty
if not os.path.exists('/home/falcon/git/portfolio-management/.env'):
    print("\n[3/3] Creating new .env file...")
    with open('/home/falcon/git/portfolio-management/.env', 'w') as f:
        f.write('# Commerce API Credentials\n')
        f.write('# Add your existing keys below:\n')
        f.write('COMMERCE_API_KEY=***        f.write('COMMERCE_API_SECRET=***    print("       ✓ New .env file created")
    print("\n[INFO] Please edit the .env file and add your actual keys.")
elif 'COMMERCE_API_KEY' not in content or 'COMMERCE_API_SECRET' not in content:
    print("\n[3/3] Adding Commerce API credentials to existing .env...")
    with open('/home/falcon/git/portfolio-management/.env', 'a') as f:
        f.write('\n# Commerce API Credentials (add your existing keys here)\n')
        f.write('COMMERCE_API_KEY=***        f.write('COMMERCE_API_SECRET=***    print("       ✓ Commerce API credentials added to .env")

print("\n" + "=" * 70)
print("NEXT STEPS:")
print("=" * 70)
print("1. Edit /home/falcon/git/portfolio-management/.env")
print("2. Add your existing Commerce API keys")
print("3. Run the balance fetch script again")
print("4. The Consumer API will use these keys for authentication")
print("=" * 70)