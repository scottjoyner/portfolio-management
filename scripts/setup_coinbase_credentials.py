#!/usr/bin/env python3
"""
Fixed Coinbase CLI credential setup with proper key extraction.
Handles both normal setup and Linux keychain-less environments.
"""

import json
import sys
import subprocess
from pathlib import Path
import os


def get_key_id_from_name(name: str) -> str | None:
    """Extract key ID from CDP key name (format: organizations/XXX/apiKeys/YYY)."""
    parts = name.split('/')
    if len(parts) >= 4 and parts[-2] == 'apiKeys':
        return parts[-1]
    return None


def setup_coinbase_env(key_file_path: str, allow_plaintext: bool = True):
    """Configure Coinbase CLI with credentials from CDP API key JSON."""
    
    key_file = Path(key_file_path)
    if not key_file.exists():
        print(f"❌ Key file not found: {key_file_path}")
        return False
    
    try:
        with open(key_file, 'r') as f:
            key_data = json.load(f)
    except json.JSONDecodeError as e:
        print(f"❌ Invalid JSON: {e}")
        return False
    
    name = key_data.get('name', '')
    private_key = key_data.get('privateKey', '').strip()
    
    if not name or not private_key:
        print("❌ Key file missing 'name' or 'privateKey' fields")
        return False
    
    key_id = get_key_id_from_name(name)
    if not key_id:
        print(f"❌ Could not extract key ID from name: {name}")
        return False
    
    org_id = name.split('/')[1] if '/' in name else "unknown"
    
    print("\n" + "="*60)
    print("🔑 COINBASE CLI CREDENTIAL SETUP")
    print("="*60)
    print(f"\n📍 Organization ID: {org_id}")
    print(f"📍 API Key ID: {key_id}")
    
    # Set environment variables for the CLI
    env = os.environ.copy()
    env['COINBASE_API_KEY'] = key_id
    
    try:
        result = subprocess.run(
            ['coinbase', 'env', 'live', 
             '--key-id', key_id, 
             '--key-secret', private_key.strip(),
             *(['--allow-plaintext-secrets'] if allow_plaintext else [])],
            capture_output=True,
            text=True,
            timeout=30
        )
        
        if result.returncode == 0:
            print(f"\n✅ Coinbase CLI configured successfully")
            
            # Verify with balance check
            verify = subprocess.run(
                ['coinbase', 'balance'],
                capture_output=True,
                text=True,
                timeout=30
            )
            
            if verify.returncode == 0:
                print("✅ Connection verified - balances accessible")
                
                # Parse and display account summary
                try:
                    accounts = json.loads(verify.stdout).get('accounts', [])
                    print(f"\n💰 Found {len(accounts)} accounts:")
                    
                    # Show key holdings summary
                    for acc in accounts[:5]:  # First 5
                        name = acc.get('name', 'Unknown')
                        currency = acc.get('currency', '')
                        available = acc.get('available_balance', {}).get('value', '0')
                        print(f"   • {name}: {available} {currency}")
                    
                    if len(accounts) > 5:
                        print(f"   ... and {len(accounts) - 5} more")
                        
                except Exception:
                    pass
                
            return True
        else:
            print(f"\n❌ Setup failed: {result.stderr}")
            return False
            
    except subprocess.TimeoutExpired:
        print("❌ Setup timed out")
        return False
    except Exception as e:
        print(f"❌ Error: {e}")
        return False


if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("Usage: python3 setup_coinbase_credentials.py <path-to-cdp_api_key.json>")
        sys.exit(1)
    
    success = setup_coinbase_env(sys.argv[1])
    sys.exit(0 if success else 1)
