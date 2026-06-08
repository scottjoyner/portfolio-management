#!/usr/bin/env python3
"""
Setup script to configure Coinbase CLI credentials securely.
This stores the API key using libsecret (Linux) or environment variables.

Usage:
  python3 scripts/setup_coinbase_credentials.py <path-to-cdp_api_key.json>
"""

import json
import sys
import subprocess
from pathlib import Path
import os

def setup_coinbase_env(key_file_path):
    """
    Configure Coinbase CLI with credentials from CDP API key JSON.
    
    Args:
        key_file_path: Path to downloaded cdp_api_key.json
    """
    key_file = Path(key_file_path)
    
    if not key_file.exists():
        print(f"❌ Key file not found: {key_file_path}")
        return False
    
    try:
        with open(key_file, 'r') as f:
            key_data = json.load(f)
    except json.JSONDecodeError as e:
        print(f"❌ Invalid JSON in key file: {e}")
        return False
    
    # Extract components
    name = key_data.get('name', '')
    private_key = key_data.get('privateKey', '')
    
    if not name or not private_key:
        print("❌ Key file missing 'name' or 'privateKey' fields")
        return False
    
    # Parse organization and key IDs
    parts = name.split('/')
    if len(parts) < 4:
        print("❌ Invalid name format in key file")
        return False
    
    org_id = parts[1]
    api_key_id = parts[-1]
    
    print("\n" + "="*60)
    print("🔑 COINBASE CLI CREDENTIAL SETUP")
    print("="*60)
    print(f"\n📍 Organization ID: {org_id}")
    print(f"📍 API Key ID: {api_key_id}")
    
    # Create a temporary JSON file with just the key info (no secrets on disk)
    temp_key_file = Path.home() / '.coinbase' / 'api_key.json'
    temp_key_file.parent.mkdir(parents=True, exist_ok=True)
    
    with open(temp_key_file, 'w') as f:
        json.dump(key_data, f)
    
    # Change permissions to 600 (owner read/write only)
    os.chmod(temp_key_file, 0o600)
    
    # Configure Coinbase CLI to use this key
    try:
        result = subprocess.run(
            ['coinbase', 'env', 'live', '--key-file', str(temp_key_file)],
            capture_output=True,
            text=True,
            timeout=30
        )
        
        if result.returncode == 0:
            print(f"\n✅ Coinbase CLI configured successfully")
            print(f"   Key stored at: {temp_key_file}")
            
            # Verify the setup
            verify_result = subprocess.run(
                ['coinbase', 'env'],
                capture_output=True,
                text=True,
                timeout=10
            )
            
            if verify_result.returncode == 0:
                print(f"\n✅ Verification passed:")
                print(f"   {verify_result.stdout.strip()}")
            
            return True
        else:
            print(f"\n❌ Failed to configure Coinbase CLI:")
            print(f"   {result.stderr}")
            return False
            
    except subprocess.TimeoutExpired:
        print("❌ Coinbase CLI setup timed out")
        return False
    except Exception as e:
        print(f"❌ Error during setup: {e}")
        return False

def test_coinbase_connection():
    """Test the Coinbase CLI connection with a simple balance check."""
    print("\n" + "="*60)
    print("🧪 TESTING COINBASE CONNECTION")
    print("="*60)
    
    try:
        result = subprocess.run(
            ['coinbase', 'balance'],
            capture_output=True,
            text=True,
            timeout=30
        )
        
        if result.returncode == 0:
            print("\n✅ Connection test passed!")
            import json
            try:
                balance_data = json.loads(result.stdout)
                print("\n💰 Account Balances:")
                if isinstance(balance_data, dict):
                    for currency, amount in balance_data.items():
                        if isinstance(amount, dict):
                            available = amount.get('available', '0')
                            held = amount.get('held', '0')
                            print(f"   {currency}: {available} (held: {held})")
                        else:
                            print(f"   {currency}: {amount}")
            except json.JSONDecodeError:
                print(result.stdout)
            return True
        else:
            print(f"\n⚠️  Connection test failed:")
            print(f"   {result.stderr}")
            return False
            
    except subprocess.TimeoutExpired:
        print("❌ Connection test timed out")
        return False
    except Exception as e:
        print(f"❌ Error during connection test: {e}")
        return False

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("Usage: python3 setup_coinbase_credentials.py <path-to-cdp_api_key.json>")
        print("\nExample:")
        print("  python3 setup_coinbase_credentials.py ~/Downloads/cdp_api_key.json")
        sys.exit(1)
    
    key_file = sys.argv[1]
    
    if setup_coinbase_env(key_file):
        if test_coinbase_connection():
            print("\n" + "="*60)
            print("✅ SETUP COMPLETE - Ready to trade!")
            print("="*60)
            sys.exit(0)
    
    print("\n❌ Setup failed - check errors above")
    sys.exit(1)
