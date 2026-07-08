#!/usr/bin/env python3
"""
Coinbase Read-Only Balance Check - Simple Working Version
"""

import subprocess


def main():
    """Check Coinbase balances using production API"""
    
    print("\n" + "="*80)
    print("💰 FETCHING COINBASE READ-ONLY ACCOUNT BALANCES")
    print("="*80)
    
    # Read API keys from .env file - handle with basic string reading
    env_file = '/home/falcon/git/portfolio-management/.env'
    
    coinbase_key = ""
    coinbase_secret = ""
    
    try:
        with open(env_file, 'r') as f:
            lines = f.readlines()
        
        for line in lines:
            if line.startswith('COINBASE_API_KEY='):
                parts = line.strip().split('=', 1)
                if len(parts) > 1:
                    coinbase_key = parts[1].strip()
            elif line.startswith('COINBASE_API_SECRET='):
                parts = line.strip().split('=', 1)
                if len(parts) > 1:
                    coinbase_secret = parts[1].strip()
    
    except Exception as e:
        print(f"\n⚠️ Error reading .env file: {e}")
        return False
    
    # Verify keys are present
    if not coinbase_key or not coinbase_secret:
        print("\n⚠️ Could not find Coinbase API keys in .env file")
        print("   Expected format:")
        print("     COINBASE_API_KEY=pk_live_xxxxxxxxxxxxxx")
        print("     COINBASE_API_SECRET=xxxxxxxxxxxxxxxx")
        return False
    
    # Write the actual API call script to a temp file
    balance_script_file = '/tmp/check_coinbase_balances_simple.py'
    
    with open(balance_script_file, 'w') as f:
        # First part - imports and header setup
        f.write('import requests\n')
        f.write('import base64\n')
        f.write('\n')
        
        # Write API keys directly (not from env args to avoid string escaping issues)
        f.write(f'api_key = "{coinbase_key}"\n')
        f.write(f'api_secret = "{coinbase_secret}"\n')
        f.write('\n')
        
        f.write('# Headers for API authentication\n')
        f.write('auth_string = api_key + ":" + api_secret\n')
        f.write('credentials = base64.b64encode(auth_string.encode()).decode()\n')
        f.write('\n')
        f.write('headers = {\n')
        f.write(f'    "Authorization": f"Basic {{credentials}}",\n')
        f.write('    "Content-Type": "application/json",\n')
        f.write('    "User-Agent": "Python-Coinbase/1.0",\n')
        f.write('    "Accept": "application/json"\n')
        f.write('}\n')
        f.write('\n')
        
        f.write('def fetch_account_balances():\n')
        f.write('    """Fetch Coinbase Commerce account balances"""\n')
        f.write('\n')
        f.write('    # Production API for live accounts\n')
        f.write('    base_url = "https://api.coinbase.com/commerce/v2/accounts"\n')
        f.write('\n')
        f.write('    print("Connecting to Coinbase PRODUCTION API...")\n')
        f.write('\n')
        f.write('    try:\n')
        f.write('        response = requests.get(base_url, headers=headers, timeout=15)\n')
        f.write('\n')
        f.write('        if response.status_code == 200:\n')
        f.write('            print("\\nConnected successfully!")\n')
        f.write('\n')
        f.write('            data = response.json()\n')
        f.write("            accounts = data.get('data', [])\n")
        f.write('\n')
        f.write('            if not accounts:\n')
        f.write('                print("No commerce accounts found")\n')
        f.write('                return\n')
        f.write('\n')
        f.write('            for account in accounts:\n')
        f.write('                resource_path = account.get(\'resource_path\')\n')
        f.write('                name = account.get(\'name\', \'Commerce Account\')\n')
        f.write('                currency = account.get(\'primary_currency\', \'N/A\')\n')
        f.write('\n')
        f.write('                print(f"Account: {name}")\n')
        f.write('                print(f"  Currency: {currency.upper()}")\n')
        f.write('\n')
        f.write('                # Fetch balances for this account\n')
        f.write('                if resource_path:\n')
        f.write("                    balances_url = f'https://api.coinbase.com/commerce/v2/accounts/{resource_path}/balances'\n")
        f.write('\n')
        f.write('                    try:\n')
        f.write('                        balances_resp = requests.get(balances_url, headers=headers, timeout=10)\n')
        f.write('\n')
        f.write('                        if balances_resp.status_code == 200:\n')
        f.write('                            balances_data = balances_resp.json()\n')
        f.write("                            assets = balances_data.get('data', [])\n")
        f.write('\n')
        f.write('                            print(f"BALANCES ({len(assets)} assets):")\n')
        f.write('\n')
        f.write('                            for asset in assets:\n')
        f.write('                                curr = asset.get(\'currency\')\n')
        f.write("                                avail = float(asset.get('available', 0)) if asset.get('available') else 0\n")
        f.write('\n')
        f.write('                                print(f"  {curr}: Available: {avail:.8f}")\n')
        f.write('\n')
        f.write('                        elif balances_resp.status_code == 404:\n')
        f.write('                            print("No balance data for this account")\n')
        f.write('\n')
        f.write('                    except Exception as e:\n')
        f.write('                        print(f"Error: {str(e)[:100]}")\n')
        f.write('\n')
        f.write('    except Exception as e:\n')
        f.write('        print(f"API error: {str(e)[:150]}")\n')
        f.write('\n')
        f.write('# Execute the balance fetch\n')
        f.write('fetch_account_balances()\n')
        f.write('\n')
        f.write('print("\\nBALANCE CHECK COMPLETE!")\n')
    
    # Run the balance fetch script
    try:
        result = subprocess.run(
            ['/home/falcon/git/portfolio-management/.venv/bin/python', balance_script_file],
            capture_output=True, text=True, timeout=90
        )
        
        if result.stdout:
            print(result.stdout)
        
        if result.stderr and result.returncode != 0:
            print("\nStderr output:")
            print(result.stderr[:1500])
            
    except subprocess.TimeoutExpired:
        print("\n⚠️ Script timed out after 90 seconds")


if __name__ == "__main__":
    main()
