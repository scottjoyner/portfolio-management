#!/usr/bin/env python3
"""
Coinbase Auth Verification Script

Checks that Coinbase CLI authentication is working in container before restart.
Verifies:
  ✅ Coinbase CLI installed and configured
  ✅ JWT/ES256 authentication valid
  ✅ Can fetch balances from live environment
  ✅ Historical data download works
"""

import subprocess
from typing import Dict, Any
import json
import logging

logger = logging.getLogger(__name__)

def verify_coinbase_cli_installed() -> bool:
    """Check if Coinbase CLI is installed."""
    try:
        result = subprocess.run(
            ['coinbase', '--version'],
            capture_output=True,
            text=True,
            timeout=10
        )
        return result.returncode == 0
    except FileNotFoundError:
        logger.error("Coinbase CLI not found")
        return False

def verify_coinbase_auth() -> bool:
    """
    Verify Coinbase authentication is working.
    
    Returns:
        True if auth is valid and connected
    """
    try:
        result = subprocess.run(
            ['coinbase', 'balance', '-e', 'live'],
            capture_output=True,
            text=True,
            timeout=30
        )
        
        if result.returncode == 0:
            logger.info("Coinbase auth verified successfully")
            return True
        else:
            error_msg = result.stderr or result.stdout
            logger.error(f"Coinbase auth failed: {error_msg}")
            return False
    except Exception as e:
        logger.error(f"Auth verification error: {e}")
        return False

def verify_historical_data_download() -> bool:
    """
    Verify historical data download works.
    
    Returns:
        True if data can be downloaded successfully
    """
    try:
        result = subprocess.run(
            [
                'coinbase', 'products', 'candles',
                'BTC-USD',
                'granularity=1h',  # Fixed: use correct format
                '-e', 'live'
            ],
            capture_output=True,
            text=True,
            timeout=60
        )
        
        if result.returncode == 0:
            data = json.loads(result.stdout)
            candles = len(data.get('data', []))
            logger.info(f"Historical data download verified ({candles} candles)")
            return True
        else:
            error_msg = result.stderr or result.stdout
            logger.error(f"Historical data download failed: {error_msg}")
            return False
    except Exception as e:
        logger.error(f"Data download verification error: {e}")
        return False

def verify_container_environment() -> Dict[str, bool]:
    """
    Verify container environment is properly configured.
    
    Returns:
        Dict with verification results
    """
    checks = {
        'coinbase_cli_installed': verify_coinbase_cli_installed(),
        'coinbase_auth_valid': verify_coinbase_auth(),
        'historical_data_download': verify_historical_data_download()
    }
    
    return checks

def main():
    """
    Main verification function.
    Runs all checks and reports results.
    """
    logger.info("="*50)
    logger.info("Coinbase Auth Verification")
    logger.info("="*50)
    
    # Run all verifications
    results = verify_container_environment()
    
    # Print summary
    print("")
    print("Verification Results:")
    print("-" * 30)
    for check, passed in results.items():
        status = "PASS" if passed else "FAIL"
        print(f"{check}: {status}")
    
    # Overall result
    all_passed = all(results.values())
    print("-" * 30)
    if all_passed:
        print("")
        print("All verifications passed!")
        print("Coinbase auth is working and ready for restart.")
    else:
        print("")
        print("Some verifications failed.")
        print("Please run: python3 scripts/setup_coinbase_credentials.py <key_file.json>")
    
    return all_passed

if __name__ == '__main__':
    main()
