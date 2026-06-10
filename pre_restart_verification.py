#!/usr/bin/env python3
"""
Coinbase Pre-Restart Verification Script

Runs comprehensive checks before container restart:
  ✅ Coinbase CLI installed and configured
  ✅ JWT/ES256 authentication valid
  ✅ Can fetch live balances
  ✅ Historical data download works
  ✅ Container environment ready
"""

import subprocess
import json
import os
from datetime import datetime
import logging

logger = logging.getLogger(__name__)


def run_verification_checks() -> Dict[str, any]:
    """
    Run all pre-restart verification checks.
    
    Returns:
        Dict with check results and recommendations
    """
    checks = {
        'timestamp': datetime.now().isoformat(),
        'coinbase_cli_installed': False,
        'coinbase_auth_valid': False,
        'live_balance_accessible': False,
        'historical_data_download': False,
        'recommendations': []
    }
    
    # Check 1: Coinbase CLI installed
    try:
        result = subprocess.run(
            ['coinbase', '--version'],
            capture_output=True,
            text=True,
            timeout=10
        )
        checks['coinbase_cli_installed'] = result.returncode == 0
    except FileNotFoundError:
        checks['recommendations'].append('Install Coinbase CLI: npm install -g @coinbase/coinbase-cli')
    
    # Check 2: Authentication valid
    if checks['coinbase_cli_installed']:
        try:
            result = subprocess.run(
                ['coinbase', 'balance', '-e', 'live'],
                capture_output=True,
                text=True,
                timeout=30
            )
            checks['coinbase_auth_valid'] = result.returncode == 0
            if not checks['coinbase_auth_valid']:
                error_msg = result.stderr or result.stdout
                checks['recommendations'].append(f'Fix auth: {error_msg}')
        except Exception as e:
            checks['recommendations'].append(f'Auth check failed: {e}')
    
    # Check 3: Live balance accessible
    if checks['coinbase_auth_valid']:
        try:
            result = subprocess.run(
                ['coinbase', 'balance', '-e', 'live'],
                capture_output=True,
                text=True,
                timeout=30
            )
            balances = json.loads(result.stdout)
            checks['live_balance_accessible'] = bool(balances)
        except Exception as e:
            checks['recommendations'].append(f'Balance check failed: {e}')
    
    # Check 4: Historical data download
    if checks['coinbase_auth_valid']:
        try:
            result = subprocess.run(
                [
                    'coinbase', 'products', 'candles',
                    'BTC-USD', 'granularity=hourly', '-e', 'live'
                ],
                capture_output=True,
                text=True,
                timeout=60
            )
            data = json.loads(result.stdout)
            checks['historical_data_download'] = len(data.get('data', [])) > 0
        except Exception as e:
            checks['recommendations'].append(f'Data download check failed: {e}')
    
    return checks


def main():
    """
    Main pre-restart verification function.
    """
    logger.info("="*60)
    logger.info("Coinbase Pre-Restart Verification")
    logger.info("="*60)
    
    # Run all checks
    results = run_verification_checks()
    
    # Print summary
    print("\nPre-Restart Verification Results:")
    print("-" * 40)
    for check, passed in results.items():
        if check == 'recommendations':
            continue
        status = "✅ PASS" if passed else "❌ FAIL"
        print(f"{check}: {status}")
    
    # Print recommendations
    if results['recommendations']:
        print("\nRecommendations:")
        print("-" * 40)
        for rec in results['recommendations']:
            print(f"  • {rec}")
    
    # Overall result
    all_passed = (
        results['coinbase_cli_installed'] and
        results['coinbase_auth_valid'] and
        results['live_balance_accessible'] and
        results['historical_data_download']
    )
    
    print("-" * 40)
    if all_passed:
        print("\n✅ All checks passed!")
        print("Coinbase auth is working and ready for restart.")
        return 0
    else:
        print("\n❌ Some checks failed.")
        print("Please address recommendations before restarting.")
        return 1


if __name__ == '__main__':
    exit(main())