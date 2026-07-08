#!/usr/bin/env python3
"""
Smoke Test Script for Data Sources

Verifies that all configured data sources are operational.
Outputs a summary report to stdout and saves detailed results to file.
"""

import asyncio
import sys
import json
import os
from datetime import datetime, timedelta

# Add src to path
sys.path.insert(0, '/home/scott/git/portfolio-management/src')

from sources.factory import DataSourceFactory
from backtest_engine import BacktestEngine


def main():
    """Run the smoke test."""
    print("="*60)
    print("DATA SOURCE SMOKE TEST")
    print("="*60)
    print(f"Started: {datetime.now().isoformat()}")
    print()
    
    # Initialize factory and engine
    factory = DataSourceFactory()
    engine = BacktestEngine(factory=factory)
    
    # Run smoke test
    results = asyncio.run(engine.run_smoke_test())
    
    # Print summary
    print()
    print("SUMMARY")
    print("-"*40)
    
    passed = results.get('passed', 0)
    failed = results.get('failed', 0)
    total = passed + failed
    
    if total > 0:
        success_rate = (passed / total) * 100
        status = "PASSED" if success_rate == 100 else "PARTIAL FAILURE"
    else:
        status = "NO SOURCES TESTED"
    
    print(f"Status: {status}")
    print(f"Passed: {passed}/{total} sources")
    print(f"Failed: {failed}/{total} sources")
    print()
    
    # Print details
    if results.get('details'):
        print("DETAILED RESULTS")
        print("-"*40)
        for name, detail in results['details'].items():
            icon = "✓" if detail.get('status') == 'ok' else "✗"
            print(f"  {icon} {name}: {detail.get('status', 'unknown')}")
            if detail.get('error'):
                print(f"      Error: {detail['error'][:100]}...")
    
    # Print source status
    print()
    print("SOURCE STATUS")
    print("-"*40)
    statuses = asyncio.run(engine.get_source_status())
    for name, status_info in statuses.items():
        icon = "✓" if status_info.get('status') == 'healthy' else "✗"
        print(f"  {icon} {name}: {status_info.get('status', 'unknown')}")
    
    # Save detailed results
    output_file = '/home/scott/git/portfolio-management/data/smoke_test_results.json'
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    
    with open(output_file, 'w') as f:
        json.dump(results, f, indent=2, default=str)
    
    print()
    print(f"Detailed results saved to: {output_file}")
    print("="*60)
    
    # Exit with appropriate code
    return 0 if failed == 0 else 1


if __name__ == '__main__':
    sys.exit(main())
