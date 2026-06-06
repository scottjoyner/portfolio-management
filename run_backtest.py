#!/usr/bin/env python3
"""
Portfolio Backtesting CLI - Easy-to-use command-line interface
Run backtests with different strategies and view results.
"""

import sys
import argparse


def main():
    parser = argparse.ArgumentParser(
        description='Portfolio Management Backtesting System',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""Examples:
  python run_backtest.py --strategy hold_all
  python run_backtest.py --strategy equal_weight --period monthly
  python run_backtest.py --data ./data/historical
"""
    )
    
    parser.add_argument(
        '--strategy',
        choices=['hold_all', 'equal_weight', 'market_timing'],
        default='hold_all',
        help='Backtesting strategy (default: hold_all)'
    )
    
    parser.add_argument(
        '--data',
        type=str,
        default='./data/historical',
        help='Path to historical data directory'
    )
    
    parser.add_argument(
        '--capital',
        type=float,
        default=100000.0,
        help='Starting capital in dollars (default: $100,000)'
    )
    
    parser.add_argument(
        '--verbose',
        '-v',
        action='store_true',
        help='Enable verbose output'
    )
    
    args = parser.parse_args()
    
    print("="*80)
    print("🚀 PORTFOLIO BACKTESTING CLI")
    print("="*80)
    print(f"\nStrategy: {args.strategy}")
    print(f"Capital: ${args.capital:,.2f}")
    print(f"Data: {args.data}\n")
    
    # Import and run backtester
    from portfolio_manager import Backtester
    
    backtester = Backtester()
    backtester.load_historical_data(args.data)
    results = backtester.run_backtest(strategy=args.strategy)
    
    return 0


if __name__ == '__main__':
    sys.exit(main())
