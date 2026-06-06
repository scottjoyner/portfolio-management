#!/usr/bin/env python3
"""Simple startup script for Kalshi <-> Polymarket arbitrage system."""

import sys
sys.path.insert(0, '/home/falcon/git/portfolio-management')
from trading_system.arbitrage.main import main


def check_api_keys():
    """Check if real API keys are configured."""
    import os
    
    has_kalshi = bool(os.getenv('KALSHI_API_KEY'))
    has_polymarket = bool(os.getenv('POLYMARKET_API_KEY'))
    
    return has_kalshi, has_polymarket


def main():
    """Simple entry point for the arbitrage system."""
    
    print("\n" + "=" * 70)
    print("Kalshi <-> Polymarket Arbitrage System")
    print("=" * 70)
    
    # Check if using real APIs or mock data
    has_kalshi, has_polymarket = check_api_keys()
    
    if has_kalshi and has_polymarket:
        mode = "REAL API MODE"
        print(f"\n[+] {mode}: Using live Kalshi and Polymarket APIs")
        print("       Make sure your API keys are valid and active!")
    else:
        mode = "MOCK DATA MODE"
        print(f"\n[-] {mode}: Running with mock data (development/testing)")
        print("       Set environment variables to use real APIs:")
        print("         export KALSHI_API_KEY=... POLYMARKET_API_KEY=...")
    
    print("=" * 70)
    
    # Run the main arbitrage system
    try:
        return main()
    except KeyboardInterrupt:
        print("\n[!] Interrupted by user")
        return 130
    except Exception as e:
        print(f"\n[✗] Error: {str(e)}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == '__main__':
    sys.exit(main())
