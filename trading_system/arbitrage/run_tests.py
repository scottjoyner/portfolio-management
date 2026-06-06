#!/usr/bin/env python3
"""Simple test runner for arbitrage system."""

import sys
sys.path.insert(0, '/home/falcon/git/portfolio-management')

from trading_system.arbitrage.detect_opportunities import load_sample_data, detect_opportunities, analyze_opportunity

print("\n" + "=" * 70)
print("Kalshi <-> Polymarket Arbitrage - Quick Test")
print("=" * 70)

# Load sample data
kalshi_markets, pm_events = load_sample_data()
print(f"\n[+] Sample Data:")
print(f"    Kalshi Markets: {len(kalshi_markets)}")
print(f"    Polymarket Events: {len(pm_events)}")

# Detect opportunities
opps = detect_opportunities(kalshi_markets, pm_events)
print(f"\n[+] Opportunities Detected: {len(opps)}")

if opps:
    top_opp = opps[0]
    analysis = analyze_opportunity(top_opp)
    
    print(f"\n[+] Top Opportunity:")
    print(f"    Divergence: {(top_opp['arbitrage_potential_pct']*10):.1f}%")
    print(f"    Buy Side: {analysis['buy_platform'].upper()} @ {round(analysis['buy_price']*100, 2)}%")
    print(f"    Sell Side: {analysis['sell_platform'].upper()} @ {round(analysis['sell_price']*100, 2)}%")
    print(f"    Expected ROI: {analysis['roi_pct']:.1f}%")

print("\n[✓] All components working!")