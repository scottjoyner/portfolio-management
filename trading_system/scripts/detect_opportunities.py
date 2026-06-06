#!/usr/bin/env python3
"""Detect and report arbitrage opportunities between Kalshi and Polymarket."""

import sys
sys.path.insert(0, '/home/falcon/git/portfolio-management')

from trading_system.arbitrage.opportunity_detector import OpportunityDetector
import json


def main():
    """Main entry point for opportunity detection."""
    
    print("=" * 80)
    print("Kalshi <-> Polymarket Arbitrage Opportunity Detection")
    print("=" * 80)
    print()
    
    # Load mock data for testing
    kalshi_data = json.load(open('/home/falcon/git/portfolio-management/trading_system/data/kalshi_mock.json'))
    polymarket_data = json.load(open('/home/falcon/git/portfolio-management/trading_system/data/polymarket_mock.json'))
    
    # Create detector and detect opportunities
    detector = OpportunityDetector()
    detector.from_dict({
        'markets': kalshi_data['markets'],
        'events': polymarket_data['events']
    })
    
    opportunities = detector.detect_opportunities()
    
    if not opportunities:
        print("No arbitrage opportunities detected.")
        return 0
    
    # Print all opportunities
    print(f"Found {len(opportunities)} opportunity(ies):\n")
    print("-" * 80)
    print(f"{'Rank':<6} {'Kalshi Market':<50} {'Polymarket Event':<50}")
    print(f"{'Rank':<6} {'Price':<10} {'Divergence':<12} {'Potential Return':<18}")
    print("-" * 80)
    
    for op in opportunities:
        kalshi_title = f"{op.kalshi_market_id[:40]}..." if len(op.kalshi_market_id) > 40 else op.kalshi_market_id
        pm_title = f"{op.polymarket_slug[:35]}..." if len(op.polymarket_slug) > 35 else op.polymarket_slug
        
        print(f"{list(opportunities).index(op)+1:<6} {kalshi_title:<50} {pm_title:<50}")
        print(f"{'':<6} {op.kalshi_price:.4%:<10} {op.divergence*100:>7.2f}%{'':<4} {op.arbitrage_potential_pct:.2%}")
        print()
    
    # Save to JSON for later use
    output_file = '/home/falcon/git/portfolio-management/trading_system/data/opportunities.json'
    with open(output_file, 'w') as f:
        json.dump(detector.to_dict(), f, indent=2)
    
    print(f"Output saved to {output_file}")
    return 0


if __name__ == '__main__':
    sys.exit(main())
