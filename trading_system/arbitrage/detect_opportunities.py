#!/usr/bin/env python3
"""
Standalone Arbitrage Opportunity Detector (No Dependencies Required)

This script demonstrates arbitrage detection between Kalshi and Polymarket.
It uses combined sample data to show realistic opportunity analysis.
Can be enhanced with real API calls or web scraping as needed.
"""

import json
import re
from datetime import datetime
from difflib import SequenceMatcher


def load_sample_data():
    """Load sample market data from both platforms."""
    
    # Kalshi sample markets (based on typical structure)
    kalshi_markets = [
        {
            'market_id': 'BTC-JAN31-100K',
            'title': 'Bitcoin will trade above $100,000 by January 31, 2025',
            'category': 'cryptocurrency',
            'ticker': 'bitcoin-price-100k',
            'bid_pct': 58.5,
        },
        {
            'market_id': 'BTC-FEB28-75K',
            'title': 'Bitcoin will trade above $75,000 by February 28, 2025',
            'category': 'cryptocurrency',
            'ticker': 'bitcoin-price-75k',
            'bid_pct': 71.8,
        },
        {
            'market_id': 'ELEC-PRES-2024',
            'title': 'Who will win the US Presidential Election in 2024',
            'category': 'elections',
            'ticker': 'us-president-winner',
            'bid_pct': 51.2,
        },
    ]
    
    # Polymarket sample events (based on typical structure)
    polymarket_events = [
        {
            'slug': 'bitcoin-100k-by-jan-31',
            'question': 'Will Bitcoin trade above $100,000 by January 31, 2025?',
            'category': 'cryptocurrency',
            'bid_pct': 46.8,
        },
        {
            'slug': 'bitcoin-75k-by-feb-28',
            'question': 'Will Bitcoin trade above $75,000 by February 28, 2025?',
            'category': 'cryptocurrency',
            'bid_pct': 60.2,
        },
        {
            'slug': 'us-president-winner-2024',
            'question': 'Who will win the US Presidential Election in 2024',
            'category': 'elections',
            'bid_pct': 51.5,
        },
    ]
    
    return kalshi_markets, polymarket_events


def normalize_string(s: str) -> str:
    """Normalize string for similarity comparison."""
    s = s.lower()
    s = ' '.join(s.split())
    s = re.sub(r'[^a-z0-9\s]', '', s)
    return s


def calculate_text_similarity(title1: str, title2: str) -> float:
    """Calculate text similarity between two strings."""
    import re
    
    norm1 = normalize_string(title1)
    norm2 = normalize_string(title2)
    
    sim = SequenceMatcher(None, norm1, norm2).ratio()
    return sim


def detect_opportunities(kalshi_markets, polymarket_events, 
                        similarity_threshold=0.75, min_divergence=0.01):
    """Detect arbitrage opportunities between Kalshi and Polymarket."""
    
    from difflib import SequenceMatcher
    
    opportunities = []
    
    for kalshi in kalshi_markets:
        title1 = kalshi['title']
        
        for pm in polymarket_events:
            title2 = pm['question']
            
            # Calculate similarity
            sim = calculate_text_similarity(title1, title2)
            
            if sim >= similarity_threshold:
                # Both platforms are offering similar markets
                
                # Parse prices (handle percentage or decimal formats)
                try:
                    kalshi_price = float(str(kalshi['bid_pct']) or '0') / 100
                    pm_bid = float(pm['bid_pct']) / 100
                except (ValueError, TypeError):
                    continue
                
                # Calculate price divergence
                divergence = abs(kalshi_price - pm_bid)
                
                if divergence >= min_divergence:
                    opp_id = f"{kalshi['market_id'][:20]}-{pm['slug'][:20]}"
                    
                    opportunities.append({
                        'id': opp_id,
                        'kalshi_market': kalshi,
                        'polymarket_event': pm,
                        'similarity': sim,
                        'kalshi_price': kalshi_price,
                        'polymarket_price': pm_bid,
                        'divergence': divergence,
                        'arbitrage_potential_pct': divergence * 100,
                        'timestamp': datetime.now().isoformat(),
                    })
    
    # Sort by potential return (highest first)
    opportunities.sort(key=lambda o: o['arbitrage_potential_pct'], reverse=True)
    
    return opportunities


def analyze_opportunity(opp):
    """Analyze an arbitrage opportunity and calculate expected returns."""
    
    # Handle both dict and simple tuple structures
    if isinstance(opp, tuple) or len(opp) == 0:
        return {
            'buy_platform': None,
            'sell_platform': None,
            'roi_pct': 0.0,
            'net_profit': 0.0,
        }
    
    kalshi_data = opp.get('kalshi', {}) if isinstance(opp, dict) else (opp[0] if isinstance(opp, (list, tuple)) else opp)
    pm_data = opp.get('polymarket_event', {}) if isinstance(opp, dict) else (opp[1] if len(opp) > 1 and isinstance(opp, (list, tuple)) else opp)
    
    # Extract prices from the opportunity data
    kalshi_price = float(str(kalshi_data.get('bid_pct') or '0') or '0') / 100
    pm_price = float(pm_data.get('bid_pct') or '50') / 100 if isinstance(pm_data, dict) else 0.5
    
    # Determine which platform is cheaper for buying
    if kalshi_price < pm_price:
        buy_platform = 'kalshi'
        buy_price = kalshi_price
        sell_platform = 'polymarket'
        # Sell the opposite outcome on Polymarket
        sell_price = 1.0 - (pm_price if opp['kalshi']['category'] == 'cryptocurrency' else 
                             float(str(opp['kalshi']['bid_pct']) or '0') / 100)
    else:
        buy_platform = 'polymarket'
        buy_price = pm_price
        sell_platform = 'kalshi'
        # Sell opposite on Kalshi  
        sell_price = 1.0 - kalshi_price
    
    # Calculate positions (assuming $5,000 investment per platform)
    position_size_usd = 5000
    contract_units = int(position_size_usd / buy_price)
    
    # Calculate expected profit
    if buy_platform == 'kalshi':
        buy_cost = contract_units * kalshi_price * 100  # $ per unit
        pm_fee_pct = 2.0  # Polymarket fees
    else:
        buy_cost = contract_units * pm_price * 100
        pm_fee_pct = 2.0
    
    if sell_platform == 'kalshi':
        sell_revenue = contract_units * sell_price * 100
        kls_fee_pct = 1.0  # Kalshi fees
    else:
        sell_revenue = contract_units * sell_price * 100
        kls_fee_pct = 1.0
    
    # Calculate fees and net profit
    buy_fees = buy_cost * (pm_fee_pct / 100 if sell_platform == 'polymarket' else kls_fee_pct / 100)
    sell_fees = sell_revenue * (kls_fee_pct / 100 if sell_platform == 'kalshi' else pm_fee_pct / 100)
    
    gross_profit = buy_cost - sell_revenue  # This will be negative for arbitrage (we want short side to have higher price)
    
    # Actually, for true arbitrage we buy low on one platform and sell high on the other
    if kalshi_price < pm_price:
        gross_profit = contract_units * (pm_price - kalshi_price) * 100
    else:
        gross_profit = contract_units * (kalshi_price - pm_price) * 100
    
    net_profit = gross_profit - buy_fees - sell_fees
    
    return {
        'buy_platform': buy_platform,
        'sell_platform': sell_platform,
        'buy_price': buy_price,
        'sell_price': sell_price,
        'position_size_usd': position_size_usd,
        'contract_units': contract_units,
        'buy_cost': round(buy_cost, 2),
        'sell_revenue': round(sell_revenue, 2),
        'gross_profit': round(gross_profit, 2),
        'buy_fees': round(buy_fees, 2),
        'sell_fees': round(sell_fees, 2),
        'net_profit': round(net_profit, 2),
        'roi_pct': (net_profit / buy_cost * 100) if buy_cost > 0 else 0,
    }


def main():
    """Main entry point for opportunity detection and analysis."""
    
    print("\n" + "=" * 80)
    print("Kalshi <-> Polymarket Arbitrage Opportunity Detector")
    print("=" * 80)
    
    # Load sample data
    print("\n[1/3] Loading Sample Market Data...")
    print("-" * 60)
    
    kalshi_markets, pm_events = load_sample_data()
    
    print(f"[+] Kalshi Markets: {len(kalshi_markets)}")
    for m in kalshi_markets[:2]:
        print(f"    • {m['title'][:70]}... @ {m['bid_pct']}%")
    
    print(f"\n[+] Polymarket Events: {len(pm_events)}")
    for e in pm_events[:2]:
        print(f"    • {e['question'][:70]}... @ {e['bid_pct']}%")
    
    # Detect opportunities
    print("\n[2/3] Detecting Arbitrage Opportunities...")
    print("-" * 60)
    
    opps = detect_opportunities(
        kalshi_markets=kalshi_markets,
        polymarket_events=pm_events,
        similarity_threshold=0.75,
        min_divergence=0.01,
    )
    
    if not opps:
        print("\n[!] No arbitrage opportunities detected")
        return 0
    
    print(f"\n[+] Found {len(opps)} opportunity(ies)")
    
    # Analyze and display top opportunities
    print("\n[3/3] Opportunity Analysis...")
    print("=" * 80)
    
    for opp in opps[:5]:  # Top 5
        analysis = analyze_opportunity(opp)
        
        print(f"\nOpportunity: {opp['id']}")
        print("-" * 60)
        print(f"  Kalshi Market:      {opp['kalshi']['market_id']}")
        print(f"    Price:            {opp['kalshi']['bid_pct']}% (Kalshi)")
        print(f"\n  Polymarket Event:   {opp['polymarket_event']['slug']}")  
        print(f"    Price:            {opp['polymarket_event']['bid_pct']}% (Polymarket)")
        print(f"\n  Divergence:         {(opp['arbitrage_potential_pct'])*10:.1f}%")
        
        print(f"\n  Trade Execution Plan:")
        print(f"    Buy Side:         {analysis['buy_platform'].upper()} @ {round(analysis['buy_price']*100, 2)}%")
        print(f"    Sell Side:        {analysis['sell_platform'].upper()} @ {round(analysis['sell_price']*100, 2)}%")
        
        print(f"\n  Position Size:      ${analysis['position_size_usd']:,.2f} per platform")
        print(f"    Contracts:        {analysis['contract_units']} units each side")
        
        print(f"\n  Financials:")
        print(f"    Buy Cost:         ${analysis['buy_cost']:,.2f}")
        print(f"    Sell Revenue:     ${analysis['sell_revenue']:,.2f}")
        print(f"    Gross Profit:     ${analysis['gross_profit']:,.2f}")
        print(f"    Fees (Total):     ${analysis['buy_fees'] + analysis['sell_fees']:.2f}")
        print(f"    Net Profit:       ${analysis['net_profit']:,.2f}")
        print(f"    ROI:              {analysis['roi_pct']:.1f}%")
        
        print("\n  Strategy: Buy on cheaper platform, sell opposite on more expensive platform")
    
    # Save results to file
    output_data = {
        'timestamp': datetime.now().isoformat(),
        'opportunities_detected': len(opps),
        'top_opportunities': opps[:5],
    }
    
    output_file = '/home/falcon/git/portfolio-management/trading_system/data/opportunity_analysis.json'
    with open(output_file, 'w') as f:
        json.dump(output_data, f, indent=2)
    
    print(f"\n[+] Results saved to {output_file}")
    
    # Print summary
    if opps:
        top_opp = opps[0]
        analysis = analyze_opportunity(top_opp)
        
        print("\n" + "=" * 80)
        print("SUMMARY")
        print("=" * 80)
        print(f"Total Opportunities Detected: {len(opps)}")
        print(f"Best Opportunity ROI:          {analysis['roi_pct']:.1f}%")
        print(f"Expected Net Profit (per trade): ${analysis['net_profit']:,.2f}")
        print("\nRecommended Actions:")
        print("  1. Set up VPS with US internet access (or VPN)")
        print("  2. Obtain Kalshi and Polymarket API keys for real-time trading")
        print("  3. Deploy to production: python3 trading_system/arbitrage/orchestrator.py")
    
    return 0


if __name__ == '__main__':
    import re
    sys.exit(main())
