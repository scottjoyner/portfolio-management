#!/usr/bin/env python3
"""Debug opportunity detection to understand why no matches are found."""

import sys
sys.path.insert(0, '/home/falcon/git/portfolio-management')

from trading_system.arbitrage.opportunity_detector import OpportunityDetector
import json

# Load mock data
with open('/home/falcon/git/portfolio-management/trading_system/data/kalshi_mock.json', 'r') as f:
    kalshi_data = json.load(f)

with open('/home/falcon/git/portfolio-management/trading_system/data/polymarket_mock.json', 'r') as f:
    polymarket_data = json.load(f)

kalshi_markets = kalshi_data['markets']
polymarket_events = polymarket_data['events']

print("=" * 70)
print("Debugging Opportunity Detection")
print("=" * 70)
print()

# Check what markets we have
print("Kalshi Markets:")
for m in kalshi_markets:
    print(f"  {m['market_id']}: {m['title'][:60]}... @ {m['bid']}%")

print()
print("Polymarket Events:")
for e in polymarket_events:
    print(f"  {e['slug']}: {e['question'][:50]}... @ {e['bid']}%")

print()

# Create detector and try matching
detector = OpportunityDetector()

# Test market-by-market
kalshi_crypto = [m for m in kalshi_markets if 'cryptocurrency' in (m.get('category') or '')]
pm_crypto = [e for e in polymarket_events if 'cryptocurrency' in (e.get('category') or '')]

print(f"Kalshi crypto markets: {len(kalshi_crypto)}")
for m in kalshi_crypto:
    print(f"  - {m['market_id']}: {m['title']}")

print(f"Polymarket crypto events: {len(pm_crypto)}")
for e in pm_crypto:
    print(f"  - {e['slug']}: {e['question']}")

print()

# Test similarity function manually
if kalshi_crypto and pm_crypto:
    km = kalshi_crypto[0]
    pm = pm_crypto[0]
    
    title1 = km['title'].lower()
    title2 = pm['question'].lower()
    
    print(f"Kalshi title: {km['title']}")
    print(f"Polymarket question: {pm['question']}")
    
    # Normalize and check similarity
    kalshi_normalized = ' '.join(title1.split())
    pm_normalized = ' '.join(title2.split())
    
    import difflib
    matcher = difflib.SequenceMatcher(None, kalshi_normalized, pm_normalized)
    similarity = matcher.ratio()
    
    print(f"Similarity: {similarity:.2%}")
    print(f"Threshold: 75%")
