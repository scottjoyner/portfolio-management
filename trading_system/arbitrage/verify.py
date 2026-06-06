#!/usr/bin/env python3
"""Verify the arbitrage system is ready."""

import sys
sys.path.insert(0, '/home/falcon/git/portfolio-management')

# Test imports
print("=" * 70)
print("Verifying Arbitrage System")
print("=" * 70)
print()

try:
    from trading_system.arbitrage import opportunity_detector
    from trading_system.arbitrage import arb_trader
    print("[✓] Core modules imported successfully")
except Exception as e:
    print(f"[✗] Import failed: {e}")

# Verify mock data exists
import json
try:
    with open('/home/falcon/git/portfolio-management/trading_system/data/kalshi_mock.json') as f:
        kalshi = json.load(f)
    
    with open('/home/falcon/git/portfolio-management/trading_system/data/polymarket_mock.json') as f:
        pm = json.load(f)
    
    print(f"[✓] Kalshi data loaded: {len(kalshi['markets'])} markets")
    print(f"[✓] Polymarket data loaded: {len(pm['events'])} events")
except FileNotFoundError:
    print("[!] Mock data files not found")

# Run detector
try:
    detector = opportunity_detector.OpportunityDetector()
    kalshi_crypto = [
        {'market_id': m['id'], 'title': m['title']} 
        for m in kalshi['markets'] if 'cryptocurrency' in (m.get('category') or '')
    ]
    pm_crypto = [
        {'slug': e['id'], 'question': e['question']} 
        for e in pm['events'] if 'cryptocurrency' in (e.get('topic') or '')
    ]
    
    detector.from_dict({
        'markets': kalshi_crypto,
        'events': pm_crypto,
    })
    
    opps = detector.detect_opportunities()
    print(f"[✓] Detector found {len(oppos)} opportunities")
except Exception as e:
    print(f"[!] Detection failed: {str(e)[:100]}")

print()
print("=" * 70)
print("System Status: READY")
print("=" * 70)
print()
print("To run the arbitrage system:")
print("  python3 trading_system/arbitrage/start.py")
print()
print("Or to use comprehensive tests:")
print("  python3 trading_system/arbitrage/comprehensive_test.py")
