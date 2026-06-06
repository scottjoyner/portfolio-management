#!/usr/bin/env python3
"""Mock data generator for Kalshi/Polymarket arbitrage testing.

Creates realistic mock market data for testing the opportunity detection
engine without requiring live API calls or credentials.

Usage:
    python scripts/mock_data_generator.py kalshi_polymarket
    
    Generates two JSON files:
    - trading_system/data/kalshi_mock.json
    - trading_system/data/polymarket_mock.json

Example output:
    {
      "markets": [
        {
          "market_id": "BTC-JAN31-100K",
          "title": "Bitcoin will trade above $100,000 by January 31, 2025",
          "bid": 48.5,
          "ask": 49.2,
          "volume": 5000
        },
        ...
      ]
    }

Run with: python scripts/mock_data_generator.py
"""

import json
from datetime import datetime, timedelta
import random


def generate_kalshi_markets():
    """Generate mock Kalshi markets."""
    
    # Bitcoin-related markets (use higher prices)
    bitcoin_markets = [
        {
            "market_id": "BTC-JAN31-100K",
            "title": "Bitcoin will trade above $100,000 by January 31, 2025",
            "bid": 58.5,
            "ask": 59.2,
            "volume": 8000,
            "category": "cryptocurrency",
            "status": "open"
        },
        {
            "market_id": "BTC-FEB28-75K",
            "title": "Bitcoin will trade above $75,000 by February 28, 2025",
            "bid": 71.8,
            "ask": 72.5,
            "volume": 6000,
            "category": "cryptocurrency",
            "status": "open"
        },
    ]
    
    # Election markets (high divergence from Polymarket)
    election_markets = [
        {
            "market_id": "ELECTION-TRUMP-BIDEN",
            "title": "Will Trump win the 2024 US Presidential election?",
            "bid": 61.8,
            "ask": 62.5,
            "volume": 8000,
            "category": "politics",
            "status": "open"
        },
        {
            "market_id": "ELECTION-BIDEN-TRUMP",
            "title": "Will Biden win the 2024 US Presidential election?",
            "bid": 57.5,
            "ask": 58.2,
            "volume": 8000,
            "category": "politics",
            "status": "open"
        },
    ]
    
    # Climate markets
    climate_markets = [
        {
            "market_id": "CLIMATE-2024-TEMP",
            "title": "Will average global temperature in 2024 exceed 1.5°C above pre-industrial?",
            "bid": 78.2,
            "ask": 79.5,
            "volume": 12000,
            "category": "climate",
            "status": "open"
        },
    ]
    
    # NFL markets
    nfl_markets = [
        {
            "market_id": "NFL-SB-CHIEFS",
            "title": "Will the Kansas City Chiefs win Super Bowl 2025?",
            "bid": 48.5,
            "ask": 51.2,
            "volume": 6000,
            "category": "sports",
            "status": "open"
        },
    ]
    
    return bitcoin_markets + election_markets + climate_markets + nfl_markets


def generate_polymarket_events():
    """Generate mock Polymarket events."""
    
    # Bitcoin markets (slightly different prices than Kalshi for arbitrage)
    # Keep these lower than Kalshi to show clear arbitrage
    bitcoin_events = [
        {
            "slug": "bitcoin-100k-by-jan-31",
            "question": "Will Bitcoin trade above $100,000 by January 31, 2025?",
            "bid": 46.8,
            "ask": 47.5,
            "volume": 15000,
            "category": "cryptocurrency",
            "status": "open"
        },
        {
            "slug": "bitcoin-75k-by-feb-28",
            "question": "Will Bitcoin trade above $75,000 by February 28, 2025?",
            "bid": 60.2,
            "ask": 61.8,
            "volume": 8000,
            "category": "cryptocurrency",
            "status": "open"
        },
    ]
    
    # Election markets (different price divergence from Kalshi)
    election_events = [
        {
            "slug": "us-pres-2024-biden-vs-trump",
            "question": "In the US Presidential Election 2024, will Donald Trump win?",
            "bid": 48.2,
            "ask": 51.5,
            "volume": 30000,
            "category": "politics",
            "status": "open"
        },
        {
            "slug": "us-pres-2024-biden-wins",
            "question": "Will Joe Biden win the 2024 US Presidential election?",
            "bid": 47.8,
            "ask": 51.2,
            "volume": 30000,
            "category": "politics",
            "status": "open"
        },
    ]
    
    # Climate markets
    climate_events = [
        {
            "slug": "glo-heat-1.5c-jan-1",
            "question": "Will January 2024 global average temperature be 1.5C or more above pre-industrial?",
            "bid": 78.5,
            "ask": 79.8,
            "volume": 12500,
            "category": "climate",
            "status": "open"
        },
    ]
    
    # NFL markets
    nfl_events = [
        {
            "slug": "sf-chiefs-super-bowl-25",
            "question": "Will the Kansas City Chiefs win Super Bowl 59 (February 2025)?",
            "bid": 48.8,
            "ask": 41.2,
            "volume": 6500,
            "category": "sports",
            "status": "open"
        },
    ]
    
    return bitcoin_events + election_events + climate_events + nfl_events


def generate_trading_history():
    """Generate mock trading history for analysis."""
    
    # Sample trades from Kalshi Bitcoin market
    kalshi_trades = [
        {"order_id": "ORD-001", "market_id": "BTC-JAN31-100K", "action": "buy", "size_usd": 500, "price": 58.5, "timestamp": "2026-06-01T10:30:00Z"},
        {"order_id": "ORD-002", "market_id": "BTC-JAN31-100K", "action": "buy", "size_usd": 1000, "price": 59.0, "timestamp": "2026-06-01T10:35:00Z"},
        {"order_id": "ORD-003", "market_id": "BTC-JAN31-100K", "action": "sell", "size_usd": 750, "price": 59.2, "timestamp": "2026-06-01T10:40:00Z"},
    ]
    
    # Sample trades from Polymarket Bitcoin market
    polymarket_trades = [
        {"order_id": "PM-001", "market_slug": "bitcoin-100k-by-jan-31", "action": "buy", "size_usd": 500, "price": 46.8, "timestamp": "2026-06-01T10:32:00Z"},
        {"order_id": "PM-002", "market_slug": "bitcoin-100k-by-jan-31", "action": "buy", "size_usd": 1500, "price": 47.2, "timestamp": "2026-06-01T10:37:00Z"},
        {"order_id": "PM-003", "market_slug": "bitcoin-100k-by-jan-31", "action": "sell", "size_usd": 1000, "price": 47.5, "timestamp": "2026-06-01T10:42:00Z"},
    ]
    
    return {
        "kalshi_trades": kalshi_trades,
        "polymarket_trades": polymarket_trades
    }


def main():
    """Generate mock data files."""
    
    print("Generating Kalshi/Polymarket Arbitrage Mock Data")
    print("=" * 60)
    
    # Generate Kalshi markets
    kalshi_markets = generate_kalshi_markets()
    
    with open('/home/falcon/git/portfolio-management/trading_system/data/kalshi_mock.json', 'w') as f:
        json.dump({
            "markets": kalshi_markets,
            "generated_at": datetime.now().isoformat(),
            "description": "Mock Kalshi market data for arbitrage testing"
        }, f, indent=2)
    
    print(f"\n✅ Generated Kalshi mock data: trading_system/data/kalshi_mock.json")
    print(f"   Markets: {len(kalshi_markets)}")
    crypto_count = sum(1 for m in kalshi_markets if 'cryptocurrency' in (m.get('category') or ''))
    print(f"   Categories: crypto={crypto_count}, politics={sum(1 for m in kalshi_markets if 'politics' in (m.get('category') or ''))}")
    
    # Generate Polymarket events
    polymarket_events = generate_polymarket_events()
    
    with open('/home/falcon/git/portfolio-management/trading_system/data/polymarket_mock.json', 'w') as f:
        json.dump({
            "events": polymarket_events,
            "generated_at": datetime.now().isoformat(),
            "description": "Mock Polymarket event data for arbitrage testing"
        }, f, indent=2)
    
    print(f"\n✅ Generated Polymarket mock data: trading_system/data/polymarket_mock.json")
    print(f"   Events: {len(polymarket_events)}")
    
    # Generate trading history
    trading_history = generate_trading_history()
    
    with open('/home/falcon/git/portfolio-management/trading_system/data/trading_history_mock.json', 'w') as f:
        json.dump({
            "kalshi_trades": trading_history["kalshi_trades"],
            "polymarket_trades": trading_history["polymarket_trades"],
            "generated_at": datetime.now().isoformat()
        }, f, indent=2)
    
    print(f"\n✅ Generated trading history: trading_system/data/trading_history_mock.json")
    print(f"   Kalshi trades: {len(trading_history['kalshi_trades'])}")
    print(f"   Polymarket trades: {len(trading_history['polymarket_trades'])}")
    
    # Summary of potential arbitrage opportunities
    print("\n" + "=" * 60)
    print("Potential Arbitrage Opportunities (by category):")
    print("-" * 60)
    
    categories = ["cryptocurrency", "politics", "climate", "sports"]
    for cat in categories:
        kalshi_crypto = [m for m in kalshi_markets if (m.get('category') or '').lower() == cat]
        pm_crypto = [e for e in polymarket_events if (e.get('category') or '').lower() == cat]
        
        if kalshi_crypto and pm_crypto:
            print(f"\n{cat.upper().capitalize()} markets:")
            for km, pm in zip(kalshi_crypto[:1], pm_crypto[:1]):  # Show first pair
                divergence = abs(km['bid'] - pm['bid']) / 100
                print(f"  Kalshi: {km['market_id']} @ {km['bid']}%")
                print(f"  Polymarket: {pm['slug']} @ {pm['bid']}%")
                print(f"  Divergence: {divergence:.2%}")


if __name__ == "__main__":
    main()
