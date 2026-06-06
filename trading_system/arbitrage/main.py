#!/usr/bin/env python3
"""
Main script for Kalshi <-> Polymarket arbitrage detection and execution.

This script:
1. Fetches market data from APIs (Kalshi, Polymarket, or web scrapers)
2. Detects arbitrage opportunities between markets
3. Executes trades on both platforms
4. Reports results and potential profits
"""

import sys
sys.path.insert(0, '/home/falcon/git/portfolio-management')
from dataclasses import dataclass
from datetime import datetime
from typing import Optional, Callable


@dataclass(frozen=True)
class MarketPair:
    """Represents a matched market pair for arbitrage."""
    kalshi_market_id: str
    polymarket_slug: str
    
    kalshi_price: float  # in decimal (0-1), e.g., 0.4726 for 47.26%
    polymarket_price: float  # in decimal (0-1), e.g., 0.4583 for 45.83%
    
    kalshi_title: str
    polymarket_question: str
    
    divergence: float  # absolute price difference
    arbitrage_potential_pct: float
    
    timestamp: datetime = None
    
    def __post_init__(self):
        if self.timestamp is None:
            object.__setattr__(self, 'timestamp', datetime.now())


# Mock Kalshi client for testing
class MockKalshiClient:
    """Mock Kalshi API client for development and testing."""
    
    def __init__(self):
        self.orders = []
        self.market_data = {}
    
    async def get_markets(self, category: str = None) -> list[dict]:
        """Fetch markets from Kalshi API or load from local file."""
        
        # Check environment variable for real API usage
        import os
        kalshi_api_key = os.getenv('KALSHI_API_KEY')
        kalshi_base_url = os.getenv('KALSHI_API_BASE_URL', 'https://api.kalshi.com/v2')
        
        if kalshi_api_key:
            # Use real Kalshi API (requires curl or requests library)
            print("[+] Using real Kalshi API...")
            
            import requests
            
            response = requests.get(
                f"{kalshi_base_url}/markets",
                headers={"Authorization": f"Bearer {kalshi_api_key}"}
            )
            
            if response.status_code == 200:
                data = response.json()
                # Convert response format to our internal format
                markets = []
                for m in data.get('items', []):
                    market_id = str(m['market_id'])
                    bid_pct = float(str(m.get('bid_price', 0)) or '0')
                    
                    markets.append({
                        'market_id': market_id,
                        'title': m['full_title'],
                        'category': m.get('category', ''),
                        'id': m.get('market_type'),
                        'bid': bid_pct,
                    })
                
                self.market_data[category or 'all'] = markets
                return markets
            
        # Fall back to mock data
        print("[+] Using mock Kalshi data...")
        
        kalshi_file = '/home/falcon/git/portfolio-management/trading_system/data/kalshi_mock.json'
        
        import json
        
        try:
            with open(kalshi_file, 'r') as f:
                data = json.load(f)
            
            markets = []
            for m in data['markets']:
                market_id = str(m['id'])
                bid_pct = float(str(m.get('bid', 0)) or '0') / 100
                
                markets.append({
                    'market_id': market_id,
                    'title': m['title'],
                    'category': m.get('category', ''),
                    'id': m.get('ticker'),
                    'bid': bid_pct,
                })
            
            self.market_data[category or 'all'] = markets
            return markets
            
        except FileNotFoundError:
            print(f"[-] Mock data file not found: {kalshi_file}")
            return []

    async def create_order(
        self,
        market_id: str,
        side: str,  # 'buy' or 'sell'
        quantity: int,
        unit_price: float,
    ) -> dict:
        """Create order on Kalshi."""
        
        import os
        
        if os.getenv('KALSHI_API_KEY'):
            print("[+] Executing real Kalshi order...")
            
            import requests
            
            url = f"https://api.kalshi.com/v2/market/{market_id}/orders"
            
            payload = {
                'order_type': 'limit',
                'instruction': side,
                'quantity': str(quantity),
                'price': str(unit_price),
                'post_only': 'false',
                'cancel_after_fill': 'true',
            }
            
            headers = {"Authorization": f"Bearer {os.getenv('KALSHI_API_KEY')}"}
            
            response = requests.post(url, json=payload, headers=headers)
            
            if response.status_code == 201:
                result = response.json()
                
                order = {
                    'order_id': result.get('order', {}).get('id'),
                    'market_id': market_id,
                    'side': side,
                    'status': 'open',
                    'quantity': quantity,
                    'unit_price': unit_price,
                }
                
                self.orders.append(order)
                return order
            
            print(f"[-] Kalshi order failed: {response.status_code} - {response.text}")
            return None
        
        # Mock order creation
        order_id = f"KLS-{datetime.now().timestamp()}-{int(quantity)}"
        
        order = {
            'order_id': order_id,
            'market_id': market_id,
            'side': side,
            'status': 'open',
            'quantity': quantity,
            'unit_price': unit_price,
            'total_cost': round(quantity * unit_price, 2),
        }
        
        self.orders.append(order)
        return order


# Mock Polymarket client for testing
class MockPolymarketClient:
    """Mock Polymarket API client for development and testing."""
    
    def __init__(self):
        self.orders = []
        self.market_data = {}
    
    async def get_events(self, category: str = None) -> list[dict]:
        """Fetch events from Polymarket API or load from local file."""
        
        # Check environment variable for real API usage
        import os
        
        polymarket_api_key = os.getenv('POLYMARKET_API_KEY')
        polymarket_base_url = os.getenv(
            'POLYMARKET_API_BASE_URL', 
            'https://api.polygon.io/v2'
        )
        
        if polymarket_api_key:
            # Use real Polymarket API
            print("[+] Using real Polymarket API...")
            
            import requests
            
            response = requests.get(
                f"{polymarket_base_url}/events",
                headers={"Authorization": f"Bearer {polymarket_api_key}"}
            )
            
            if response.status_code == 200:
                data = response.json()
                
                events = []
                for e in data.get('results', []):
                    event_slug = str(e['slug'])
                    bid_pct = float(str(e.get('volume_1h') or '0') or '0')
                    
                    events.append({
                        'slug': event_slug,
                        'question': e.get('title', ''),
                        'category': e.get('primaryTopic', ''),
                        'id': event_slug,
                        'bid': bid_pct,
                    })
                
                self.market_data[category or 'all'] = events
                return events
            
        # Fall back to mock data
        print("[+] Using mock Polymarket data...")
        
        pm_file = '/home/falcon/git/portfolio-management/trading_system/data/polymarket_mock.json'
        
        import json
        
        try:
            with open(pm_file, 'r') as f:
                data = json.load(f)
            
            events = []
            for e in data['events']:
                event_slug = str(e['id'])
                bid_pct = float(str(e.get('bid', 0)) or '0') / 100
                
                events.append({
                    'slug': event_slug,
                    'question': e['question'],
                    'category': e.get('topic', ''),
                    'id': event_slug,
                    'bid': bid_pct,
                })
            
            self.market_data[category or 'all'] = events
            return events
            
        except FileNotFoundError:
            print(f"[-] Mock data file not found: {pm_file}")
            return []

    async def create_order(
        self,
        slug: str,
        side: str,  # 'buy' or 'sell'
        quantity: float,
        unit_price: float,
    ) -> dict:
        """Create order on Polymarket."""
        
        import os
        
        if os.getenv('POLYMARKET_API_KEY'):
            print("[+] Executing real Polymarket order...")
            
            import requests
            
            url = f"https://api.polygon.io/v2/events/{slug}/orders"
            
            payload = {
                'side': side,
                'order_type': 'limit',
                'price': str(unit_price),
                'quantity': str(int(quantity)),
            }
            
            headers = {"Authorization": f"Bearer {os.getenv('POLYMARKET_API_KEY')}"}
            
            response = requests.post(url, json=payload, headers=headers)
            
            if response.status_code == 201:
                result = response.json()
                
                order = {
                    'order_id': result.get('order', {}).get('id'),
                    'market': slug,
                    'side': side,
                    'status': 'open',
                    'quantity': quantity,
                    'price': unit_price,
                    'total_cost': round(quantity * unit_price, 2),
                }
                
                self.orders.append(order)
                return order
            
            print(f"[-] Polymarket order failed: {response.status_code} - {response.text}")
            return None
        
        # Mock order creation
        order_id = f"PM-{datetime.now().timestamp()}-{int(quantity)}"
        
        order = {
            'order_id': order_id,
            'market': slug,
            'side': side,
            'status': 'open',
            'quantity': quantity,
            'price': unit_price,
            'total_cost': round(quantity * unit_price, 2),
        }
        
        self.orders.append(order)
        return order


def main():
    """Main entry point for arbitrage detection and execution."""
    
    print("=" * 80)
    print("Kalshi <-> Polymarket Arbitrage System")
    print("=" * 80)
    print()
    
    # Initialize API clients
    kalshi_client = MockKalshiClient()
    polymarket_client = MockPolymarketClient()
    
    # Fetch market data
    print("[1/4] Fetching Kalshi markets...")
    try:
        kalshi_markets = kalshi_client.get_markets(category='cryptocurrency')
        print(f"      Loaded {len(kalshi_markets)} Kalshi markets")
    except Exception as e:
        print(f"      Error loading Kalshi markets: {str(e)}")
        return 1
    
    print("[2/4] Fetching Polymarket events...")
    try:
        polymarket_events = polymarket_client.get_events(category='cryptocurrency')
        print(f"      Loaded {len(polymarket_events)} Polymarket events")
    except Exception as e:
        print(f"      Error loading Polymarket events: {str(e)}")
        return 1
    
    # Detect arbitrage opportunities
    print("[3/4] Detecting arbitrage opportunities...")
    
    from trading_system.arbitrage.opportunity_detector import OpportunityDetector
    
    detector = OpportunityDetector()
    detector.from_dict({
        'markets': [{'market_id': m['market_id'], 'title': m['title']} for m in kalshi_markets],
        'events': [{'slug': e['slug'], 'question': e['question']} for e in polymarket_events],
    })
    
    opportunities = detector.detect_opportunities()
    print(f"      Found {len(opportunities)} arbitrage opportunity(ies)")
    
    if not opportunities:
        print("      No arbitrage opportunities detected")
        return 0
    
    # Execute trades
    print("[4/4] Executing arbitrage trades...")
    
    from trading_system.arbitrage.arb_trader import ArbitrageTrader
    
    trader = ArbitrageTrader()
    results = trader.execute_all_opportunities(
        [detector.to_dict(op) for op in opportunities],
    )
    
    # Print summary
    print("\n" + "=" * 80)
    print("Trade Execution Summary")
    print("=" * 80)
    
    for result in results:
        print(f"\nTrade:")
        print(f"  Kalshi Order ID: {result.kalshi_order_id}")
        print(f"  Polymarket Order ID: {result.polymarket_order_id}")
        print(f"  Kalshi Status: {result.kalshi_status}")
        print(f"  Polymarket Status: {result.polymarket_status}")
    
    # Save results to JSON
    import json
    
    output_file = '/home/falcon/git/portfolio-management/trading_system/data/arbitrage_results.json'
    
    results_data = {
        'timestamp': datetime.now().isoformat(),
        'opportunities': [detector.to_dict(op) for op in opportunities],
        'trade_results': [
            {
                'kalshi_order_id': r.kalshi_order_id,
                'polymarket_order_id': r.polymarket_order_id,
                'kalshi_status': r.kalshi_status,
                'polymarket_status': r.polymarket_status,
            } for r in results
        ],
    }
    
    with open(output_file, 'w') as f:
        json.dump(results_data, f, indent=2)
    
    print(f"\nResults saved to {output_file}")
    
    return 0


if __name__ == '__main__':
    import sys
    sys.exit(main())
