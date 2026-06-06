#!/usr/bin/env python3
"""
Complete Kalshi <-> Polymarket Arbitrage Orchestrator

This module integrates:
1. Web scraping (no API keys required) for fetching market data
2. Opportunity detection between matched markets
3. Trade execution with risk management
4. Performance monitoring and logging

Designed for production use with VPS deployment.
"""

import sys
sys.path.insert(0, '/home/falcon/git/portfolio-management')

import os
import json
import time
from datetime import datetime
from typing import List, Dict, Optional
import logging


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('/home/falcon/git/portfolio-management/trading_system/arbitrage/arb_logs.log'),
        logging.StreamHandler(),
    ]
)


class ArbitrageOrchestrator:
    """Main orchestrator for Kalshi <-> Polymarket arbitrage."""
    
    def __init__(self):
        self.kalshi_api_key = os.getenv('KALSHI_API_KEY')
        self.polymarket_api_key = os.getenv('POLYMARKET_API_KEY')
        
        from trading_system.arbitrage.web_scraper import CombinedMarketScraper
        from trading_system.arbitrage.real_time_arbitrage import ArbitrageManager
        
        self.scraper = CombinedMarketScraper()
        self.manager = ArbitrageManager(
            kalshi_api_key=self.kalshi_api_key,
            polymarket_api_key=self.polymarket_api_key,
        )
        
        self.last_run_time = None
    
    def run_opportunity_detection(self, category: str = 'all', limit: int = 100) -> List[Dict]:
        """Detect arbitrage opportunities from web-scraped data."""
        
        print("\n" + "=" * 70)
        print("[STEP 1] Fetching Market Data")
        print("=" * 70)
        
        start_time = time.time()
        
        # Scrape fresh market data
        market_data = self.scraper.scrape_markets(category=category, limit=limit)
        
        kalshi_markets = market_data['kalshi_markets']
        polymarket_events = market_data['polymarket_events']
        
        elapsed = time.time() - start_time
        
        print(f"\n[+] Kalshi Markets: {len(kalshi_markets)} ({elapsed:.2f}s)")
        print(f"[+] Polymarket Events: {len(polymarket_events)}")
        
        # Detect opportunities
        from trading_system.arbitrage.real_time_arbitrage import detect_opportunities
        
        print("\n[STEP 2] Detecting Arbitrage Opportunities")
        print("=" * 70)
        
        opps = detect_opportunities(
            kalshi_markets=kalshi_markets,
            polymarket_events=polymarket_events,
            similarity_threshold=0.75,
            min_divergence=0.01,
        )
        
        if not opps:
            print("\n[!] No arbitrage opportunities detected")
            return []
        
        # Sort and show top opportunities
        opps.sort(key=lambda o: o.estimate_profit()['roi_pct'], reverse=True)
        
        print(f"\n[+] Found {len(opps)} opportunity(ies)")
        
        for i, opp in enumerate(opps[:5]):  # Show top 5
            profit = opp.estimate_profit()
            print(f"\n  Opportunity #{i+1}:")
            print(f"    Kalshi: {opp.market_pair.kalshi_market_id[:60]}...")
            print(f"    Polymarket: {opp.market_pair.polymarket_slug[:40]}...")
            print(f"    Divergence: {round(opp.market_pair.divergence*100, 2)}%")
            print(f"    Buy side: {opp.buy_platform.upper()} @ {round(opp.market_pair.kalshi_price*100 if opp.buy_platform=='kalshi' else opp.market_pair.polymarket_price*100, 2)}%")
            print(f"    Sell side: {opp.sell_platform.upper()} @ {round((1-opp.market_pair.kalshi_price)*100 if opp.buy_platform=='kalshi' else (1-opp.market_pair.polymarket_price)*100, 2)}%")
            print(f"    Buy units: {opp.buy_units}, Sell units: {opp.sell_units}")
            print(f"    Expected net profit: ${round(profit['net_profit'], 2)} ({profit['roi_pct']:.1f}%)")
        
        # Save opportunities to file
        opps_file = '/home/falcon/git/portfolio-management/trading_system/data/web_scraped_markets.json'
        with open(opps_file, 'w') as f:
            json.dump(market_data, f, indent=2)
        
        print(f"\n[+] Market data saved to {opps_file}")
        
        return [o.__dict__ for o in opps]
    
    def execute_top_opportunities(self, top_n: int = 3, strategy: str = 'balanced') -> List[Dict]:
        """Execute trades for top N opportunities."""
        
        print(f"\n" + "=" * 70)
        print(f"[STEP 3] Executing Top {top_n} Arbitrage Trades")
        print("=" * 70)
        
        if not self.manager.last_opportunities:
            return []
        
        results = []
        for i, opp in enumerate(self.manager.last_opportunities[:top_n]):
            try:
                print(f"\n  Executing Trade {i+1}: Strategy={strategy}")
                
                # Execute trades using the manager
                trade_result = self.manager._execute_single_trade(opp)
                
                if trade_result:
                    results.append({
                        'opportunity': opp.market_pair.kalshi_market_id,
                        'strategy': strategy,
                        'buy_order_id': trade_result.get('buy_order_id'),
                        'sell_order_id': trade_result.get('sell_order_id'),
                        'expected_profit': opp.estimate_profit()['net_profit'],
                        'status': 'executed',
                    })
                    
            except Exception as e:
                print(f"    [!] Trade execution failed: {e}")
        
        return results
    
    def run_full_pipeline(self, category: str = 'all', top_n: int = 3):
        """Run full detection and execution pipeline."""
        
        opps = self.run_opportunity_detection(category=category)
        if opps:
            results = self.execute_top_opportunities(top_n=top_n)
            
            # Save complete results
            self._save_complete_results(opps, results)
            
            return opps, results
        
        return [], []
    
    def _execute_single_trade(self, opportunity) -> Optional[Dict]:
        """Execute a single arbitrage trade."""
        
        from trading_system.arbitrage.real_time_arbitrage import ArbitrageOpportunity
        
        # Convert Opportunity object to dict if needed
        if isinstance(opportunity, dict):
            opp = ArbitrageOpportunity(
                MarketPair(**opportunity),
                strategy=opportunity.get('strategy', 'balanced'),
            )
        else:
            opp = opportunity
        
        # Calculate allocation
        opp.calc_allocations()
        
        # Execute buy order on cheaper platform
        if opp.buy_platform == 'kalshi':
            buy_result = self.kalshi_manager.place_order(
                market_id=opp.market_pair.kalshi_market_id,
                side='buy',
                quantity=opp.buy_units,
                unit_price=opp.market_pair.kalshi_price,
            )
        else:
            buy_result = self.polymarket_manager.place_order(
                slug=opp.market_pair.polymarket_slug,
                side='buy',
                quantity=opp.buy_units,
                unit_price=opp.market_pair.polymarket_price,
            )
        
        # Execute sell order on more expensive platform
        if opp.sell_platform == 'kalshi':
            sell_result = self.kalshi_manager.place_order(
                market_id=opp.market_pair.kalshi_market_id,
                side='sell',
                quantity=opp.sell_units,
                unit_price=1.0 - opp.market_pair.kalshi_price,
            )
        else:
            sell_result = self.polymarket_manager.place_order(
                slug=opp.market_pair.polymarket_slug,
                side='sell',
                quantity=opp.sell_units,
                unit_price=1.0 - opp.market_pair.polymarket_price,
            )
        
        return {
            'buy_result': buy_result,
            'sell_result': sell_result,
        }


# Mock classes for demonstration (would be imported from real_time_arbitrage in production)
class MarketPair:
    def __init__(self, **kwargs):
        for k, v in kwargs.items():
            setattr(self, k, v)
