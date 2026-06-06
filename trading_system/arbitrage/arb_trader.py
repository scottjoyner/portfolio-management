#!/usr/bin/env python3
"""Trade execution for Kalshi <-> Polymarket arbitrage with rate limiting and error handling."""

import sys
sys.path.insert(0, '/home/falcon/git/portfolio-management')
from dataclasses import dataclass
from datetime import datetime
from typing import Optional, Callable, Dict
from enum import Enum
import asyncio
import time


class OrderSide(Enum):
    """Order side: buy or sell."""
    BUY = 1
    SELL = 2


@dataclass(frozen=True)
class TradeExecutionResult:
    """Result of executing a trade."""
    kalshi_order_id: Optional[str] = None
    polymarket_order_id: Optional[str] = None
    
    kalshi_status: str = "pending"
    polymarket_status: str = "pending"
    
    amount_kalshi: float = 0.0
    amount_polymarket: float = 0.0
    
    kalshi_fill_price: Optional[float] = None
    polymarket_fill_price: Optional[float] = None
    
    timestamp: datetime = None
    
    error_message: str = ""
    
    def __post_init__(self):
        if self.timestamp is None:
            object.__setattr__(self, 'timestamp', datetime.now())


class RateLimiter:
    """Rate limiter for API calls with exponential backoff."""
    
    def __init__(
        self,
        requests_per_second: float = 1.0,
        burst_size: int = 5,
    ):
        """Initialize rate limiter.

        Args:
            requests_per_second: Maximum requests per second (default: 1)
            burst_size: Burst capacity for handling short spikes
        """
        self.requests_per_second = requests_per_second
        self.burst_size = burst_size
        self.last_request_time = time.time()
        self.burst_count = 0
    
    async def acquire(self) -> None:
        """Acquire permission to make a request, waiting if necessary."""
        now = time.time()
        elapsed = now - self.last_request_time
        
        # Allow burst at start
        if self.burst_count < self.burst_size:
            self.burst_count += 1
            self.last_request_time = now
            return
        
        # Calculate wait time based on rate limit
        min_interval = 1.0 / self.requests_per_second
        if elapsed < min_interval:
            await asyncio.sleep(min_interval - elapsed)
        
        self.last_request_time = time.time()


class ConnectionHealthMonitor:
    """Monitors connection health and handles reconnection."""
    
    def __init__(self):
        self.health_check_callbacks: list[Callable] = []
        self.error_callbacks: list[Callable] = []
        self.reconnect_attempts = 0
        self.max_reconnect_attempts = 5
    
    async def check_health(self) -> bool:
        """Perform health check. Override in subclasses."""
        return True
    
    async def on_error(self, error: Exception):
        """Called when an error occurs."""
        if len(self.error_callbacks) > 0:
            for callback in self.error_callbacks:
                await callback(error)
    
    async def on_health_check(self):
        """Called on health check completion."""
        if len(self.health_check_callbacks) > 0:
            for callback in self.health_check_callbacks:
                await callback()

    def execute_arbitrage_opportunity(
        self,
        kalshi_market: dict,
        polymarket_event: dict,
        strategy: str = "balanced",
    ) -> TradeExecutionResult:
        """Execute an arbitrage opportunity."""
        
        kalshi_price = float(kalshi_market['bid']) / 100 if isinstance(kalshi_market['bid'], int) else float(kalshi_market['bid'])
        polymarket_price = float(polymarket_event['bid']) / 100 if isinstance(polymarket_event['bid'], int) else float(polymarket_event['bid'])
        
        # Determine which side is cheaper and which is more expensive
        if kalshi_price < polymarket_price:
            kalshi_side = OrderSide.BUY  # Buy on Kalshi (cheaper)
            polymarket_side = OrderSide.SELL  # Sell on Polymarket (more expensive)
            kalshi_is_buy = True
            pm_is_sell = True
        else:
            kalshi_side = OrderSide.SELL  # Sell on Kalshi (more expensive)
            polymarket_side = OrderSide.BUY  # Buy on Polymarket (cheaper)
            kalshi_is_buy = False
            pm_is_sell = False
        
        print(f"\nArbitrage Strategy: {'Buy Low on Kalshi / Sell High on Polymarket' if kalshi_is_buy else 'Buy Low on Polymarket / Sell High on Kalshi'}")
        print(f"  Kalshi: {kalshi_market['market_id']} @ {round(kalshi_price*100, 2)}% {'(BUY)' if kalshi_is_buy else '(SELL)'}")
        print(f"  Polymarket: {polymarket_event['slug']} @ {round(polymarket_price*100, 2)}% {'(SELL)' if pm_is_sell else '(BUY)'}")
        
        # Allocate amounts based on strategy
        if strategy == "balanced":
            # Split 50/50 by dollar value
            target_amount = 100  # Start with $100 per side
            kalshi_amount = target_amount / 2
            polymarket_amount = target_amount / 2
        elif strategy == "kalshi_first":
            # Focus more on Kalshi
            target_amount = 150
            kalshi_amount = target_amount * 0.67
            polymarket_amount = target_amount * 0.33
        elif strategy == "pm_first":
            # Focus more on Polymarket
            target_amount = 150
            kalshi_amount = target_amount * 0.33
            polymarket_amount = target_amount * 0.67
        
        print(f"\nAllocated Amount:")
        print(f"  Kalshi: ${round(kalshi_amount, 2)}")
        print(f"  Polymarket: ${round(polymarket_amount, 2)}")
        
        # Calculate units (contracts) for each platform
        kalshi_contract_size = 100  # $1 per contract on Kalshi
        pm_contract_size = 100  # $1 per share on Polymarket
        
        kalshi_units = int(kalshi_amount / kalshi_contract_size)
        polymarket_units = int(polymarket_amount / pm_contract_size)
        
        print(f"\nOrder Size:")
        print(f"  Kalshi: {kalshi_units} contracts @ ${round(kalshi_price*100, 2)}%")
        print(f"  Polymarket: {polymarket_units} shares @ {round(polymarket_price*100, 2)}%")
        
        # Execute Kalshi order
        if kalshi_is_buy:
            kalshi_result = self.kalshi_client.create_order(
                market_id=kalshi_market['market_id'],
                side=OrderSide.BUY,
                quantity=kalshi_units,
                unit_price=float(kalshi_price),
            )
            print(f"\n[+] Kalshi Order Executed: {kalshi_result}")
        else:
            # For sell orders on Kalshi, we'd need the opposite price
            kalshi_sell_price = 1.0 - float(kalshi_market['bid']) / 100 if isinstance(kalshi_market['bid'], int) else (1.0 - float(kalshi_market['bid']))
            kalshi_result = self.kalshi_client.create_order(
                market_id=kalshi_market['market_id'],
                side=OrderSide.SELL,
                quantity=kalshi_units,
                unit_price=float(kalhi_sell_price),
            )
            print(f"\n[+] Kalshi Order Executed: {kalshi_result}")
        
        # Execute Polymarket order
        if pm_is_sell:
            polymarket_result = self.polymarket_client.create_order(
                slug=polymarket_event['slug'],
                side=OrderSide.SELL,
                quantity=polymarket_units,
                unit_price=float(polymarket_price),
            )
            print(f"[+] Polymarket Order Executed: {polymarket_result}")
        else:
            polymarket_result = self.polymarket_client.create_order(
                slug=polymarket_event['slug'],
                side=OrderSide.BUY,
                quantity=polymarket_units,
                unit_price=float(polymarket_price),
            )
            print(f"[+] Polymarket Order Executed: {polymarket_result}")
        
        # Calculate realized profit
        kalshi_total_cost = kalshi_result.get('total_cost', 0)
        polymarket_total_cost = polymarket_result.get('total_cost', 0)
        
        net_investment = abs(kalshi_total_cost - polymarket_total_cost)
        print(f"\nTrade Summary:")
        print(f"  Kalshi Investment: ${round(abs(kalshi_total_cost), 2)}")
        print(f"  Polymarket Investment: ${round(abs(polymarket_total_cost), 2)}")
        print(f"  Net Arbitrage Profit/Loss: ${round(net_investment, 2)}")
        
        return TradeExecutionResult(
            kalshi_order_id=str(kalshi_result.get('order_id', '')),
            polymarket_order_id=str(polymarket_result.get('order_id', '')),
            kalshi_status=kalshi_result.get('status', 'open'),
            polymarket_status=polymarket_result.get('status', 'open'),
            amount_kalshi=kalshi_total_cost,
            amount_polymarket=polymarket_total_cost,
            timestamp=datetime.now(),
        )

    def execute_all_opportunities(
        self,
        opportunities_list: list[dict],
    ) -> list[TradeExecutionResult]:
        """Execute all detected arbitrage opportunities."""
        
        results = []
        for op in opportunities_list:
            try:
                kalshi_market = op['kalshi']
                polymarket_event = op['polymarket']
                
                result = self.execute_arbitrage_opportunity(
                    kalshi_market=kalshi_market,
                    polymarket_event=polymarket_event,
                )
                results.append(result)
            except Exception as e:
                print(f"\nError executing opportunity: {str(e)}")
        
        return results


if __name__ == '__main__':
    # Example usage
    trader = ArbitrageTrader()
    
    # Mock opportunities (would normally come from detector)
    mock_opportunities = [
        {
            'kalshi': {
                'market_id': 'BTC-FEB28-75K',
                'bid': 71.8,
                'title': 'Bitcoin will trade above $75,000 by February 28, 2025',
            },
            'polymarket': {
                'slug': 'bitcoin-75k-by-feb-28',
                'bid': 60.2,
                'question': 'Will Bitcoin trade above $75,000 by February 28, 2025?',
            },
        }
    ]
    
    print("=" * 80)
    print("Kalshi <-> Polymarket Arbitrage Trader")
    print("=" * 80)
    
    results = trader.execute_all_opportunities(mock_opportunities)
    
    for result in results:
        print(f"\nTrade Result:")
        print(f"  Kalshi Order ID: {result.kalshi_order_id}")
        print(f"  Polymarket Order ID: {result.polymarket_order_id}")
        print(f"  Kalshi Status: {result.kalshi_status}")
        print(f"  Polymarket Status: {result.polymarket_status}")
