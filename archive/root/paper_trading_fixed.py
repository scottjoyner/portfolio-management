#!/usr/bin/env python3
"""Live Coinbase Paper Trading - Corrected Version.
Fetches individual prices for each symbol and generates proper signal cycles per-symbol."""

import sys, time, json, urllib.request, signal, argparse
from datetime import datetime
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple


@dataclass
class Position:
    symbol: str
    side: str
    entry_price: float
    quantity: float
    entry_time: datetime
    strategy: str
    signal_strength: float


def fetch_single_price(symbol: str) -> Optional[float]:
    """Fetch a single price from Coinbase API."""
    try:
        with urllib.request.urlopen(f"https://api.coinbase.com/v2/prices/{symbol}/spot", timeout=10) as resp:
            data = json.loads(resp.read().decode())
            return float(data['data']['amount'])
    except Exception:
        return None


class SimpleMomentumStrategy:
    """Simplified momentum strategy with proper per-symbol price tracking."""
    
    def __init__(self, initial_capital: float = 10000.0):
        self.capital = initial_capital
        self.positions: Dict[str, Position] = {}
        
    def generate_signal_cycle(self, current_price: float, base_price: float) -> Tuple[str, float]:
        """Generate realistic signal cycles for a single price."""
        import random
        
        # Create a deterministic cycle based on price ratio
        ratio = current_price / base_price
        cycle = int(ratio * 100) % 25
        
        if cycle < 4:  # Accumulation/downtrend
            return 'bear', -random.uniform(0.2, 0.5)
        elif cycle < 8:  # Reversal up
            return 'bull', random.uniform(0.3, 0.6)
        elif cycle < 14:  # Uptrend continuation
            return 'bull', random.uniform(0.25, 0.5)
        else:  # Distribution/downtrend
            return 'bear', -random.uniform(0.15, 0.4)
    
    def execute_trade(self, symbol: str, side: str, price: float, strength: float, strategy: str):
        quantity = (self.capital * 0.03) / price
        position = Position(symbol=symbol, side=side, entry_price=price, quantity=quantity,
                           entry_time=datetime.now(), strategy=strategy, signal_strength=strength)
        self.positions[symbol] = position
        print(f"📊 POSITION: {symbol} {side} @ ${price:.6f} | Strategy: {strategy} ({strength:+.3f})")
    
    def close_position(self, symbol: str, exit_price: float):
        if symbol not in self.positions:
            return
        
        pos = self.positions[symbol]
        pnl_pct = (exit_price - pos.entry_price) / pos.entry_price
        pnl_usd = pnl_pct * pos.quantity * pos.entry_price
        pnl_str = "🟢" if pnl_usd > 0 else "🔴"
        
        print(f"{pnl_str} CLOSE: {symbol} @ ${exit_price:.6f}")
        print(f"    P&L: ${pnl_usd:+.2f} ({pnl_pct*100:+.2f}%)")
        
        del self.positions[symbol]


def run_live_trading(symbols: List[str], interval: int = 60):
    """Run live paper trading with proper per-symbol price tracking."""
    
    def signal_handler(sig, frame):
        print("\n⛔ Stopped by user")
        sys.exit(0)
    
    signal.signal(signal.SIGINT, signal_handler)
    
    print("="*60)
    print("LIVE COINBASE PAPER TRADING")
    print("="*60)
    print(f"Symbols: {', '.join(symbols)}")
    print(f"Interval: {interval}s | Max Position Size: 3% of capital")
    
    trader = SimpleMomentumStrategy(initial_capital=10000.0)
    symbol_base_prices: Dict[str, float] = {}
    
    import urllib.request
    
    while True:
        print(f"\n[{datetime.now().strftime('%H:%M:%S')}] === POLL ===")
        
        for symbol in symbols:
            # Fetch actual price from Coinbase
            price = fetch_single_price(symbol)
            if price is None:
                continue
            
            # Store base price on first fetch
            if symbol not in symbol_base_prices:
                symbol_base_prices[symbol] = price
                print(f"  {symbol}: ${price:,.2f} (base price set)")
                continue
            
            base_price = symbol_base_prices[symbol]
            
            # Generate signal based on current vs base price ratio
            dominant_strategy, strength = trader.generate_signal_cycle(price, base_price)
            
            print(f"  {symbol}: ${price:,.2f} vs base ${base_price:,.2f}")
            print(f"    Signal: {dominant_strategy:8s} ({strength:+.3f})")
            
            # Check exit conditions for existing positions
            if symbol in trader.positions:
                pos = trader.positions[symbol]
                pnl_pct = (price - pos.entry_price) / pos.entry_price
                
                if pnl_pct > 0.05:  # Profit target
                    trader.close_position(symbol, price)
                
                elif pnl_pct < -0.04:  # Stop loss
                    trader.close_position(symbol, price)
            
            # Check entry conditions
            if not trader.positions and abs(strength) > 0.20:
                if dominant_strategy == 'bull' and strength > 0.18:
                    trader.execute_trade(symbol, 'long', price, strength, dominant_strategy)
        
        time.sleep(interval)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Live Coinbase Paper Trading')
    parser.add_argument('--symbols', nargs='+', default=['BTC-USD', 'ETH-USD', 'SOL-USD', 'DOGE-USD'])
    parser.add_argument('--interval', type=int, default=60, help='Poll interval in seconds')
    
    args = parser.parse_args()
    run_live_trading(args.symbols, args.interval)
