#!/usr/bin/env python3
"""Simple runner for live Coinbase paper trading.
Runs continuous polling of Coinbase prices with signal-based entry/exit logic.

Usage:
    python3 run_paper_trading.py [--symbols BTC-USD ETH-USD ...] [--interval 60]
    
Press Ctrl+C to stop.
"""

import sys, time, signal, argparse, json, urllib.request
from datetime import datetime
sys.path.insert(0, '/home/scott/git/portfolio-management')

from paper_trading_system import CoinbasePaperTrader, generate_constrained_prices


def main():
    parser = argparse.ArgumentParser(description='Live Coinbase Paper Trading')
    parser.add_argument('--symbols', nargs='+', default=['BTC-USD', 'ETH-USD', 'SOL-USD', 'DOGE-USD'],
                       help='Trading symbols')
    parser.add_argument('--interval', type=int, default=60, help='Poll interval in seconds')
    
    args = parser.parse_args()
    
    print("="*60)
    print("LIVE COINBASE PAPER TRADING")
    print("="*60)
    print(f"Symbols: {', '.join(args.symbols)}")
    print(f"Poll Interval: {args.interval} seconds")
    print("Press Ctrl+C to stop")
    
    trader = CoinbasePaperTrader(initial_capital=10000.0)
    trader.poll_interval = args.interval
    
    # Override run_live_paper_trading with simpler implementation
    import urllib.request
    
    def fetch_price(symbol):
        try:
            with urllib.request.urlopen(f"https://api.coinbase.com/v2/prices/{symbol}/spot", timeout=10) as resp:
                data = json.loads(resp.read().decode())
                return float(data['data']['amount'])
        except Exception as e:
            return None
    
    running = True
    def handler(sig, frame):
        nonlocal running
        running = False
    
    signal.signal(signal.SIGINT, handler)
    
    print("\n🔄 Starting live paper trading...")
    
    while running:
        for symbol in args.symbols:
            price = fetch_price(symbol.upper()) if not symbol.endswith('-USD') else fetch_price(symbol)
            
            if price is None:
                continue
            
            # Generate synthetic historical data for signal computation
            history = generate_constrained_prices(40, 0.5 * price)
            
            dominant_strategy, combined_strength, signals = trader.get_signal_strength(history, price)
            
            # Check exit conditions first
            for existing_symbol in list(trader.positions.keys()):
                pos = trader.positions[existing_symbol]
                pnl = (price - pos.entry_price) / pos.entry_price
                if pnl < -0.04:
                    trader.close_position(existing_symbol, price, 'stop_loss')
                elif pnl > 0.05:
                    trader.close_position(existing_symbol, price, 'profit_target')
            
            # Check entry conditions
            if not trader.check_trading_rules(combined_strength):
                continue
            
            for strategy_name, signal_info in signals.items():
                if signal_info.get('signal') == 'bull' and combined_strength > 0.20:
                    if not trader.positions:
                        trader.execute_trade(symbol, 'long', price, combined_strength, strategy_name)
        
        time.sleep(args.interval)


if __name__ == '__main__':
    main()
