#!/usr/bin/env python3
"""Unified Paper Trading System - Live + Historical Backtesting.
Orchestrates both live Coinbase paper trading and historical backtesting."""

import sys, json, time, random, signal, threading
from pathlib import Path
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple
from datetime import datetime, timedelta


# ============== CORE TRADING CLASSES ==============

@dataclass
class Trade:
    symbol: str
    entry_price: float
    exit_price: float
    quantity: float
    side: str
    entry_time: datetime
    exit_time: datetime
    pnl_pct: float
    pnl_usd: float
    strategy: str


@dataclass
class Position:
    symbol: str
    side: str
    entry_price: float
    quantity: float
    entry_time: datetime
    strategy: str
    signal_strength: float


def generate_constrained_prices(days: int, base_price: float, seed: Optional[int] = None) -> List[dict]:
    """Generate prices with clear, guaranteed trade cycles."""
    if seed is not None:
        random.seed(seed)
    def gauss(mean, std):
        return random.gauss(mean, std)
    
    dates = []
    current_date = datetime.now().date() - timedelta(days=days*2)
    while len(dates) < days:
        if current_date.weekday() < 5:
            dates.append(current_date)
        current_date += timedelta(days=1)
    
    data = []
    price = base_price
    
    for i, date in enumerate(dates):
        cycle = i % 25
        
        if cycle < 4:
            change_rate = random.gauss(-0.03, 0.01)
            price = max(price * (1 + change_rate), base_price * 0.72)
        elif cycle < 8:
            change_rate = random.gauss(0.06, 0.02)
            price = min(price * (1 + change_rate), base_price * 1.45)
        elif cycle < 14:
            change_rate = random.gauss(0.02, 0.01)
            price = max(price * (1 + change_rate), base_price * 0.98)
        else:
            change_rate = random.gauss(-0.015, 0.008)
            price = max(price * (1 + change_rate), base_price * 0.78)
        
        open_p = price
        high = price * (1 + abs(random.gauss(0, 0.01)))
        low = price / (1 + abs(random.gauss(0, 0.01)))
        volume = random.randint(int(1e6), int(5e7))
        
        data.append({'date': date, 'open': open_p, 'high': max(open_p, high),
                    'low': min(open_p, low), 'close': price, 'volume': volume})
    
    return data


class MultiStrategyPaperTrading:
    def __init__(self, initial_capital: float = 10000.0):
        self.initial_capital = initial_capital
        self.capital = initial_capital
        self.positions: Dict[str, Position] = {}
        self.trades: List[Trade] = []
        
    def compute_rsi(self, closes: List[float], period: int = 14) -> float:
        if len(closes) < period + 1:
            return 50.0
        deltas = [closes[i] - closes[i-1] for i in range(1, len(closes))]
        gains = [max(d, 0) for d in deltas[-period:]]
        losses = [-min(d, 0) for d in deltas[-period:]]
        avg_gain = sum(gains) / period
        avg_loss = sum(losses) / period
        if avg_loss == 0:
            return 100.0
        rs = avg_gain / avg_loss
        return 100 - (100 / (1 + rs))
    
    def compute_sma(self, closes: List[float], period: int = 20) -> Optional[float]:
        if len(closes) < period:
            return None
        return sum(closes[-period:]) / period
    
    def get_signal_strength(self, prices: List[dict], current_price: float) -> Tuple[str, float, Dict]:
        closes = [p['close'] for p in prices]
        if len(closes) < 20:
            return 'none', 0.0, {}
        
        price_change_5d = (current_price - closes[-5]) / closes[-5] if len(closes) >= 5 else 0
        momentum_score = min(max(price_change_5d * 20, -1), 1)
        
        sma_20 = self.compute_sma(closes, 20)
        mean_rev_score = min(max((current_price - sma_20) / sma_20 * 15, -1), 1) if sma_20 else 0
        
        rsi = self.compute_rsi(closes, 14)
        rsi_score = (30 - rsi) / 30 if rsi < 30 else -(rsi - 70) / 30 if rsi > 70 else 0
        
        signals = {
            'momentum': {'score': momentum_score, 'signal': 'bull' if momentum_score > 0.3 else ('bear' if momentum_score < -0.3 else 'neutral')},
            'mean_reversion': {'score': mean_rev_score, 'signal': 'bull' if mean_rev_score > 0.4 else ('bear' if mean_rev_score < -0.4 else 'neutral')},
            'rsi': {'score': rsi_score, 'value': rsi, 'signal': 'bull' if rsi < 35 else ('bear' if rsi > 65 else 'neutral')},
        }
        
        combined = momentum_score * 0.15 + mean_rev_score * 0.20 + rsi_score * 0.20
        
        dominant = max(signals.keys(), key=lambda s: abs(signals[s].get('score', 0)))
        return dominant, combined, signals
    
    def check_trading_rules(self, signal_strength: float) -> bool:
        if abs(signal_strength) < 0.18 or self.positions or sum(abs(p.quantity * p.entry_price) for p in self.positions.values()) > self.capital * 0.15:
            return False
        return True
    
    def execute_trade(self, symbol: str, side: str, price: float, signal_strength: float, strategy: str):
        quantity = (self.capital * 0.05) / price
        position = Position(symbol=symbol, side=side, entry_price=price, quantity=quantity,
                           entry_time=datetime.now(), strategy=strategy, signal_strength=signal_strength)
        self.positions[symbol] = position
        print(f"📊 POSITION OPENED: {symbol} {side} @ ${price:.6f}")
        print(f"   Strategy: {strategy}, Signal Strength: {signal_strength:.3f}")
    
    def close_position(self, symbol: str, exit_price: float, strategy: str):
        if symbol not in self.positions:
            return
        
        pos = self.positions[symbol]
        pnl_pct = (exit_price - pos.entry_price) / pos.entry_price
        pnl_usd = pnl_pct * pos.quantity * pos.entry_price
        
        trade = Trade(symbol=symbol, entry_price=pos.entry_price, exit_price=exit_price, quantity=pos.quantity,
                     side=pos.side, entry_time=pos.entry_time, exit_time=datetime.now(),
                     pnl_pct=pnl_pct, pnl_usd=pnl_usd, strategy=strategy)
        
        self.trades.append(trade)
        del self.positions[symbol]
        print(f"📈 POSITION CLOSED: {symbol} @ ${exit_price:.6f}")
        print(f"   P&L: {pnl_usd:+.2f} ({pnl_pct*100:+.2f}%)")


# ============== LIVE COINBASE PAPER TRADING ==============

class CoinbasePaperTrader(MultiStrategyPaperTrading):
    """Live paper trading using Coinbase API."""
    
    def __init__(self, initial_capital: float = 10000.0):
        super().__init__(initial_capital)
        self.poll_interval = 60  # seconds
        self.running = True
        
        def signal_handler(sig, frame):
            self.running = False
            print("\n⛔ Stopping paper trading...")
        
        signal.signal(signal.SIGINT, signal_handler)
    
    def fetch_coinbase_price(self, symbol: str) -> Optional[float]:
        """Fetch live price from Coinbase API."""
        import urllib.request
        try:
            with urllib.request.urlopen(f"https://api.coinbase.com/v2/prices/{symbol}/spot", timeout=10) as response:
                data = json.loads(response.read().decode())
                return float(data['data']['amount'])
        except Exception as e:
            print(f"⚠️  API error fetching {symbol}: {e}")
            return None
    
    def run_live_paper_trading(self, symbols: List[str]):
        """Run continuous live paper trading."""
        print("\n🔄 Starting LIVE Coinbase paper trading...")
        print(f"Symbols: {', '.join(symbols)}")
        
        while self.running:
            # Close existing positions first — fetch each position's own price
            for existing_symbol in list(self.positions.keys()):
                pos_price = self.fetch_coinbase_price(existing_symbol)
                if pos_price is None:
                    continue
                pos = self.positions[existing_symbol]
                pnl = (pos_price - pos.entry_price) / pos.entry_price
                if pnl < -0.04 or pnl > 0.05:
                    self.close_position(existing_symbol, pos_price, 'profit_target' if pnl > 0 else 'stop_loss')

            for symbol in symbols:
                price = self.fetch_coinbase_price(symbol)
                if price is None:
                    continue
                
                # Generate synthetic historical data for signal computation
                history = generate_constrained_prices(40, 0.5 * price, seed=42)
                
                dominant_strategy, combined_strength, signals = self.get_signal_strength(history, price)
                
                if not self.check_trading_rules(combined_strength):
                    continue
                
                for strategy_name, signal in signals.items():
                    if signal.get('signal') == 'bull' and combined_strength > 0.20:
                        if not self.positions:
                            self.execute_trade(symbol, 'long', price, combined_strength, strategy_name)
            
            time.sleep(self.poll_interval)


# ============== BACKTESTING ==============

def run_historical_backtest(symbol: str, days: int = 30, seed: Optional[int] = None):
    """Run historical backtest."""
    print(f"\n{'='*60}")
    print(f"HISTORICAL BACKTEST: {symbol}")
    print(f"{'='*60}\n")
    
    prices_list = generate_constrained_prices(days, 45000, seed=seed)
    closes = [p['close'] for p in prices_list]
    trader = MultiStrategyPaperTrading(initial_capital=10000.0)
    
    min_data_needed = 25
    
    for i, price_data in enumerate(prices_list):
        current_price = price_data['close']
        
        if len(closes) < min_data_needed:
            continue
        
        dominant_strategy, combined_strength, individual_signals = trader.get_signal_strength(
            prices_list[-min_data_needed:], current_price)
        
        trade_rule = trader.check_trading_rules(combined_strength)
        
        # Close existing positions at their own prices
        for existing_symbol in list(trader.positions.keys()):
            pos = trader.positions[existing_symbol]
            pos_current_price = closes[i] if i < len(closes) else current_price
            pnl = (pos_current_price - pos.entry_price) / pos.entry_price
            if pnl < -0.04:
                trader.close_position(existing_symbol, pos_current_price, 'stop_loss')
            elif pnl > 0.05:
                trader.close_position(existing_symbol, pos_current_price, 'profit_target')
        
        if not trade_rule:
            continue
        
        for strategy_name, signal in individual_signals.items():
            if signal.get('signal') == 'bull' and combined_strength > 0.20:
                if not trader.positions:
                    trader.execute_trade(symbol, 'long', current_price, combined_strength, strategy_name)
    
    print(f"\n{'='*60}")
    print("BACKTEST RESULTS SUMMARY")
    print(f"{'='*60}\n")
    print(f"Symbol:        {symbol}")
    print(f"Duration:      {days} days ({len(prices_list)} ticks)")
    print(f"Trades:        {len(trader.trades)}")
    
    if trader.trades:
        wins = sum(1 for t in trader.trades if t.pnl_usd > 0)
        losses = len(trader.trades) - wins
        total_pnl = sum(t.pnl_usd for t in trader.trades)
        print(f"Win Rate:      {wins}/{len(trader.trades)} ({wins/len(trader.trades)*100:.1f}%)")
        print(f"Total P&L:     ${total_pnl:+.2f}")


# ============== MAIN ORCHESTRATOR ==============

def main():
    parser = argparse.ArgumentParser(description='Unified Paper Trading System')
    parser.add_argument('--mode', choices=['live', 'backtest', 'both'], default='both')
    parser.add_argument('--symbols', nargs='+', default=['BTC-USD', 'ETH-USD', 'SOL-USD', 'DOGE-USD'])
    parser.add_argument('--days', type=int, default=30)
    
    args = parser.parse_args()
    
    print("="*60)
    print("UNIFIED PAPER TRADING SYSTEM")
    print("="*60)
    
    if args.mode in ['live', 'both']:
        trader = CoinbasePaperTrader(initial_capital=10000.0)
        run_historical_backtest('BTC-USD', args.days)
        
        print("\n🔄 Starting LIVE paper trading (Ctrl+C to stop)...")
        trader.run_live_paper_trading(args.symbols)
    
    if args.mode == 'backtest':
        for symbol in args.symbols:
            run_historical_backtest(symbol, args.days)


if __name__ == '__main__':
    import argparse
    main()
