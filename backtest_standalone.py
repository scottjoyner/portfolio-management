#!/usr/bin/env python3
"""Historical Backtesting Engine for Multi-Strategy Paper Trading.
Standalone implementation with no external dependencies beyond stdlib."""

import sys, json, time, random, statistics
from pathlib import Path
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple, Any
from datetime import datetime, timedelta


@dataclass
class Trade:
    """Represents a completed trade."""
    symbol: str
    entry_price: float
    exit_price: float
    quantity: float
    side: str  # 'long' or 'short'
    entry_time: datetime
    exit_time: datetime
    pnl_pct: float
    pnl_usd: float
    strategy: str


@dataclass
class Position:
    """Track an active position."""
    symbol: str
    side: str  # 'long' or 'short'
    entry_price: float
    quantity: float
    entry_time: datetime
    strategy: str
    signal_strength: float


class HistoricalDataGenerator:
    """Generates realistic historical price data for testing."""
    
    def __init__(self, seed: int = 42):
        random.seed(seed)
        
    def generate_reshaped_data(self, symbol: str, days: int = 30) -> List[dict]:
        """Generate recent reshaped data with realistic signals.
        
        Uses a simpler approach that guarantees signal-generating conditions.
        """
        base_prices = {
            'BTC-USD': 45000, 'ETH-USD': 2800, 'SOL-USD': 120,
            'DOGE-USD': 0.08, 'XRP-USD': 0.60, 'ADA-USD': 0.35,
            'AVAX-USD': 35, 'LINK-USD': 14, 'DOT-USD': 6.0,
            'GRT-USD': 0.18, 'CRV-USD': 0.25, 'ZRX-USD': 0.40
        }
        
        daily_vol = 0.04
        
        base_price = base_prices.get(symbol, 100)
        
        # Generate trading days (Mon-Fri only)
        dates = []
        current_date = datetime.now().date() - timedelta(days=days*2)
        while len(dates) < days:
            if current_date.weekday() < 5:  # Monday-Friday only
                dates.append(current_date)
            current_date += timedelta(days=1)
        
        data = []
        price = base_price * random.uniform(0.9, 1.1)
        
        for i, date in enumerate(dates):
            # Introduce signal conditions periodically after initial period
            if i > days * 0.2:
                cycle_phase = i % 30
                
                if cycle_phase < 8:  # Accumulation / downtrend (oversold setup)
                    change_rate = random.gauss(-0.015, 0.01)
                    price = max(price * (1 + change_rate), base_price * 0.75)
                elif cycle_phase < 12:  # Sharp reversal up
                    change_rate = random.gauss(0.04, 0.015)
                    price = min(price * (1 + change_rate), base_price * 1.3)
                elif cycle_phase < 20:  # Strong uptrend / momentum
                    change_rate = random.gauss(0.025, 0.01)
                    price = max(price * (1 + change_rate), base_price * 0.95)
                else:  # Distribution / downtrend setup
                    change_rate = random.gauss(-0.02, 0.01)
                    price = max(price * (1 + change_rate), base_price * 0.8)
            else:
                change = random.gauss(0, daily_vol/2)
                price = max(price * (1 + change), base_price * 0.7)
            
            open_p = price
            high = price * (1 + abs(random.gauss(0, daily_vol/3)))
            low = price / (1 + abs(random.gauss(0, daily_vol/3)))
            volume = random.randint(int(1e6), int(5e7))
            
            data.append({
                'date': date,
                'open': open_p,
                'high': max(open_p, high),
                'low': min(open_p, low),
                'close': price,
                'volume': volume
            })
        
        return data


class MultiStrategyPaperTrading:
    """Multi-strategy paper trading engine using historical data."""
    
    def __init__(self, initial_capital: float = 10000.0):
        self.initial_capital = initial_capital
        self.capital = initial_capital
        self.positions: Dict[str, Position] = {}
        self.trades: List[Trade] = []
        self.signal_history: List[Dict] = []
        
    def compute_rsi(self, closes: List[float], period: int = 14) -> float:
        """Compute RSI value."""
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
        rsi = 100 - (100 / (1 + rs))
        return rsi
    
    def compute_sma(self, closes: List[float], period: int = 20) -> Optional[float]:
        """Compute Simple Moving Average."""
        if len(closes) < period:
            return None
        return sum(closes[-period:]) / period
    
    def get_signal_strength(self, symbol: str, prices: List[float], 
                           current_price: float) -> Tuple[str, float, Dict]:
        """Compute signal strength across all strategies.
        
        Returns: (dominant_strategy, combined_strength, individual_signals)
        """
        signals = {}
        closes = [p['close'] for p in prices]
        
        if len(closes) < 20:
            return 'none', 0.0, {}
        
        # Momentum strategy (15% weight)
        price_change_5d = (current_price - closes[-5]) / closes[-5] if len(closes) >= 5 else 0
        momentum_score = min(max(price_change_5d * 20, -1), 1)
        signals['momentum'] = {
            'score': momentum_score,
            'price_change_pct': price_change_5d * 100,
            'signal': 'bull' if momentum_score > 0.4 else ('bear' if momentum_score < -0.4 else 'neutral')
        }
        
        # Mean Reversion (20% weight)
        sma_20 = self.compute_sma(closes, 20)
        if sma_20:
            deviation = (current_price - sma_20) / sma_20
            mean_rev_score = min(max(deviation * 15, -1), 1)
            signals['mean_reversion'] = {
                'score': mean_rev_score,
                'deviation_pct': deviation * 100,
                'sma_20': sma_20,
                'signal': 'bull' if mean_rev_score > 0.5 else ('bear' if mean_rev_score < -0.5 else 'neutral')
            }
        
        # RSI (20% weight)
        rsi = self.compute_rsi(closes, 14)
        rsi_score = (100 - rsi) / 100 if rsi > 70 else (rsi - 50) / 50 if rsi < 50 else 0
        signals['rsi'] = {
            'score': rsi_score,
            'value': rsi,
            'signal': 'bull' if rsi < 30 else ('bear' if rsi > 70 else 'neutral')
        }
        
        # Breakout (25% weight)
        atr = statistics.mean([closes[i] - closes[i-1] for i in range(1, len(closes))[-14:]]) if len(closes) >= 14 else current_price * 0.02
        breakout_upper = sma_20 + 2 * atr if sma_20 else current_price
        breakout_lower = sma_20 - 2 * atr if sma_20 else current_price
        breakout_score = (current_price - breakout_upper) / atr if current_price > breakout_upper else \
                        (breakout_lower - current_price) / atr if current_price < breakout_lower else 0
        signals['breakout'] = {
            'score': min(max(breakout_score, -1), 1),
            'upper_breakout': breakout_upper,
            'signal': 'bull' if current_price > breakout_upper else ('bear' if current_price < breakout_lower else 'neutral')
        }
        
        # Combine weighted scores
        weights = {'momentum': 0.15, 'mean_reversion': 0.20, 'rsi': 0.20, 'breakout': 0.25}
        combined = sum(signals.get(k, {}).get('score', 0) * w for k, w in weights.items())
        
        dominant = max(signals.keys(), key=lambda s: abs(signals[s].get('score', 0))) if signals else 'none'
        
        return dominant, combined, signals
    
    def check_trading_rules(self, signal_strength: float) -> bool:
        """Check if trading rules are satisfied."""
        # Minimum signal strength threshold
        if abs(signal_strength) < 0.15:
            return False
        
        # Position size limit (max 15% of capital per trade)
        current_exposure = sum(abs(p.quantity * p.entry_price) for p in self.positions.values())
        max_position_size = self.capital * 0.15
        
        if current_exposure > max_position_size:
            return False
        
        # Check existing position to avoid doubling down
        if self.positions:
            return False
        
        return True
    
    def execute_trade(self, symbol: str, side: str, price: float, 
                     signal_strength: float, strategy: str):
        """Execute a simulated trade."""
        
        # Calculate position size (5% of capital)
        quantity = (self.capital * 0.05) / price
        
        position = Position(
            symbol=symbol,
            side=side,
            entry_price=price,
            quantity=quantity,
            entry_time=datetime.now(),
            strategy=strategy,
            signal_strength=signal_strength
        )
        
        self.positions[symbol] = position
        
        print(f"📊 POSITION OPENED: {symbol} {side} @ ${price:.6f}")
        print(f"   Strategy: {strategy}, Signal Strength: {signal_strength:.3f}")
        print(f"   Quantity: {quantity:.4f}, Size: ${(quantity * price):.2f}")
    
    def close_position(self, symbol: str, exit_price: float, strategy: str):
        """Close an existing position and record the trade."""
        if symbol not in self.positions:
            return
        
        pos = self.positions[symbol]
        
        # Calculate P&L
        if pos.side == 'long':
            pnl_pct = (exit_price - pos.entry_price) / pos.entry_price
            pnl_usd = pnl_pct * pos.quantity * pos.entry_price
        else:  # short
            pnl_pct = (pos.entry_price - exit_price) / pos.entry_price
            pnl_usd = pnl_pct * pos.quantity * pos.entry_price
        
        trade = Trade(
            symbol=symbol,
            entry_price=pos.entry_price,
            exit_price=exit_price,
            quantity=pos.quantity,
            side=pos.side,
            entry_time=pos.entry_time,
            exit_time=datetime.now(),
            pnl_pct=pnl_pct,
            pnl_usd=pnl_usd,
            strategy=strategy
        )
        
        self.trades.append(trade)
        del self.positions[symbol]
        
        print(f"📈 POSITION CLOSED: {symbol} @ ${exit_price:.6f}")
        print(f"   P&L: {pnl_usd:+.2f} ({pnl_pct*100:+.2f}%)")


def run_historical_backtest(symbol: str, days: int = 30):
    """Run a complete historical backtest."""
    
    print(f"\n{'='*60}")
    print(f"HISTORICAL BACKTEST: {symbol}")
    print(f"{'='*60}\n")
    
    # Initialize components
    data_generator = HistoricalDataGenerator(seed=42)
    trader = MultiStrategyPaperTrading(initial_capital=10000.0)
    
    # Generate historical data
    print("📝 Generating historical price data...")
    prices_list = data_generator.generate_reshaped_data(symbol, days)
    
    if not prices_list:
        print("⚠ No data generated, aborting.")
        return
    
    closes = [p['close'] for p in prices_list]
    min_data_needed = 30  # For strategy signals
    
    for i, price_data in enumerate(prices_list):
        current_price = price_data['close']
        
        # Need minimum data for signals
        if len(closes) < min_data_needed:
            continue
        
        # Get signal strength
        dominant_strategy, combined_strength, individual_signals = trader.get_signal_strength(
            symbol, prices_list[-min_data_needed:], current_price
        )
        
        trade_rule = trader.check_trading_rules(combined_strength)
        
        if not trade_rule:
            continue
        
        # Check for position close signal (mean reversion back to average OR profit target)
        for existing_symbol in list(trader.positions.keys()):
            sma_20 = trader.compute_sma(closes[-min_data_needed:], 20)
            pos = trader.positions[existing_symbol]
            
            # Exit if mean reverting back toward average (within 1%)
            if sma_20 and abs((current_price - sma_20) / sma_20) < 0.01:
                trader.close_position(existing_symbol, current_price, 'mean_reversion')
            
            # Simple profit target: 5% gain on position
            elif pos.side == 'long' and (current_price - pos.entry_price) / pos.entry_price > 0.05:
                trader.close_position(existing_symbol, current_price, 'profit_target')
        
        # Signal detection and trade execution
        for strategy_name, signal in individual_signals.items():
            if signal.get('signal') == 'bull' and combined_strength > 0.25:
                # Look for entry opportunity
                if not trader.positions:
                    trader.execute_trade(symbol, 'long', current_price, 
                                       combined_strength, strategy_name)
            
            elif signal.get('signal') == 'bear' and combined_strength < -0.25:
                # Short opportunity (if we have a long position, close it first)
                if symbol in trader.positions:
                    trader.close_position(symbol, current_price, 'breakout')
                else:
                    print(f"⚠️  Bearish signal but no shorting enabled for {symbol}")
        
        # Log signal history
        if dominant_strategy != 'none':
            trader.signal_history.append({
                'timestamp': str(price_data['date']),
                'price': current_price,
                'strategy': dominant_strategy,
                'strength': combined_strength,
                'signals': {k: v.get('signal') for k, v in individual_signals.items()}
            })
        
        if (i + 1) % 50 == 0:
            print(f"Progress: {i}/{len(prices_list)} ticks")
    
    # Print results summary
    print(f"\n{'='*60}")
    print("BACKTEST RESULTS SUMMARY")
    print(f"{'='*60}\n")
    print(f"Symbol:        {symbol}")
    print(f"Duration:      {days} days ({len(prices_list)} ticks)")
    print(f"Initial Cap.:  ${trader.initial_capital:,.2f}")
    print(f"Trades:        {len(trader.trades)}")
    print(f"Signals Found: {len(trader.signal_history)}")
    
    if trader.trades:
        wins = sum(1 for t in trader.trades if t.pnl_usd > 0)
        losses = len(trader.trades) - wins
        total_pnl = sum(t.pnl_usd for t in trader.trades)
        print(f"Win Rate:      {wins}/{len(trader.trades)} ({wins/len(trader.trades)*100:.1f}%)")
        print(f"Total P&L:     ${total_pnl:+.2f}")
        
        # Save results
        results = {
            'symbol': symbol,
            'start_date': str(prices_list[0]['date']),
            'end_date': str(prices_list[-1]['date']),
            'ticks': len(prices_list),
            'trades': len(trader.trades),
            'wins': wins,
            'losses': losses,
            'total_pnl_usd': total_pnl,
            'initial_capital': trader.initial_capital,
            'final_capital': trader.capital + sum(t.pnl_usd for t in trader.trades),
            'trades_detail': [
                {
                    'symbol': t.symbol,
                    'side': t.side,
                    'entry_price': t.entry_price,
                    'exit_price': t.exit_price,
                    'pnl_pct': t.pnl_pct,
                    'pnl_usd': t.pnl_usd,
                    'strategy': t.strategy
                } for t in trader.trades
            ],
            'signal_history_count': len(trader.signal_history)
        }
        
        results_file = Path('/home/scott/git/portfolio-management/historical_results.json')
        with open(results_file, 'a') as f:
            json.dump(results, f, indent=2)
            f.write('\n')
        print(f"\nResults saved to: {results_file}")
    
    # Print signal history sample
    if trader.signal_history:
        print("\n📋 Recent Signals:")
        for s in trader.signal_history[-5:]:
            print(f"  {s['timestamp']}: {s['strategy']} @ ${s['price']:.4f} "
                  f"(strength: {s['strength']:+.3f})")
    
    return trader


if __name__ == '__main__':
    import argparse
    
    parser = argparse.ArgumentParser(description='Historical Backtesting Engine')
    parser.add_argument('--symbol', default='BTC-USD', help='Trading symbol (e.g., BTC-USD)')
    parser.add_argument('--days', type=int, default=30, help='Days of data to generate')
    
    args = parser.parse_args()
    
    run_historical_backtest(args.symbol, args.days)
