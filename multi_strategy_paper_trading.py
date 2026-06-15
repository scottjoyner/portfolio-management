#!/usr/bin/env python3
"""Multi-Strategy Paper Trading System with 6-hour benchmark."""

import sys, asyncio, json, time, datetime, math
from pathlib import Path
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple, Any
from enum import Enum
import statistics

# Add portfolio-management to path for trading_system imports
PM_ROOT = Path("/home/scott/git/portfolio-management")
if str(PM_ROOT) not in sys.path:
    sys.path.insert(0, str(PM_ROOT))


@dataclass
class Signal:
    """Trading signal from a strategy."""
    symbol: str
    action: str  # 'BUY', 'SELL', 'HOLD'
    strength: float  # -1 to +1
    reason: str
    strategy: str


class StrategyType(Enum):
    MOMENTUM = "momentum"
    MEAN_REVERSION = "mean_reversion"
    BREAKOUT = "breakout"
    RSI_OVERSOLD_OVERBOUGHT = "rsi_oscillator"
    VOLATILITY_BREAKOUT = "volatility_breakout"


class Strategy:
    """Base strategy class."""
    
    def __init__(self, name: str, strategy_type: StrategyType):
        self.name = name
        self.type = strategy_type
        self.signals: List[Signal] = []
        
    def generate_signal(self, symbol: str, price_data: dict, history: list) -> Optional[Signal]:
        raise NotImplementedError


class MomentumStrategy(Strategy):
    """Momentum-based strategy: buy strong performers, short weak ones."""
    
    def __init__(self):
        super().__init__("Momentum", StrategyType.MOMENTUM)
        
    def generate_signal(self, symbol: str, price_data: dict, history: list) -> Optional[Signal]:
        change_pct = price_data.get('price_percentage_change_24h', 0)
        volume = float(price_data.get('volume_24h', 0))
        
        # Strong momentum entry (buy when up > +3% with good volume)
        if change_pct > 3.0 and volume > 1e6:
            return Signal(symbol, 'BUY', change_pct / 50.0, 
                         f"Strong upward momentum: Δ{change_pct:.1f}%", "momentum")
        
        # Momentum exit (sell when down < -2%)
        if change_pct < -2.0:
            return Signal(symbol, 'SELL', abs(change_pct) / 50.0, 
                         f"Weak momentum: Δ{change_pct:.1f}%", "momentum")
        
        return None


class MeanReversionStrategy(Strategy):
    """Mean reversion: buy when oversold, sell when overbought."""
    
    def __init__(self, lookback_period: int = 20):
        super().__init__("Mean Reversion", StrategyType.MEAN_REVERSION)
        self.lookback = lookback_period
        
    def generate_signal(self, symbol: str, price_data: dict, history: list) -> Optional[Signal]:
        if len(history) < self.lookback:
            return None
            
        prices = [h['close'] for h in history[-self.lookback:] if 'close' in h]
        current_price = float(price_data.get('price', 0))
        change_pct = price_data.get('price_percentage_change_24h', 0)
        
        # Calculate Bollinger Bands-like metrics
        mean_price = statistics.mean(prices)
        std_price = statistics.stdev(prices) if len(prices) > 1 else 1
        
        z_score = (current_price - mean_price) / max(std_price, 0.01)
        
        # Buy when significantly below mean (>2 std devs down)
        if z_score < -2.5 and change_pct < 0:
            strength = abs(z_score) / 20.0
            return Signal(symbol, 'BUY', min(strength, 1.0),
                         f"Oversold (z={z_score:.1f}), down {change_pct:.1f}%", "mean_reversion")
        
        # Sell when significantly above mean (>2 std devs up)
        if z_score > 2.5 and change_pct > 2:
            strength = abs(z_score) / 20.0
            return Signal(symbol, 'SELL', min(strength, 1.0),
                         f"Overbought (z={z_score:.1f}), up {change_pct:.1f}%", "mean_reversion")
        
        return None


class RSIStrategy(Strategy):
    """RSI-based overbought/oversold strategy."""
    
    def __init__(self, period: int = 14, oversold: float = 30, overbought: float = 70):
        super().__init__("RSI Oscillator", StrategyType.RSI_OVERSOLD_OVERBOUGHT)
        self.period = period
        self.oversold_threshold = oversold
        self.overbought_threshold = overbought
        
    def calculate_rsi(self, prices: list) -> float:
        if len(prices) < 2:
            return 50.0
            
        gains = []
        losses = []
        for i in range(1, min(len(prices), self.period + 1)):
            change = prices[i] - prices[i-1]
            gains.append(max(change, 0))
            losses.append(abs(min(change, 0)))
            
        avg_gain = statistics.mean(gains) if gains else 0
        avg_loss = statistics.mean(losses) if losses else 1
        
        rs = avg_gain / max(avg_loss, 0.01)
        rsi = 100 - (100 / (1 + rs))
        return rsi
        
    def generate_signal(self, symbol: str, price_data: dict, history: list) -> Optional[Signal]:
        if len(history) < self.period:
            return None
            
        prices = [h['close'] for h in history[-self.period*2:] if 'close' in h]
        rsi = self.calculate_rsi(prices)
        
        current_price = float(price_data.get('price', 0))
        change_pct = price_data.get('price_percentage_change_24h', 0)
        
        # RSI oversold - buy opportunity
        if rsi < self.oversold_threshold and change_pct < -1:
            strength = (self.oversold_threshold - rsi) / 50.0
            return Signal(symbol, 'BUY', min(strength, 1.0),
                         f"RSI oversold: {rsi:.0f} (<{self.oversold_threshold})", "rsi")
        
        # RSI overbought - sell signal
        if rsi > self.overbought_threshold and change_pct > 2:
            strength = (rsi - self.overbought_threshold) / 50.0
            return Signal(symbol, 'SELL', min(strength, 1.0),
                         f"RSI overbought: {rsi:.0f} (>{self.overbought_threshold})", "rsi")
        
        return None


class BreakoutStrategy(Strategy):
    """Breakout strategy using support/resistance levels."""
    
    def __init__(self, lookback_period: int = 50):
        super().__init__("Breakout", StrategyType.BREAKOUT)
        self.lookback = lookback_period
        
    def generate_signal(self, symbol: str, price_data: dict, history: list) -> Optional[Signal]:
        if len(history) < self.lookback:
            return None
            
        ohlvs = [h for h in history[-self.lookback:] if all(k in h for k in ['open', 'high', 'low', 'close'])]
        
        if not ohlvs:
            return None
            
        highs = [o['high'] for o in ohlvs]
        lows = [o['low'] for o in ohlvs]
        
        resistance = max(highs)
        support = min(lows)
        current_price = float(price_data.get('price', 0))
        change_pct = price_data.get('price_percentage_change_24h', 0)
        
        # Breakout above resistance - buy signal
        if current_price > resistance * 1.005 and change_pct > 2:
            strength = (current_price / resistance - 1) / 0.01
            return Signal(symbol, 'BUY', min(strength, 1.0),
                         f"Breakout above resistance (${resistance:.2f})", "breakout")
        
        # Breakdown below support - sell signal
        if current_price < support * 0.995 and change_pct < -1:
            strength = (support / current_price - 1) / 0.01
            return Signal(symbol, 'SELL', min(strength, 1.0),
                         f"Breakdown below support (${support:.2f})", "breakout")
        
        return None


class VolatilityStrategy(Strategy):
    """Volatility-based mean reversion."""
    
    def __init__(self, atr_period: int = 14):
        super().__init__("ATR Volatility", StrategyType.VOLATILITY_BREAKOUT)
        self.atr_period = atr_period
        
    def calculate_atr(self, history: list) -> float:
        if len(history) < self.atr_period + 1:
            return 0.0
            
        trs = []
        for i in range(1, min(len(history), self.atr_period + 2)):
            h, l, c = history[i]['high'], history[i]['low'], history[i-1].get('close', 0)
            tr = max(h - l, abs(h - c), abs(l - c))
            trs.append(tr)
            
        atr = statistics.mean(trs) if trs else 0.0
        return atr
        
    def generate_signal(self, symbol: str, price_data: dict, history: list) -> Optional[Signal]:
        if len(history) < self.atr_period:
            return None
            
        current_price = float(price_data.get('price', 0))
        change_pct = price_data.get('price_percentage_change_24h', 0)
        atr = self.calculate_atr(history)
        
        if atr == 0:
            return None
            
        # Large move down (more than 3x ATR) - potential reversal entry
        if change_pct < -3.0 and float(price_data.get('price', 0)) > 0:
            strength = 1.5 / max(3.0, abs(change_pct) / 2.0)
            return Signal(symbol, 'BUY', min(strength, 1.0),
                         f"Oversold bounce (Δ{change_pct:.1f}%, ATR={atr:.4F})", "volatility")
        
        # Large move up - profit taking
        if change_pct > 3.5:
            strength = 1.5 / max(3.5, change_pct / 2.0)
            return Signal(symbol, 'SELL', min(strength, 1.0),
                         f"Overextended (Δ{change_pct:.1f}%), take profit", "volatility")
        
        return None


class PaperTradingSystem:
    """Paper trading system managing multiple strategies."""
    
    def __init__(self, initial_capital: float = 10000.0):
        self.initial_capital = initial_capital
        self.capital = initial_capital
        self.positions: Dict[str, dict] = {}
        self.trades: List[dict] = []
        self.snapshots: List[dict] = []
        
    async def connect(self):
        """Initialize the trading system."""
        print(f"🔌 Connected to Paper Trading System")
        print(f"💰 Initial capital: ${self.capital:,.2f}")
        
    def calculate_portfolio_value(self) -> float:
        """Calculate total portfolio value."""
        position_value = sum(p['value'] for p in self.positions.values())
        return self.capital + position_value
    
    def get_position_pnl(self, symbol: str) -> Tuple[float, float]:
        """Get unrealized PnL for a position."""
        if symbol not in self.positions:
            return 0.0, 0.0
            
        pos = self.positions[symbol]
        current_price = float(pos.get('current_price', 0))
        avg_cost = pos['avg_cost']
        qty = pos['quantity']
        
        pnl = (current_price - avg_cost) * qty
        pnl_pct = (pnl / max(avg_cost * qty, 1)) * 100
        
        return pnl, pnl_pct
    
    def execute_signal(self, signal: Signal, current_price: float):
        """Execute a trading signal."""
        if signal.action == 'BUY':
            # Check if already have position, if so skip (would be complex to manage)
            if signal.symbol in self.positions:
                return
                
            target_value = self.capital * 0.15  # Max 15% per position
            qty = int(target_value / current_price)
            
            if qty == 0:
                return
                
            order = {
                'timestamp': datetime.datetime.now().isoformat(),
                'action': 'BUY',
                'symbol': signal.symbol,
                'price': current_price,
                'quantity': qty,
                'reason': signal.reason,
                'strategy': signal.strategy
            }
            
            self.positions[signal.symbol] = {
                'symbol': signal.symbol,
                'quantity': qty,
                'avg_cost': current_price,
                'value': round(qty * current_price, 2),
                'entry_reason': signal.reason
            }
            
            self.trades.append(order)
            print(f"   🟢 BUY {signal.symbol} @ ${current_price:,.2f} | {signal.reason}")
            
        elif signal.action == 'SELL' and signal.symbol in self.positions:
            pos = self.positions[signal.symbol]
            qty = pos['quantity']
            
            order = {
                'timestamp': datetime.datetime.now().isoformat(),
                'action': 'SELL',
                'symbol': signal.symbol,
                'price': current_price,
                'quantity': qty,
                'reason': signal.reason,
                'strategy': signal.strategy
            }
            
            pnl, pnl_pct = self.get_position_pnl(signal.symbol)
            cash_proceeds = qty * current_price
            
            del self.positions[signal.symbol]
            self.capital += cash_proceeds
            
            self.trades.append(order)
            print(f"   🔴 SELL {signal.symbol} @ ${current_price:,.2f} | PnL: ${pnl:,.2F} ({pnl_pct:.1f}%) | {signal.reason}")


async def fetch_top_pairs_by_volume(top_n=20):
    """Fetch market data for many coins and rank by volume_24h."""
    from trading_system.connectors.coinbase_v3 import CoinbaseConnectorV3 as CB
    
    cb = CB()
    
    candidates = [
        'BTC-USD', 'ETH-USD', 'SOL-USD', 'DOGE-USD', 'XRP-USD',
        'ADA-USD', 'AVAX-USD', 'LINK-USD', 'DOT-USD', 'MATIC-USD',
        'UNI-USD', 'AAVE-USD', 'CRV-USD', 'MKR-USD', 'GRT-USD',
        'SNX-USD', 'COMP-USD', 'YFI-USD', 'BAL-USD', 'ZRX-USD'
    ]
    
    results = {}
    for sym in candidates:
        try:
            data = cb.get_price(sym)
            if isinstance(data, str):
                data = json.loads(data)
            
            if not isinstance(data, dict):
                continue
                
            vol_str = data.get('volume_24h', '0')
            results[sym] = {
                'symbol': sym,
                'price': float(data['price']),
                'volume': float(vol_str),
                'change_pct': float(data.get('price_percentage_change_24h', 0))
            }
        except Exception as e:
            pass
    
    sorted_pairs = sorted(results.values(), key=lambda x: x['volume'], reverse=True)
    return sorted_pairs[:top_n], sorted_pairs


class MultiStrategyPaperTrading:
    """Main paper trading orchestrator."""
    
    def __init__(self, initial_capital: float = 10000.0):
        self.system = PaperTradingSystem(initial_capital)
        self.strategies: List[Strategy] = [
            MomentumStrategy(),
            MeanReversionStrategy(lookback_period=20),
            RSIStrategy(period=14, oversold=30, overbought=70),
            BreakoutStrategy(lookback_period=50),
            VolatilityStrategy(atr_period=14)
        ]
        
    async def run_benchmark(self, duration_hours: int = 6, poll_interval: float = 60.0):
        """Run a multi-strategy benchmark."""
        
        print("\n" + "=" * 80)
        print("🚀 MULTI-STRATEGY PAPER TRADING BENCHMARK")
        print("=" * 80)
        print(f"📊 Duration: {duration_hours} hours | Poll interval: {poll_interval}s")
        print("=" * 80 + "\n")
        
        # Initialize
        await self.system.connect()
        
        top_pairs, all_pairs = await fetch_top_pairs_by_volume(20)
        symbols = [p['symbol'] for p in top_pairs]
        
        print(f"📋 Trading {len(symbols)} pairs:")
        for i, p in enumerate(top_pairs[:8]):
            sym = p['symbol'].split('-')[0]
            price = f"${p['price']:,.2f}"
            change_str = f"{p['change_pct']:.1f}%"
            print(f"   {i+1:2}. {sym:>5s}: {price:>13s} | Δ{change_str}")
        if len(symbols) > 8:
            print(f"   ... and {len(symbols)-8} more\n")
        
        # Strategy initialization - load historical data
        print("📚 Loading historical data for strategies...\n")
        history_store: Dict[str, list] = {}
        for sym in symbols[:5]:  # Load history for first 5 pairs to save time
            history_store[sym] = []
            
        tick_num = 0
        start_time = time.time()
        
        print("=" * 80)
        print("🔄 STARTING LIVE TRADING LOOP")
        print("=" * 72 + "\n")
        
        # Run for specified duration
        max_ticks = int(duration_hours * 3600 / poll_interval)
        
        while tick_num < max_ticks:
            try:
                live_data = {}
                for sym in symbols[:5]:  # Trade first 5 pairs per iteration
                    data = self.system._coinbase_connector.get_price(sym) if hasattr(self.system, '_coinbase_connector') else None
                    if isinstance(data, str):
                        data = json.loads(data)
                    
                    if not isinstance(data, dict):
                        continue
                    
                    live_data[sym] = {
                        'price': float(data['price']),
                        'change_pct': float(data.get('price_percentage_change_24h', 0)),
                        'volume': data.get('volume_24h', 'N/A'),
                        'high_24h': float(data.get('high_24h', 0)),
                        'low_24h': float(data.get('low_24h', 0))
                    }
                    
                # Generate signals from all strategies
                all_signals: List[Signal] = []
                for strategy in self.strategies:
                    for sym, pdata in live_data.items():
                        history = history_store.get(sym, [])
                        
                        signal = strategy.generate_signal(sym, pdata, history)
                        if signal:
                            all_signals.append(signal)
                
                # Signal aggregation - weighted voting
                buy_count = sum(1 for s in all_signals if s.action == 'BUY')
                sell_count = sum(1 for s in all_signals if s.action == 'SELL')
                
                # Print signals summary every 10 ticks
                if tick_num % 10 == 0:
                    print(f"\n   ⏰ Tick #{tick_num} | Signals: 🟢{buy_count} BUY, 🔴{sell_count} SELL")
                    
                # Execute strongest signal for each direction
                sorted_signals = sorted(all_signals, key=lambda s: abs(s.strength), reverse=True)
                
                executed_signals = []
                seen_symbols = set()
                
                for sig in sorted_signals:
                    if sig.symbol in seen_symbols:
                        continue
                    
                    # Only execute if signal strength > threshold
                    if sig.action == 'BUY' and sig.strength > 0.15 and buy_count > 0:
                        price = live_data.get(sig.symbol, {}).get('price', 0)
                        self.system.execute_signal(sig, price)
                        executed_signals.append(sig)
                        seen_symbols.add(sig.symbol)
                        
                    elif sig.action == 'SELL' and sig.strength > 0.15 and sell_count > 0:
                        price = live_data.get(sig.symbol, {}).get('price', 0)
                        self.system.execute_signal(sig, price)
                        executed_signals.append(sig)
                        seen_symbols.add(sig.symbol)
                
                tick_num += 1
                
                # Progress indicator
                if tick_num % 50 == 0 and tick_num > 0:
                    elapsed = time.time() - start_time
                    print(f"\n   ⏱️  Progress: {tick_num}/{max_ticks} ticks ({elapsed/3600:.2f}h)")
                    
            except Exception as e:
                print(f"⚠️ Error at tick {tick_num}: {e}")
                
        # Final summary
        print("\n" + "=" * 80)
        print("🏁 BENCHMARK COMPLETE")
        print("=" * 72 + "\n")
        
        total_value = self.system.calculate_portfolio_value()
        initial_value = self.system.initial_capital
        
        pnl = total_value - initial_value
        pnl_pct = (pnl / max(initial_value, 1)) * 100
        
        # Collect signal distribution
        strategy_sigs: Dict[str, int] = {}
        for trade in self.system.trades:
            strat = trade.get('strategy', 'unknown')
            action = trade['action']
            key = f"{strat}:{action}"
            strategy_sigs[key] = strategy_sigs.get(key, 0) + 1
            
        print(f"📈 FINAL SUMMARY")
        print("-" * 40)
        print(f"   Duration:          {time.time() - start_time / 3600:.1f}h")
        print(f"   Total ticks:       {tick_num}")
        print(f"   Signals generated: {sum(strategy_sigs.values())}")
        print(f"   Trades executed:   {len(self.system.trades)}")
        print(f"   Positions held:    {len(self.system.positions)}")
        print(f"\n   📊 Performance:")
        print(f"   Initial capital:   ${initial_value:,.2f}")
        print(f"   Final value:       ${total_value:,.2f}")
        print(f"   Absolute P&L:      ${pnl:,.2F} ({pnl_pct:.1f}%)")
        
        print(f"\n📋 Signal distribution by strategy:")
        for key, count in sorted(strategy_sigs.items()):
            print(f"   {key}: {count}")
            
        # Save results
        results_file = Path('multi_strategy_6hr_results.json')
        with open(results_file, 'w') as f:
            json.dump({
                'start_time': datetime.datetime.now().isoformat(),
                'end_time': datetime.datetime.now().isoformat(),
                'duration_hours': duration_hours,
                'initial_capital': initial_value,
                'final_capital': self.system.capital,
                'total_trades': len(self.system.trades),
                'positions_held': len(self.system.positions),
                'pnl_absolute': pnl,
                'pnl_percent': pnl_pct,
                'trades': self.system.trades[-100:],  # Last 100 trades
                'signal_distribution': strategy_sigs,
                'tick_count': tick_num
            }, f, indent=2)
        
        print(f"\n💾 Results saved to {results_file}")
        return {
            'pnl_pct': pnl_pct,
            'total_trades': len(self.system.trades),
            'positions_held': len(self.system.positions),
            'signal_distribution': strategy_sigs
        }


if __name__ == "__main__":
    import os
    
    async def main():
        # Create and run the multi-strategy paper trading system
        bt = MultiStrategyPaperTrading(initial_capital=10000.0)
        
        results = await bt.run_benchmark(duration_hours=6, poll_interval=60.0)
        
        print("\n✅ 6-hour benchmark complete!")
    
    asyncio.run(main())
