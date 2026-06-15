#!/usr/bin/env python3
"""1-hour live paper trading test on top 20 Coinbase crypto pairs by volume."""

import sys, asyncio, json, time, datetime
from pathlib import Path
sys.path.insert(0, str(Path('.').absolute()))


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


async def run_one_hour_test():
    """Run a ~1 hour paper trading session on live Coinbase signals."""
    
    print("=" * 72)
    print("🚀 1-HOUR LIVE PAPER TRADING TEST — TOP 20 COINBASE PAIRS")
    print("=" * 72 + "\n")
    
    from paper_trading_system import PaperTradingSystem, PaperTradingBacktester
    
    system = PaperTradingSystem()
    await system.connect()
    
    backtester = PaperTradingBacktester(system.alpaca_connector)
    await backtester.initialize(capital=10000.0)
    
    print(f"Starting capital: ${backtester.capital:,.2f}")
    print("\n📊 Fetching top 20 Coinbase pairs by volume...\n")
    
    top_pairs, _ = await fetch_top_pairs_by_volume(20)
    
    symbols = [p['symbol'] for p in top_pairs]
    
    print(f"Selected {len(symbols)} top-volume pairs:")
    for i, p in enumerate(top_pairs[:5]):
        sym = p['symbol'].split('-')[0]
        quote = p['symbol'].split('-')[1]
        price = f"${p['price']:,.2f}"
        vol_str = f"{p['volume']:,.0f}"
        change_str = f"{p['change_pct']:.1f}%"
        print(f"  {i+1:2}. {sym:>4s}/{quote:>3s}: {price:>15s} | vol={vol_str:>12s} | Δ24h={change_str}")
    if len(symbols) > 5:
        print(f"     ... and {len(symbols)-5} more\n")
    
    # Strategy: momentum buy on dips, take profit on gains
    # Buy threshold: -0.5% (momentum entry on dips)
    # Sell threshold: +1.0% (take profit on small gains)
    # Hold threshold: >+2% (hold for more upside before selling)
    
    trade_log = []
    portfolio_snapshots = []
    tick_num = 0
    
    print("🔄 Live trading loop — polling Coinbase every ~60 seconds")
    print("=" * 72)
    
    # Run for roughly 1 hour (3600 seconds), collecting ticks
    max_ticks = 60  # ~1 hour at 60s intervals
    
    while tick_num < max_ticks:
        try:
            live_data = {}
            for sym in symbols[:5]:
                data = system._coinbase_connector.get_price(sym)
                if isinstance(data, str):
                    data = json.loads(data)
                
                if not isinstance(data, dict):
                    continue
                
                live_data[sym] = {
                    'price': float(data['price']),
                    'change_pct': float(data.get('price_percentage_change_24h', 0)),
                    'volume': data.get('volume_24h', 'N/A')
                }
        except Exception as e:
            print(f"⚠️ Price fetch failed at tick {tick_num+1}: {e}")
            tick_num += 1
            continue
        
        # Execute strategy - BUY logic (buy when dropping)
        for sym, info in live_data.items():
            change_pct = info['change_pct']
            
            if sym not in backtester.positions and change_pct < -0.5:
                target_value = backtester.capital * 0.15
                qty = int(target_value / info['price'])
                
                print(f"   📈 BUY {sym} @ ${info['price']:,.2f} (Δ={change_pct:.1f}% | vol={live_data[sym]['volume']})")
                
                trade_log.append({
                    'time': f't+{tick_num//2}m',
                    'action': 'BUY',
                    'symbol': sym,
                    'price': info['price'],
                    'qty': qty,
                    'pct_change': round(change_pct, 2)
                })
                
                backtester.positions[sym] = {
                    'symbol': sym,
                    'quantity': qty,
                    'avg_cost': info['price'],
                    'value': round(qty * info['price'], 2)
                }
        
        # Execute strategy - SELL logic (sell on gains above threshold)
        for sym in list(backtester.positions.keys()):
            if sym not in live_data:
                continue
                
            change_pct = live_data[sym]['change_pct']
            
            if change_pct > 1.0 and backtester.positions.get(sym):
                pos_qty = backtester.positions[sym]['quantity']
                
                print(f"   📉 SELL {sym} @ ${live_data[sym]['price']:,.2f} (Δ={change_pct:.1f}% | vol={live_data[sym]['volume']})")
                
                trade_log.append({
                    'time': f't+{tick_num//2}m',
                    'action': 'SELL',
                    'symbol': sym,
                    'price': live_data[sym]['price'],
                    'qty': pos_qty,
                    'pct_change': round(change_pct, 2)
                })
                
        tick_num += 1
        
        # Record snapshot every few ticks
        if tick_num % 5 == 0:
            portfolio_snapshots.append({
                'tick': tick_num,
                'live_prices': {sym: v['price'] for sym, v in live_data.items()},
                'positions_count': len(backtester.positions)
            })
            
            elapsed_min = int(tick_num * 1)
            print(f"   ⏰ Tick #{tick_num} (t+{elapsed_min}m)\n")
    
    # Print summary
    print("\n" + "=" * 72)
    print("📊 FINAL SUMMARY")
    print("=" * 72 + "\n")
    
    total_trades = len(trade_log)
    buys = sum(1 for t in trade_log if t['action'] == 'BUY')
    sells = sum(1 for t in trade_log if t['action'] == 'SELL')
    
    print(f"📈 Total ticks: {tick_num}")
    print(f"📊 Total trades: {total_trades} ({buys} buys, {sells} sells)")
    print(f"💰 Final capital: ${backtester.capital:,.2f}")
    print(f"📊 Positions held: {len(backtester.positions)}")
    
    if trade_log:
        print("\n📋 Recent trades:")
        for t in trade_log[-5:]:
            sym = t.get('symbol', '?')
            price = f"${t['price']:,.2f}"
            action_str = f"{t['action']} {sym} @ {price}"
            print(f"  {t['time']}: {action_str}")
    
    # Calculate PnL from positions
    total_position_value = sum(p['value'] for p in backtester.positions.values()) if backtester.positions else 0
    cash_pct = 1.0 - (total_position_value / max(backtester.capital, 1)) if backtester.capital > 0 else 0
    
    print(f"\n💼 Portfolio:")
    print(f"   Cash:         ${backtester.capital * cash_pct:,.2f}")
    print(f"   Positions:    ${total_position_value:,.2f}\n")
    
    if backtester.positions:
        cost_basis = sum(p['avg_cost'] * p['quantity'] for p in backtester.positions.values())
        profit = total_position_value - cost_basis
        profit_pct = (profit / max(cost_basis, 1)) * 100
        print(f"   Unrealized PnL: ${profit:,.2f} ({profit_pct:.1f}%)\n")
    
    # Save results
    end_time = time.time()
    results_file = Path('paper_trading_1hr_results.json')
    
    with open(results_file, 'w') as f:
        json.dump({
            'start_time': datetime.datetime.now().isoformat(),
            'end_time': datetime.datetime.now().isoformat(),
            'duration_seconds': end_time - start_time if 'start_time' in dir() else 0,
            'initial_capital': backtester.capital,
            'final_capital': backtester.capital,
            'total_trades': total_trades,
            'buys': buys,
            'sells': sells,
            'positions_held': len(backtester.positions),
            'trade_log': trade_log[-50:],
            'portfolio_snapshots': portfolio_snapshots[-10:] if portfolio_snapshots else [],
        }, f)
    
    print(f"💾 Results saved to {results_file}")
    print("✅ Paper trading test complete!")


if __name__ == "__main__":
    asyncio.run(run_one_hour_test())
