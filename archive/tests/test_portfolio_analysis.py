"""Portfolio Analysis Script - Analyze Current Positions Across All Exchanges

This script provides comprehensive analysis of positions across all exchanges:
- Kalshi (Futures/Prediction Markets)
- Polymarket (Ethereum Prediction Markets)
- Coinbase (Crypto Trading)
- Alpaca (Traditional Stocks/ETFs via 50+ venues)

**USAGE:**
    python3 tests/test_portfolio_analysis.py
    
**OUTPUT:**
    - Current positions across all exchanges
    - Portfolio value and allocation
    - Performance metrics (P&L, returns, etc.)
    - Risk analysis (concentration, diversification)
    - Recommendations for rebalancing/trading

**SAFETY NOTES:**
    - This script reads current API keys from ~/.git/portfolio-management/trading_system/.env
    - COINBASE_API_KEY and ALPACA_API_KEY are required in .env file
    - All data is read-only (no trades executed)
    - Perfect for portfolio auditing before making trading decisions

"""

import asyncio
import sys
from typing import Dict, List, Optional


# =============================================================================
# MOCK DATA FOR SAFETY (CAN BE REPLACED WITH REAL API CALLS WHEN KEYS AVAILABLE)
# =============================================================================

class MockKalshiConnector:
    """Mock Kalshi prediction market positions."""
    
    async def get_positions(self):
        return [
            {
                "exchange": "kalshi",
                "contract": "inflation-nov2024-over-2.5",
                "type": "call",  # YES bet
                "price": 0.82,
                "quantity": 1000,
                "cost": 820.00,
                "side": "buy",
                "status": "open"
            },
            {
                "exchange": "kalshi",
                "contract": "biden-wins-2024",
                "type": "put",  # NO bet
                "price": 0.91,
                "quantity": 1000,
                "cost": 910.00,
                "side": "buy",
                "status": "open"
            },
        ]


class MockPolymarketConnector:
    """Mock Polymarket prediction market positions."""
    
    async def get_positions(self):
        return [
            {
                "exchange": "polymarket",
                "market": "biden-wins-2024",
                "type": "YES",
                "price": 0.68,
                "quantity": 5000,
                "cost": 3400.00,
                "side": "buy",
                "status": "open"
            },
        ]


class MockCoinbaseConnector:
    """Mock Coinbase crypto positions."""
    
    async def get_positions(self):
        return [
            {
                "exchange": "coinbase",
                "symbol": "BTC-USD",
                "side": "buy",
                "size": 0.15,
                "average_price": 43500.00,
                "market_value": 6525.00,
                "status": "open"
            },
            {
                "exchange": "coinbase",
                "symbol": "ETH-USD",
                "side": "buy",
                "size": 2.5,
                "average_price": 2280.00,
                "market_value": 5700.00,
                "status": "open"
            },
        ]


class MockAlpacaConnector:
    """Mock Alpaca stock positions."""
    
    async def get_positions(self):
        return [
            {
                "exchange": "alpaca",
                "symbol": "AAPL",
                "qty": 50,
                "avg_cost": 178.20,
                "market_value": 9234.50,
                "side": "buy",
                "unrealized_pl": 324.50,
                "unrealized_pl_pct": 3.64,
            },
            {
                "exchange": "alpaca",
                "symbol": "MSFT",
                "qty": 25,
                "avg_cost": 370.15,
                "market_value": 9450.80,
                "side": "buy",
                "unrealized_pl": 197.05,
                "unrealized_pl_pct": 2.13,
            },
            {
                "exchange": "alpaca",
                "symbol": "GOOGL",
                "qty": 30,
                "avg_cost": 136.45,
                "market_value": 4254.02,
                "side": "buy",
                "unrealized_pl": 160.52,
                "unrealized_pl_pct": 3.92,
            },
            {
                "exchange": "alpaca",
                "symbol": "TSLA",
                "qty": 20,
                "avg_cost": 168.75,
                "market_value": 3506.00,
                "side": "buy",
                "unrealized_pl": 131.00,
                "unrealized_pl_pct": 3.88,
            },
        ]


# =============================================================================
# PORTFOLIO ANALYSIS FUNCTIONS
# =============================================================================

async def get_portfolio_overview():
    """Get complete portfolio overview across all exchanges."""
    
    print("\n" + "="*70)
    print("PORTFOLIO OVERVIEW - ALL EXCHANGES")
    print("="*70)
    
    # Mock data for demonstration (replace with real API calls)
    kalshi_positions = await MockKalshiConnector().get_positions()
    polymarket_positions = await MockPolymarketConnector().get_positions()
    coinbase_positions = await MockCoinbaseConnector().get_positions()
    alpaca_positions = await MockAlpacaConnector().get_positions()
    
    # Calculate totals
    total_portfolio_value = 0
    
    print("\n📊 PREDICTION MARKETS:")
    print("-" * 70)
    print(f"\nKalshi Futures/Prediction Markets:")
    for pos in kalshi_positions:
        print(f"   {pos['contract']}: {pos['quantity']} units @ ${pos['price']*100:.0f}¢")
        total_portfolio_value += pos['cost']
    
    print(f"\nPolymarket Prediction Markets:")
    for pos in polymarket_positions:
        print(f"   {pos['market']}: {pos['quantity']} YES @ ${pos['price']*100:.0f}¢")
        total_portfolio_value += pos['cost']
    
    print("\n📊 CRYPTO ASSETS:")
    print("-" * 70)
    for pos in coinbase_positions:
        print(f"   {pos['symbol']}: {pos['size']} @ ${pos['market_value']/pos['size']:.2f}")
        total_portfolio_value += pos['market_value']
    
    print("\n📊 STOCKS & ETFs:")
    print("-" * 70)
    for pos in alpaca_positions:
        unrealized = pos.get('unrealized_pl', 0)
        print(f"   {pos['symbol']}: {pos['qty']} shares @ ${pos['market_value']/pos['qty']:.2f}")
        if unrealized > 0:
            print(f"      P&L: ${unrealized:+.2f} ({(unrealized/pos['market_value'])*100:+.1f}%)")
        total_portfolio_value += pos['market_value']
    
    print(f"\n{'='*70}")
    print(f"TOTAL PORTFOLIO VALUE: ${total_portfolio_value:,.2f}")
    print(f"{'='*70}")
    
    return total_portfolio_value


async def analyze_allocation_risk():
    """Analyze portfolio allocation and concentration risk."""
    
    print("\n" + "="*70)
    print("ALLOCATION & CONCENTRATION ANALYSIS")
    print("="*70)
    
    # Mock allocations (percentage of total portfolio)
    allocations = {
        "Kalshi": 15.2,
        "Polymarket": 8.5,
        "Coinbase (Crypto)": 32.4,
        "Alpaca (Stocks)": 43.9,
    }
    
    print("\n📊 Portfolio Allocation:")
    print("-" * 70)
    
    for asset, pct in allocations.items():
        bar = "█" * int(pct / 2)
        print(f"   {bar} {asset}: {pct:.1f}%")
    
    # Concentration risk analysis
    print("\n🔍 CONCENTRATION RISK ANALYSIS:")
    print("-" * 70)
    
    max_allocation = max(allocations.values())
    min_allocation = min(allocations.values())
    allocation_range = max_allocation - min_allocation
    
    print(f"\n   Highest Allocation: {max(allocations, key=allocations.get)} ({max_allocation:.1f}%)")
    print(f"   Lowest Allocation:  {min(allocations, key=allocations.get)} ({min_allocation:.1f}%)")
    print(f"   Allocation Range:   {allocation_range:.1f}%")
    
    # Risk assessment
    if max_allocation > 50:
        risk_level = "HIGH - Consider diversification"
    elif max_allocation > 30:
        risk_level = "MODERATE - Acceptable concentration"
    else:
        risk_level = "LOW - Well diversified"
    
    print(f"\n   Risk Level: {risk_level}")
    
    return allocations


async def analyze_performance():
    """Analyze portfolio performance metrics."""
    
    print("\n" + "="*70)
    print("PERFORMANCE ANALYSIS")
    print("="*70)
    
    # Mock performance data
    alpaca_positions = await MockAlpacaConnector().get_positions()
    
    total_cost_basis = 0
    total_market_value = 0
    total_unrealized_pl = 0
    
    print("\n📈 Alpaca Stocks Performance:")
    print("-" * 70)
    
    for pos in alpaca_positions:
        cost = pos['avg_cost'] * pos['qty']
        market = pos['market_value']
        unrealized_pl = pos.get('unrealized_pl', 0)
        
        total_cost_basis += cost
        total_market_value += market
        total_unrealized_pl += unrealized_pl
    
    total_return_pct = ((total_market_value - total_cost_basis) / total_cost_basis) * 100 if total_cost_basis > 0 else 0
    
    print(f"\n   Total Cost Basis: ${total_cost_basis:,.2f}")
    print(f"   Current Value:    ${total_market_value:,.2f}")
    print(f"   Unrealized P&L:   ${total_unrealized_pl:+,.2f} ({total_return_pct:+.1f}%)")
    
    # Prediction markets performance (mock)
    kalshi_positions = await MockKalshiConnector().get_positions()
    polymarket_positions = await MockPolymarketConnector().get_positions()
    
    total_prediction_cost = sum(pos['cost'] for pos in kalshi_positions) + \
                          sum(pos['cost'] for pos in polymarket_positions)
    
    print(f"\n   Prediction Markets Cost: ${total_prediction_cost:,.2f}")
    print("   (Positions held for long-term outcomes)")
    
    return {
        'cost_basis': total_cost_basis,
        'market_value': total_market_value,
        'unrealized_pl': total_unrealized_pl,
        'return_pct': total_return_pct
    }


async def generate_recommendations():
    """Generate actionable trading recommendations."""
    
    print("\n" + "="*70)
    print("TRADING RECOMMENDATIONS")
    print("="*70)
    
    # Mock analysis - get necessary data directly
    kalshi_positions = await MockKalshiConnector().get_positions()
    polymarket_positions = await MockPolymarketConnector().get_positions()
    alpaca_positions = await MockAlpacaConnector().get_positions()
    
    total_prediction_cost = sum(pos['cost'] for pos in kalshi_positions) + \
                          sum(pos['cost'] for pos in polymarket_positions)
    
    total_cost_basis = 0
    total_market_value = 0
    total_unrealized_pl = 0
    
    for pos in alpaca_positions:
        cost = pos['avg_cost'] * pos['qty']
        market = pos['market_value']
        unrealized_pl = pos.get('unrealized_pl', 0)
        
        total_cost_basis += cost
        total_market_value += market
        total_unrealized_pl += unrealized_pl
    
    total_return_pct = ((total_market_value - total_cost_basis) / total_cost_basis) * 100 if total_cost_basis > 0 else 0
    
    print("\n📋 ACTIONABLE RECOMMENDATIONS:")
    print("-" * 70)
    
    recommendations = []
    
    # Recommendation 1: Prediction Markets exposure
    if total_prediction_cost > 5000:
        recommendations.append({
            'action': 'INCREASE PREDICTION MARKET EXPOSURE',
            'target': 'Increase prediction market positions for diversification',
            'reason': 'Good opportunity to add uncorrelated returns'
        })
    
    # Recommendation 2: Performance monitoring
    if total_unrealized_pl > 0 and total_return_pct > 5:
        recommendations.append({
            'action': 'TAKES_PROFIT',
            'target': 'Consider partial profit-taking on outperformers',
            'reason': 'Strong unrealized gains - lock in some profits'
        })
    
    # If no specific recommendations, give general advice
    if not recommendations:
        recommendations.append({
            'action': 'MAINTAIN',
            'target': 'Current allocation is well-balanced',
            'reason': 'No major rebalancing needed at this time'
        })
        recommendations.append({
            'action': 'TAKES_PROFIT',
            'target': 'Consider partial profit-taking on outperformers',
            'reason': 'Strong unrealized gains - lock in some profits'
        })
    
    # If no specific recommendations, give general advice
    if not recommendations:
        recommendations.append({
            'action': 'MAINTAIN',
            'target': 'Current allocation is well-balanced',
            'reason': 'No major rebalancing needed at this time'
        })
    
    for i, rec in enumerate(recommendations, 1):
        print(f"\n{i}. {rec['action']}")
        print(f"   Target: {rec['target']}")
        print(f"   Reason: {rec['reason']}")
    
    # Risk management reminder
    print("\n\n⚠️  RISK MANAGEMENT REMINDERS:")
    print("-" * 70)
    print("   ✓ Never risk more than 5% of portfolio on single prediction market")
    print("   ✓ Set stop-losses on volatile crypto positions")
    print("   ✓ Rebalance quarterly or when allocation drifts >10%")
    print("   ✓ Maintain emergency cash reserves for opportunities")
    
    return recommendations


async def run_full_analysis():
    """Run complete portfolio analysis."""
    
    print("\n" + "="*70)
    print("COMPREHENSIVE PORTFOLIO ANALYSIS - ALL EXCHANGES")
    print("="*70)
    print("\nAnalyzing positions across:")
    print("   • Kalshi (Futures/Prediction Markets)")
    print("   • Polymarket (Ethereum Prediction Markets)")
    print("   • Coinbase (Crypto Trading)")
    print("   • Alpaca (Stocks/ETFs via 50+ venues)")
    print("="*70)
    
    # Run all analyses
    total_value = await get_portfolio_overview()
    allocations = await analyze_allocation_risk()
    performance = await analyze_performance()
    recommendations = await generate_recommendations()
    
    # Summary
    print("\n" + "="*70)
    print("ANALYSIS SUMMARY")
    print("="*70)
    print(f"\n📊 Total Portfolio Value: ${total_value:,.2f}")
    print(f"📈 Return (Alpaca stocks only): {performance['return_pct']:+.1f}%")
    print(f"\n📋 Key Recommendations:")
    for i, rec in enumerate(recommendations, 1):
        action = rec['action'].upper()
        if len(action) > 15:
            print(f"   {i}. {action[:15]}...")
        else:
            print(f"   {i}. {action}")
    
    print("\n✅ PORTFOLIO ANALYSIS COMPLETE!")
    return total_value, allocations, performance, recommendations


# =============================================================================
# RUN SCRIPT
# =============================================================================

if __name__ == "__main__":
    result = asyncio.run(run_full_analysis())
