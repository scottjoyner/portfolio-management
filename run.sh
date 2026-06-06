#!/bin/bash
# Portfolio Management System - Entry Point Script
# Run backtesting with different strategies

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PORTFOLIO_MANAGER="${SCRIPT_DIR}/portfolio_manager.py"
BACKTESTER="${SCRIPT_DIR}/backtester.py"
DATA_COLLECTOR="${SCRIPT_DIR}/data_collector.py"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

usage() {
    echo "Portfolio Management System - Backtesting CLI"
    echo ""
    echo "Usage: $0 [COMMAND] [OPTIONS]"
    echo ""
    echo "Commands:"
    echo "  run <strategy>     Run backtest with specified strategy"
    echo "  collect            Fetch live market data"
    echo "  alerts             Check portfolio health and send alerts"
    echo "  dashboard          Generate performance dashboard"
    echo ""
    echo "Strategies: hold_all, equal_weight, market_timing"
    echo ""
    echo "Examples:"
    echo "  $0 run hold_all                    # Run hold-all strategy"
    echo "  $0 run equal_weight                # Run equal-weight rebalancing"
    echo "  $0 collect                         # Fetch live API data"
    echo "  $0 alerts                          # Check portfolio health"
    echo ""
}

run_strategy() {
    local STRATEGY=${1:-hold_all}
    
    if [ ! -f "$PORTFOLIO_MANAGER" ]; then
        RED "ERROR: portfolio_manager.py not found"
        usage
        exit 1
    fi
    
    echo "=========================================="
    echo "🚀 RUNNING BACKTEST: $STRATEGY"
    echo "=========================================="
    
    python3 "$PORTFOLIO_MANAGER" --strategy "$STRATEGY"
}

collect_data() {
    echo "=========================================="
    echo "📡 FETCHING LIVE MARKET DATA"
    echo "=========================================="
    
    if [ -f "$DATA_COLLECTOR" ]; then
        python3 "$DATA_COLLECTOR"
    else
        echo "⚠️  Data collector not available, skipping..."
    fi
}

check_alerts() {
    echo "=========================================="
    echo "🔔 CHECKING PORTFOLIO HEALTH"
    echo "=========================================="
    
    if [ -f "${SCRIPT_DIR}/alerts.py" ]; then
        python3 "${SCRIPT_DIR}/alerts.py"
    else
        echo "⚠️  Alerts service not available, running simple check..."
        
        # Simple PnL calculation
        python3 << 'EOF'
import sys
sys.path.insert(0, '/home/falcon/git/portfolio-management')
from portfolio_manager import *

backtester = Backtester()
backtester.load_historical_data('/home/falcon/git/portfolio-management/data/historical')
results = backtester.run_backtest('hold_all')

print("\n📊 PORTFOLIO SUMMARY:")
print(f"  Initial:    $100,000.00")
print(f"  Current:    ${results['total_portfolio_value']:,.2f}")
print(f"  Total PnL:  ${results['total_unrealized_pnl']:,.2f}")
EOF
    fi
}

generate_dashboard() {
    echo "=========================================="
    echo "📊 GENERATING PERFORMANCE DASHBOARD"
    echo "=========================================="
    
    python3 << 'EOF'
import sys
sys.path.insert(0, '/home/falcon/git/portfolio-management')
from portfolio_manager import *

print("\n" + "="*80)
print("📈 PORTFOLIO PERFORMANCE DASHBOARD")
print("="*80 + "\n")

backtester = Backtester()
backtester.load_historical_data('/home/falcon/git/portfolio-management/data/historical')
results = backtester.run_backtest('hold_all')
summary = backtester.portfolio.get_summary()

print("📊 FINAL PORTFOLIO ALLOCATION:")
print("-" * 40)
for symbol, prices_list in backtester.prices_data.items():
    if len(prices_list) > 0:
        position = backtester.portfolio.positions.get(symbol)
        if position:
            current_price = prices_list[0]["close"]
            value = position.quantity * current_price
            print(f"  {symbol}: ${value:,.2f}")

print("\n📈 PERFORMANCE METRICS:")
print("-" * 40)
initial = 100000.0
final = summary['total_portfolio_value']
pnl = final - initial
print(f"  Initial Investment:    $${initial:,.2f}")
print(f"  Final Portfolio Value: ${final:,.2f}")
print(f"  Total PnL:             ${pnl:,.2f} ({pnl/initial*100:.1f}%)")

print("\n✅ BACKTESTING COMPLETE!")
EOF
}

# Main execution
case "${1:-}" in
    run)
        if [ -z "$2" ]; then
            echo "ERROR: Please specify a strategy"
            usage
            exit 1
        fi
        run_strategy "$2"
        ;;
    collect)
        collect_data
        ;;
    alerts)
        check_alerts
        ;;
    dashboard)
        generate_dashboard
        ;;
    help|--help|-h)
        usage
        ;;
    *)
        usage
        exit 1
        ;;
esac
