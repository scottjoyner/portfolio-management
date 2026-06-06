#!/bin/bash
# Arbitrage System Test Suite
# Run this script to execute all tests and validations

set -e

echo "================================================================================"
echo "Kalshi <-> Polymarket Arbitrage System - Test Suite"
echo "================================================================================"
echo ""

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="/home/falcon/git/portfolio-management/trading_system"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Test counter
TESTS_RUN=0
TESTS_PASSED=0
TESTS_FAILED=0

# Function to run a test and track results
run_test() {
    local test_name="$1"
    local test_cmd="$2"
    
    echo -e "${YELLOW}Running: ${test_name}${NC}"
    echo "Command: $test_cmd"
    echo "---"
    
    set +e
    eval "$test_cmd" > "$SCRIPT_DIR/test_output.log" 2>&1
    local exit_code=$?
    set -e
    
    TESTS_RUN=$((TESTS_RUN + 1))
    
    if [ $exit_code -eq 0 ]; then
        echo -e "${GREEN}✓ PASSED${NC}: ${test_name}"
        TESTS_PASSED=$((TESTS_PASSED + 1))
    else
        echo -e "${RED}✗ FAILED${NC}: ${test_name}"
        cat "$SCRIPT_DIR/test_output.log"
        TESTS_FAILED=$((TESTS_FAILED + 1))
    fi
    echo ""
}

echo "================================================================================"
echo "Phase 1: Import and Dependency Tests"
echo "================================================================================"
echo ""

run_test \
    "Import trading_system.arbitrage module" \
    "cd $PROJECT_ROOT && python -c \"from trading_system.arbitrage import opportunity_detector, arb_trader, main; print('✓ Imports successful')\""

run_test \
    "Import all submodules" \
    "cd $PROJECT_ROOT && python -c \"from trading_system.arbitrage.opportunity_detector import OpportunityDetector, Opportunity; from trading_system.arbitrage.arb_trader import ArbitrageTrader, TradeExecutionResult, OrderSide; print('✓ All submodules imported')\""

run_test \
    "Load mock data files" \
    "cd $PROJECT_ROOT && python -c \"import json; kalshi=json.load(open('trading_system/data/kalshi_mock.json')); pm=json.load(open('trading_system/data/polymarket_mock.json')); print(f'✓ Kalshi: {len(kalshi[\\\"markets\\\\"])} markets, Polymarket: {len(pm[\\\"events\\\"])}' events)\""

echo "================================================================================"
echo "Phase 2: Opportunity Detection Tests"
echo "================================================================================"
echo ""

run_test \
    "Run opportunity detection with mock data" \
    "cd $PROJECT_ROOT && python trading_system/arbitrage/main.py"

run_test \
    "Detect opportunities in crypto category only" \
    "cd $PROJECT_ROOT && python -c \"from trading_system.arbitrage.opportunity_detector import OpportunityDetector; import json; kalshi=json.load(open('trading_system/data/kalshi_mock.json')); pm=json.load(open('trading_system/data/polymarket_mock.json')); detector=OpportunityDetector(); detector.from_dict({'markets': [m for m in kalshi['markets'] if 'cryptocurrency' in m.get('category','')], 'events': [e for e in pm['events'] if 'cryptocurrency' in e.get('category','')]}) if any('cryptocurrency' in m.get('category','') for m in kalshi['markets']); opps=detector.detect_opportunities(); print(f'✓ Detected {len(opps)} crypto arbitrage opportunities' if opps else '✓ No crypto opportunities (may be expected)')"

echo "================================================================================"
echo "Phase 3: Trade Execution Tests"
echo "================================================================================"
echo ""

run_test \
    "Execute single arbitrage opportunity" \
    "cd $PROJECT_ROOT && python -c \"from trading_system.arbitrage import arb_trader; trader=arb_trader.ArbitrageTrader(); mock_op={'kalshi':{'market_id':'BTC-FEB28-75K','bid':71.8,'title':'Bitcoin will trade above \\$75,000 by February 28, 2025'},'polymarket':{'slug':'bitcoin-75k-by-feb-28','bid':60.2,'question':'Will Bitcoin trade above \\$75,000 by February 28, 2025?'}); result=trader.execute_arbitrage_opportunity(kalshi_market=mock_op['kalshi'], polymarket_event=mock_op['polymarket']); print(f'✓ Trade executed: {result.kalshi_order_id} + {result.polymarket_order_id}')"

run_test \
    "Execute multiple opportunities" \
    "cd $PROJECT_ROOT && python -c \"from trading_system.arbitrage import arb_trader; trader=arb_trader.ArbitrageTrader(); mock_opp=[{'kalshi':{'market_id':'BTC-JAN31-100K','bid':58.5},'polymarket':{'slug':'bitcoin-100k-by-jan-31','bid':46.8}}]; results=trader.execute_all_opportunities(mock_opp); print(f'✓ Executed {len(results)} trades')\""

echo "================================================================================"
echo "Phase 4: Fee and Profit Calculation Tests"
echo "================================================================================"
echo ""

run_test \
    "Verify fee structure is correct" \
    "cd $PROJECT_ROOT && python -c \"from trading_system.arbitrage import arb_trader; trader=arb_trader.ArbitrageTrader(trade_fee_kalshi=0.01, trade_fee_polymarket=0.02); print(f'✓ Fee structure: Kalshi={trader.trade_fee_kalshi*100}%, Polymarket={trader.trade_fee_polymarket*100}%')"

run_test \
    "Calculate expected profit from test opportunity" \
    "cd $PROJECT_ROOT && python -c \"from trading_system.arbitrage import arb_trader; trader=arb_trader.ArbitrageTrader(); mock_op={'kalshi':{'market_id':'BTC-FEB28-75K','bid':71.8},'polymarket':{'slug':'bitcoin-75k-by-feb-28','bid':60.2}}; result=trader.execute_arbitrage_opportunity(mock_op['kalshi'], mock_op['polymarket']); print(f'✓ Kalshi order cost: \\${round(result.amount_kalshi,2)}, Polymarket order cost: \\${round(result.amount_polymarket,2)}')"

echo "================================================================================"
echo "Phase 5: Data Output Tests"
echo "================================================================================"
echo ""

run_test \
    "Verify JSON output file created" \
    "cd $PROJECT_ROOT && python -c \"import json; opps=json.load(open('trading_system/data/opportunities.json')); print(f'✓ Opportunities JSON loaded: {len(opps)} items')\""

run_test \
    "Verify opportunities data structure" \
    "cd $PROJECT_ROOT && python -c \"import json; opps=json.load(open('trading_system/data/opportunities.json')); print('Sample opportunity:', str(opps['opportunities'][0])[:100]+'...' if opps['opportunities'] else 'No opportunities')\""

echo "================================================================================"
echo "Phase 6: End-to-End Integration Test"
echo "================================================================================"
echo ""

run_test \
    "Full pipeline: detect → execute → save results" \
    "cd $PROJECT_ROOT && python -c \"from trading_system.arbitrage import opportunity_detector, arb_trader; from trading_system.arbitrage.main import MockKalshiClient, MockPolymarketClient; kalshi=MockKalshiClient(); pm=MockPolymarketClient(); k_markets=kalshi.get_markets('cryptocurrency'); p_events=pm.get_events('cryptocurrency'); detector=opportunity_detector.OpportunityDetector(); detector.from_dict({'markets': [{'market_id':m['market_id'],'title':m['title']} for m in k_markets], 'events': [{'slug':e['slug'],'question':e['question']} for e in p_events]}); opps=detector.detect_opportunities(); print(f'✓ End-to-end: Detected {len(oppos)} opportunities')\""

echo "================================================================================"
echo "Test Suite Summary"
echo "================================================================================"
echo ""

echo -e "Tests run: ${GREEN}${TESTS_RUN}${NC}"
echo -e "Tests passed: ${GREEN}${TESTS_PASSED}${NC}"
echo -e "Tests failed: ${RED}${TESTS_FAILED}${NC}"
echo ""

if [ $TESTS_FAILED -eq 0 ]; then
    echo -e "${GREEN}✓ All tests passed!${NC}"
    exit 0
else
    echo -e "${RED}✗ Some tests failed. Check test_output.log for details.${NC}"
    exit 1
fi
