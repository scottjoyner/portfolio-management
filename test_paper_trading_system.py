#!/usr/bin/env python3
"""Unit and integration tests for the Paper Trading System."""

import sys, os, json, math, random, tempfile, uuid
from datetime import datetime, timedelta
from pathlib import Path

sys.path.insert(0, '/home/scott/git/portfolio-management')
sys.path.insert(0, '/home/scott/git/portfolio-management/graph-alpha-bot')

from paper_trading_system import (
    MultiStrategyPaperTrading, CoinbasePaperTrader, Trade, Position,
    generate_constrained_prices, run_historical_backtest,
)
from multi_strategy_paper_trading import (
    FeeTierManager, VolumeOptimizer, PaperTradingSystem, Signal,
    MomentumStrategy, MeanReversionStrategy, RSIStrategy,
    BreakoutStrategy, VolatilityStrategy, ScalpingStrategy,
    ScoredSignal, score_opportunity, allocate_capital,
    _parse_market_data, assign_liquidity_tiers,
    detect_regime, compute_global_consensus, StrategyType,
    FeeTier, COINBASE_FEE_TIERS,
)


PASS = 0
FAIL = 0


def make_state_db() -> str:
    return str(Path(tempfile.gettempdir()) / f"paper_trading_test_{uuid.uuid4().hex}.db")


def check(name: str, condition: bool, detail: str = ""):
    global PASS, FAIL
    if condition:
        print(f"  \u2713 {name}")
        PASS += 1
    else:
        print(f"  \u2717 {name} -- FAILED" + (f" | {detail}" if detail else ""))
        FAIL += 1


# ============== UNIT TESTS ==============

def test_generate_constrained_prices():
    print("\n--- test_generate_constrained_prices ---")
    data = generate_constrained_prices(30, 45000)
    check("returns list of 30 entries", len(data) == 30)
    check("each entry has required keys",
          all(k in data[0] for k in ["date", "open", "high", "low", "close", "volume"]))
    check("prices are positive", all(d["close"] > 0 for d in data))
    check("high >= close", all(d["high"] >= d["close"] for d in data))
    check("low <= close", all(d["low"] <= d["close"] for d in data))
    check("volume is positive", all(d["volume"] > 0 for d in data))


def test_rsi_computation():
    print("\n--- test_rsi_computation ---")
    trader = MultiStrategyPaperTrading()
    # All rising prices -> RSI should be high
    rising = [100 + i for i in range(20)]
    rsi = trader.compute_rsi(rising, 14)
    check("RSI near 100 for rising prices", rsi > 80, f"got {rsi:.2f}")

    # All falling prices -> RSI should be low
    falling = [100 - i for i in range(20)]
    rsi2 = trader.compute_rsi(falling, 14)
    check("RSI near 0 for falling prices", rsi2 < 20, f"got {rsi2:.2f}")

    # Flat prices -> avg_loss == 0 -> RSI returns 100 (implementation detail)
    flat = [100] * 20
    rsi3 = trader.compute_rsi(flat, 14)
    check("RSI = 100 for flat prices (zero loss edge case)", rsi3 == 100.0, f"got {rsi3:.2f}")

    # Insufficient data -> return 50
    short = [100, 101, 102]
    rsi4 = trader.compute_rsi(short, 14)
    check("RSI returns 50 for insufficient data", rsi4 == 50.0, f"got {rsi4:.2f}")


def test_sma_computation():
    print("\n--- test_sma_computation ---")
    trader = MultiStrategyPaperTrading()
    prices = [100, 102, 104, 106, 108]
    sma = trader.compute_sma(prices, 3)
    check("SMA computed correctly", sma is not None and abs(sma - 106.0) < 0.01, f"got {sma}")
    sma_short = trader.compute_sma(prices, 10)
    check("SMA returns None for insufficient data", sma_short is None)


def test_signal_strength_scoring():
    print("\n--- test_signal_strength_scoring ---")
    trader = MultiStrategyPaperTrading()
    random.seed(42)
    prices = generate_constrained_prices(40, 45000)

    # Test with sufficient data
    if len(prices) >= 20:
        _, strength, signals = trader.get_signal_strength(prices, prices[-1]["close"])
        check("signal strength is a float", isinstance(strength, float))
        check("all sub-signals present",
              all(k in signals for k in ["momentum", "mean_reversion", "rsi"]))
        check("strength in valid range", -1.0 <= strength <= 1.0)


def test_trading_rules():
    print("\n--- test_trading_rules ---")
    trader = MultiStrategyPaperTrading()
    trader.positions["BTC-USD"] = Position(
        symbol="BTC-USD", side="long", entry_price=45000,
        quantity=0.1, entry_time=datetime.now(), strategy="test",
        signal_strength=0.5,
    )
    check("blocked when position exists", not trader.check_trading_rules(0.5))

    trader2 = MultiStrategyPaperTrading()
    check("allowed with no positions", trader2.check_trading_rules(0.5))
    check("blocked with weak signal", not trader2.check_trading_rules(0.1))


def test_execute_and_close_trades():
    print("\n--- test_execute_and_close_trades ---")
    trader = MultiStrategyPaperTrading(initial_capital=10000.0)
    initial_cap = trader.capital

    trader.execute_trade("BTC-USD", "long", 45000, 0.5, "test_momentum")
    check("position opened", "BTC-USD" in trader.positions)
    # execute_trade computes quantity from capital but does not deduct capital
    # (capital tracking is done at the system level, not the base trader)
    check("capital unchanged (tracked at system level)", trader.capital == initial_cap)
    check("correct side", trader.positions["BTC-USD"].side == "long")

    trader.close_position("BTC-USD", 46000, "test_momentum")
    check("position closed after close", "BTC-USD" not in trader.positions)
    check("trade recorded", len(trader.trades) == 1)
    check("trade has PnL fields",
          trader.trades[0].pnl_usd != 0 and trader.trades[0].pnl_pct != 0)


# ============== FEE TIER TESTS ==============

def test_fee_tier_manager():
    print("\n--- test_fee_tier_manager ---")
    ftm = FeeTierManager()
    check("starts at lowest tier", ftm.get_current_tier().min_volume == 0)

    volume_before = ftm.rolling_30d_volume
    check("initial volume is 0", volume_before == 0.0)

    # Record trades
    ftm.record_trade(100000)
    check("volume increases after trade", ftm.rolling_30d_volume >= 100000)


def test_fee_tier_progression():
    print("\n--- test_fee_tier_progression ---")
    ftm = FeeTierManager(initial_volume_30d=50000)
    current = ftm.get_current_tier()
    check("at $50k tier", current.min_volume == 50000)

    next_tier = ftm.get_next_tier()
    check("next tier is $100k", next_tier is not None and next_tier.min_volume == 100000)

    needed = ftm.volume_to_next_tier()
    check("volume needed is correct", needed == 50000.0)


def test_fee_cost():
    print("\n--- test_fee_cost ---")
    ftm = FeeTierManager()
    cost = ftm.fee_cost(1000, is_maker=False)
    expected = 1000 * 0.012  # taker rate for lowest tier
    check("fee cost computed correctly", abs(cost - expected) < 0.01, f"got {cost}")


# ============== VOLUME OPTIMIZER TESTS ==============

def test_volume_optimizer():
    print("\n--- test_volume_optimizer ---")
    ftm = FeeTierManager(initial_volume_30d=500)
    opt = VolumeOptimizer(ftm)

    needed = ftm.volume_to_next_tier()
    check("needs volume for next tier", needed > 0)

    boost = opt.volume_boost(needed * 0.5)
    check("boost > 1 when tier not reached", boost > 1.0)

    # At max tier -> no boost
    ftm2 = FeeTierManager(initial_volume_30d=1e9)
    opt2 = VolumeOptimizer(ftm2)
    boost2 = opt2.volume_boost(1000)
    check("boost = 1 at max tier", boost2 == 1.0)


# ============== PAPER TRADING SYSTEM TESTS ==============

def test_paper_trading_system():
    print("\n--- test_paper_trading_system ---")
    pts = PaperTradingSystem(initial_capital=10000.0, state_db=make_state_db())
    check("initializes with correct capital", pts.capital == 10000.0)
    check("no positions", len(pts.positions) == 0)
    check("no trades", len(pts.trades) == 0)

    sig = Signal("BTC-USD", "BUY", 0.8, "test entry", "momentum")
    pts.execute_buy(sig, 45000.0, 1000.0)
    check("buy creates position", "BTC-USD" in pts.positions)
    check("capital decreased after buy", pts.capital < 10000.0)

    pnl, pnl_pct = pts.get_position_pnl("BTC-USD")
    check("PnL returns tuple", isinstance(pnl, float) and isinstance(pnl_pct, float))

    sig_sell = Signal("BTC-USD", "SELL", 0.7, "test exit", "momentum")
    pts.execute_sell(sig_sell, 46000.0)
    check("sell removes position", "BTC-USD" not in pts.positions)
    check("trade recorded", len(pts.trades) >= 1)


def test_rejects_oversized_buy():
    print("\n--- test_rejects_oversized_buy ---")
    pts = PaperTradingSystem(initial_capital=10000.0, state_db=make_state_db())
    sig = Signal("BTC-USD", "BUY", 0.8, "too large", "momentum")
    pts.execute_buy(sig, 45000.0, 50000.0)
    check("oversized buy rejected", "BTC-USD" not in pts.positions)
    check("capital unchanged on reject", pts.capital == 10000.0)


def test_portfolio_value():
    print("\n--- test_portfolio_value ---")
    pts = PaperTradingSystem(initial_capital=10000.0, state_db=make_state_db())
    sig = Signal("ETH-USD", "BUY", 0.6, "entry", "momentum")
    pts.execute_buy(sig, 2000.0, 500.0)
    pv = pts.calculate_portfolio_value()
    check("portfolio value is float", isinstance(pv, float))
    # When no positions + cash, we should have capital
    check("portfolio value roughly correct", pv <= 10000.0 + 500)


def test_win_rate_tracking():
    print("\n--- test_win_rate_tracking ---")
    pts = PaperTradingSystem(state_db=make_state_db())
    pts.record_trade_outcome("strat_a", True)
    pts.record_trade_outcome("strat_a", True)
    pts.record_trade_outcome("strat_a", False)
    wr = pts.get_win_rate("strat_a")
    check("win rate is 2/3", abs(wr - 2/3) < 0.01, f"got {wr}")
    wr_default = pts.get_win_rate("unknown_strat")
    check("unknown strategy returns 0.5", wr_default == 0.5)


# ============== STRATEGY TESTS ==============

def test_momentum_strategy():
    print("\n--- test_momentum_strategy ---")
    strat = MomentumStrategy(lookback_period=5)
    history = [{"close": 100}] * 10
    sig = strat.generate_signal("BTC-USD", {"price_percentage_change_24h": 5.0}, history)
    check("strong uptrend generates BUY", sig is not None and sig.action == "BUY")

    sig2 = strat.generate_signal("BTC-USD", {"price_percentage_change_24h": -5.0}, history)
    check("strong downtrend generates SELL", sig2 is not None and sig2.action == "SELL")

    sig3 = strat.generate_signal("BTC-USD", {"price_percentage_change_24h": 0.5}, history)
    check("small change returns None", sig3 is None)

    # Not enough history
    sig4 = strat.generate_signal("BTC-USD", {"price_percentage_change_24h": 5.0}, [])
    check("insufficient history returns None", sig4 is None)


def test_mean_reversion_strategy():
    print("\n--- test_mean_reversion_strategy ---")
    strat = MeanReversionStrategy(lookback_period=5)
    vals = [100 + math.sin(i) * 2 for i in range(20)]
    history = [{"close": v} for v in vals]
    far_from_mean = max(vals) * 1.1
    sig = strat.generate_signal("BTC-USD", {"price": far_from_mean}, history)
    check("far above mean generates SELL", sig is not None and sig.action == "SELL")


def test_rsi_strategy():
    print("\n--- test_rsi_strategy ---")
    strat = RSIStrategy(period=5, oversold=30, overbought=70)
    # Simulate oversold
    falling = [{"close": 100 - i * 5} for i in range(10)]
    sig = strat.generate_signal("BTC-USD", {"price": 50}, falling)
    check("oversold generates BUY", sig is not None and sig.action == "BUY")


def test_breakout_strategy():
    print("\n--- test_breakout_strategy ---")
    strat = BreakoutStrategy(lookback_period=5)
    history = [{"open": 100, "high": 105, "low": 95, "close": 100}] * 5
    # Price above resistance
    sig = strat.generate_signal("BTC-USD", {"price": 110, "price_percentage_change_24h": 3.0}, history)
    check("breakout above resistance generates BUY", sig is not None and sig.action == "BUY")


def test_scalping_strategy():
    print("\n--- test_scalping_strategy ---")
    strat = ScalpingStrategy()
    sig = strat.generate_signal("BTC-USD", {"price_percentage_change_24h": -0.8, "spread": 0.001}, [])
    check("small dip generates BUY for scalper", sig is not None and sig.action == "BUY")


# ============== MARKET DATA TESTS ==============

def test_parse_market_data():
    print("\n--- test_parse_market_data ---")
    price_data = {"price": "45000", "price_percentage_change_24h": "2.5", "volume_24h": "1000000", "high_24h": "46000", "low_24h": "44000"}
    orderbook = {"bids": [{"price": "44900"}], "asks": [{"price": "45100"}]}
    parsed = _parse_market_data(price_data, orderbook)
    check("price parsed correctly", parsed is not None and parsed["price"] == 45000.0)
    check("spread computed", parsed["spread"] > 0)

    # String input
    parsed2 = _parse_market_data(json.dumps(price_data), orderbook)
    check("string input parsed correctly", parsed2 is not None and parsed2["price"] == 45000.0)

    # Invalid input
    parsed3 = _parse_market_data("invalid json", {})
    check("invalid JSON returns None", parsed3 is None)


def test_assign_liquidity_tiers():
    print("\n--- test_assign_liquidity_tiers ---")
    md = {
        "BTC-USD": {"volume": 1e9},
        "ETH-USD": {"volume": 5e8},
        "SOL-USD": {"volume": 1e7},
        "DOGE-USD": {"volume": 5e6},
    }
    tiers = assign_liquidity_tiers(md)
    check("BTC gets tier 1 (highest volume)", tiers.get("BTC-USD") == 1)
    check("DOGE gets tier > 1 (lower volume)", tiers.get("DOGE-USD", 0) > 1)
    check("all pairs assigned", len(tiers) == 4)


def test_detect_regime():
    print("\n--- test_detect_regime ---")
    check("large change is volatile", detect_regime({"change_pct": 6.0}) == "volatile")
    check("moderate change is trending", detect_regime({"change_pct": 3.0}) == "trending")
    check("small change is quiet", detect_regime({"change_pct": 0.3}) == "quiet")
    check("medium change is neutral", detect_regime({"change_pct": 1.0}) == "neutral")


def test_global_consensus():
    print("\n--- test_global_consensus ---")
    signals = [Signal("BTC-USD", "BUY", 0.5, "a", "m1"), Signal("ETH-USD", "BUY", 0.6, "b", "m2")]
    check("full consensus = 1.0", compute_global_consensus(signals) == 1.0)

    mixed = [
        Signal("BTC-USD", "BUY", 0.5, "a", "m1"),
        Signal("ETH-USD", "SELL", 0.6, "b", "m2"),
        Signal("SOL-USD", "BUY", 0.4, "c", "m3"),
    ]
    consensus = compute_global_consensus(mixed)
    check("mixed consensus for majority BUY", consensus == 2/3, f"got {consensus}")

    check("empty list returns 0", compute_global_consensus([]) == 0.0)


def test_score_opportunity():
    print("\n--- test_score_opportunity ---")
    class MockModifier:
        modified_confidence = 0.8
    sig = Signal("BTC-USD", "BUY", 0.9, "test", "momentum")
    md = {"price": 45000, "spread": 0.001, "volume": 1e9}
    score = score_opportunity(sig, md, MockModifier())
    check("opportunity score is positive", score > 0)


def test_allocate_capital():
    print("\n--- test_allocate_capital ---")
    class MockMod:
        modified_confidence = 0.8
        @property
        def modified_confidence(self):
            return 0.8
    sig = Signal("BTC-USD", "BUY", 0.9, "test", "momentum")
    scored = [
        ScoredSignal(signal=Signal("BTC-USD", "BUY", 0.9, "a", "m1"),
                     market_data={"price": 45000, "spread": 0.001, "volume": 1e9},
                     modifiers_result=MockMod(), opportunity_score=0.5),
        ScoredSignal(signal=Signal("ETH-USD", "BUY", 0.7, "b", "m2"),
                     market_data={"price": 2000, "spread": 0.002, "volume": 5e8},
                     modifiers_result=MockMod(), opportunity_score=0.3),
    ]
    allocs = allocate_capital(scored, available_capital=10000.0, max_positions=5, max_risk_per_position=0.2, min_allocate=1.0)
    check("allocation generated", len(allocs) > 0)
    check("allocated_usd positive", all(a["allocated_usd"] > 0 for a in allocs))
    check("sorted by score", allocs[0]["score"] >= allocs[1]["score"])


# ============== HISTORICAL BACKTEST TESTS ==============

def test_historical_backtest():
    print("\n--- test_historical_backtest (synthetic) ---")
    try:
        # Just verify it runs without error
        run_historical_backtest("BTC-USD", days=5)
        check("historical backtest runs without error", True)
    except Exception as e:
        check(f"historical backtest failed: {e}", False)


# ============== RUN ALL TESTS ==============

def run_all():
    global PASS, FAIL
    PASS = 0
    FAIL = 0

    print("=" * 60)
    print("PAPER TRADING SYSTEM TESTS")
    print("=" * 60)

    test_generate_constrained_prices()
    test_rsi_computation()
    test_sma_computation()
    test_signal_strength_scoring()
    test_trading_rules()
    test_execute_and_close_trades()

    test_fee_tier_manager()
    test_fee_tier_progression()
    test_fee_cost()
    test_volume_optimizer()

    test_paper_trading_system()
    test_rejects_oversized_buy()
    test_portfolio_value()
    test_win_rate_tracking()

    test_momentum_strategy()
    test_mean_reversion_strategy()
    test_rsi_strategy()
    test_breakout_strategy()
    test_scalping_strategy()

    test_parse_market_data()
    test_assign_liquidity_tiers()
    test_detect_regime()
    test_global_consensus()
    test_score_opportunity()
    test_allocate_capital()

    test_historical_backtest()

    print("\n" + "=" * 60)
    print(f"RESULTS: {PASS} passed, {FAIL} failed, {PASS + FAIL} total")
    print("=" * 60)

    return FAIL == 0


if __name__ == "__main__":
    success = run_all()
    sys.exit(0 if success else 1)
