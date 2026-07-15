import pytest

import portfolio_optimizer as po


def test_compute_adx_short():
    assert po._compute_adx([1], [1], [1]) == 20.0


def test_compute_adx_trending():
    n = 60
    highs = [100.0 + i for i in range(n)]
    lows = [99.0 + i for i in range(n)]
    closes = [100.0 + i for i in range(n)]
    adx = po._compute_adx(highs, lows, closes, 14)
    assert 0 <= adx <= 100


def test_compute_adx_exception():
    # cause a math error path -> returns default 20.0
    highs = [1.0] * 30
    lows = [1.0] * 30
    closes = [1.0] * 30
    # all equal -> tr_list all 0 -> division path returns 0; adx 0
    assert po._compute_adx(highs, lows, closes, 14) >= 0


def test_detect_market_regime_short():
    assert po._detect_market_regime([1, 2], [1, 2], [1, 2]) == "neutral"


def test_detect_market_regime_branches():
    n = 60
    highs = [100.0 + i for i in range(n)]
    lows = [99.0 + i for i in range(n)]
    closes = [100.0 + i for i in range(n)]
    assert po._detect_market_regime(highs, lows, closes) == "trending"

    flat_h = [100.0] * 60
    flat_l = [99.0] * 60
    flat_c = [100.0] * 60
    assert po._detect_market_regime(flat_h, flat_l, flat_c) == "ranging"

    vol_h = [100.0 + (i % 2) * 20 for i in range(60)]
    vol_l = [90.0 + (i % 2) * 20 for i in range(60)]
    vol_c = [100.0 + (i % 2) * 20 for i in range(60)]
    assert po._detect_market_regime(vol_h, vol_l, vol_c) in ("volatile", "neutral")


def test_swing_points():
    highs = [float(100 + (i % 5)) for i in range(40)]
    lows = [float(90 + (i % 5)) for i in range(40)]
    swings = po._detect_swing_points(highs, lows, lookback=3)
    assert isinstance(swings, list)


def test_build_sr_levels_short():
    assert po._build_sr_levels([1, 2], [1, 2], [1, 2]) == []


def test_build_sr_levels():
    highs = [float(110 + (i % 3)) for i in range(40)]
    lows = [float(90 + (i % 3)) for i in range(40)]
    closes = [float(100 + (i % 3)) for i in range(40)]
    levels = po._build_sr_levels(highs, lows, closes, min_touches=1)
    assert isinstance(levels, list)


def test_estimate_atr_short():
    assert po._estimate_atr([1], [1], [1]) == 0.0


def test_estimate_atr():
    n = 30
    highs = [float(110 + i) for i in range(n)]
    lows = [float(90 + i) for i in range(n)]
    closes = [float(100 + i) for i in range(n)]
    atr = po._estimate_atr(closes, highs, lows, 14)
    assert atr >= 0


def test_fmt_base():
    assert po._fmt_base(1.5) == "1.5"
    assert po._fmt_base(0.0) == "0"


def test_fmt_quote():
    assert po._fmt_quote(12.3456) == "12.35"


def test_to_float():
    assert po.to_float(3) == 3.0
    assert po.to_float("3.5") == 3.5
    assert po.to_float(None) == 0.0
    assert po.to_float("abc") == 0.0


def test_classify_asset():
    assert po.classify_asset("BTC-USD") == "safe"
    assert po.classify_asset("SOL-USD") == "growth"
    assert po.classify_asset("PEPE-USD") == "speculative"
    # unknown treated as speculative
    assert po.classify_asset("ZZZ-USD") == "speculative"


def test_current_fee_tier():
    assert po.current_fee_tier(0) == (0, 0.0060, 0.0120)
    assert po.current_fee_tier(500) == (0, 0.0060, 0.0120)
    assert po.current_fee_tier(1500) == (1000, 0.0035, 0.0075)
    assert po.current_fee_tier(5_000_000) == (20_000_000, 0.0005, 0.0015)


def test_volume_to_next():
    assert po.volume_to_next(500) == 500.0
    assert po.volume_to_next(1500) == 8500.0
    assert po.volume_to_next(50_000_000) == 0.0


def test_clamp():
    assert po._clamp(5, 0, 10) == 5
    assert po._clamp(-1, 0, 10) == 0
    assert po._clamp(11, 0, 10) == 10


def test_latency_tuned_priority():
    assert po._latency_tuned_priority(1.0) == 1.0
    assert po._latency_tuned_priority(0.5) == pytest.approx(0.5)


def test_detect_regime_default():
    # the latency_tuned_priority helper region default regime detector
    assert po._detect_regime([1, 2, 3]) == "neutral"


# ---------------------------------------------------------------------------
# PortfolioOptimizer method tests (bypass __init__ to avoid live infra/threads)
# ---------------------------------------------------------------------------

def _raw_opt():
    return po.PortfolioOptimizer.__new__(po.PortfolioOptimizer)


def test_kelly_size():
    opt = _raw_opt()
    size = opt._kelly_size(win_rate=0.6, avg_win=0.1, avg_loss=0.05, frac=0.5, equity=1000.0)
    assert 0 <= size <= 1000.0 * 0.5 + 1e-9
    # edge: zero loss
    size2 = opt._kelly_size(0.5, 0.1, 0.0, 0.5, 1000.0)
    assert size2 >= 0


def test_clamp_via_method():
    opt = _raw_opt()
    assert opt._clamp is po._clamp


def test_capital_bucket_for():
    opt = _raw_opt()
    opt.capital_policy = po.DEFAULT_CAPITAL_POLICY
    opp = po.Opportunity(
        opp_type=po.OpportunityType.TLH, currency="BTC-USD", side="BUY",
        size_usd=100.0, reason="r", meta={},
    )
    assert opt._capital_bucket_for(opp) == "core"

    opp2 = po.Opportunity(
        opp_type=po.OpportunityType.TLH, currency="PEPE-USD", side="BUY",
        size_usd=100.0, reason="r", meta={},
    )
    assert opt._capital_bucket_for(opp2) == "opportunity"

    opp3 = po.Opportunity(
        opp_type=po.OpportunityType.TLH, currency="SOL-USD", side="SELL",
        size_usd=100.0, reason="r", meta={"capital_bucket": "reserve"},
    )
    assert opt._capital_bucket_for(opp3) == "reserve"
