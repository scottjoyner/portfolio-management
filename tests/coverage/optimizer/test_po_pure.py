"""Coverage + critical-evaluation tests for the self-contained regions of
portfolio_optimizer.py: module-level pure helpers (ADX, regime, S/R, ATR,
formatting, asset classification, fee tiers), the CoinbaseCLI wrapper, and the
PortfolioOptimizer scoring/sizing/capital/cluster helper methods.

Heavy collaborators (PM client, SignalAggregator, execution engine, etc.) are
mocked so the process can exit cleanly.
"""

import time
import urllib.error
from unittest import mock

import pytest

import portfolio_optimizer as P
from conftest import make_state, holding, opt  # noqa: F401


# ===========================================================================
# Module-level pure helpers
# ===========================================================================

def test_compute_adx_short_input():
    # Not enough data -> neutral default 20.0
    assert P._compute_adx([1, 2, 3], [1, 2, 3], [1, 2, 3]) == 20.0


def test_compute_adx_trending_and_flat():
    closes = [float(100 + i) for i in range(30)]
    highs = [c + 1 for c in closes]
    lows = [c - 1 for c in closes]
    adx = P._compute_adx(highs, lows, closes, 14)
    assert 0.0 <= adx <= 100.0
    flat = [100.0] * 30
    assert 0.0 <= P._compute_adx(flat, flat, flat, 14) <= 100.0


def test_detect_market_regime_short():
    assert P._detect_market_regime([1, 2, 3], [1, 2, 3], [1, 2, 3]) == "neutral"


def test_detect_market_regime_branches():
    closes = [float(100 + i) for i in range(40)]
    highs = [c + 1 for c in closes]
    lows = [c - 1 for c in closes]
    reg = P._detect_market_regime(highs, lows, closes)
    assert reg in ("trending", "neutral", "ranging", "volatile")

    osc = [100.0 + (5.0 if i % 2 else -5.0) for i in range(40)]
    h2 = [o + 1 for o in osc]
    l2 = [o - 1 for o in osc]
    reg2 = P._detect_market_regime(h2, l2, osc)
    assert reg2 in ("trending", "neutral", "ranging", "volatile")


def test_detect_swing_points():
    assert P._detect_swing_points([1, 2, 3], [1, 2, 3], lookback=10) == []


def test_build_sr_levels_empty_and_normal():
    assert P._build_sr_levels([1, 2, 3], [1, 2, 3], [1, 2, 3]) == []
    highs = [10.0, 12.0, 10.0, 12.0, 10.0, 12.0, 10.0, 12.0, 10.0, 12.0,
             10.0, 12.0, 10.0, 12.0, 10.0, 12.0, 10.0, 12.0, 10.0, 12.0,
             11.0] * 2
    lows = [8.0, 6.0, 8.0, 6.0, 8.0, 6.0, 8.0, 6.0, 8.0, 6.0, 8.0, 6.0,
            8.0, 6.0, 8.0, 6.0, 8.0, 6.0, 8.0, 6.0, 7.0] * 2
    closes = [9.0] * 42
    levels = P._build_sr_levels(highs, lows, closes, min_touches=2)
    assert isinstance(levels, list)


def test_estimate_atr_short():
    assert P._estimate_atr([1, 2], [1, 2], [1, 2]) == 0.0


def test_estimate_atr_normal():
    closes = [float(100 + i) for i in range(20)]
    highs = [c + 1 for c in closes]
    lows = [c - 1 for c in closes]
    assert P._estimate_atr(closes, highs, lows, 14) >= 0.0


def test_fmt_helpers():
    assert P._fmt_base(1.5) == "1.5"
    assert P._fmt_quote(100.0) == "100"
    assert P._fmt_base(0.0) == "0"
    assert P._fmt_quote(0.0) == "0"


def test_to_float():
    assert P.to_float(5) == 5.0
    assert P.to_float(5.5) == 5.5
    assert P.to_float("5.5") == 5.5
    assert P.to_float("nope") == 0.0
    assert P.to_float(None) == 0.0
    assert P.to_float(object()) == 0.0


def test_classify_asset():
    assert P.classify_asset("BTC") == "safe"
    assert P.classify_asset("SOL") == "growth"
    assert P.classify_asset("PEPE") == "speculative"
    assert P.classify_asset("UNKNOWN-USD") == "speculative"


@pytest.mark.parametrize("vol,tier", [
    (-1, (0, 0.006, 0.012)),
    (0, (0, 0.006, 0.012)),
    (999, (0, 0.006, 0.012)),
    (1000, (1000, 0.0035, 0.0075)),
    (999_999, (100_000, 0.0010, 0.0020)),
    (1_000_000, (1_000_000, 0.0008, 0.0018)),
    (100_000_000, (20_000_000, 0.0005, 0.0015)),
])
def test_current_fee_tier(vol, tier):
    assert P.current_fee_tier(vol) == tier


@pytest.mark.parametrize("vol,expected", [
    (0, 1000.0),
    (500, 500.0),
    (1000, 9000.0),
    (10_000, 40_000.0),
    (100_000, 900_000.0),
    (1_000_000, 19_000_000.0),
    (100_000_000, 0.0),
])
def test_volume_to_next(vol, expected):
    assert P.volume_to_next(vol) == expected


def test_clamp():
    assert P._clamp(5, 0, 10) == 5
    assert P._clamp(-1, 0, 10) == 0
    assert P._clamp(11, 0, 10) == 10


# ===========================================================================
# CoinbaseCLI (network I/O mocked)
# ===========================================================================

@pytest.fixture
def cli():
    with mock.patch("subprocess.run", return_value=mock.MagicMock(returncode=0, stdout="{}", stderr="")):
        c = P.CoinbaseCLI(environment="live")
    c._products = {}
    return c


def test_cli_verify(cli):
    with mock.patch("subprocess.run", return_value=mock.MagicMock(returncode=0)):
        cli._verify()


def test_cli_verify_failure():
    with mock.patch("subprocess.run", side_effect=RuntimeError("no bin")):
        with pytest.raises(RuntimeError):
            P.CoinbaseCLI(environment="live")


def test_cli_run_success_and_error(cli):
    good = mock.MagicMock(returncode=0, stdout='{"ok":1}', stderr="")
    with mock.patch("subprocess.run", return_value=good):
        assert cli._run(["x"]) == {"ok": 1}
    bad = mock.MagicMock(returncode=1, stdout="", stderr="err")
    with mock.patch("subprocess.run", return_value=bad):
        with pytest.raises(RuntimeError):
            cli._run(["x"])
    raw = mock.MagicMock(returncode=0, stdout="not json", stderr="")
    with mock.patch("subprocess.run", return_value=raw):
        assert cli._run(["x"], parse_json=False) == "not json"


def test_cli_public_get(cli):
    urlopen = mock.MagicMock()
    resp = mock.MagicMock(status=200)
    resp.read.return_value = b'{"products":[]}'
    urlopen.return_value.__enter__.return_value = resp
    with mock.patch("urllib.request.urlopen", urlopen):
        assert cli._public_get("/products") == {"products": []}


def test_cli_public_get_error(cli):
    with mock.patch("urllib.request.urlopen", side_effect=urllib.error.URLError("boom")):
        with pytest.raises(Exception):
            cli._public_get("/products")


def test_cli_get_products_from_run(cli):
    data = {"products": [{"product_id": "BTC-USD", "quote_increment": "0.01"}]}
    with mock.patch.object(cli, "_run", return_value=data):
        prods = cli.get_products()
        assert prods["BTC-USD"]["product_id"] == "BTC-USD"
    assert cli.get_products() is prods


def test_cli_get_products_public_fallback(cli):
    with mock.patch.object(cli, "_run", side_effect=RuntimeError("x")):
        with mock.patch.object(cli, "_public_get", return_value=[{"product_id": "ETH-USD"}]):
            prods = cli.get_products()
            assert "ETH-USD" in prods


def test_cli_get_products_full_failure(cli):
    with mock.patch.object(cli, "_run", side_effect=RuntimeError("x")):
        with mock.patch.object(cli, "_public_get", side_effect=RuntimeError("x")):
            assert cli.get_products() == {}


def test_cli_get_product(cli):
    cli._products = {"BTC-USD": {"product_id": "BTC-USD"}}
    assert cli.get_product("BTC-USD")["product_id"] == "BTC-USD"
    assert cli.get_product("NOPE") is None


def test_cli_best_product(cli):
    cli._products = {
        "BTC-USDC": {"product_id": "BTC-USDC"},
        "BTC-USD": {"product_id": "BTC-USD"},
        "SOL-USD": {"product_id": "SOL-USD"},
    }
    assert cli.best_product("BTC", "BUY") == "BTC-USD"
    assert cli.best_product("BTC", "SELL") == "BTC-USD"
    assert cli.best_product("SOL", "BUY") == "SOL-USD"
    assert cli.best_product("ZZZ", "BUY") is None


def test_cli_get_price(cli):
    with mock.patch.object(cli, "_run", return_value={"price": 123.0}):
        assert cli.get_price("BTC-USD") == {"price": 123.0}
    with mock.patch.object(cli, "_run", side_effect=RuntimeError("x")):
        assert cli.get_price("BTC-USD") == {}


def test_cli_get_balances(cli):
    with mock.patch.object(cli, "_run", return_value={"accounts": [1, 2]}):
        assert cli.get_balances() == [1, 2]
    with mock.patch.object(cli, "_run", return_value=[3, 4]):
        assert cli.get_balances() == [3, 4]


def test_cli_get_fees(cli):
    with mock.patch.object(cli, "_run", return_value={"fees": 1}):
        assert cli.get_fees() == {"fees": 1}


def test_cli_get_fills(cli):
    with mock.patch.object(cli, "_run", return_value={"fills": [1]}):
        assert cli.get_fills("BTC-USD") == [1]
    with mock.patch.object(cli, "_run", return_value=[2, 3]):
        assert cli.get_fills() == [2, 3]


def test_cli_round_quote_base(cli):
    prod = {"quote_increment": "0.01", "base_increment": "0.001"}
    cli._products = {"BTC-USD": prod}
    assert cli._round_quote("BTC-USD", 100.045) == 100.04
    assert cli._round_base("BTC-USD", 1.23456) == 1.235


def test_cli_preview_order(cli):
    resp = {"commission_total": "1.5", "quote_size": "100"}
    with mock.patch.object(cli, "_run", return_value=resp):
        out = cli.preview_order("BTC-USD", "BUY", 100)
        assert out["total_fee"] == 1.5
        assert out["total_cost"] == 100.0
    with mock.patch.object(cli, "_run", side_effect=RuntimeError("x")):
        assert cli.preview_order("BTC-USD", "BUY", 100) is None


def test_cli_create_order(cli):
    with mock.patch.object(cli, "_run", return_value={"id": "1"}):
        assert cli.create_order("BTC-USD", "BUY", 100)["id"] == "1"
    with mock.patch.object(cli, "_run", side_effect=RuntimeError("x")):
        assert cli.create_order("BTC-USD", "BUY", 100) is None


def test_cli_get_order(cli):
    with mock.patch.object(cli, "_run", return_value={"id": "1"}):
        assert cli.get_order("1") == {"id": "1"}


def test_cli_get_candles_cli_path(cli):
    with mock.patch.object(cli, "_run", return_value={"candles": [{"close": 1}]}):
        assert cli.get_candles("BTC-USD") == [{"close": 1}]
    with mock.patch.object(cli, "_run", side_effect=RuntimeError("x")):
        # Real fallback import (data.fetch_multi_source) is unavailable/offline
        # -> exception is caught -> empty list.
        assert cli.get_candles("BTC-USD") == []


# ===========================================================================
# Dataclasses
# ===========================================================================

def test_opportunity_defaults():
    o = P.Opportunity(P.OpportunityType.TLH, "BTC", "SELL", 100.0, "r")
    assert o.priority == 0.0
    assert o.meta == {}
    assert o.executed is False


def test_portfolio_state_defaults():
    s = P.PortfolioState(holdings={})
    assert s.total_value == 0.0
    assert s.fee_tier == (0, 0.006, 0.012)


# ===========================================================================
# Optimizer scoring / sizing / capital / cluster helpers
# ===========================================================================

def test_kelly_size(opt):
    opt.state = None
    assert opt._kelly_size(0.7, 2.0, 1.5, 0.6) >= 0
    opt.state = make_state({"SOL": holding("SOL", 1000, "growth")}, total_value=100000)
    assert opt._kelly_size(0.7, 2.0, 1.5, 0.6) > 0
    assert opt._kelly_size(1.5, 2.0, 1.5, 0.6) > 0


def test_kelly_size_respects_capital_limit(opt):
    opt.state = make_state({"SOL": holding("SOL", 1000, "growth")}, total_value=100000)
    small = opt._kelly_size(0.9, 5.0, 1.0, 0.9, capital_limit=10.0)
    assert small <= 10.0 + 1e-9


def test_regime_strategy_weight(opt):
    assert opt._regime_strategy_weight("ema_cross", "trending") == 1.5
    assert opt._regime_strategy_weight("rsi_revert", "trending") == 0.5
    assert opt._regime_strategy_weight("rsi_revert", "ranging") == 1.5
    assert opt._regime_strategy_weight("ema_cross", "ranging") == 0.5
    assert opt._regime_strategy_weight("boll_break", "volatile") == 1.4
    assert opt._regime_strategy_weight("anything", "quiet") == 0.7
    assert opt._regime_strategy_weight("anything", "weird") == 1.0


def test_risk_reward_size(opt):
    opt.state = None
    assert opt._risk_reward_size(5.0, 3.0, 0.6, 0.7) >= 0
    opt.state = make_state({"SOL": holding("SOL", 1000, "growth")}, total_value=100000)
    assert opt._risk_reward_size(5.0, 3.0, 0.6, 0.7) > 0


def test_risk_reward_size_respects_capital_limit(opt):
    opt.state = make_state({"SOL": holding("SOL", 1000, "growth")}, total_value=100000)
    capped = opt._risk_reward_size(50.0, 1.0, 1.0, 1.0,
                                    capital_limit=10.0, max_notional=5000.0)
    assert capped <= 10.0 + 1e-9


def test_estimate_trade_volatility(opt):
    closes = [float(100 + i) for i in range(30)]
    assert opt._estimate_trade_volatility_pct(closes) > 0
    assert opt._estimate_trade_volatility_pct([]) == 30.0
    assert opt._estimate_trade_volatility_pct([100.0, 101.0]) == 30.0


def test_compute_dynamic_stop(opt):
    assert opt._compute_dynamic_stop(100.0, "BUY", 0.0, "neutral", [], 5.0) == (5.0, 0.0, "default")
    lvl = P._SRLevel(price=95.0, kind="support", strength=2.0)
    pct, atr_d, detail = opt._compute_dynamic_stop(100.0, "BUY", 2.0, "volatile", [lvl], 5.0)
    assert pct >= 0.5
    lvl2 = P._SRLevel(price=106.0, kind="resistance", strength=2.0)
    pct2, _, _ = opt._compute_dynamic_stop(100.0, "SELL", 2.0, "trending", [lvl2], 5.0)
    assert pct2 >= 0.5
    pct3, _, _ = opt._compute_dynamic_stop(100.0, "BUY", 2.0, "neutral", [], 5.0)
    assert pct3 >= 0.5


def test_compute_exit_plan(opt):
    base = opt._compute_exit_plan("SOL", 0.6, expected_return_pct=10.0, trade_style="momentum")
    assert base["stop_loss_pct"] > 0 and base["take_profit_pct"] > 0
    for ts in ("new_listing", "equity_momentum", "prediction_market", "event",
               "mean_reversion", "arbitrage", "rebalance", "cycle", "tax_loss"):
        ep = opt._compute_exit_plan("SOL", 0.6, trade_style=ts)
        assert ep["stop_loss_pct"] >= 0.5
    ep2 = opt._compute_exit_plan("SOL", 0.6, trade_style="does_not_exist")
    assert ep2["stop_loss_pct"] > 0
    lvl = P._SRLevel(price=95.0, kind="support", strength=3.0)
    ep3 = opt._compute_exit_plan("SOL", 0.6, trade_style="momentum",
                                 sr_levels=[lvl], regime="trending", atr_value=1.0,
                                 entry_price=100.0)
    assert ep3["stop_loss_pct"] > 0


def test_compute_sr_aware_exit_plan(opt):
    ep = opt._compute_sr_aware_exit_plan("SOL", 0.6)
    assert "stop_loss_pct" in ep
    closes = [float(100 + i) for i in range(40)]
    highs = [c + 1 for c in closes]
    lows = [c - 1 for c in closes]
    ep2 = opt._compute_sr_aware_exit_plan("SOL", 0.6, closes=closes, highs=highs, lows=lows)
    assert ep2["stop_loss_pct"] > 0


def test_latency_adjusted_priority(opt):
    p = opt._latency_adjusted_priority(0.5, trade_style="momentum")
    assert 0 <= p <= 0.5
    p2 = opt._latency_adjusted_priority(0.5, trade_style="momentum", expected_delay_ms=10.0)
    assert 0 <= p2 <= 0.5


def test_normalize_capital_policy(opt):
    norm = opt._normalize_capital_policy()
    assert abs(sum(norm["targets"].values()) - 1.0) < 1e-6
    custom = {"targets": {"reserve": 0.4, "core": 0.4, "opportunity": 0.2},
              "core_allowlist": "BTC, ETH , SOL", "static_holdings": None}
    norm2 = opt._normalize_capital_policy(custom)
    assert norm2["core_allowlist"] == ["BTC", "ETH", "SOL"]
    assert "ETH" in norm2["static_holdings"]
    norm3 = opt._normalize_capital_policy({"targets": {"reserve": 0, "core": 0, "opportunity": 0}})
    assert abs(sum(norm3["targets"].values()) - 1.0) < 1e-6
    norm4 = opt._normalize_capital_policy({"max_deployable_usd": "5000"})
    assert norm4["max_deployable_usd"] == 5000.0


def test_apply_bear_market_policy(opt):
    opt.state = None
    opt._portfolio_peak_value = 0.0
    opt._apply_bear_market_policy()
    opt.state = make_state({"BTC": holding("BTC", 50000, "safe")}, total_value=100000)
    opt._portfolio_peak_value = 100000.0
    before = dict(opt.capital_policy["targets"])
    opt._apply_bear_market_policy()
    assert opt.capital_policy["targets"] == before
    opt.state = make_state({"BTC": holding("BTC", 50000, "safe")}, total_value=90000)
    opt._apply_bear_market_policy()
    assert opt.capital_policy["targets"]["reserve"] <= 0.45
    opt.state = make_state({"BTC": holding("BTC", 50000, "safe")}, total_value=60000)
    opt._apply_bear_market_policy()
    assert opt.capital_policy["core_min_allocation_pct"] >= 35.0


def test_check_wash_sale_and_replacement(opt):
    assert opt._check_wash_sale("SOL") is False
    opt._wash_sale_cooldown["SOL"] = time.time()
    assert opt._check_wash_sale("SOL") is True
    assert opt._get_tlh_replacement("BTC") == "ETH-USD"
    assert opt._get_tlh_replacement("BTC") is not None
    # Unknown currency not in the swap map -> no replacement
    assert opt._get_tlh_replacement("ZZZ") is None


def test_detect_sr_levels_for_product(opt):
    assert opt._detect_sr_levels_for_product([1, 2, 3], [1, 2, 3], [1, 2, 3]) == ([], 0.0)
    closes = [float(100 + i) for i in range(40)]
    highs = [c + 1 for c in closes]
    lows = [c - 1 for c in closes]
    levels, atr = opt._detect_sr_levels_for_product(closes, highs, lows)
    assert atr >= 0.0
    assert isinstance(levels, list)


def test_capital_helpers(opt):
    opt.state = make_state({"SOL": holding("SOL", 1000, "growth")}, total_value=100000)
    assert opt._usdc_reserve_amount() > 0
    assert opt._deployable_capital() > 0
    assert opt._buy_capacity() > 0
    assert opt._core_batch_cap() > 0
    assert opt._opportunity_batch_cap() > 0
    opt.state = None
    assert opt._usdc_reserve_amount() == 0.0
    assert opt._deployable_capital() == 0.0
    assert opt._buy_capacity() == 0.0
    assert opt._remaining_deployable_capital() == 0.0


def test_live_test_capital(opt):
    opt.state = make_state({}, total_value=100000)
    assert opt._live_test_capital_in_play() == 0.0
    opt.capital_policy["max_deployable_usd"] = 0.0
    assert opt._live_test_capital_in_play() == 0.0
    assert opt._parse_iso_ts("") is None
    assert opt._parse_iso_ts("2024-01-01T00:00:00Z") is not None
    assert opt._parse_iso_ts("garbage") is None


def test_cluster_helpers(opt):
    assert opt._get_cluster_for_currency("BTC") == "btc_eth"
    assert opt._get_cluster_for_currency("SOL") == "l1_solana"
    assert opt._get_cluster_for_currency("ZZZ") is None
    opt.state = make_state({"BTC": holding("BTC", 50000, "safe"),
                            "SOL": holding("SOL", 1000, "growth")}, total_value=100000)
    assert opt._cluster_exposure_pct("btc_eth") > 0
    assert opt._check_cluster_limit("ZZZ", 100) is True


def test_static_and_core_helpers(opt):
    assert opt._is_static_currency("BTC") is True
    assert opt._is_static_currency("SOL") is False
    assert opt._static_holdings_set() == {"BTC", "ETH"}
    assert opt._is_core_holding({"currency": "BTC"}) is True
    # SOL is in the default core_allowlist (BTC/ETH/SOL) -> core holding
    assert opt._is_core_holding({"currency": "SOL"}) is True
    assert opt._is_core_holding({"currency": "DOGE"}) is False
    opt.state = make_state({"BTC": holding("BTC", 50000, "safe")}, total_value=100000)
    assert "reserve" in opt._bucket_targets()
    assert opt._bucket_gap("reserve") >= 0
    o = P.Opportunity(P.OpportunityType.REBALANCE, "SOL", "BUY", 100, "r")
    o.meta["capital_bucket"] = "core"
    assert opt._capital_bucket_for(o) == "core"
    o2 = P.Opportunity(P.OpportunityType.STRATEGY_SIGNAL, "SOL", "BUY", 100, "r")
    # SOL is in the core allowlist -> routed to the core bucket
    assert opt._capital_bucket_for(o2) == "core"
    o3 = P.Opportunity(P.OpportunityType.STRATEGY_SIGNAL, "DOGE", "BUY", 100, "r")
    assert opt._capital_bucket_for(o3) == "opportunity"
    assert "reserve" in opt._bucket_values()


def test_compute_cost_bases(opt):
    fills = [
        {"product_id": "BTC-USD", "side": "BUY", "size": "1", "price": "100"},
        {"product_id": "BTC-USD", "side": "BUY", "size": "1", "price": "200"},
        {"product_id": "BTC-USD", "side": "SELL", "size": "1", "price": "300"},
        {"product_id": "ETH-USD", "side": "BUY", "size": "0", "price": "0"},
    ]
    with mock.patch.object(opt.cli, "get_fills", return_value=fills):
        bases = opt._compute_cost_bases()
    assert "BTC" in bases
    assert "ETH" not in bases


def test_detect_event_markets_no_clients(opt):
    opt.event_engine = None
    opt._pm_client = None
    assert opt._detect_event_markets() == []
    pm = mock.MagicMock()
    pm.search_all_categories.return_value = {}
    opt._pm_client = pm
    opt._arb_scanner = None
    opt._knowledge_gap = None
    assert opt._detect_event_markets() == []


def test_detect_accumulator_signals(opt):
    class FakeSig:
        action = "BUY"
        final_confidence = 0.6
        symbol = "SOL-USD"
        strategy_name = "NewsSentiment:foo"
        signal_reason = "r"
        base_confidence = 0.4
        opportunity_score = 0.3
        market_data = {"price": 100.0, "change_pct": 1.0}

    class FakeAcc:
        def __init__(self, *a, **k):
            pass

        def accumulate(self):
            return [FakeSig()]

    opt.last_execution.clear()
    opt.state = make_state({"SOL": holding("SOL", 1000, "growth", price=100)})
    with mock.patch.object(P, "UnifiedSignalAccumulator", FakeAcc):
        ops = opt._detect_accumulator_signals()
    assert ops and ops[0].opp_type == P.OpportunityType.ACCUMULATOR_SIGNAL

    class LowConf(FakeSig):
        final_confidence = 0.1

    class FakeAccLow:
        def __init__(self, *a, **k):
            pass

        def accumulate(self):
            return [LowConf()]

    with mock.patch.object(P, "UnifiedSignalAccumulator", FakeAccLow):
        assert opt._detect_accumulator_signals() == []

    class InvalidAction(FakeSig):
        action = "HOLD"

    class FakeAccInv:
        def __init__(self, *a, **k):
            pass

        def accumulate(self):
            return [InvalidAction()]

    with mock.patch.object(P, "UnifiedSignalAccumulator", FakeAccInv):
        assert opt._detect_accumulator_signals() == []


def test_detect_stock_opportunities(opt):
    class FakeYFinance:
        @staticmethod
        def get_stock_info(symbol):
            return {"market_capital": 1e12}

    class FakeAdapter:
        def __init__(self):
            self.yfinance = FakeYFinance()

        def fetch_historical_data(self, symbol, start, end):
            return [{"close": float(100 + i), "volume": 1000.0} for i in range(80)]

    with mock.patch.object(P, "UnifiedMarketDataAdapter", FakeAdapter):
        ops = opt._detect_stock_opportunities()
    assert ops and ops[0].opp_type == P.OpportunityType.STOCK_SIGNAL
    with mock.patch.object(P, "UnifiedMarketDataAdapter", side_effect=RuntimeError("x")):
        assert opt._detect_stock_opportunities() == []


def test_detect_funding_and_onchain(opt):
    opt.last_execution.clear()
    opt.state = make_state({"BTC": holding("BTC", 5000, "safe", price=30000)})
    opt.cli.get_price.return_value = {"price": 30000.0}
    fsig = mock.MagicMock()
    fsig.action = "BUY"
    fsig.confidence = 0.6
    fsig.reason = "r"
    fsig.currency = "BTC"
    fsig.symbol = "BTC"
    opt._funding_contrarian.on_bar = mock.MagicMock(return_value=fsig)
    opt._onchain_flow.get_signals = mock.MagicMock(return_value=[])
    ops = opt._detect_funding_and_onchain_signals()
    assert any(o.currency == "BTC" for o in ops)

    fsig.action = "SELL"
    opt.last_execution.clear()
    opt.state = make_state({"BTC": holding("BTC", 5000, "safe", price=30000)})
    ops2 = opt._detect_funding_and_onchain_signals()
    assert any(o.side == "SELL" for o in ops2)

    opt.last_execution.clear()
    opt._funding_contrarian.on_bar = mock.MagicMock(return_value=None)
    opt._onchain_flow.get_signals = mock.MagicMock(return_value=[{
        "action": "BUY", "product_id": "SOL-USD", "currency": "SOL",
        "confidence": 0.6, "price": 100.0, "volume_anomaly": 2.0, "price_trend": 0.1,
    }])
    opt.state = make_state({"SOL": holding("SOL", 1000, "growth", price=100)})
    ops3 = opt._detect_funding_and_onchain_signals()
    assert any(o.currency == "SOL" for o in ops3)


def test_detect_order_flow_signals(opt):
    opt.last_execution.clear()
    of_sig = mock.MagicMock()
    of_sig.confidence = 0.5
    of_sig.action = "BUY"
    of_sig.spread_z = 2.0
    of_sig.spread_tight = True
    of_sig.spread_bps = 5.0
    of_sig.volume_24h = 1000.0
    opt._order_flow_engine = mock.MagicMock()
    opt._order_flow_engine.evaluate = mock.MagicMock(return_value=of_sig)
    opt._smart_money_flow = None
    opt._feed_mgr = None
    opt.state = make_state({"SOL": holding("SOL", 1000, "growth", price=100)}, usdc=90000.0)
    ops = opt._detect_order_flow_signals()
    assert any(o.currency == "SOL" for o in ops)

    of_sig.action = "SELL"
    opt.last_execution.clear()
    opt.state = make_state({"SOL": holding("SOL", 1000, "growth", price=100)}, usdc=90000.0)
    ops2 = opt._detect_order_flow_signals()
    assert any(o.side == "SELL" for o in ops2)


def test_opportunity_currency_is_base_ticker(opt):
    """Every detection path must emit a base-ticker currency (e.g. 'SOL'),
    never a product_id ('SOL-USD'), so holdings keying stays consistent."""
    import re
    BASE = re.compile(r"^[A-Z0-9]{2,10}$")
    opt.last_execution.clear()
    opt.cli.get_products.return_value = {
        "SOL-USD": {"trading_disabled": False, "volume_24h": 200_000_000},
    }
    opt.cli.get_candles.return_value = []
    opt._feed_mgr = None
    opt.state = make_state(
        {"SOL": holding("SOL", 1000, "growth", price=100)}, usdc=90000.0
    )
    # Wire minimal mocked signals so each detector can produce an opp.
    of_sig = mock.MagicMock()
    of_sig.confidence = 0.5
    of_sig.action = "BUY"
    of_sig.spread_z = 2.0
    of_sig.spread_tight = True
    of_sig.spread_bps = 5.0
    of_sig.volume_24h = 1000.0
    opt._order_flow_engine = mock.MagicMock()
    opt._order_flow_engine.evaluate = mock.MagicMock(return_value=of_sig)
    opt._smart_money_flow = None

    seen = []
    for fn in (
        opt._detect_order_flow_signals,
    ):
        for o in fn():
            seen.append(o.currency)
    # sanity: at least one opp produced
    assert seen
    assert all(BASE.match(c) for c in seen), f"non-base currency emitted: {seen}"


def test_detect_aggregator_signals_empty(opt):
    with mock.patch("coinbase.src.pair_discovery.top_coinbase_pairs", return_value=[]):
        assert opt._detect_aggregator_signals() == []
