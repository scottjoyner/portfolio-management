"""Coverage tests for event_markets.crypto_divergence."""
import math
from datetime import datetime, timezone
from unittest import TestCase, mock

from event_markets.unified_client import PredictionMarket
import event_markets.crypto_divergence as CD
from event_markets.crypto_divergence import (
    _norm_cdf, _inv_norm_cdf, _parse_price, _parse_date_str,
    CryptoDivergence, CryptoPriceDivergenceDetector,
)


class FakeSeries:
    def __init__(self, vals):
        self.v = list(vals)

    def __len__(self):
        return len(self.v)

    def pct_change(self):
        out = []
        for i in range(1, len(self.v)):
            out.append((self.v[i] - self.v[i - 1]) / self.v[i - 1] if self.v[i - 1] else 0.0)
        return FakeSeries(out)

    def dropna(self):
        return FakeSeries([x for x in self.v if x is not None])

    def std(self):
        if not self.v:
            return 0.0
        m = sum(self.v) / len(self.v)
        return math.sqrt(sum((x - m) ** 2 for x in self.v) / len(self.v))


class FakeDF:
    def __init__(self, data):
        self._d = data

    @property
    def columns(self):
        return list(self._d.keys())

    def __getitem__(self, col):
        return FakeSeries(self._d.get(col, []))

    @property
    def empty(self):
        return not self._d


class FakePandas:
    def DataFrame(self, data):
        return FakeDF(data)


def mk_market(question, mid=0.5, symbol=None, **kw):
    base = dict(
        platform="kalshi", market_id="KX", question=question,
        outcomes=["YES", "NO"], outcome_prices={"YES": mid, "NO": 1 - mid},
        volume=100000.0, end_date="2026-12-31T00:00:00Z", is_open=True,
        yes_bid=mid - 0.02, yes_ask=mid + 0.02, spread=0.04,
        liquidity_score=0.5, category="crypto", raw_data={},
    )
    base.update(kw)
    return PredictionMarket(**base)


class TestMathHelpers(TestCase):
    def test_norm_cdf(self):
        self.assertAlmostEqual(_norm_cdf(0), 0.5)
        self.assertLess(_norm_cdf(-1), 0.5)
        self.assertGreater(_norm_cdf(1), 0.5)

    def test_inv_norm_cdf_bounds(self):
        self.assertEqual(_inv_norm_cdf(0), -8.0)
        self.assertEqual(_inv_norm_cdf(1), 8.0)

    def test_inv_norm_cdf_regions(self):
        self.assertLess(_inv_norm_cdf(0.01), 0)
        self.assertAlmostEqual(_inv_norm_cdf(0.5), 0.0, places=1)
        self.assertGreater(_inv_norm_cdf(0.99), 0)

    def test_parse_price(self):
        self.assertEqual(_parse_price("$100,000"), 100000.0)
        self.assertEqual(_parse_price("100k"), 100000.0)
        self.assertEqual(_parse_price("1.5m"), 1500000.0)
        self.assertEqual(_parse_price("2billion"), 2000000000.0)
        self.assertEqual(_parse_price("500"), 500.0)
        self.assertIsNone(_parse_price("abc"))
        self.assertIsNone(_parse_price(""))

    def test_parse_date_str_end_of_year(self):
        ts = _parse_date_str("end of year")
        self.assertIsNotNone(ts)
        self.assertEqual(datetime.fromtimestamp(ts, tz=timezone.utc).month, 12)

    def test_parse_date_str_end_of_year_specific(self):
        ts = _parse_date_str("end of 2027")
        d = datetime.fromtimestamp(ts, tz=timezone.utc)
        self.assertEqual((d.year, d.month, d.day), (2026, 12, 31))

    def test_parse_date_str_quarter(self):
        ts = _parse_date_str("by q2 2026")
        self.assertIsNotNone(ts)

    def test_parse_date_str_month_day_year(self):
        ts = _parse_date_str("by December 25 2026")
        d = datetime.fromtimestamp(ts, tz=timezone.utc)
        self.assertEqual((d.year, d.month, d.day), (2026, 12, 25))

    def test_parse_date_str_month_year(self):
        ts = _parse_date_str("by December 2026")
        d = datetime.fromtimestamp(ts, tz=timezone.utc)
        self.assertEqual((d.year, d.month), (2026, 12))

    def test_parse_date_str_year(self):
        ts = _parse_date_str("in 2027")
        d = datetime.fromtimestamp(ts, tz=timezone.utc)
        self.assertEqual(d.year, 2027)

    def test_parse_date_str_none(self):
        self.assertIsNone(_parse_date_str("no date here"))

    def test_parse_date_str_value_error(self):
        # month_day_year with invalid day -> ValueError -> None
        self.assertIsNone(_parse_date_str("by February 30 2026"))


class TestCryptoDivergence(TestCase):
    def _mk(self, **kw):
        base = dict(
            market_id="KX", platform="kalshi", question="q", category="crypto",
            asset_symbol="BTC", target_price=100000.0, target_date="Dec 2026",
            market_probability=0.5, fair_probability=0.4, divergence=0.1,
            divergence_pct=0.25, spot_price=85000.0, years_to_expiry=1.5,
            annualized_vol=0.6, implied_vol=0.7, kelly_fraction=0.1,
            confidence=0.5, is_significant=True, signal="PM_OVERPRICING_YES",
            volume=100000.0, spread=0.04, liquidity_score=0.5,
        )
        base.update(kw)
        return CryptoDivergence(**base)

    def test_summary(self):
        s = self._mk(divergence=0.1).summary()
        self.assertIn("above fair", s)
        s2 = self._mk(divergence=-0.1).summary()
        self.assertIn("below fair", s2)

    def test_summary_kelly(self):
        s = self._mk(kelly_fraction=0.0).summary()
        self.assertNotIn("Kelly", s)

    def test_to_dict(self):
        d = self._mk().to_dict()
        self.assertEqual(d["asset_symbol"], "BTC")
        self.assertEqual(d["market_probability"], 0.5)


class TestDetector(TestCase):
    def test_init(self):
        d = CryptoPriceDivergenceDetector(coinbase_prices={"BTC-USD": 1.0})
        self.assertEqual(d.coinbase_prices, {"BTC-USD": 1.0})

    def test_set_coinbase_prices(self):
        d = CryptoPriceDivergenceDetector()
        d.set_coinbase_prices({"ETH-USD": 2.0})
        self.assertEqual(d.coinbase_prices, {"ETH-USD": 2.0})

    def test_compute_historical_vol_no_pd(self):
        d = CryptoPriceDivergenceDetector()
        with mock.patch.object(CD, "pd", None):
            self.assertIsNone(d.compute_historical_vol("BTC-USD"))

    def test_compute_historical_vol_no_client(self):
        d = CryptoPriceDivergenceDetector()
        self.assertIsNone(d.compute_historical_vol("BTC-USD"))

    def test_compute_historical_vol_success(self):
        d = CryptoPriceDivergenceDetector(coinbase_client=object())
        df = FakeDF({"close": [100, 130, 90, 140, 80, 150]})
        with mock.patch.object(CD, "pd", FakePandas()):
            with mock.patch("coinbase.src.data.fetch_candles_df", return_value=df):
                vol = d.compute_historical_vol("BTC-USD")
        self.assertGreater(vol, 0.01)

    def test_compute_historical_vol_short(self):
        d = CryptoPriceDivergenceDetector(coinbase_client=object())
        df = FakeDF({"close": [100, 101, 102]})
        with mock.patch.object(CD, "pd", FakePandas()):
            with mock.patch("coinbase.src.data.fetch_candles_df", return_value=df):
                self.assertIsNone(d.compute_historical_vol("BTC-USD"))

    def test_compute_historical_vol_empty(self):
        d = CryptoPriceDivergenceDetector(coinbase_client=object())
        with mock.patch.object(CD, "pd", FakePandas()):
            with mock.patch("coinbase.src.data.fetch_candles_df", return_value=FakeDF({})):
                self.assertIsNone(d.compute_historical_vol("BTC-USD"))

    def test_compute_historical_vol_exception(self):
        d = CryptoPriceDivergenceDetector(coinbase_client=object())
        with mock.patch.object(CD, "pd", FakePandas()):
            with mock.patch("coinbase.src.data.fetch_candles_df", side_effect=RuntimeError("x")):
                self.assertIsNone(d.compute_historical_vol("BTC-USD"))

    def test_refresh_historical_vols(self):
        d = CryptoPriceDivergenceDetector(coinbase_client=object())
        df = FakeDF({"close": [100, 130, 90, 140, 80, 150]})
        with mock.patch.object(CD, "pd", FakePandas()):
            with mock.patch("coinbase.src.data.fetch_candles_df", return_value=df):
                d.refresh_historical_vols(["BTC-USD"])
        self.assertIn("BTC", d.annualized_vol)

    def test_compute_historical_vol_none_df(self):
        d = CryptoPriceDivergenceDetector(coinbase_client=object())
        with mock.patch.object(CD, "pd", FakePandas()):
            with mock.patch("coinbase.src.data.fetch_candles_df", return_value=None):
                self.assertIsNone(d.compute_historical_vol("BTC-USD"))

    def test_implied_volatility_too_large(self):
        # extreme moneyness + tiny T -> iv > 10 -> returns None
        self.assertIsNone(
            CD.CryptoPriceDivergenceDetector.implied_volatility(0.999, 100.0, 5000.0, 0.01))

    def test_analyze_no_date_no_enddate(self):
        d = CryptoPriceDivergenceDetector(coinbase_prices={"BTC-USD": 85000.0})
        m = mk_market("Will BTC reach $100k?", mid=0.3, end_date="")
        r = d.analyze(m)
        self.assertIsNotNone(r)

    def test_implied_volatility_tiny(self):
        iv = CD.CryptoPriceDivergenceDetector.implied_volatility(0.01, 85000, 100000, 1.5)
        self.assertIsNotNone(iv)

    def test_implied_volatility_edge(self):
        self.assertIsNone(CD.CryptoPriceDivergenceDetector.implied_volatility(0.5, 0, 100, 1))
        self.assertIsNone(CD.CryptoPriceDivergenceDetector.implied_volatility(0.5, 100, 0, 1))
        self.assertIsNone(CD.CryptoPriceDivergenceDetector.implied_volatility(1.0, 100, 100, 1))

    def test_implied_volatility_normal(self):
        iv = CD.CryptoPriceDivergenceDetector.implied_volatility(0.7, 85000, 100000, 1.5)
        self.assertGreater(iv, 0)

    def test_extract_empty(self):
        d = CryptoPriceDivergenceDetector()
        self.assertIsNone(d.extract(""))

    def test_extract_btc_reach(self):
        d = CryptoPriceDivergenceDetector()
        r = d.extract("Will BTC reach $100k by Dec 2026?")
        self.assertIsNotNone(r)
        self.assertEqual(r["symbol"], "BTC")
        self.assertEqual(r["target_price"], 100000.0)

    def test_extract_eth_above(self):
        d = CryptoPriceDivergenceDetector()
        r = d.extract("ETH above $5000")
        self.assertEqual(r["symbol"], "ETH")
        self.assertEqual(r["target_price"], 5000.0)

    def test_extract_solana_to_hit(self):
        d = CryptoPriceDivergenceDetector()
        r = d.extract("Solana to hit $1000")
        self.assertEqual(r["symbol"], "SOL")
        self.assertEqual(r["target_price"], 1000.0)

    def test_extract_reverse_order(self):
        d = CryptoPriceDivergenceDetector()
        r = d.extract("$100k Bitcoin")
        self.assertEqual(r["symbol"], "BTC")
        self.assertEqual(r["target_price"], 100000.0)

    def test_extract_asset_price(self):
        d = CryptoPriceDivergenceDetector()
        r = d.extract("bitcoin $100k")
        self.assertEqual(r["symbol"], "BTC")

    def test_extract_no_match(self):
        d = CryptoPriceDivergenceDetector()
        self.assertIsNone(d.extract("What will the weather be tomorrow?"))

    def test_pattern_id(self):
        d = CryptoPriceDivergenceDetector()
        for i, p in enumerate(CD.CryptoPriceDivergenceDetector.PATTERNS):
            self.assertEqual(d._PATTERN_ID(p), i)
        self.assertEqual(d._PATTERN_ID(__import__("re").compile("x")), -1)

    def test_extract_date(self):
        d = CryptoPriceDivergenceDetector()
        ts = d._extract_date("Will BTC reach $100k by Dec 2026?")
        self.assertIsNotNone(ts)
        self.assertGreater(ts, datetime.now(timezone.utc).timestamp())

    def test_extract_date_past(self):
        d = CryptoPriceDivergenceDetector()
        # date in the past -> returns max (past) candidate
        ts = d._extract_date("Will BTC reach $100k by December 2000?")
        self.assertIsNotNone(ts)

    def test_extract_date_none(self):
        d = CryptoPriceDivergenceDetector()
        self.assertIsNone(d._extract_date("no date"))

    def test_get_spot_price(self):
        d = CryptoPriceDivergenceDetector(coinbase_prices={"BTC-USD": 85000.0})
        self.assertEqual(d._get_spot_price("BTC"), 85000.0)
        self.assertIsNone(d._get_spot_price("DOGE"))

    def test_fair_probability_edge(self):
        d = CryptoPriceDivergenceDetector()
        self.assertEqual(d.fair_probability(0, 100, 1, "BTC"), 0.5)
        self.assertEqual(d.fair_probability(100, 0, 1, "BTC"), 0.5)
        self.assertEqual(d.fair_probability(100, 100, 0, "BTC"), 0.5)

    def test_fair_probability_normal(self):
        d = CryptoPriceDivergenceDetector()
        fp = d.fair_probability(85000, 100000, 1.5, "BTC")
        self.assertGreater(fp, 0)
        self.assertLess(fp, 1)

    def test_analyze_no_extract(self):
        d = CryptoPriceDivergenceDetector(coinbase_prices={"BTC-USD": 85000.0})
        m = mk_market("What is the weather?")
        self.assertIsNone(d.analyze(m))

    def test_analyze_no_spot(self):
        d = CryptoPriceDivergenceDetector(coinbase_prices={})
        m = mk_market("Will BTC reach $100k by Dec 2026?")
        self.assertIsNone(d.analyze(m))

    def test_analyze_overpricing(self):
        d = CryptoPriceDivergenceDetector(coinbase_prices={"BTC-USD": 85000.0})
        m = mk_market("Will BTC reach $100k by Dec 2026?", mid=0.7)
        r = d.analyze(m)
        self.assertIsNotNone(r)
        self.assertEqual(r.signal, "PM_OVERPRICING_YES")

    def test_analyze_underpricing(self):
        d = CryptoPriceDivergenceDetector(coinbase_prices={"BTC-USD": 85000.0})
        m = mk_market("Will BTC reach $100k by Dec 2026?", mid=0.1)
        r = d.analyze(m)
        self.assertIsNotNone(r)
        self.assertEqual(r.signal, "PM_UNDERPRICING_YES")

    def test_analyze_fair(self):
        d = CryptoPriceDivergenceDetector(coinbase_prices={"BTC-USD": 85000.0})
        m = mk_market("Will BTC reach $100k by Dec 2026?", mid=0.41)
        r = d.analyze(m)
        self.assertIsNotNone(r)
        self.assertEqual(r.signal, "FAIR")

    def test_analyze_end_date_fallback(self):
        d = CryptoPriceDivergenceDetector(coinbase_prices={"BTC-USD": 85000.0})
        m = mk_market("Will BTC reach $100k?", mid=0.3,
                      end_date="2027-06-30T00:00:00Z")
        r = d.analyze(m)
        self.assertIsNotNone(r)

    def test_analyze_end_date_bad(self):
        d = CryptoPriceDivergenceDetector(coinbase_prices={"BTC-USD": 85000.0})
        m = mk_market("Will BTC reach $100k?", mid=0.3,
                      end_date="not-a-date")
        r = d.analyze(m)
        self.assertIsNotNone(r)

    def test_analyze_markets_dedup_and_sort(self):
        d = CryptoPriceDivergenceDetector(coinbase_prices={"BTC-USD": 85000.0})
        m1 = mk_market("Will BTC reach $100k by Dec 2026?", mid=0.3, market_id="A")
        m2 = mk_market("Will BTC reach $100k by Dec 2026?", mid=0.3, market_id="A")
        m3 = mk_market("Will BTC reach $100k by Dec 2026?", mid=0.7, market_id="B")
        res = d.analyze_markets([m1, m2, m3])
        self.assertEqual(len(res), 2)

    def test_analyze_markets_exception(self):
        d = CryptoPriceDivergenceDetector(coinbase_prices={"BTC-USD": 85000.0})
        bad = mk_market("Will BTC reach $100k by Dec 2026?", mid=0.3)
        # force analyze to raise
        with mock.patch.object(d, "analyze", side_effect=RuntimeError("x")):
            res = d.analyze_markets([bad])
        self.assertEqual(res, [])


class TestMain(TestCase):
    def test_main_empty(self):
        fake_client = mock.MagicMock()
        fake_client.search_all_categories.return_value = {"crypto": []}
        fake_client.get_crypto_markets.return_value = []
        with mock.patch.dict(CD.__dict__, {"UnifiedPredictionMarketClient": lambda **k: fake_client}):
            with mock.patch.dict("os.environ", {}, clear=True):
                with mock.patch("coinbase.src.cb_client.CBClient") as CB:
                    cb = CB.return_value
                    cb.best_bid_ask.return_value = {
                        "pricebooks": [{"product_id": "BTC-USD",
                                        "bids": [{"price": "85000"}],
                                        "asks": [{"price": "85100"}]}]}
                    CD.main()

    def test_main_with_divergence(self):
        m = mk_market("Will BTC reach $100k by Dec 2026?", mid=0.7, market_id="A")
        fake_client = mock.MagicMock()
        fake_client.search_all_categories.return_value = {"crypto": [m]}
        fake_client.get_crypto_markets.return_value = [m]
        with mock.patch.dict(CD.__dict__, {"UnifiedPredictionMarketClient": lambda **k: fake_client}):
            with mock.patch.dict("os.environ", {}, clear=True):
                with mock.patch("coinbase.src.cb_client.CBClient") as CB:
                    cb = CB.return_value
                    cb.best_bid_ask.return_value = {
                        "pricebooks": [{"product_id": "BTC-USD",
                                        "bids": [{"price": "85000"}],
                                        "asks": [{"price": "85100"}]}]}
                    CD.main()


if __name__ == "__main__":
    import unittest
    unittest.main()


# ── supplementary branch coverage ──────────────────────────────────
def test_parse_date_str_end_of_year_explicit():
    ts = _parse_date_str("end of year", end_of_year_year=2028)
    d = datetime.fromtimestamp(ts, tz=timezone.utc)
    assert (d.year, d.month, d.day) == (2028, 12, 31)


def test_parse_date_str_month_only():
    assert _parse_date_str("by December") is None


def test_extract_date_end_of_quarter():
    d = CryptoPriceDivergenceDetector()
    assert d._extract_date("end of q2") is not None


def test_extract_date_year_range():
    d = CryptoPriceDivergenceDetector()
    assert d._extract_date("2025-2027") is not None


def test_extract_date_month_year():
    d = CryptoPriceDivergenceDetector()
    assert d._extract_date("by December 2026") is not None


def test_extract_date_invalid_month_day():
    d = CryptoPriceDivergenceDetector()
    assert d._extract_date("by February 30 2026") is None


def test_extract_non_alias_pattern0():
    d = CryptoPriceDivergenceDetector()
    assert d.extract("Will Tesla reach $100k?") is None


def test_extract_zero_price_pattern0():
    d = CryptoPriceDivergenceDetector()
    assert d.extract("Will BTC reach $0?") is None


def test_extract_non_alias_pattern1():
    d = CryptoPriceDivergenceDetector()
    assert d.extract("Tesla above $5000") is None


def test_extract_zero_price_pattern1():
    d = CryptoPriceDivergenceDetector()
    assert d.extract("BTC above $0") is None


def test_extract_non_alias_pattern3():
    d = CryptoPriceDivergenceDetector()
    assert d.extract("$100k Tesla") is None


def test_extract_zero_price_pattern3():
    d = CryptoPriceDivergenceDetector()
    assert d.extract("$0 Tesla") is None


def test_fair_probability_tiny_years():
    d = CryptoPriceDivergenceDetector()
    fp = d.fair_probability(85000, 100000, 0.0001, "BTC")
    assert fp >= 0.0


def test_refresh_historical_vols_skips_low():
    d = CryptoPriceDivergenceDetector(coinbase_client=object())
    df = FakeDF({"close": [100, 101]})
    with mock.patch.object(CD, "pd", FakePandas()):
        with mock.patch("coinbase.src.data.fetch_candles_df", return_value=df):
            d.refresh_historical_vols(["BTC-USD"])
    assert "BTC" not in d.annualized_vol


def test_analyze_markets_skips_none():
    d = CryptoPriceDivergenceDetector(coinbase_prices={})
    m = mk_market("What is the weather?")
    assert d.analyze_markets([m]) == []


def test_main_cointimeout(monkeypatch):
    fake_client = mock.MagicMock()
    fake_client.search_all_categories.return_value = {"crypto": []}
    fake_client.get_crypto_markets.return_value = []
    cb_mock = mock.MagicMock()
    cb_mock.best_bid_ask.side_effect = RuntimeError("boom")
    with mock.patch.object(CD, "UnifiedPredictionMarketClient", lambda **k: fake_client, create=True):
        with mock.patch.dict("os.environ", {}, clear=True):
            with mock.patch("coinbase.src.cb_client.CBClient", return_value=cb_mock):
                CD.main()

