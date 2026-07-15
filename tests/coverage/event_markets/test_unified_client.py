import os
from unittest.mock import MagicMock, patch

import pytest

from event_markets.unified_client import UnifiedPredictionMarketClient, PredictionMarket
from em_helpers import (
    make_kalshi_market, make_polymarket_market, UrlRouter, FakeResp, make_book,
)


def _pm(**kw):
    base = dict(platform="k", market_id="x", question="q", outcomes=["YES", "NO"],
                outcome_prices={"YES": 0.5}, volume=1000, end_date="2026-01-01", is_open=True)
    base.update(kw)
    return PredictionMarket(**base)


def _make_client():
    client = UnifiedPredictionMarketClient()
    client._kalshi = MagicMock()
    client._polymarket = MagicMock()
    client._polymarket_relayer = MagicMock()
    return client


def test_prediction_market_mid_price():
    m = _pm(outcome_prices={"YES": 0.7, "NO": 0.3})
    assert m.mid_price == 0.7
    m2 = _pm(outcomes=["a"], outcome_prices={"a": 0.2})
    assert m2.mid_price == 0.2
    m3 = _pm(outcomes=[], outcome_prices={})
    assert m3.mid_price == 0.0


def test_probability_extremity():
    assert _pm(outcome_prices={"YES": 0.8}).probability_extremity == pytest.approx(0.6)
    assert _pm(outcome_prices={"YES": 0.5}).probability_extremity == 0.0
    assert _pm(outcome_prices={"YES": 0.0}).probability_extremity == 0.0


def test_is_relevant_and_format():
    m = _pm(question="Will bitcoin hit 100k?")
    assert m.is_relevant is True
    m2 = _pm(question="Who wins the world cup?")
    assert m2.is_relevant is False
    assert "bitcoin" in m.format()


def test_detect_category():
    assert UnifiedPredictionMarketClient._detect_category("Will bitcoin reach 100k?") == "crypto"
    assert UnifiedPredictionMarketClient._detect_category("Who wins the super bowl?") == "sports"
    assert UnifiedPredictionMarketClient._detect_category("Will inflation exceed 3%?") == "economics"
    assert UnifiedPredictionMarketClient._detect_category("random unrelated question about xyz") == "general"


def test_kalshi_to_unified_filters():
    client = _make_client()
    raw = [
        make_kalshi_market(ticker="open1", title="Will bitcoin go up?", volume=900, yes_bid=0.4, yes_ask=0.45),
        make_kalshi_market(ticker="settled", title="old btc market", settled=True, volume=99999),
        make_kalshi_market(ticker="wide", title="Will ETH rise?", volume=99999, yes_bid=0.1, yes_ask=0.9),
        make_kalshi_market(ticker="good", title="Will BTC hit 100k?", volume=99999, yes_bid=0.4, yes_ask=0.45),
    ]
    out = client._kalshi_to_unified(raw, limit=10, min_volume=1000, max_spread=0.15)
    ids = {m.market_id for m in out}
    assert "settled" not in ids and "wide" not in ids and "open1" not in ids
    assert "good" in ids
    assert all(isinstance(m, PredictionMarket) for m in out)
    assert out[0].volume >= out[-1].volume


def test_polymarket_to_unified_filters_and_book_fallback():
    client = _make_client()
    pm_no_book = make_polymarket_market(condition_id="c1", question="Will BTC reach 100k?",
                                        spread=0.0, token_ids=["tok1"])
    pm_good = make_polymarket_market(condition_id="c2", question="Will ETH reach 5000?",
                                     spread=0.03, volume=99999)
    raw = [pm_no_book, pm_good]
    client._polymarket.get_order_book.return_value = make_book(
        bids=((0.39, 50),), asks=((0.41, 50),), spread=0.02, mid=0.40)
    out = client._polymarket_to_unified(raw, limit=10, min_volume=1000, max_spread=0.15)
    ids = {m.market_id for m in out}
    assert "c2" in ids
    assert "c1" in ids


def test_polymarket_to_unified_closed_and_no_token_book():
    client = _make_client()
    # closed market is skipped
    pm_closed = make_polymarket_market(condition_id="cz", question="Will BTC go up?",
                                       closed=True, accepting_orders=True)
    # spread<=0 with tokens -> book fallback fetches (mocked) and uses real spread
    pm_no_tokens = make_polymarket_market(condition_id="cnt", question="Will BTC go up?",
                                          spread=0.0, token_ids=["tok-x"])
    client._polymarket.get_order_book.return_value = make_book(
        bids=((0.39, 50),), asks=((0.41, 50),), spread=0.02, mid=0.40)
    out = client._polymarket_to_unified([pm_closed, pm_no_tokens], limit=10,
                                        min_volume=0, max_spread=1.0)
    ids = {m.market_id for m in out}
    assert "cz" not in ids
    assert "cnt" in ids


def test_search_kalshi_no_auth():
    client = _make_client()
    client._kalshi.email = ""
    client._kalshi.password = ""
    client._kalshi.api_key_id = ""
    client._kalshi.private_key_path = ""
    assert client.search_kalshi() == []


def test_search_kalshi_with_auth():
    client = _make_client()
    client._kalshi.api_key_id = "kid"
    client._kalshi.private_key_path = "/tmp/key.pem"
    client._kalshi.get_relevant_markets.return_value = [
        make_kalshi_market(ticker="g", title="Will BTC go up?", volume=99999, yes_bid=0.4, yes_ask=0.45),
    ]
    out = client.search_kalshi(limit=5)
    assert len(out) == 1
    client._kalshi.get_relevant_markets.assert_called_once()


def test_search_polymarket_term_and_categories():
    client = _make_client()
    client._polymarket.search_markets.return_value = [
        make_polymarket_market(condition_id="c", question="Will ETH reach 5000?", spread=0.03),
    ]
    out = client.search_polymarket(term="ethereum", limit=5)
    assert len(out) == 1
    assert out[0].category == "crypto"


def test_search_all_combines_and_handles_errors():
    client = _make_client()
    client._kalshi.api_key_id = "kid"
    client._kalshi.private_key_path = "/tmp/k.pem"
    client._kalshi.get_relevant_markets.return_value = [
        make_kalshi_market(ticker="k", title="Will BTC go up?", volume=99999, yes_bid=0.4, yes_ask=0.45),
    ]
    client._polymarket.search_markets.return_value = [
        make_polymarket_market(condition_id="p", question="Will ETH reach 5000?", spread=0.03),
    ]
    out = client.search_all(limit=10)
    assert {m.market_id for m in out} == {"k", "p"}

    client._polymarket.search_markets.side_effect = RuntimeError("boom")
    out = client.search_all(limit=10)
    assert len(out) == 1


def test_search_all_categories_cache_and_paths():
    client = _make_client()
    client._kalshi.api_key_id = "kid"
    client._kalshi.private_key_path = "/tmp/k.pem"
    client._polymarket.fetch_markets.return_value = [
        make_polymarket_market(condition_id="p", question="Will BTC reach 100k?", spread=0.03, volume=99999),
    ]
    client._kalshi.get_markets_by_categories.return_value = {
        "Crypto": [make_kalshi_market(ticker="k", title="Will BTC go up?", volume=99999, yes_bid=0.4, yes_ask=0.45)],
    }
    res = client.search_all_categories(limit_per_platform=5)
    assert "crypto" in res and res["crypto"]
    client._polymarket.fetch_markets.reset_mock()
    res2 = client.search_all_categories(limit_per_platform=5)
    assert res2 == res
    client._polymarket.fetch_markets.assert_not_called()


def test_search_all_categories_broad_fallback():
    client = _make_client()
    client._kalshi.api_key_id = ""
    client._kalshi.private_key_path = ""
    client._polymarket.fetch_markets.return_value = [
        make_polymarket_market(condition_id="p", question="Will BTC reach 100k?", spread=0.03, volume=99999),
    ]
    client._kalshi.search_broad.return_value = [
        make_kalshi_market(ticker="k", title="Will BTC go up?", volume=99999, yes_bid=0.4, yes_ask=0.45),
    ]
    res = client.search_all_categories(limit_per_platform=5)
    assert res["crypto"]
    client._kalshi.search_broad.assert_called_once()


def test_search_all_categories_kalshi_unmapped_category():
    client = _make_client()
    client._kalshi.api_key_id = "kid"
    client._kalshi.private_key_path = "/tmp/k.pem"
    client._polymarket.fetch_markets.return_value = [
        make_polymarket_market(condition_id="p", question="Will BTC reach 100k?", spread=0.03, volume=99999),
    ]
    # A Kalshi category that maps to "general" (not a tracked category) must be skipped
    client._kalshi.get_markets_by_categories.return_value = {
        "Climate and Weather": [make_kalshi_market(ticker="w", title="Will BTC go up?",
                                                     volume=99999, yes_bid=0.4, yes_ask=0.45)],
    }
    res = client.search_all_categories(limit_per_platform=5)
    assert "general" not in res
    assert not any(m.market_id == "w" for cat in res.values() for m in cat)


def test_search_all_categories_kalshi_auth_exception():
    client = _make_client()
    client._kalshi.api_key_id = "kid"
    client._kalshi.private_key_path = "/tmp/k.pem"
    client._polymarket.fetch_markets.return_value = [
        make_polymarket_market(condition_id="p", question="Will BTC reach 100k?", spread=0.03, volume=99999),
    ]
    client._kalshi.get_markets_by_categories.side_effect = RuntimeError("boom")
    res = client.search_all_categories(limit_per_platform=5)
    assert res["crypto"]  # polymarket path still succeeds
    client._kalshi.search_broad.side_effect = RuntimeError("boom")
    res2 = client.search_all_categories(limit_per_platform=5)
    assert res2["crypto"]


def test_get_crypto_and_other_category_methods():
    client = _make_client()
    client._polymarket.get_crypto_markets.return_value = [
        make_polymarket_market(condition_id="p", question="Will BTC reach 100k?", spread=0.03),
    ]
    client._kalshi.get_relevant_markets.return_value = [
        make_kalshi_market(ticker="k", title="Will BTC go up?", volume=99999, yes_bid=0.4, yes_ask=0.45),
    ]
    assert len(client.get_crypto_markets(limit=5)) == 2
    for fn in ("get_sports_markets", "get_politics_markets", "get_entertainment_markets",
               "get_economics_markets", "get_technology_markets"):
        with patch.object(client, "_polymarket_to_unified", return_value=[]) as u:
            getattr(client, fn)(limit=5)
            u.assert_called_once()


def test_format_markets():
    markets = [_pm(question="Will BTC go up?", outcome_prices={"YES": 0.6}, volume=5000,
                   spread=0.05, liquidity_score=0.4)]
    out = UnifiedPredictionMarketClient().format_markets(markets)
    assert "BTC" in out


def test_relayer_keys_and_order_book_passthrough():
    client = _make_client()
    client.get_polymarket_relayer_keys()
    client._polymarket_relayer.list_api_keys.assert_called_once()
    client.get_kalshi_order_book_depth("t")
    client._kalshi.get_order_book.assert_called_once_with("t")
    client.get_polymarket_order_book("tok")
    client._polymarket.get_order_book.assert_called_once_with("tok")


def test_search_kalshi_email_auth_with_term():
    client = _make_client()
    client._kalshi.email = "e@x.com"
    client._kalshi.password = "secret"
    client._kalshi.api_key_id = ""
    client._kalshi.private_key_path = ""

    def fake_search(term="", limit=30):
        assert term == "btc"
        return [make_kalshi_market(ticker="k", title="Will BTC go up?",
                                   volume=99999, yes_bid=0.4, yes_ask=0.45)]
    client._kalshi.search_markets.side_effect = fake_search
    out = client.search_kalshi(term="btc", limit=5)
    assert len(out) == 1
    client._kalshi.search_markets.assert_called_once()


def test_search_kalshi_email_auth_no_term():
    client = _make_client()
    client._kalshi.email = "e@x.com"
    client._kalshi.password = "secret"
    client._kalshi.api_key_id = ""
    client._kalshi.private_key_path = ""
    client._kalshi.get_relevant_markets.return_value = [
        make_kalshi_market(ticker="k", title="Will BTC go up?", volume=99999, yes_bid=0.4, yes_ask=0.45),
    ]
    out = client.search_kalshi(limit=5)
    assert len(out) == 1
    client._kalshi.get_relevant_markets.assert_called_once()


def test_search_polymarket_no_term_detects_from_question():
    client = _make_client()
    client._polymarket.search_markets.return_value = [
        make_polymarket_market(condition_id="c", question="Will bitcoin reach 100k?", spread=0.03),
    ]
    out = client.search_polymarket(limit=5)
    assert len(out) == 1
    assert out[0].category == "crypto"


def test_search_all_kalshi_error_continues():
    client = _make_client()
    client._kalshi.api_key_id = "kid"
    client._kalshi.private_key_path = "/tmp/k.pem"
    client._kalshi.get_relevant_markets.side_effect = RuntimeError("boom")
    client._polymarket.search_markets.return_value = [
        make_polymarket_market(condition_id="p", question="Will ETH reach 5000?", spread=0.03),
    ]
    out = client.search_all(limit=10)
    assert len(out) == 1
    assert out[0].platform == "polymarket"


def test_detect_category_short_keywords():
    # short keywords (<4 chars) use word-boundary matching
    assert UnifiedPredictionMarketClient._detect_category("Will btc go up?") == "crypto"
    assert UnifiedPredictionMarketClient._detect_category("Will doge moon?") == "crypto"
    assert UnifiedPredictionMarketClient._detect_category("Will xrp rise?") == "crypto"
    assert UnifiedPredictionMarketClient._detect_category("Will eth climb?") == "crypto"
    assert UnifiedPredictionMarketClient._detect_category("Will sol pump?") == "crypto"
    # long keyword substring match
    assert UnifiedPredictionMarketClient._detect_category("Will inflation exceed 3%?") == "economics"
    # word boundary: 'pol' must not match 'politics'
    assert UnifiedPredictionMarketClient._detect_category("What is politics?") == "general"


def test_main_cli(monkeypatch, capsys):
    import event_markets.unified_client as uc
    monkeypatch.setattr("sys.argv", ["uc", "--category", "crypto", "--limit", "5"])
    fake = MagicMock()
    fake.get_crypto_markets.return_value = [
        make_polymarket_market(condition_id="c", question="Will BTC reach 100k?", spread=0.03, volume=9999),
    ]
    with patch("event_markets.unified_client.UnifiedPredictionMarketClient", return_value=fake):
        uc.main()
    out = capsys.readouterr().out
    assert "Crypto Markets" in out


def test_main_cli_all(monkeypatch, capsys):
    import event_markets.unified_client as uc
    monkeypatch.setattr("sys.argv", ["uc", "--category", "all"])
    fake = MagicMock()
    fake.search_all_categories.return_value = {
        "crypto": [make_polymarket_market(condition_id="c", question="Will BTC reach 100k?",
                                          spread=0.03, volume=9999)],
        "sports": [],
    }
    with patch("event_markets.unified_client.UnifiedPredictionMarketClient", return_value=fake):
        uc.main()
    assert "CRYPTO" in capsys.readouterr().out


def test_main_cli_unknown_category(monkeypatch, capsys):
    # NOTE: argparse restricts --category to its choices list, so the
    # "Unknown category" branch in main() is unreachable via the CLI.
    # The branch is defensive; we instead assert argparse rejects bad input.
    import event_markets.unified_client as uc
    monkeypatch.setattr("sys.argv", ["uc", "--category", "bogus"])
    with pytest.raises(SystemExit):
        uc.main()


def test_get_crypto_markets_exceptions():
    client = _make_client()
    client._polymarket.get_crypto_markets.side_effect = RuntimeError("boom")
    client._kalshi.get_relevant_markets.side_effect = RuntimeError("boom")
    assert client.get_crypto_markets(limit=5) == []


def test_all_singular_category_methods_raise():
    client = _make_client()
    for fn in ("get_sports_markets", "get_politics_markets", "get_entertainment_markets",
               "get_economics_markets", "get_technology_markets"):
        with patch.object(client, "_polymarket_to_unified", return_value=[]):
            client._polymarket.reset_mock()
            getattr(client._polymarket, fn).side_effect = RuntimeError("boom")
            assert getattr(client, fn)(limit=5) == []


def test_search_all_categories_kalshi_exception():
    client = _make_client()
    client._kalshi.api_key_id = "kid"
    client._kalshi.private_key_path = "/tmp/k.pem"
    client._kalshi.get_markets_by_categories.side_effect = RuntimeError("boom")
    client._polymarket.fetch_markets.return_value = [
        make_polymarket_market(condition_id="p", question="Will BTC reach 100k?",
                               spread=0.03, volume=99999),
    ]
    res = client.search_all_categories(limit_per_platform=5)
    assert res["crypto"]  # polymarket path succeeded despite kalshi failure
