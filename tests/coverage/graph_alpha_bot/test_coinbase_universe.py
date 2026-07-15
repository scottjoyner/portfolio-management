from graph_alpha_bot.app.strategies.coinbase_universe import (
    COINBASE_SPOT_PAIRS,
    COINBASE_BASES,
    SAFE_BASES,
    GROWTH_BASES,
    SPECULATIVE_BASES,
    base_to_product,
    product_to_base,
    FEED_PRODUCTS,
)


def test_constants_count():
    assert len(COINBASE_SPOT_PAIRS) == 34
    assert len(COINBASE_BASES) == 34
    assert len(FEED_PRODUCTS) == 18


def test_pairs_are_unique_usd():
    assert len(set(COINBASE_SPOT_PAIRS)) == len(COINBASE_SPOT_PAIRS)
    assert all(p.endswith("-USD") for p in COINBASE_SPOT_PAIRS)


def test_classification_covers_universe():
    covered = GROWTH_BASES | SPECULATIVE_BASES | (SAFE_BASES & COINBASE_BASES)
    assert covered == COINBASE_BASES
    assert len(SAFE_BASES & GROWTH_BASES) == 0
    assert len(GROWTH_BASES & SPECULATIVE_BASES) == 0
    assert len(SAFE_BASES & SPECULATIVE_BASES) == 0


def test_base_to_product():
    assert base_to_product("btc") == "BTC-USD"
    assert base_to_product("BTC-USD") == "BTC-USD"
    assert base_to_product("eth") == "ETH-USD"


def test_product_to_base():
    assert product_to_base("BTC-USD") == "BTC"
    assert product_to_base("btc-usdc") == "BTC"
