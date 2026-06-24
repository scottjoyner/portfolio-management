"""Canonical Coinbase spot universe — single source of truth for supported trading pairs.

All modules that iterate over Coinbase products should import from here to keep
the universe consistent. Update ONE file when adding/removing coverage.
"""

# ── Full Coinbase spot universe (USD pairs) ─────────────────────────────
# Ordered by approximate market cap / liquidity. Every symbol here is a real,
# tradeable Coinbase Advanced Trading product.
COINBASE_SPOT_PAIRS = [
    # Core — top 10 by market cap (always covered)
    "BTC-USD",
    "ETH-USD",
    "SOL-USD",
    "XRP-USD",
    "ADA-USD",
    "DOGE-USD",
    "AVAX-USD",
    "DOT-USD",
    "LINK-USD",
    "UNI-USD",
    # Large-cap L1 / L2 — significant liquidity, news coverage
    "POL-USD",
    "ATOM-USD",
    "LTC-USD",
    "BCH-USD",
    "NEAR-USD",
    "APT-USD",
    "SUI-USD",
    "ARB-USD",
    "OP-USD",
    "FIL-USD",
    "INJ-USD",
    "SEI-USD",
    "TIA-USD",
    # Mid-cap L1 / infra — growing volume
    "ALGO-USD",
    "XLM-USD",
    "STX-USD",
    "HBAR-USD",
    "ICP-USD",
    "GRT-USD",
    # Meme / hype — high retail volume, strong news signal
    "SHIB-USD",
    "PEPE-USD",
    "BONK-USD",
    "TRUMP-USD",
    "FLOKI-USD",
]

# Base currencies (without -USD suffix) for quick lookups
COINBASE_BASES = {p.replace("-USD", "") for p in COINBASE_SPOT_PAIRS}

# Classification buckets for portfolio allocation
SAFE_BASES = {"BTC", "ETH", "USDC", "USDT", "DAI"}
GROWTH_BASES = {
    "SOL", "XRP", "ADA", "DOGE", "AVAX", "DOT", "LINK", "UNI",
    "POL", "ATOM", "LTC", "BCH", "NEAR", "APT", "SUI", "ARB",
    "OP", "FIL", "INJ", "SEI", "TIA",
}
SPECULATIVE_BASES = {
    "ALGO", "XLM", "STX", "HBAR", "ICP", "GRT",
    "SHIB", "PEPE", "BONK", "TRUMP", "FLOKI",
}

# Map between base currency and USD product ID
def base_to_product(base: str) -> str:
    base = base.upper().replace("-USD", "")
    return f"{base}-USD"

def product_to_base(product_id: str) -> str:
    return product_id.upper().replace("-USD", "").replace("-USDC", "")

# Coinbase WebSocket / feed products (subset of SPOT_PAIRS for live feed)
# Typically the most actively traded — balance coverage vs bandwidth
FEED_PRODUCTS = [
    "BTC-USD", "ETH-USD", "SOL-USD",
    "XRP-USD", "ADA-USD", "DOGE-USD",
    "AVAX-USD", "DOT-USD", "LINK-USD",
    "POL-USD", "ATOM-USD", "LTC-USD",
    "NEAR-USD", "APT-USD", "SUI-USD",
    "SHIB-USD", "PEPE-USD", "TRUMP-USD",
]
