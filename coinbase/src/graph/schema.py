from __future__ import annotations

GRAPH_CONSTRAINTS = [
    "CREATE CONSTRAINT cg_asset_id IF NOT EXISTS FOR (a:Asset) REQUIRE a.cg_id IS UNIQUE",
    "CREATE CONSTRAINT cg_asset_symbol IF NOT EXISTS FOR (a:Asset) REQUIRE a.symbol_key IS UNIQUE",
    "CREATE CONSTRAINT cg_token_key IF NOT EXISTS FOR (t:Token) REQUIRE t.key IS UNIQUE",
    "CREATE CONSTRAINT cg_network_key IF NOT EXISTS FOR (n:Network) REQUIRE n.key IS UNIQUE",
    "CREATE CONSTRAINT cg_category_name IF NOT EXISTS FOR (c:Category) REQUIRE c.name IS UNIQUE",
    "CREATE CONSTRAINT cg_wallet_address IF NOT EXISTS FOR (w:Wallet) REQUIRE w.address IS UNIQUE",
    "CREATE CONSTRAINT cg_project_slug IF NOT EXISTS FOR (p:Project) REQUIRE p.slug IS UNIQUE",
    "CREATE CONSTRAINT cg_tx_hash IF NOT EXISTS FOR (tx:Transaction) REQUIRE tx.hash IS UNIQUE",
]

GRAPH_INDEXES = [
    "CREATE INDEX cg_asset_rank IF NOT EXISTS FOR (a:Asset) ON (a.market_cap_rank)",
    "CREATE INDEX cg_asset_available IF NOT EXISTS FOR (a:Asset) ON (a.available_on_coinbase)",
    "CREATE INDEX cg_token_address IF NOT EXISTS FOR (t:Token) ON (t.address)",
    "CREATE INDEX cg_tx_timestamp IF NOT EXISTS FOR (tx:Transaction) ON (tx.timestamp)",
]


def schema_statements() -> list[str]:
    return [*GRAPH_CONSTRAINTS, *GRAPH_INDEXES]
