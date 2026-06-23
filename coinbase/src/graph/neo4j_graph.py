from __future__ import annotations

import os
from contextlib import contextmanager
from typing import Any, Iterable, Iterator

try:
    from neo4j import GraphDatabase
except Exception:  # pragma: no cover - optional dependency path
    GraphDatabase = None

from .models import GraphAsset, GraphAssetSignal, TokenContract, WalletObservation
from .schema import schema_statements


class CryptoGraphStore:
    """Modern Neo4j store for crypto asset, token, wallet, and project research."""

    def __init__(
        self,
        uri: str | None = None,
        user: str | None = None,
        password: str | None = None,
        database: str | None = None,
    ):
        if GraphDatabase is None:
            raise RuntimeError("neo4j package is not installed")
        self.uri = uri or os.getenv("NEO4J_URI", "bolt://localhost:7687")
        self.user = user or os.getenv("NEO4J_USER", "neo4j")
        self.password = password or os.getenv("NEO4J_PASSWORD", "please_change_me")
        self.database = database or os.getenv("NEO4J_DATABASE", "neo4j")
        self._driver = GraphDatabase.driver(self.uri, auth=(self.user, self.password))

    def close(self) -> None:
        self._driver.close()

    @contextmanager
    def session(self) -> Iterator[Any]:
        with self._driver.session(database=self.database) as session:
            yield session

    def apply_schema(self) -> None:
        with self.session() as session:
            for statement in schema_statements():
                session.run(statement)

    def upsert_asset(self, asset: GraphAsset) -> None:
        q = """
        MERGE (a:Asset {cg_id: $cg_id})
        SET a.symbol = $symbol,
            a.symbol_key = $symbol_key,
            a.name = $name,
            a.product_id = $product_id,
            a.market_cap_rank = $market_cap_rank,
            a.market_cap = $market_cap,
            a.available_on_coinbase = $available_on_coinbase,
            a.updated_at = datetime()
        WITH a
        UNWIND $categories AS category
        MERGE (c:Category {name: category})
        MERGE (a)-[:HAS_CATEGORY]->(c)
        """
        with self.session() as session:
            session.run(q, **asset.to_dict())

    def upsert_token(self, token: TokenContract) -> None:
        q = """
        MERGE (a:Asset {cg_id: $asset_id})
        MERGE (n:Network {key: $network})
        SET n.name = coalesce(n.name, $network)
        MERGE (t:Token {key: $key})
        SET t.address = toLower($address),
            t.symbol = $symbol,
            t.name = $name,
            t.decimals = $decimals,
            t.updated_at = datetime()
        MERGE (t)-[:IMPLEMENTS]->(a)
        MERGE (t)-[:ON_NETWORK]->(n)
        """
        with self.session() as session:
            session.run(q, **token.to_dict())

    def upsert_wallet_observation(self, wallet: WalletObservation) -> None:
        q = """
        MERGE (w:Wallet {address: toLower($address)})
        SET w.source = $source,
            w.first_seen = coalesce(w.first_seen, $first_seen),
            w.labels = $labels,
            w.updated_at = datetime()
        WITH w
        FOREACH (_ IN CASE WHEN $project_slug <> '' THEN [1] ELSE [] END |
          MERGE (p:Project {slug: $project_slug})
          MERGE (w)-[:MEMBER_OF]->(p)
        )
        """
        with self.session() as session:
            session.run(q, **wallet.to_dict())

    def upsert_assets(self, assets: Iterable[GraphAsset]) -> int:
        count = 0
        for asset in assets:
            self.upsert_asset(asset)
            count += 1
        return count

    def upsert_tokens(self, tokens: Iterable[TokenContract]) -> int:
        count = 0
        for token in tokens:
            self.upsert_token(token)
            count += 1
        return count

    def asset_signal(self, product_id: str) -> GraphAssetSignal:
        symbol = product_id.split("-")[0].upper()
        q = """
        MATCH (a:Asset)
        WHERE a.product_id = $product_id OR a.symbol_key = $symbol
        OPTIONAL MATCH (a)-[:HAS_CATEGORY]->(c:Category)
        OPTIONAL MATCH (t:Token)-[:IMPLEMENTS]->(a)
        OPTIONAL MATCH (t)-[:ON_NETWORK]->(n:Network)
        OPTIONAL MATCH (w:Wallet)-[:HOLDS]->(t)
        OPTIONAL MATCH (w)-[:SENT|RECEIVED]-(tx:Transaction)
        RETURN a.symbol_key AS symbol,
               a.product_id AS product_id,
               a.market_cap_rank AS market_cap_rank,
               coalesce(a.available_on_coinbase, false) AS available_on_coinbase,
               count(DISTINCT c) AS category_count,
               count(DISTINCT n) AS network_count,
               count(DISTINCT t) AS token_count,
               count(DISTINCT w) AS wallet_count,
               count(DISTINCT tx) AS tx_count
        LIMIT 1
        """
        with self.session() as session:
            row = session.run(q, product_id=product_id, symbol=symbol).single()
        if row is None:
            return GraphAssetSignal(product_id=product_id, symbol=symbol, graph_score=0.0, reasons=["asset not found in graph"])
        data = row.data()
        score = _score_graph_features(data)
        reasons = _signal_reasons(data)
        return GraphAssetSignal(
            product_id=data.get("product_id") or product_id,
            symbol=data.get("symbol") or symbol,
            graph_score=score,
            category_count=int(data.get("category_count") or 0),
            network_count=int(data.get("network_count") or 0),
            token_count=int(data.get("token_count") or 0),
            wallet_count=int(data.get("wallet_count") or 0),
            tx_count=int(data.get("tx_count") or 0),
            market_cap_rank=data.get("market_cap_rank"),
            available_on_coinbase=bool(data.get("available_on_coinbase")),
            reasons=reasons,
        )

    def top_graph_assets(self, limit: int = 25, only_coinbase: bool = True) -> list[GraphAssetSignal]:
        q = """
        MATCH (a:Asset)
        WHERE ($only_coinbase = false OR coalesce(a.available_on_coinbase, false) = true)
        OPTIONAL MATCH (a)-[:HAS_CATEGORY]->(c:Category)
        OPTIONAL MATCH (t:Token)-[:IMPLEMENTS]->(a)
        OPTIONAL MATCH (t)-[:ON_NETWORK]->(n:Network)
        RETURN a.symbol_key AS symbol,
               a.product_id AS product_id,
               a.market_cap_rank AS market_cap_rank,
               coalesce(a.available_on_coinbase, false) AS available_on_coinbase,
               count(DISTINCT c) AS category_count,
               count(DISTINCT n) AS network_count,
               count(DISTINCT t) AS token_count,
               0 AS wallet_count,
               0 AS tx_count
        ORDER BY coalesce(a.market_cap_rank, 999999) ASC
        LIMIT $limit
        """
        out: list[GraphAssetSignal] = []
        with self.session() as session:
            for row in session.run(q, only_coinbase=only_coinbase, limit=int(limit)):
                data = row.data()
                out.append(GraphAssetSignal(
                    product_id=data.get("product_id") or f"{data.get('symbol')}-USD",
                    symbol=data.get("symbol") or "",
                    graph_score=_score_graph_features(data),
                    category_count=int(data.get("category_count") or 0),
                    network_count=int(data.get("network_count") or 0),
                    token_count=int(data.get("token_count") or 0),
                    wallet_count=0,
                    tx_count=0,
                    market_cap_rank=data.get("market_cap_rank"),
                    available_on_coinbase=bool(data.get("available_on_coinbase")),
                    reasons=_signal_reasons(data),
                ))
        return sorted(out, key=lambda x: x.graph_score, reverse=True)


def _score_graph_features(data: dict[str, Any]) -> float:
    category_count = float(data.get("category_count") or 0)
    network_count = float(data.get("network_count") or 0)
    token_count = float(data.get("token_count") or 0)
    wallet_count = float(data.get("wallet_count") or 0)
    tx_count = float(data.get("tx_count") or 0)
    rank = data.get("market_cap_rank") or 999999
    rank_score = max(0.0, 1.0 - min(float(rank), 500.0) / 500.0)
    raw = (
        0.30 * rank_score
        + 0.20 * min(category_count / 6.0, 1.0)
        + 0.20 * min(network_count / 8.0, 1.0)
        + 0.15 * min(token_count / 10.0, 1.0)
        + 0.10 * min(wallet_count / 100.0, 1.0)
        + 0.05 * min(tx_count / 1000.0, 1.0)
    )
    return round(max(0.0, min(1.0, raw)), 6)


def _signal_reasons(data: dict[str, Any]) -> list[str]:
    reasons: list[str] = []
    if data.get("available_on_coinbase"):
        reasons.append("available_on_coinbase")
    if (data.get("market_cap_rank") or 999999) <= 100:
        reasons.append("top_100_market_cap")
    if (data.get("network_count") or 0) >= 2:
        reasons.append("multi_network_presence")
    if (data.get("category_count") or 0) >= 2:
        reasons.append("multi_category_asset")
    if not reasons:
        reasons.append("limited_graph_evidence")
    return reasons
