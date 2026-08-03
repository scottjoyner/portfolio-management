"""Neo4j Store — persistent graph storage for the Portfolio Optimizer.

Mirrors the StateStore SQLite interface against a Neo4j database,
enabling cross-system analytics (e.g. joining optimizer trades with
graph-alpha-bot market data).

Usage:
    import os

    store = Neo4jStore(
        uri=os.environ.get("NEO4J_URI", "bolt://localhost:7687"),
        user=os.environ.get("NEO4J_USER", "neo4j"),
        password=os.environ["NEO4J_PASSWORD"],
        database=os.environ.get("NEO4J_DATABASE", "trading"),
    )
    store.save_trade({"type": "rebalance", "size_usd": 1000, ...})
    trades = store.load_trades()
"""

import json
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from neo4j import GraphDatabase


class Neo4jStore:
    """Neo4j-backed store matching the StateStore interface."""

    def __init__(
        self,
        uri: str = "bolt://localhost:7687",
        user: str = "neo4j",
        password: str = "",
        database: str = "trading",
    ):
        self._uri = uri
        self._user = user
        self._password = password
        self._database = database
        self._driver: Optional[GraphDatabase.driver] = None
        self._connect()
        self._ensure_database()
        self._init_schema()

    # ── Connection management ──────────────────────────────────────

    def _connect(self):
        self._driver = GraphDatabase.driver(
            self._uri, auth=(self._user, self._password), max_connection_pool_size=10
        )

    def close(self):
        if self._driver:
            self._driver.close()

    def _ensure_database(self):
        try:
            with self._driver.session(database="system") as s:
                s.run(f"CREATE DATABASE {self._database} IF NOT EXISTS").consume()
        except Exception:
            self._database = "neo4j"

    def _session(self):
        return self._driver.session(database=self._database)

    def _init_schema(self):
        with self._session() as s:
            for constraint in [
                "CREATE CONSTRAINT trade_id IF NOT EXISTS FOR (t:Trade) REQUIRE t.id IS UNIQUE",
                "CREATE CONSTRAINT snapshot_id IF NOT EXISTS FOR (s:Snapshot) REQUIRE s.id IS UNIQUE",
                "CREATE CONSTRAINT bt_cache_key IF NOT EXISTS FOR (b:BacktestCache) REQUIRE b.key IS UNIQUE",
                "CREATE CONSTRAINT position_age_currency IF NOT EXISTS FOR (p:PositionAge) REQUIRE p.currency IS UNIQUE",
            ]:
                try:
                    s.run(constraint).consume()
                except Exception:
                    pass

    # ── Trades ────────────────────────────────────────────────────

    def save_trade(self, trade: Dict[str, Any]) -> str:
        trade_id = trade.get("id") or str(uuid.uuid4())
        now = datetime.now(timezone.utc).isoformat()
        payload = {**trade, "id": trade_id, "created_at": trade.get("created_at", now)}
        with self._session() as s:
            s.run(
                "MERGE (t:Trade {id: $id}) SET t += $payload",
                id=trade_id,
                payload=payload,
            ).consume()
        return trade_id

    def load_trades(self, limit: int = 1000) -> List[Dict[str, Any]]:
        with self._session() as s:
            rows = s.run(
                "MATCH (t:Trade) RETURN t ORDER BY t.created_at DESC LIMIT $limit",
                limit=limit,
            )
            return [dict(row["t"]) for row in rows]

    def save_snapshot(self, snapshot: Dict[str, Any]) -> str:
        snapshot_id = snapshot.get("id") or str(uuid.uuid4())
        now = datetime.now(timezone.utc).isoformat()
        payload = {**snapshot, "id": snapshot_id, "created_at": snapshot.get("created_at", now)}
        with self._session() as s:
            s.run(
                "MERGE (snapshot:Snapshot {id: $id}) SET snapshot += $payload",
                id=snapshot_id,
                payload=payload,
            ).consume()
        return snapshot_id

    def load_snapshots(self, limit: int = 1000) -> List[Dict[str, Any]]:
        with self._session() as s:
            rows = s.run(
                "MATCH (snapshot:Snapshot) RETURN snapshot "
                "ORDER BY snapshot.created_at DESC LIMIT $limit",
                limit=limit,
            )
            return [dict(row["snapshot"]) for row in rows]

    def cache_backtest(self, cache_key: str, result: Dict[str, Any]) -> None:
        now = datetime.now(timezone.utc).isoformat()
        with self._session() as s:
            s.run(
                "MERGE (backtest:BacktestCache {key: $key}) "
                "SET backtest.result_json = $result_json, backtest.updated_at = $updated_at",
                key=cache_key,
                result_json=json.dumps(result),
                updated_at=now,
            ).consume()

    def load_cached_backtest(self, cache_key: str) -> Optional[Dict[str, Any]]:
        with self._session() as s:
            row = s.run(
                "MATCH (backtest:BacktestCache {key: $key}) RETURN backtest.result_json AS result_json",
                key=cache_key,
            ).single()
            return json.loads(row["result_json"]) if row and row["result_json"] else None

    def save_position_age(self, currency: str, first_seen_at: str) -> None:
        with self._session() as s:
            s.run(
                "MERGE (position:PositionAge {currency: $currency}) "
                "SET position.first_seen_at = coalesce(position.first_seen_at, $first_seen_at), "
                "position.updated_at = $updated_at",
                currency=currency,
                first_seen_at=first_seen_at,
                updated_at=datetime.now(timezone.utc).isoformat(),
            ).consume()

    def load_position_ages(self) -> Dict[str, str]:
        with self._session() as s:
            rows = s.run("MATCH (position:PositionAge) RETURN position.currency AS currency, position.first_seen_at AS first_seen_at")
            return {row["currency"]: row["first_seen_at"] for row in rows}
