"""Neo4j Store — persistent graph storage for the Portfolio Optimizer.

Mirrors the StateStore SQLite interface against a Neo4j database,
enabling cross-system analytics (e.g. joining optimizer trades with
graph-alpha-bot market data).

Usage:
    store = Neo4jStore(
        uri="bolt://100.64.43.123:7687",
        user="neo4j",
        password="knowledge_graph_2026",
        database="trading",
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

    def save_trade(self, trade: dict):
        ts = trade.get("timestamp", datetime.now(timezone.utc).isoformat())
        q = """
        MERGE (t:Trade {id: $id})
        SET t.timestamp = datetime($timestamp),
            t.type = $type,
            t.side = $side,
            t.currency = $currency,
            t.size_usd = $size_usd,
            t.fee = $fee,
            t.reason = $reason,
            t.order_id = $order_id,
            t.dry_run = $dry_run,
            t.created_at = datetime()
        """
        with self._session() as s:
            s.run(
                q,
                id=trade.get("order_id") or str(uuid.uuid4()),
                timestamp=ts,
                type=trade.get("type", ""),
                side=trade.get("side", ""),
                currency=trade.get("currency", ""),
                size_usd=trade.get("size_usd", 0),
                fee=trade.get("fee", 0),
                reason=trade.get("reason", ""),
                order_id=trade.get("order_id", ""),
                dry_run=bool(trade.get("dry_run")),
            ).consume()

    def load_trades(self, limit: int = 100) -> List[dict]:
        q = """
        MATCH (t:Trade)
        RETURN t.id AS id, t.timestamp AS timestamp, t.type AS type,
               t.side AS side, t.currency AS currency, t.size_usd AS size_usd,
               t.fee AS fee, t.reason AS reason, t.order_id AS order_id,
               t.dry_run AS dry_run
        ORDER BY t.timestamp DESC
        LIMIT $limit
        """
        with self._session() as s:
            result = s.run(q, limit=limit)
            return [
                {
                    "id": r.get("id"),
                    "timestamp": str(r.get("timestamp") or ""),
                    "type": r.get("type"),
                    "side": r.get("side"),
                    "currency": r.get("currency"),
                    "size_usd": r.get("size_usd", 0),
                    "fee": r.get("fee", 0),
                    "reason": r.get("reason"),
                    "order_id": r.get("order_id", ""),
                    "dry_run": 1 if r.get("dry_run") else 0,
                }
                for r in result
            ]

    # ── Snapshots ─────────────────────────────────────────────────

    def save_snapshot(self, state) -> dict:
        holdings_json = json.dumps(
            {
                k: {
                    "currency": v["currency"],
                    "total": v["total"],
                    "price": v["price"],
                    "value": v["value"],
                    "classification": v["classification"],
                    "allocation_pct": v["allocation_pct"],
                }
                for k, v in state.holdings.items()
            }
        )
        sid = str(uuid.uuid4())
        ts = datetime.now(timezone.utc).isoformat()
        q = """
        MERGE (s:Snapshot {id: $id})
        SET s.timestamp = datetime($timestamp),
            s.total_value = $total_value,
            s.holding_count = $holding_count,
            s.usdc_balance = $usdc_balance,
            s.fee_volume_30d = $fee_volume_30d,
            s.fee_tier_min_volume = $fee_tier_min_volume,
            s.fee_tier_maker = $fee_tier_maker,
            s.fee_tier_taker = $fee_tier_taker,
            s.holdings_json = $holdings_json,
            s.created_at = datetime()
        """
        with self._session() as s:
            s.run(
                q,
                id=sid,
                timestamp=ts,
                total_value=round(state.total_value, 2),
                holding_count=len(state.holdings),
                usdc_balance=round(state.usdc_balance, 2),
                fee_volume_30d=round(state.fee_volume_30d, 2),
                fee_tier_min_volume=state.fee_tier[0],
                fee_tier_maker=state.fee_tier[1],
                fee_tier_taker=state.fee_tier[2],
                holdings_json=holdings_json,
            ).consume()
        return {"id": sid, "timestamp": ts}

    def load_snapshots(self, limit: int = 50) -> List[dict]:
        q = """
        MATCH (s:Snapshot)
        RETURN s.id AS id, s.timestamp AS timestamp,
               s.total_value AS total_value, s.holding_count AS holding_count,
               s.usdc_balance AS usdc_balance,
               s.fee_volume_30d AS fee_volume_30d,
               s.fee_tier_min_volume AS fee_tier_min_volume,
               s.fee_tier_maker AS fee_tier_maker,
               s.fee_tier_taker AS fee_tier_taker,
               s.holdings_json AS holdings_json
        ORDER BY s.timestamp DESC
        LIMIT $limit
        """
        with self._session() as s:
            result = s.run(q, limit=limit)
            out = []
            for r in result:
                d = {
                    "id": r.get("id"),
                    "timestamp": str(r.get("timestamp") or ""),
                    "total_value": r.get("total_value", 0),
                    "holding_count": r.get("holding_count", 0),
                    "usdc_balance": r.get("usdc_balance", 0),
                    "fee_volume_30d": r.get("fee_volume_30d", 0),
                    "fee_tier_min_volume": r.get("fee_tier_min_volume", 0),
                    "fee_tier_maker": r.get("fee_tier_maker", 0),
                    "fee_tier_taker": r.get("fee_tier_taker", 0),
                }
                j = r.get("holdings_json")
                d["holdings"] = json.loads(j) if j else {}
                out.append(d)
            return out

    # ── Backtest cache ────────────────────────────────────────────

    def save_bt_cache(self, key: str, verdict) -> None:
        q = """
        MERGE (b:BacktestCache {key: $key})
        SET b.created_at = $created_at,
            b.strategy = $strategy,
            b.currency = $currency,
            b.total_trades = $total_trades,
            b.winning_trades = $winning_trades,
            b.losing_trades = $losing_trades,
            b.win_rate = $win_rate,
            b.total_return_pct = $total_return_pct,
            b.sharpe_ratio = $sharpe_ratio,
            b.profit_factor = $profit_factor,
            b.max_drawdown_pct = $max_drawdown_pct,
            b.regime = $regime,
            b.passed = $passed,
            b.reason = $reason
        """
        import time as _time
        with self._session() as s:
            s.run(
                q,
                key=key,
                created_at=_time.time(),
                strategy=verdict.strategy,
                currency=verdict.currency,
                total_trades=verdict.total_trades,
                winning_trades=verdict.winning_trades,
                losing_trades=verdict.losing_trades,
                win_rate=verdict.win_rate,
                total_return_pct=verdict.total_return_pct,
                sharpe_ratio=verdict.sharpe_ratio,
                profit_factor=verdict.profit_factor,
                max_drawdown_pct=verdict.max_drawdown_pct,
                regime=verdict.regime,
                passed=verdict.passed,
                reason=verdict.reason,
            ).consume()

    def load_bt_cache(self, ttl: float = 3600) -> Dict[str, dict]:
        import time as _time
        cutoff = _time.time() - ttl
        q = """
        MATCH (b:BacktestCache)
        WHERE b.created_at > $cutoff
        RETURN b.key AS key, b.strategy AS strategy,
               b.currency AS currency, b.total_trades AS total_trades,
               b.winning_trades AS winning_trades,
               b.losing_trades AS losing_trades,
               b.win_rate AS win_rate,
               b.total_return_pct AS total_return_pct,
               b.sharpe_ratio AS sharpe_ratio,
               b.profit_factor AS profit_factor,
               b.max_drawdown_pct AS max_drawdown_pct,
               b.regime AS regime, b.passed AS passed, b.reason AS reason
        """
        with self._session() as s:
            result = s.run(q, cutoff=cutoff)
            out = {}
            for r in result:
                out[r["key"]] = {
                    "strategy": r.get("strategy"),
                    "currency": r.get("currency"),
                    "total_trades": r.get("total_trades", 0),
                    "winning_trades": r.get("winning_trades", 0),
                    "losing_trades": r.get("losing_trades", 0),
                    "win_rate": r.get("win_rate", 0),
                    "total_return_pct": r.get("total_return_pct", 0),
                    "sharpe_ratio": r.get("sharpe_ratio", 0),
                    "profit_factor": r.get("profit_factor", 0),
                    "max_drawdown_pct": r.get("max_drawdown_pct", 0),
                    "regime": r.get("regime", ""),
                    "passed": r.get("passed", False),
                    "reason": r.get("reason", ""),
                }
            return out

    def prune_bt_cache(self, ttl: float = 86400):
        import time as _time
        cutoff = _time.time() - ttl
        q = "MATCH (b:BacktestCache) WHERE b.created_at < $cutoff DELETE b"
        with self._session() as s:
            s.run(q, cutoff=cutoff).consume()

    # ── Position ages ─────────────────────────────────────────────

    def save_position_ages(self, ages: Dict[str, int]) -> None:
        with self._session() as s:
            s.run("MATCH (p:PositionAge) DELETE p").consume()
            for currency, age in ages.items():
                s.run(
                    """
                    MERGE (p:PositionAge {currency: $currency})
                    SET p.age = $age
                    """,
                    currency=currency,
                    age=age,
                ).consume()

    def load_position_ages(self) -> Dict[str, int]:
        q = "MATCH (p:PositionAge) RETURN p.currency AS currency, p.age AS age"
        with self._session() as s:
            return {r["currency"]: r["age"] for r in s.run(q)}

    # ── Meta (key-value) ──────────────────────────────────────────

    def get_meta(self, key: str, default: Any = None) -> Optional[str]:
        q = """
        MATCH (m:Meta {key: $key})
        RETURN m.value AS value
        """
        with self._session() as s:
            r = s.run(q, key=key).single()
            return r["value"] if r else default

    def set_meta(self, key: str, value: str) -> None:
        q = """
        MERGE (m:Meta {key: $key})
        SET m.value = $value
        """
        with self._session() as s:
            s.run(q, key=key, value=value).consume()

    # ── Utility ───────────────────────────────────────────────────

    def stats(self) -> dict:
        with self._session() as s:
            trade_count = s.run("MATCH (t:Trade) RETURN count(t) AS c").single()["c"]
            snap_count = s.run("MATCH (s:Snapshot) RETURN count(s) AS c").single()["c"]
            cache_count = s.run("MATCH (b:BacktestCache) RETURN count(b) AS c").single()["c"]
            return {
                "trades": trade_count,
                "snapshots": snap_count,
                "bt_cache_entries": cache_count,
                "uri": self._uri,
                "database": self._database,
            }

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()
