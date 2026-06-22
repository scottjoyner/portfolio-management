#!/usr/bin/env python3
"""
State Store — persistent SQLite storage for the Portfolio Optimizer.

Survives restarts: trades, portfolio snapshots, backtest cache,
fee tier history, and position ages are all saved to disk.

Usage:
    store = StateStore("optimizer_state.db")
    store.save_trade({"type": "rebalance", "size_usd": 1000, ...})
    trades = store.load_trades()
    snapshots = store.load_snapshots(limit=10)
"""

import json
import sqlite3
import threading
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional


class StateStore:
    """Thread-safe SQLite store for optimizer persistence."""

    def __init__(self, db_path: str = "optimizer_state.db"):
        self._db_path = db_path
        self._lock = threading.Lock()
        self._init_schema()

    def _conn(self):
        conn = sqlite3.connect(self._db_path)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        return conn

    def _init_schema(self):
        with self._lock:
            conn = self._conn()
            conn.executescript("""
                CREATE TABLE IF NOT EXISTS trades (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp TEXT NOT NULL,
                    type TEXT NOT NULL,
                    side TEXT NOT NULL,
                    currency TEXT NOT NULL,
                    size_usd REAL NOT NULL,
                    fee REAL DEFAULT 0,
                    symbol TEXT DEFAULT '',
                    price REAL DEFAULT 0,
                    quantity REAL DEFAULT 0,
                    strategy TEXT DEFAULT '',
                    pnl_usd REAL DEFAULT 0,
                    reason TEXT,
                    order_id TEXT DEFAULT '',
                    dry_run INTEGER DEFAULT 1
                );
                CREATE TABLE IF NOT EXISTS snapshots (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp TEXT NOT NULL,
                    total_value REAL NOT NULL,
                    holding_count INTEGER NOT NULL,
                    usdc_balance REAL NOT NULL,
                    fee_volume_30d REAL NOT NULL,
                    fee_tier_min_volume REAL NOT NULL,
                    fee_tier_maker REAL NOT NULL,
                    fee_tier_taker REAL NOT NULL,
                    holdings_json TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS bt_cache (
                    key TEXT PRIMARY KEY,
                    verdict_json TEXT NOT NULL,
                    created_at REAL NOT NULL
                );
                CREATE TABLE IF NOT EXISTS position_ages (
                    currency TEXT PRIMARY KEY,
                    age INTEGER NOT NULL DEFAULT 0
                );
                CREATE TABLE IF NOT EXISTS meta (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_trades_timestamp ON trades(timestamp);
                CREATE INDEX IF NOT EXISTS idx_snapshots_timestamp ON snapshots(timestamp);
            """)
            for col_def in [
                ("symbol", "TEXT DEFAULT ''"),
                ("price", "REAL DEFAULT 0"),
                ("quantity", "REAL DEFAULT 0"),
                ("strategy", "TEXT DEFAULT ''"),
                ("pnl_usd", "REAL DEFAULT 0"),
            ]:
                try:
                    conn.execute(f"ALTER TABLE trades ADD COLUMN {col_def[0]} {col_def[1]}")
                except sqlite3.OperationalError:
                    pass
            conn.commit()

    # ── Trades ────────────────────────────────────────────────────

    def save_trade(self, trade: dict):
        with self._lock:
            conn = self._conn()
            conn.execute(
                """INSERT INTO trades (timestamp, type, side, currency, size_usd, fee, symbol, price, quantity, strategy, pnl_usd, reason, order_id, dry_run)
                   VALUES (:timestamp, :type, :side, :currency, :size_usd, :fee, :symbol, :price, :quantity, :strategy, :pnl_usd, :reason, :order_id, :dry_run)""",
                {
                    "timestamp": trade.get("timestamp", datetime.now(timezone.utc).isoformat()),
                    "type": trade.get("type", trade.get("action", "")),
                    "side": trade.get("side", trade.get("action", "")),
                    "currency": trade.get("currency", trade.get("symbol", "")),
                    "size_usd": trade.get("size_usd", 0),
                    "fee": trade.get("fee", 0),
                    "symbol": trade.get("symbol", trade.get("currency", "")),
                    "price": trade.get("price", 0),
                    "quantity": trade.get("quantity", 0),
                    "strategy": trade.get("strategy", trade.get("type", trade.get("action", ""))),
                    "pnl_usd": trade.get("pnl_usd", 0),
                    "reason": trade.get("reason", ""),
                    "order_id": trade.get("order_id", ""),
                    "dry_run": 1 if trade.get("dry_run") else 0,
                },
            )
            conn.commit()

    def load_trades(self, limit: int = 100) -> List[dict]:
        with self._lock:
            conn = self._conn()
            rows = conn.execute(
                "SELECT * FROM trades ORDER BY id DESC LIMIT ?", (limit,)
            ).fetchall()
            return [dict(r) for r in rows]

    # ── Snapshots ─────────────────────────────────────────────────

    def save_snapshot(self, state) -> dict:
        """Save a PortfolioState as a snapshot. Returns the row id."""
        holdings_json = json.dumps({
            k: {
                "currency": v["currency"],
                "total": v["total"],
                "price": v["price"],
                "value": v["value"],
                "classification": v["classification"],
                "allocation_pct": v["allocation_pct"],
            }
            for k, v in state.holdings.items()
        })
        ts = datetime.now(timezone.utc).isoformat()
        with self._lock:
            conn = self._conn()
            cur = conn.execute(
                """INSERT INTO snapshots
                   (timestamp, total_value, holding_count, usdc_balance,
                    fee_volume_30d, fee_tier_min_volume, fee_tier_maker, fee_tier_taker,
                    holdings_json)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    ts,
                    round(state.total_value, 2),
                    len(state.holdings),
                    round(state.usdc_balance, 2),
                    round(state.fee_volume_30d, 2),
                    state.fee_tier[0],
                    state.fee_tier[1],
                    state.fee_tier[2],
                    holdings_json,
                ),
            )
            conn.commit()
            return {"id": cur.lastrowid, "timestamp": ts}

    def load_snapshots(self, limit: int = 50) -> List[dict]:
        with self._lock:
            conn = self._conn()
            rows = conn.execute(
                "SELECT * FROM snapshots ORDER BY id DESC LIMIT ?", (limit,)
            ).fetchall()
            result = []
            for r in rows:
                d = dict(r)
                d["holdings"] = json.loads(d.pop("holdings_json"))
                result.append(d)
            return result

    # ── Backtest cache ────────────────────────────────────────────

    def save_bt_cache(self, key: str, verdict) -> None:
        with self._lock:
            conn = self._conn()
            conn.execute(
                """INSERT OR REPLACE INTO bt_cache (key, verdict_json, created_at)
                   VALUES (?, ?, ?)""",
                (key, json.dumps({
                    "strategy": verdict.strategy,
                    "currency": verdict.currency,
                    "total_trades": verdict.total_trades,
                    "winning_trades": verdict.winning_trades,
                    "losing_trades": verdict.losing_trades,
                    "win_rate": verdict.win_rate,
                    "total_return_pct": verdict.total_return_pct,
                    "sharpe_ratio": verdict.sharpe_ratio,
                    "profit_factor": verdict.profit_factor,
                    "max_drawdown_pct": verdict.max_drawdown_pct,
                    "regime": verdict.regime,
                    "passed": verdict.passed,
                    "reason": verdict.reason,
                }), time.time()),
            )
            conn.commit()

    def load_bt_cache(self, ttl: float = 3600) -> Dict[str, dict]:
        cutoff = time.time() - ttl
        with self._lock:
            conn = self._conn()
            rows = conn.execute(
                "SELECT key, verdict_json FROM bt_cache WHERE created_at > ?",
                (cutoff,),
            ).fetchall()
            return {r["key"]: json.loads(r["verdict_json"]) for r in rows}

    def prune_bt_cache(self, ttl: float = 86400):
        cutoff = time.time() - ttl
        with self._lock:
            conn = self._conn()
            conn.execute("DELETE FROM bt_cache WHERE created_at < ?", (cutoff,))
            conn.commit()

    # ── Position ages ─────────────────────────────────────────────

    def save_position_ages(self, ages: Dict[str, int]) -> None:
        with self._lock:
            conn = self._conn()
            conn.execute("DELETE FROM position_ages")
            for currency, age in ages.items():
                conn.execute(
                    "INSERT INTO position_ages (currency, age) VALUES (?, ?)",
                    (currency, age),
                )
            conn.commit()

    def load_position_ages(self) -> Dict[str, int]:
        with self._lock:
            conn = self._conn()
            rows = conn.execute("SELECT currency, age FROM position_ages").fetchall()
            return {r["currency"]: r["age"] for r in rows}

    # ── Meta (key-value) ──────────────────────────────────────────

    def get_meta(self, key: str, default: Any = None) -> Optional[str]:
        with self._lock:
            conn = self._conn()
            row = conn.execute("SELECT value FROM meta WHERE key = ?", (key,)).fetchone()
            return row["value"] if row else default

    def set_meta(self, key: str, value: str) -> None:
        with self._lock:
            conn = self._conn()
            conn.execute(
                "INSERT OR REPLACE INTO meta (key, value) VALUES (?, ?)",
                (key, value),
            )
            conn.commit()

    # ── Utility ────────────────────────────────────────────────────

    def stats(self) -> dict:
        with self._lock:
            conn = self._conn()
            trade_count = conn.execute("SELECT COUNT(*) as c FROM trades").fetchone()["c"]
            snapshot_count = conn.execute("SELECT COUNT(*) as c FROM snapshots").fetchone()["c"]
            cache_count = conn.execute("SELECT COUNT(*) as c FROM bt_cache").fetchone()["c"]
            return {
                "trades": trade_count,
                "snapshots": snapshot_count,
                "bt_cache_entries": cache_count,
                "db_path": self._db_path,
            }
