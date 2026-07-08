"""
Strategy Performance Registry - persists backtest results to SQLite.
Enables dynamic strategy selection based on historical performance.
"""

import sqlite3
import json
import time
import logging
import threading
from dataclasses import dataclass, asdict
from typing import Dict, List, Optional, Any
from pathlib import Path

log = logging.getLogger(__name__)

_DB_PATH = Path("data/strategy_perf.db")
_DB_LOCK = threading.Lock()


@dataclass
class StrategyPerf:
    """Per-strategy per-product performance record."""
    strategy_name: str
    product_id: str
    asset_class: str  # "safe", "growth", "speculative"
    
    # Backtest metrics
    win_rate: float = 0.0
    sharpe_ratio: float = 0.0
    profit_factor: float = 0.0
    total_trades: int = 0
    total_return_pct: float = 0.0
    max_drawdown_pct: float = 0.0
    avg_holding_hours: float = 0.0
    
    # Calibration
    calibration_slope: float = 1.0
    calibration_intercept: float = 0.0
    calibration_samples: int = 0
    
    # Live tracking
    live_trades: int = 0
    live_win_rate: float = 0.0
    live_pnl: float = 0.0
    
    # Metadata
    last_backtest_ts: float = 0.0
    last_live_update_ts: float = 0.0
    is_active: bool = True
    min_trades_for_active: int = 20
    
    @property
    def is_ready_for_live(self) -> bool:
        return (self.total_trades >= self.min_trades_for_active and 
                self.win_rate >= 0.45 and 
                self.sharpe_ratio >= 0.5 and 
                self.profit_factor >= 1.1)
    
    @property
    def quality_score(self) -> float:
        """Composite quality score for ranking strategies."""
        if self.total_trades < 5:
            return 0.0
        wr_score = min(self.win_rate / 0.6, 1.0) * 0.4
        sharpe_score = min(max(self.sharpe_ratio, 0) / 1.5, 1.0) * 0.3
        pf_score = min(max(self.profit_factor, 0) / 2.0, 1.0) * 0.2
        trade_score = min(self.total_trades / 100, 1.0) * 0.1
        return wr_score + sharpe_score + pf_score + trade_score


class StrategyRegistry:
    """Thread-safe strategy performance registry backed by SQLite."""
    
    def __init__(self, db_path: Path = _DB_PATH):
        self.db_path = db_path
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_db()
        self._cache: Dict[str, StrategyPerf] = {}
        self._cache_lock = threading.Lock()
        self._load_cache()
    
    def _init_db(self):
        with _DB_LOCK:
            conn = sqlite3.connect(self.db_path)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS strategy_perf (
                    strategy_name TEXT NOT NULL,
                    product_id TEXT NOT NULL,
                    asset_class TEXT NOT NULL,
                    
                    win_rate REAL DEFAULT 0,
                    sharpe_ratio REAL DEFAULT 0,
                    profit_factor REAL DEFAULT 0,
                    total_trades INTEGER DEFAULT 0,
                    total_return_pct REAL DEFAULT 0,
                    max_drawdown_pct REAL DEFAULT 0,
                    avg_holding_hours REAL DEFAULT 0,
                    
                    calibration_slope REAL DEFAULT 1,
                    calibration_intercept REAL DEFAULT 0,
                    calibration_samples INTEGER DEFAULT 0,
                    
                    live_trades INTEGER DEFAULT 0,
                    live_win_rate REAL DEFAULT 0,
                    live_pnl REAL DEFAULT 0,
                    
                    last_backtest_ts REAL DEFAULT 0,
                    last_live_update_ts REAL DEFAULT 0,
                    is_active INTEGER DEFAULT 1,
                    min_trades_for_active INTEGER DEFAULT 20,
                    
                    PRIMARY KEY (strategy_name, product_id)
                )
            """)
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_strategy_perf_asset 
                ON strategy_perf(asset_class, is_active, win_rate DESC)
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS backtest_history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    strategy_name TEXT NOT NULL,
                    product_id TEXT NOT NULL,
                    timestamp REAL NOT NULL,
                    win_rate REAL,
                    sharpe_ratio REAL,
                    profit_factor REAL,
                    total_trades INTEGER,
                    total_return_pct REAL,
                    max_drawdown_pct REAL,
                    passed INTEGER DEFAULT 0,
                    raw_verdict TEXT
                )
            """)
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_backtest_history_strat_prod
                ON backtest_history(strategy_name, product_id, timestamp DESC)
            """)
            conn.commit()
            conn.close()
    
    def _load_cache(self):
        with _DB_LOCK:
            conn = sqlite3.connect(self.db_path)
            conn.row_factory = sqlite3.Row
            rows = conn.execute("SELECT * FROM strategy_perf WHERE is_active=1").fetchall()
            conn.close()
        
        with self._cache_lock:
            for row in rows:
                perf = StrategyPerf(
                    strategy_name=row["strategy_name"],
                    product_id=row["product_id"],
                    asset_class=row["asset_class"],
                    win_rate=row["win_rate"],
                    sharpe_ratio=row["sharpe_ratio"],
                    profit_factor=row["profit_factor"],
                    total_trades=row["total_trades"],
                    total_return_pct=row["total_return_pct"],
                    max_drawdown_pct=row["max_drawdown_pct"],
                    avg_holding_hours=row["avg_holding_hours"],
                    calibration_slope=row["calibration_slope"],
                    calibration_intercept=row["calibration_intercept"],
                    calibration_samples=row["calibration_samples"],
                    live_trades=row["live_trades"],
                    live_win_rate=row["live_win_rate"],
                    live_pnl=row["live_pnl"],
                    last_backtest_ts=row["last_backtest_ts"],
                    last_live_update_ts=row["last_live_update_ts"],
                    is_active=bool(row["is_active"]),
                    min_trades_for_active=row["min_trades_for_active"],
                )
                self._cache[f"{perf.strategy_name}/{perf.product_id}"] = perf
    
    def get(self, strategy_name: str, product_id: str) -> Optional[StrategyPerf]:
        with self._cache_lock:
            return self._cache.get(f"{strategy_name}/{product_id}")
    
    def get_top_strategies(self, asset_class: str, limit: int = 10) -> List[StrategyPerf]:
        """Get top-K strategies for an asset class by quality score."""
        with self._cache_lock:
            candidates = [p for p in self._cache.values() 
                         if p.asset_class == asset_class and p.is_ready_for_live]
            candidates.sort(key=lambda x: x.quality_score, reverse=True)
            return candidates[:limit]
    
    def update_backtest(self, perf: StrategyPerf) -> None:
        """Update or insert backtest results."""
        key = f"{perf.strategy_name}/{perf.product_id}"
        
        with _DB_LOCK:
            conn = sqlite3.connect(self.db_path)
            conn.execute("""
                INSERT INTO strategy_perf (
                    strategy_name, product_id, asset_class,
                    win_rate, sharpe_ratio, profit_factor, total_trades,
                    total_return_pct, max_drawdown_pct, avg_holding_hours,
                    calibration_slope, calibration_intercept, calibration_samples,
                    last_backtest_ts, is_active, min_trades_for_active
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(strategy_name, product_id) DO UPDATE SET
                    asset_class=excluded.asset_class,
                    win_rate=excluded.win_rate,
                    sharpe_ratio=excluded.sharpe_ratio,
                    profit_factor=excluded.profit_factor,
                    total_trades=excluded.total_trades,
                    total_return_pct=excluded.total_return_pct,
                    max_drawdown_pct=excluded.max_drawdown_pct,
                    avg_holding_hours=excluded.avg_holding_hours,
                    calibration_slope=excluded.calibration_slope,
                    calibration_intercept=excluded.calibration_intercept,
                    calibration_samples=excluded.calibration_samples,
                    last_backtest_ts=excluded.last_backtest_ts,
                    is_active=excluded.is_active,
                    min_trades_for_active=excluded.min_trades_for_active
            """, (
                perf.strategy_name, perf.product_id, perf.asset_class,
                perf.win_rate, perf.sharpe_ratio, perf.profit_factor, perf.total_trades,
                perf.total_return_pct, perf.max_drawdown_pct, perf.avg_holding_hours,
                perf.calibration_slope, perf.calibration_intercept, perf.calibration_samples,
                perf.last_backtest_ts, int(perf.is_active), perf.min_trades_for_active,
            ))
            
            # Also log to history
            conn.execute("""
                INSERT INTO backtest_history (
                    strategy_name, product_id, timestamp,
                    win_rate, sharpe_ratio, profit_factor, total_trades,
                    total_return_pct, max_drawdown_pct, passed
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                perf.strategy_name, perf.product_id, time.time(),
                perf.win_rate, perf.sharpe_ratio, perf.profit_factor, perf.total_trades,
                perf.total_return_pct, perf.max_drawdown_pct, int(perf.is_ready_for_live)
            ))
            
            conn.commit()
            conn.close()
        
        with self._cache_lock:
            self._cache[key] = perf
    
    def record_live_trade(self, strategy_name: str, product_id: str, pnl: float, is_win: bool) -> None:
        """Record a live trade outcome for online learning."""
        key = f"{strategy_name}/{product_id}"
        
        with self._cache_lock:
            perf = self._cache.get(key)
            if not perf:
                return
            
            perf.live_trades += 1
            perf.live_win_rate = (perf.live_win_rate * (perf.live_trades - 1) + (1.0 if is_win else 0.0)) / perf.live_trades
            perf.live_pnl += pnl
            perf.last_live_update_ts = time.time()
            
            # Update calibration using Platt scaling approximation
            # Simple online update: track average confidence vs outcome
            # This is a simplified version - full Platt would need a batch fit
        
        with _DB_LOCK:
            conn = sqlite3.connect(self.db_path)
            conn.execute("""
                UPDATE strategy_perf SET
                    live_trades=?, live_win_rate=?, live_pnl=?, last_live_update_ts=?
                WHERE strategy_name=? AND product_id=?
            """, (perf.live_trades, perf.live_win_rate, perf.live_pnl, perf.last_live_update_ts,
                  strategy_name, product_id))
            conn.commit()
            conn.close()
    
    def calibrate_confidence(self, strategy_name: str, product_id: str, 
                             raw_confidence: float) -> float:
        """Apply Platt scaling calibration to raw confidence."""
        perf = self.get(strategy_name, product_id)
        if not perf or perf.calibration_samples < 10:
            return raw_confidence
        
        # Platt scaling: calibrated = 1 / (1 + exp(-(A * raw + B)))
        import math
        calibrated = 1.0 / (1.0 + math.exp(-(perf.calibration_slope * raw_confidence + perf.calibration_intercept)))
        return max(0.0, min(1.0, calibrated))
    
    def deactivate_strategy(self, strategy_name: str, product_id: str) -> None:
        """Mark strategy as inactive for a product."""
        key = f"{strategy_name}/{product_id}"
        with _DB_LOCK:
            conn = sqlite3.connect(self.db_path)
            conn.execute("UPDATE strategy_perf SET is_active=0 WHERE strategy_name=? AND product_id=?", 
                        (strategy_name, product_id))
            conn.commit()
            conn.close()
        with self._cache_lock:
            if key in self._cache:
                self._cache[key].is_active = False
    
    def get_all_active(self) -> List[StrategyPerf]:
        with self._cache_lock:
            return [p for p in self._cache.values() if p.is_active]


# Global instance
_REGISTRY: Optional[StrategyRegistry] = None
_REGISTRY_LOCK = threading.Lock()


def get_registry() -> StrategyRegistry:
    global _REGISTRY
    with _REGISTRY_LOCK:
        if _REGISTRY is None:
            _REGISTRY = StrategyRegistry()
        return _REGISTRY