# src/backtest/adapters_portfolio.py
from __future__ import annotations
from typing import List, Dict, Any
import pandas as pd

from .engine import BaseAdapter, BTConfig, DataPortal

# import your portfolio constructor (already uses Neo4j category equalization & halving tilt)
try:
    from src.strategies import portfolio_suite as pm
except Exception:
    import src.strategies.portfolio_suite as pm

class PortfolioRebalanceAdapter(BaseAdapter):
    """
    Calls portfolio_suite.build_portfolio() on the first bar of each month and emits a 'rebalance' signal.
    Uses the backtest portal data via a fetcher monkeypatch (no live calls).
    """
    def __init__(self, portal: DataPortal, cfg: BTConfig):
        super().__init__(portal, cfg, "pm_rebalance_monthly")
        self._last_month = None
        self._ctx = pm.PMContext(cb=None, products=portal.cfg.products,
                                 granularity=portal.cfg.granularity,
                                 lookback_days=portal.cfg.lookback_days)

    def _mk_fetcher(self, t_idx: int):
        def fetcher(_cb_unused, product_id: str, lookback_days: int, granularity: str) -> pd.DataFrame:
            return self.portal.window(product_id, t_idx)
        return fetcher

    def on_bar(self, t_idx: int) -> List[Dict[str,Any]]:
        ts = self.portal.time_index()[t_idx]
        month_key = (ts.year, ts.month)
        if self._last_month is None:
            self._last_month = month_key
            return []  # start next month
        if month_key == self._last_month:
            return []
        # month changed → run portfolio builder
        self._last_month = month_key
        # patch fetcher so portfolio_suite uses historical window
        pm.fetch_candles_df = self._mk_fetcher(t_idx)
        sig = pm.build_portfolio(self._ctx)
        if not sig or sig.get("type") != "rebalance":
            return []
        # Keep only assets present in backtest universe
        tw = {k: v for k, v in (sig.get("target_weights") or {}).items() if k in self.portal.cfg.products}
        if not tw:
            return []
        return [{"name": "pm_rebalance_v2", "type":"rebalance", "target_weights": tw, "notes": sig.get("notes", {})}]
