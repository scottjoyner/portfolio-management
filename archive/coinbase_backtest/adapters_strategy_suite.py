# src/backtest/adapters_strategy_suite.py
from __future__ import annotations
from typing import List, Dict, Any, Callable

import pandas as pd

from .engine import BaseAdapter, BTConfig, DataPortal
# import your strategy suite
try:
    from src.strategies import strategy_suite as ss
except Exception:
    import src.strategies.strategy_suite as ss  # adjust if needed

class StrategySuiteAdapter(BaseAdapter):
    """
    Wraps strategy_suite.generate_signals to run on the backtest portal data.
    We monkeypatch ss.fetch_candles_df so strategies read the historical window.
    """
    def __init__(self, portal: DataPortal, cfg: BTConfig, which: List[Callable]=None):
        super().__init__(portal, cfg, "strategy_suite")
        self.which = which or ss.STRATEGIES
        self._ctx = ss.StrategyContext(cb=None, products=portal.cfg.products,
                                       granularity=portal.cfg.granularity,
                                       lookback_days=portal.cfg.lookback_days)

    def _mk_fetcher(self, t_idx: int):
        def fetcher(_cb_unused, product_id: str, lookback_days: int, granularity: str) -> pd.DataFrame:
            # Return the rolling window up to t_idx
            return self.portal.window(product_id, t_idx)
        return fetcher

    def on_bar(self, t_idx: int) -> List[Dict[str,Any]]:
        # monkeypatch fetch_candles_df inside the strategy suite module
        ss.fetch_candles_df = self._mk_fetcher(t_idx)
        out: List[Dict[str,Any]] = []
        for fn in self.which:
            try:
                sigs = fn(self._ctx) or []
                out.extend(sigs)
            except Exception as e:
                out.append({"type":"error","name":getattr(fn, "__name__", "unknown"),"error":str(e)})
        return out
