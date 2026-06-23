from __future__ import annotations
from typing import List, Dict, Any, Optional, Callable

import pandas as pd

from .engine_v2 import EnhancedBacktestEngine, BacktestConfig, DataPortalV2
from ..protocols import Direction, Bar, BracketSetup, BaseStrategy
from ..opportunity_scanner import (
    OpportunityScanner, StrategyEngineAdapter, AlphaSetupAdapter,
    FuturesSignalAdapter, ScannerConfig,
)


def run_backtest_with_strategy_engine(
    cb: Any,
    products: List[str],
    strategy_keys: Optional[List[str]] = None,
    granularity: str = "ONE_HOUR",
    lookback_days: int = 240,
    initial_cash: float = 10_000.0,
    risk_per_trade: float = 0.01,
    enable_short: bool = True,
    enable_futures: bool = True,
    max_leverage: float = 3.0,
    start: Optional[str] = None,
    end: Optional[str] = None,
    warmup: int = 200,
) -> Dict[str, Any]:
    """Run a backtest using strategy_engine.py strategies via the unified adapter.

    Wires strategy_engine.py's 27 strategies through OpportunityScanner
    into EnhancedBacktestEngine with short/futures support.
    """
    scanner_cfg = ScannerConfig(
        enable_short=enable_short,
        enable_futures=enable_futures,
        max_leverage=max_leverage,
    )
    scanner = OpportunityScanner(scanner_cfg)

    if strategy_keys:
        for key in strategy_keys:
            scanner.register(StrategyEngineAdapter(key))
    else:
        scanner.register(AlphaSetupAdapter())
        from strategy_engine import ALL_STRATEGIES
        core_keys = [
            "ema_cross", "rsi_revert", "boll_break", "zscore_revert",
            "vol_mom", "macd", "vwap_revert", "obv_div", "cmo",
            "trix", "adx", "keltner", "chaikin_mf", "williams_r",
            "psar", "hma", "force_idx", "vpt", "donchian", "aroon",
        ]
        for key in core_keys:
            if key in ALL_STRATEGIES:
                scanner.register(StrategyEngineAdapter(key))
        if enable_futures:
            scanner.register(FuturesSignalAdapter())

    portal = DataPortalV2(cb, products, granularity, lookback_days, start, end)
    cfg = BacktestConfig(
        initial_cash=initial_cash,
        risk_per_trade=risk_per_trade,
        enable_short=enable_short,
        enable_futures=enable_futures,
        max_leverage=max_leverage,
    )
    engine = EnhancedBacktestEngine(portal, cfg)

    class ScannerStrategy(BaseStrategy):
        def __init__(self, scanner: OpportunityScanner):
            self.scanner = scanner

        def name(self) -> str:
            return "scanner_wrapper"

        def on_bar(self, bar: Bar, history: List[Bar]) -> Optional[BracketSetup]:
            return None

        def scan_all(self, portal: DataPortalV2, t_idx: int,
                     strategies: List[BaseStrategy]) -> Dict[str, List]:
            results = {}
            for p in portal.products:
                try:
                    bar = portal.at(p, t_idx)
                    hist = portal.history_bars(p, t_idx, 300)
                    atr = _compute_atr_from_bars(hist + [bar])
                    opps = self.scanner.scan(p, bar, hist, atr)
                    if opps:
                        results[p] = opps
                except Exception:
                    continue
            return results

    scanner_strat = ScannerStrategy(scanner)
    idx = portal.time_index()

    for t_idx, ts in enumerate(idx):
        if t_idx < warmup:
            continue

        prices_now = {}
        for p in portal.products:
            try:
                prices_now[p] = portal.price(p, t_idx)
            except Exception:
                continue

        opportunities = scanner_strat.scan_all(portal, t_idx, [])

        for p, opps in opportunities.items():
            if p in engine.positions:
                continue
            for opp in opps:
                if opp.direction == Direction.SHORT and not enable_short:
                    continue
                if opp.instrument_type != "spot" and not enable_futures:
                    continue
                bid, ask = portal.bid_ask(p, t_idx)
                bar = portal.at(p, t_idx)
                fill = engine._apply_fill(
                    opp.direction, opp.entry_price,
                    engine._size_position(
                        engine._equity(prices_now),
                        opp.entry_price, opp.stop_price,
                        opp.leverage,
                    ),
                    bid, ask, bar.volume,
                )
                if fill.size <= 0:
                    continue
                notional = fill.size * fill.price
                if notional < cfg.min_notional:
                    continue
                if opp.direction == Direction.LONG:
                    if notional > engine.cash:
                        continue
                    engine.cash -= notional + fill.fees
                else:
                    engine.cash += notional - fill.fees
                engine.positions[p] = type('obj', (object,), {
                    "product": p,
                    "direction": opp.direction,
                    "qty": fill.size,
                    "entry_px": fill.price,
                    "stop_px": opp.stop_price,
                    "target_px": opp.target_price,
                    "atr": opp.atr,
                    "strategy": opp.strategy_name,
                    "instrument": opp.instrument_type,
                    "leverage": opp.leverage,
                    "opened_ts": ts,
                    "entry_fees": fill.fees,
                    "cum_funding": 0.0,
                })
                engine.trades.append({
                    "ts": ts.isoformat(),
                    "product": p,
                    "direction": opp.direction.value,
                    "qty": fill.size,
                    "entry": fill.price,
                    "stop": opp.stop_price,
                    "target": opp.target_price,
                    "reason": f"entry_{opp.strategy_name}",
                    "pnl": 0.0,
                    "fees": fill.fees,
                    "strategy": opp.strategy_name,
                    "instrument": opp.instrument_type.value if hasattr(opp.instrument_type, 'value') else str(opp.instrument_type),
                    "leverage": opp.leverage,
                })

        for p, pos in list(engine.positions.items()):
            px = prices_now.get(p)
            if px is None:
                continue
            engine._update_trailing(pos, px)
            if pos.direction == Direction.LONG:
                if px <= pos.stop_px or px >= pos.target_px:
                    _close_position(engine, p, px, ts)
            else:
                if px >= pos.stop_px or px <= pos.target_px:
                    _close_position(engine, p, px, ts)

        eq = engine._equity(prices_now)
        engine.eq_curve.loc[ts] = eq

    if len(idx) > 0:
        _final_liquidation(engine, portal, idx[-1])

    daily_series = engine.eq_curve.resample("1D").last().dropna()
    metrics = engine._compute_metrics(engine.eq_curve, daily_series)
    return {
        "metrics": metrics,
        "equity_curve": engine.eq_curve,
        "trades": pd.DataFrame(engine.trades) if engine.trades else pd.DataFrame(),
        "config": {
            "products": products,
            "initial_cash": initial_cash,
            "risk_per_trade": risk_per_trade,
            "enable_short": enable_short,
            "enable_futures": enable_futures,
            "max_leverage": max_leverage,
            "strategy_keys": strategy_keys or "all_core",
        },
    }


def _compute_atr_from_bars(bars: List[Bar], period: int = 14) -> float:
    if len(bars) < period + 1:
        return 0.0
    tr_vals = []
    for i in range(1, len(bars)):
        tr = max(
            bars[i].high - bars[i].low,
            abs(bars[i].high - bars[i - 1].close),
            abs(bars[i].low - bars[i - 1].close),
        )
        tr_vals.append(tr)
    if not tr_vals:
        return 0.0
    return sum(tr_vals[-period:]) / period


def _close_position(engine: EnhancedBacktestEngine, p: str, px: float, ts):
    from ..protocols import Direction
    pos = engine.positions.pop(p, None)
    if pos is None:
        return
    bid, ask = 0.0, 0.0
    try:
        for pp in engine.portal.products:
            if pp == p:
                bid, ask = engine.portal.bid_ask(p, 0)
                break
    except Exception:
        pass
    vol = 0.0
    if pos.direction == Direction.LONG:
        fill = engine._apply_fill(Direction.SHORT, px, pos.qty, bid, ask, vol)
        engine.cash += pos.qty * fill.price - fill.fees
        pnl = (fill.price - pos.entry_px) * pos.qty
    else:
        fill = engine._apply_fill(Direction.LONG, px, pos.qty, bid, ask, vol)
        engine.cash -= pos.qty * fill.price + fill.fees
        pnl = (pos.entry_px - fill.price) * pos.qty
    r = pnl / max(1e-9, abs(pos.entry_px - pos.stop_px) * pos.qty)
    engine.trades.append({
        "ts": ts.isoformat() if hasattr(ts, 'isoformat') else str(ts),
        "product": p, "direction": pos.direction.value,
        "qty": -pos.qty, "entry": pos.entry_px,
        "exit": fill.price, "reason": "exit",
        "pnl": pnl, "r_multiple": r, "fees": fill.fees,
        "strategy": pos.strategy,
    })


def _final_liquidation(engine: EnhancedBacktestEngine, portal: DataPortalV2, last_ts):
    for p, pos in list(engine.positions.items()):
        try:
            px = portal.price(p, len(portal.time_index()) - 1)
        except Exception:
            px = pos.entry_px
        _close_position(engine, p, px, last_ts)
    eq = engine._equity({p: portal.price(p, len(portal.time_index()) - 1) for p in portal.products})
    engine.eq_curve.loc[last_ts] = eq
