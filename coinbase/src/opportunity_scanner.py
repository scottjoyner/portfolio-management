from __future__ import annotations
from dataclasses import dataclass, field
from typing import List, Optional, Dict, Any, Callable, Set
import math

from .protocols import (
    Direction, InstrumentType, Bar, BracketSetup, Opportunity,
    OpportunityAggregator, BaseStrategy,
)
try:
    from ..data import compute_atr, rsi as _rsi
    from ..alpha.alpha import (
        donchian_breakout_setup,
        trend_rsi_pullback_setup,
        donchian_breakdown_setup,
        trend_rsi_rip_setup,
        rsi_failure_swing_setup,
        volatility_compression_breakout_setup,
        impulse_exhaustion_reversal_setup,
    )
except ImportError:
    from .data import compute_atr, rsi as _rsi
    from .alpha.alpha import (
        donchian_breakout_setup,
        trend_rsi_pullback_setup,
        donchian_breakdown_setup,
        trend_rsi_rip_setup,
        rsi_failure_swing_setup,
        volatility_compression_breakout_setup,
        impulse_exhaustion_reversal_setup,
    )


ALPHA_SETUP_FUNCTIONS = [
    ("donchian_breakout", donchian_breakout_setup, True),
    ("trend_rsi_pullback", trend_rsi_pullback_setup, False),
    ("donchian_breakdown", donchian_breakdown_setup, True),
    ("trend_rsi_rip", trend_rsi_rip_setup, False),
    ("rsi_failure_swing", rsi_failure_swing_setup, True),
    ("vol_compression_breakout", volatility_compression_breakout_setup, True),
    ("impulse_exhaustion", impulse_exhaustion_reversal_setup, True),
]

STRATEGY_ENGINE_ALL_KEYS = [
    "ema_cross", "rsi_revert", "boll_break", "zscore_revert",
    "vol_mom", "macd", "vwap_revert", "obv_div", "cmo", "trix",
    "adx", "keltner", "chaikin_mf", "williams_r", "psar", "hma",
    "force_idx", "vpt", "donchian", "aroon", "price_eff", "scci",
    "range_exp_idx", "ema_dev", "snr_idx", "kalshi", "polymarket",
]

NICHE_STRATEGY_NAMES = [
    "MultiTimeframeRSIMomentumStrategy",
    "BollingerSqueezeBreakoutStrategy",
    "RegimeAwareAdaptiveStrategy",
    "AnchoredVWAPMeanReversionStrategy",
    "LiquidityVacuumReversalStrategy",
    "DonchianPullbackContinuationStrategy",
    "RSIFailureSwingReversalStrategy",
    "VolatilityCompressionBreakoutStrategy",
    "ImpulseExhaustionReversalStrategy",
    "VolRegimeSwitchStrategy",
    "SentimentMomentumCompositeStrategy",
    "OnChainRegimeWhaleFlowStrategy",
]


@dataclass
class ScannerConfig:
    min_rr: float = 1.5
    stop_atr_mult: float = 2.0
    target_atr_mult: float = 3.0
    confidence_threshold: float = 0.1
    enable_short: bool = True
    enable_futures: bool = True
    max_leverage: float = 3.0
    leverage_by_atr: bool = True
    max_positions_per_product: int = 1
    include_se_all: bool = True
    include_niche: bool = True
    include_alpha: bool = True
    include_futures: bool = True
    include_market_making: bool = True
    include_orderbook: bool = True
    include_novel: bool = True
    strategy_filter: Optional[List[str]] = None
    exclude_strategies: Optional[Set[str]] = None


class _AlphaSetupBase(BaseStrategy):
    def __init__(self, name: str, setup_fn, needs_stop_target: bool,
                 stop_atr_mult: float = 2.0, target_atr_mult: float = 3.0):
        self._name = name
        self._fn = setup_fn
        self._needs_stop_target = needs_stop_target
        self._stop_k = stop_atr_mult
        self._target_k = target_atr_mult

    def name(self) -> str:
        return self._name

    def on_bar(self, bar: Bar, history: List[Bar]) -> Optional[BracketSetup]:
        df = self._bars_to_df(history + [bar])
        if len(df) < 220:
            return None
        atr_value = float(compute_atr(df).iloc[-1]) if hasattr(compute_atr(df), 'iloc') else 0.0
        try:
            if self._needs_stop_target:
                s = self._fn(df, self._stop_k, self._target_k)
            else:
                s = self._fn(df, self._stop_k)
        except Exception:
            return None
        if not s or s.get("rr", 0) < 1.0:
            return None
        direction = Direction.LONG if s["side"] == "buy" else Direction.SHORT
        return BracketSetup(
            direction=direction,
            entry_price=s["entry"],
            stop_price=s["stop"],
            target_price=s["target"],
            risk_reward=s["rr"],
            confidence=0.6,
            reason=s.get("name", self._name),
            strategy_name=self._name,
            atr=atr_value,
        )

    @staticmethod
    def _bars_to_df(bars: List[Bar]) -> "pd.DataFrame":
        import pandas as pd
        records = [{"open": b.open, "high": b.high, "low": b.low, "close": b.close, "volume": b.volume} for b in bars]
        return pd.DataFrame(records)


class MarketMakingStrategy(BaseStrategy):
    def __init__(self, min_spread_bps: float = 10.0,
                 max_spread_bps: float = 50.0,
                 position_duration_bars: int = 5):
        self.min_spread = min_spread_bps
        self.max_spread = max_spread_bps
        self._name = "market_making"
        self._bars_since_trade: Dict[str, int] = {}

    def name(self) -> str:
        return self._name

    def on_bar(self, bar: Bar, history: List[Bar]) -> Optional[BracketSetup]:
        closes = [b.close for b in history] + [bar.close]
        if len(closes) < 20:
            return None
        atr = self._estimate_atr(closes, [b.high for b in history] + [bar.high],
                                 [b.low for b in history] + [bar.low])
        if atr <= 0:
            return None
        spread_bps = atr / max(bar.close, 1e-9) * 10000
        if spread_bps < self.min_spread or spread_bps > self.max_spread:
            return None
        avg_vol = sum(b.volume for b in history[-10:]) / 10 if len(history) >= 10 else bar.volume
        vol_spike = bar.volume > avg_vol * 2.0
        if vol_spike:
            return None
        rsi_val = self._rsi(closes, 14)
        if rsi_val < 35:
            direction = Direction.LONG
            stop = bar.close - atr * 1.2
            target = bar.close + atr * 1.8
        elif rsi_val > 65:
            direction = Direction.SHORT
            stop = bar.close + atr * 1.2
            target = bar.close - atr * 1.8
        else:
            return None
        rr = abs(target - bar.close) / max(abs(bar.close - stop), 1e-9)
        return BracketSetup(
            direction=direction,
            entry_price=bar.close,
            stop_price=stop,
            target_price=target,
            risk_reward=rr,
            confidence=0.35,
            reason=f"MM: rsi={rsi_val:.0f} spread={spread_bps:.0f}bps",
            strategy_name="market_making",
            atr=atr,
        )

    @staticmethod
    def _rsi(closes: List[float], period: int = 14) -> float:
        if len(closes) < period + 1:
            return 50.0
        deltas = [closes[i] - closes[i - 1] for i in range(-period, 0)]
        gains = sum(d for d in deltas if d > 0)
        losses = sum(abs(d) for d in deltas if d < 0)
        if losses == 0:
            return 100.0
        rs = (gains / period) / (losses / period)
        return 100.0 - (100.0 / (1.0 + rs))

    @staticmethod
    def _estimate_atr(closes: List[float], highs: List[float],
                       lows: List[float], period: int = 14) -> float:
        if len(closes) < period + 1:
            return 0.0
        tr_vals = []
        for i in range(1, min(period + 1, len(closes))):
            tr = max(highs[-i] - lows[-i],
                     abs(highs[-i] - closes[-i - 1]),
                     abs(lows[-i] - closes[-i - 1]))
            tr_vals.append(tr)
        return sum(tr_vals) / len(tr_vals) if tr_vals else 0.0


class CrossProductArbitrageStrategy(BaseStrategy):
    def __init__(self, pair_map: Optional[Dict[str, str]] = None,
                 zscore_entry: float = 2.0, zscore_exit: float = 0.5):
        self._name = "cross_arb"
        self.pair_map = pair_map or {"ETH-USD": "BTC-USD", "SOL-USD": "BTC-USD", "AVAX-USD": "ETH-USD"}
        self.z_entry = zscore_entry
        self.z_exit = zscore_exit

    def name(self) -> str:
        return self._name

    def on_bar(self, bar: Bar, history: List[Bar]) -> Optional[BracketSetup]:
        return None


class StrategyEngineAdapter(BaseStrategy):
    def __init__(self, strategy_key: str, asset_class: str = "growth"):
        self.strategy_key = strategy_key
        self.asset_class = asset_class
        self._name = f"se_{strategy_key}"

    def name(self) -> str:
        return self._name

    def on_bar(self, bar: Bar, history: List[Bar]) -> Optional[BracketSetup]:
        from strategy_engine import ALL_STRATEGIES, VOLUME_STRATEGIES, HIGH_LOW_STRATEGIES
        cls = ALL_STRATEGIES.get(self.strategy_key)
        if not cls:
            return None
        strat = cls()
        closes = [b.close for b in history] + [bar.close]
        volumes = [b.volume for b in history] + [bar.volume]
        highs = [b.high for b in history] + [bar.high]
        lows = [b.low for b in history] + [bar.low]
        atr = self._compute_atr(closes, highs, lows)
        needs_hl = self.strategy_key in HIGH_LOW_STRATEGIES
        needs_vol = self.strategy_key in VOLUME_STRATEGIES
        try:
            if needs_hl:
                sig = strat.on_bar(bar.close, closes, volumes=volumes if needs_vol else None, highs=highs, lows=lows)
            elif needs_vol:
                sig = strat.on_bar(bar.close, closes, volumes=volumes)
            else:
                sig = strat.on_bar(bar.close, closes)
        except Exception:
            return None
        if sig is None or sig.action == "HOLD":
            return None
        direction = Direction.LONG if sig.action == "BUY" else Direction.SHORT
        stop_dist = atr * 2.0
        target_dist = atr * 3.0
        entry = sig.price or bar.close
        if direction == Direction.LONG:
            stop = entry - stop_dist
            target = entry + target_dist
        else:
            stop = entry + stop_dist
            target = entry - target_dist
        rr = abs(target - entry) / max(abs(entry - stop), 1e-9)
        return BracketSetup(
            direction=direction,
            entry_price=entry,
            stop_price=stop,
            target_price=target,
            risk_reward=rr,
            confidence=sig.confidence,
            reason=sig.reason,
            strategy_name=self.strategy_key,
            atr=atr,
        )

    @staticmethod
    def _compute_atr(closes: List[float], highs: List[float], lows: List[float]) -> float:
        if len(closes) < 15:
            return 0.0
        tr_vals = []
        for i in range(1, min(15, len(closes))):
            tr = max(highs[-i] - lows[-i],
                     abs(highs[-i] - closes[-i - 1]) if len(closes) > i else 0,
                     abs(lows[-i] - closes[-i - 1]) if len(closes) > i else 0)
            tr_vals.append(tr)
        return sum(tr_vals) / max(len(tr_vals), 1) if tr_vals else 0.0


class FuturesSignalAdapter(BaseStrategy):
    def __init__(self, btc_dominance_threshold: float = 0.45,
                 eth_dominance_threshold: float = 0.18):
        self.btc_dom = btc_dominance_threshold
        self.eth_dom = eth_dominance_threshold
        self._name = "futures_signal"

    def name(self) -> str:
        return self._name

    def on_bar(self, bar: Bar, history: List[Bar]) -> Optional[BracketSetup]:
        if len(history) < 50:
            return None
        closes = [b.close for b in history] + [bar.close]
        volumes = [b.volume for b in history] + [bar.volume]
        rsi_val = self._calc_rsi(closes, 14)
        vol_ratio = volumes[-1] / max(sum(volumes[-21:-1]) / 20, 1e-9)
        if not hasattr(self, '_regime'):
            self._regime = self._detect_regime(closes)
        trend = self._regime
        if rsi_val < 30 and trend == "uptrend" and vol_ratio > 1.5:
            return BracketSetup(
                direction=Direction.LONG, entry_price=closes[-1],
                stop_price=closes[-1] * 0.95, target_price=closes[-1] * 1.08,
                risk_reward=1.6, confidence=0.55,
                reason="Futures: oversold bounce in uptrend",
                strategy_name="futures_momentum",
                atr=closes[-1] * 0.02,
                instrument_type=InstrumentType.PERP_FUTURES, leverage=2.0,
            )
        if rsi_val > 70 and trend == "downtrend" and vol_ratio > 1.5:
            return BracketSetup(
                direction=Direction.SHORT, entry_price=closes[-1],
                stop_price=closes[-1] * 1.05, target_price=closes[-1] * 0.92,
                risk_reward=1.6, confidence=0.55,
                reason="Futures: overbought in downtrend",
                strategy_name="futures_momentum",
                atr=closes[-1] * 0.02,
                instrument_type=InstrumentType.PERP_FUTURES, leverage=2.0,
            )
        return None

    @staticmethod
    def _calc_rsi(closes: List[float], period: int = 14) -> float:
        if len(closes) < period + 1:
            return 50.0
        deltas = [closes[i] - closes[i - 1] for i in range(-period, 0)]
        gains = sum(d for d in deltas if d > 0)
        losses = sum(abs(d) for d in deltas if d < 0)
        if losses == 0:
            return 100.0
        rs = (gains / period) / (losses / period)
        return 100.0 - (100.0 / (1.0 + rs))

    @staticmethod
    def _detect_regime(closes: List[float]) -> str:
        if len(closes) < 50:
            return "unknown"
        ma50 = sum(closes[-50:]) / 50
        ma20 = sum(closes[-20:]) / 20
        if ma20 > ma50 * 1.02:
            return "uptrend"
        elif ma20 < ma50 * 0.98:
            return "downtrend"
        return "ranging"


class StrategyUniverse:
    @staticmethod
    def all_se_adapters(keys: Optional[List[str]] = None) -> List[BaseStrategy]:
        if keys is None:
            keys = STRATEGY_ENGINE_ALL_KEYS
        return [StrategyEngineAdapter(k) for k in keys]

    @staticmethod
    def all_alpha_setups(stop_atr_mult: float = 2.0,
                         target_atr_mult: float = 3.0) -> List[BaseStrategy]:
        return [
            _AlphaSetupBase(name, fn, needs_ss, stop_atr_mult, target_atr_mult)
            for name, fn, needs_ss in ALPHA_SETUP_FUNCTIONS
        ]

    @staticmethod
    def all_niche_strategies() -> List[BaseStrategy]:
        try:
            from .backtest.niche_adapter import wrap_all_niche_strategies
            return wrap_all_niche_strategies()
        except Exception:
            return []

    @staticmethod
    def all_novel_strategies() -> List[BaseStrategy]:
        strategies: List[BaseStrategy] = []
        try:
            from .strat_price_action import PriceActionSRStrategy
            strategies.append(PriceActionSRStrategy())
        except Exception:
            pass
        try:
            from .strat_hmm import HMMRegimeStrategy
            strategies.append(HMMRegimeStrategy())
        except Exception:
            pass
        try:
            from .strat_candlestick import CandlestickPatternStrategy
            strategies.append(CandlestickPatternStrategy())
        except Exception:
            pass
        try:
            from .strat_pairs import CointegratedPairsStrategy
            strategies.append(CointegratedPairsStrategy())
        except Exception:
            pass
        try:
            from .strat_orderflow import SmartMoneyFlowStrategy
            strategies.append(SmartMoneyFlowStrategy())
        except Exception:
            pass
        try:
            from .strat_grid import GridTradingStrategy
            strategies.append(GridTradingStrategy())
        except Exception:
            pass
        try:
            from .strat_dca import DCAAccumulationStrategy
            strategies.append(DCAAccumulationStrategy())
        except Exception:
            pass
        try:
            from .strat_funding import FundingRateCaptureStrategy
            strategies.append(FundingRateCaptureStrategy())
        except Exception:
            pass
        try:
            from .strat_scalper import VolatilityScalperStrategy
            strategies.append(VolatilityScalperStrategy())
        except Exception:
            pass
        try:
            from .strat_momaccel import MomentumAccelerationStrategy
            strategies.append(MomentumAccelerationStrategy())
        except Exception:
            pass
        return strategies

    @staticmethod
    def all_strategies(config: Optional[ScannerConfig] = None) -> List[BaseStrategy]:
        cfg = config or ScannerConfig()
        strategies: List[BaseStrategy] = []
        if cfg.include_se_all:
            strategies.extend(StrategyUniverse.all_se_adapters())
        if cfg.include_alpha:
            strategies.extend(StrategyUniverse.all_alpha_setups(
                cfg.stop_atr_mult, cfg.target_atr_mult))
        if cfg.include_niche:
            strategies.extend(StrategyUniverse.all_niche_strategies())
        if cfg.include_futures:
            strategies.append(FuturesSignalAdapter())
        if cfg.include_market_making:
            strategies.append(MarketMakingStrategy())
        if cfg.include_orderbook:
            try:
                from .confluence import OrderBookImbalanceStrategy
                strategies.append(OrderBookImbalanceStrategy())
            except Exception:
                pass
        if cfg.include_novel:
            strategies.extend(StrategyUniverse.all_novel_strategies())
        if cfg.strategy_filter:
            strategies = [s for s in strategies if s.name() in cfg.strategy_filter]
        if cfg.exclude_strategies:
            strategies = [s for s in strategies if s.name() not in cfg.exclude_strategies]
        return strategies

    @staticmethod
    def summary() -> Dict[str, int]:
        novel_count = 5
        new_count = 5
        return {
            "se_strategies": len(STRATEGY_ENGINE_ALL_KEYS),
            "alpha_setups": len(ALPHA_SETUP_FUNCTIONS),
            "niche_strategies": len(NICHE_STRATEGY_NAMES),
            "futures_signals": 1,
            "market_making": 1,
            "orderbook": 1,
            "novel_strategies": novel_count,
            "new_strategies": new_count,
            "total": len(STRATEGY_ENGINE_ALL_KEYS) + len(ALPHA_SETUP_FUNCTIONS) + len(NICHE_STRATEGY_NAMES) + 3 + novel_count + new_count,
        }


class OpportunityScanner:
    def __init__(self, config: Optional[ScannerConfig] = None):
        self.config = config or ScannerConfig()
        self._strategies: List[BaseStrategy] = []

    def register(self, strategy: BaseStrategy):
        self._strategies.append(strategy)

    def register_all(self, strategies: List[BaseStrategy]):
        self._strategies.extend(strategies)

    def register_defaults(self):
        self._strategies = StrategyUniverse.all_strategies(self.config)

    def scan(self, product_id: str, bar: Bar, history: List[Bar],
             atr: float) -> List[Opportunity]:
        setups: List[BracketSetup] = []
        for strategy in self._strategies:
            try:
                strategy.set_product_id(product_id)
                setup = strategy.on_bar(bar, history)
                if setup is None:
                    continue
                if setup.direction == Direction.SHORT and not self.config.enable_short:
                    continue
                if setup.instrument_type != InstrumentType.SPOT and not self.config.enable_futures:
                    continue
                if setup.risk_reward < self.config.min_rr:
                    continue
                if setup.confidence < self.config.confidence_threshold:
                    continue
                if self.config.leverage_by_atr and setup.atr > 0:
                    vol_bps = setup.atr / max(setup.entry_price, 1e-9) * 10000
                    if vol_bps < 50:
                        setup.leverage = min(self.config.max_leverage, 3.0)
                    elif vol_bps < 150:
                        setup.leverage = min(self.config.max_leverage, 2.0)
                    else:
                        setup.leverage = 1.0
                setups.append(setup)
            except Exception:
                continue

        aggregator = OpportunityAggregator(product_id, setups, bar.close, atr)
        opportunities = aggregator.all_ranked()

        for opp in opportunities:
            opp.score = opp.confidence * opp.risk_reward * (1.0 if opp.direction == Direction.LONG else 0.9)
            if opp.instrument_type != InstrumentType.SPOT:
                opp.score *= 1.1

        opportunities.sort(key=lambda o: o.score, reverse=True)

        if self.config.max_positions_per_product > 0:
            seen_strategies = set()
            deduped = []
            for o in opportunities:
                key = (o.strategy_name, o.direction.value)
                if key not in seen_strategies:
                    seen_strategies.add(key)
                    deduped.append(o)
                    if len(deduped) >= self.config.max_positions_per_product * 3:
                        break
            opportunities = deduped

        return opportunities[:self.config.max_positions_per_product]

    def scan_multi(self, products: Dict[str, Dict[str, Any]],
                   price_fn: Callable) -> Dict[str, List[Opportunity]]:
        results: Dict[str, List[Opportunity]] = {}
        for pid, data in products.items():
            bar = data.get("bar")
            history = data.get("history", [])
            atr = data.get("atr", 0.0)
            if bar is None or not history:
                continue
            for s in self._strategies:
                s.set_product_id(pid)
            results[pid] = self.scan(pid, bar, history, atr)
        return results
