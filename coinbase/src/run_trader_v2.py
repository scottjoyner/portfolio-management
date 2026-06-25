from __future__ import annotations
import time
import logging
import argparse
import json
import os
import shutil
import signal
import sys
import threading
from datetime import datetime, timezone
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass, field

if __package__ in (None, ""):
    sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
    __package__ = "coinbase.src"

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "graph-alpha-bot", "app", "strategies"))
try:
    from coinbase_universe import COINBASE_SPOT_PAIRS
except Exception:
    COINBASE_SPOT_PAIRS = [
        "BTC-USD", "ETH-USD", "SOL-USD", "XRP-USD", "ADA-USD",
        "DOGE-USD", "AVAX-USD", "DOT-USD", "LINK-USD", "UNI-USD",
        "POL-USD", "ATOM-USD", "LTC-USD", "BCH-USD", "NEAR-USD",
        "APT-USD", "SUI-USD", "ARB-USD", "OP-USD", "FIL-USD",
        "INJ-USD", "SEI-USD", "TIA-USD", "ALGO-USD", "XLM-USD",
        "STX-USD", "HBAR-USD", "ICP-USD", "GRT-USD", "SHIB-USD",
        "PEPE-USD", "BONK-USD", "TRUMP-USD", "FLOKI-USD",
    ]

from .protocols import (
    Direction, InstrumentType, Bar, Opportunity, BracketSetup, BaseStrategy,
)
from .fill_model import AdaptiveFillModel
from .opportunity_scanner import OpportunityScanner
from .cb_client import CBClient
from .risk_manager import RiskManager, KellySizer, RiskLimit
from .risk_appetite import DynamicRiskController, RiskAppetiteSnapshot
from .orchestrator import ExecutionOrchestrator, TradeMode as ExecutionMode
from .paper_live_bridge import PerformanceTracker, DeploymentPipeline
from .feed import TickerCache, PollingFeed, Ticker, FeedSource
try:
    from .correlation import CorrelationAwareSizer
except Exception:
    class CorrelationAwareSizer:
        def adjust_size(self, product_id, base_size, positions):
            return type("Adj", (), {"adjusted_size": base_size})()
from .ensemble import BayesianSignalBlender, StrategyConfidenceAggregator
from .regime import RegimeDetector, AdaptiveStrategySelector
from .confluence import MultiTimeframeConfluence, ConfluenceResult, TimeframeSignal
from .risk_parity import RiskParityPortfolio
from .exec_algo import TWAPAlgo, VWAPAlgo, IcebergAlgo
from .product_rotation import ProductRotator, MomentumRotationStrategy
from .adaptive_mode import AdaptiveModeSelector
from .dual_mm import DualMarketMaker, MarketMakingStrategy
from .ranking import StrategyRanking, StrategyRankingFilter, RANKING_STATE_PATH
from .fear_greed import FearGreedSignalAdapter, FearGreedIndex
from .news_risk import NewsRiskAdjuster, NewsAwareRiskStrategy
from .market_condition import MarketConditionProfile, MarketConditionStrategySelector

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
log = logging.getLogger("run_trader_v2")

VOL_ESTIMATES = {
    "BTC-USD": 20_000, "ETH-USD": 200_000, "SOL-USD": 500_000,
    "XRP-USD": 600_000, "ADA-USD": 400_000, "DOGE-USD": 1_000_000,
    "AVAX-USD": 300_000, "DOT-USD": 250_000, "LINK-USD": 150_000,
    "UNI-USD": 100_000, "POL-USD": 500_000, "ATOM-USD": 100_000,
    "LTC-USD": 300_000, "BCH-USD": 100_000, "NEAR-USD": 200_000,
    "APT-USD": 150_000, "SUI-USD": 300_000, "ARB-USD": 200_000,
    "OP-USD": 150_000, "SHIB-USD": 800_000, "PEPE-USD": 500_000,
    "TRUMP-USD": 400_000,
}

STATIC_LONG_TERM_ASSETS = {"BTC", "ETH"}
STATIC_LONG_TERM_PRODUCTS = {f"{asset}-USD" for asset in STATIC_LONG_TERM_ASSETS}


@dataclass
class TraderConfig:
    products: List[str] = field(default_factory=lambda: list(COINBASE_SPOT_PAIRS))
    poll_interval_secs: float = 30.0
    execution_mode: ExecutionMode = ExecutionMode.PAPER
    equity: float = 100.0
    max_positions: int = 10
    target_vol: float = 0.15
    dry_run: bool = True

    @classmethod
    def from_env(cls) -> TraderConfig:
        mode_str = os.environ.get("TRADER_MODE", "paper").lower()
        mode_map = {
            "paper": ExecutionMode.PAPER,
            "approval": ExecutionMode.LIVE_APPROVAL,
            "live": ExecutionMode.LIVE,
            "futures": ExecutionMode.FUTURES,
        }
        raw_products = os.environ.get("TRADER_PRODUCTS", "").strip()
        products = [p.strip() for p in raw_products.split(",") if p.strip()] if raw_products else list(COINBASE_SPOT_PAIRS)
        dry_run_env = os.environ.get("COINBASE_DRY_RUN")
        return cls(
            execution_mode=mode_map.get(mode_str, ExecutionMode.PAPER),
            products=products,
            equity=float(os.environ.get("TRADER_EQUITY", "100")),
            dry_run=(dry_run_env.lower() == "true") if dry_run_env is not None else (mode_str not in {"live", "futures"}),
        )


class UnifiedTrader:
    def __init__(self, config: Optional[TraderConfig] = None):
        self.config = config or TraderConfig()
        self._shutdown_requested = False
        self._health: Dict[str, Any] = {
            "status": "starting", "tick_count": 0, "last_tick_ok": None,
            "last_error": None, "uptime_seconds": 0.0, "_started_at": time.time(),
        }
        self._lock = threading.Lock()

        self.fill_model = AdaptiveFillModel()
        self.scanner = OpportunityScanner()
        self.scanner.register_defaults()
        try:
            from .graph.register import register_graph_strategy
            register_graph_strategy(self.scanner, min_graph_score=0.45)
        except Exception as e:
            log.debug("Graph strategy registration skipped: %s", e)

        if self.config.execution_mode == ExecutionMode.PAPER:
            risk_limit = RiskLimit.AGGRESSIVE
        elif self.config.execution_mode == ExecutionMode.FUTURES:
            risk_limit = RiskLimit.MODERATE
        else:
            risk_limit = RiskLimit.MODERATE
        self.risk_mgr = RiskManager(limit=risk_limit)
        self.kelly = KellySizer()
        self.risk_appetite = DynamicRiskController()
        self.cb_client = CBClient()
        self.orchestrator = ExecutionOrchestrator(
            cb=self.cb_client, mode=self.config.execution_mode, dry_run=self.config.dry_run,
        )
        self.orchestrator.set_risk_appetite(self.risk_appetite)

        self.regime = RegimeDetector()
        self.blender = BayesianSignalBlender()
        self.confluence = MultiTimeframeConfluence()
        self.selector = AdaptiveStrategySelector()
        self.performance = PerformanceTracker()
        self.pipeline = DeploymentPipeline(self.performance)
        self.risk_parity = RiskParityPortfolio(target_vol=self.config.target_vol)
        self.corr_sizer = CorrelationAwareSizer()

        self.ticker_cache = TickerCache(ttl=10.0)
        self.feed = PollingFeed(
            cb_client=self.cb_client,
            cache=self.ticker_cache,
            poll_interval=5.0,
        )
        self.feed.subscribe(self.config.products)

        self.product_rotator = ProductRotator(top_n=3)
        self.rotation_strategy = MomentumRotationStrategy(self.product_rotator)
        self.mode_selector = AdaptiveModeSelector()
        self.dual_mm = DualMarketMaker()
        self.mm_strategy = MarketMakingStrategy(self.dual_mm)
        self.strategy_ranking = StrategyRanking()
        self.strategy_ranking.load()
        self.ranking_filter = StrategyRankingFilter(self.strategy_ranking)
        self.fear_greed = FearGreedIndex()
        self.fg_adapter = FearGreedSignalAdapter()

        for pid in self.config.products:
            vol = VOL_ESTIMATES.get(pid, 100_000)
            self.orchestrator.set_liquidity_24h(pid, vol)

        self._price_buffer: Dict[str, List[float]] = {}
        self._warm_price_buffer()
        self._last_regime_str: str = "unknown"
        self._last_regime_features: Optional[Any] = None
        self._last_market_profile: Optional[MarketConditionProfile] = None
        self._equity_last: float = 0.0
        self._tick_count: int = 0
        self._graph_overlay_cache: Dict[str, float] = {}
        self._graph_overlay_cache_ts: float = 0.0
        self._graph_overlay_ttl: float = 300.0
        self._static_products = set() if self.config.execution_mode == ExecutionMode.FUTURES else set(STATIC_LONG_TERM_PRODUCTS)

    def _warm_price_buffer(self):
        for pid in self.config.products:
            if pid not in self._price_buffer or len(self._price_buffer[pid]) < 30:
                seed = VOL_ESTIMATES.get(pid, 50000)
                noise = [seed * (1 + (i % 7 - 3) * 0.002) for i in range(30)]
                self._price_buffer[pid] = noise

    @staticmethod
    def _make_bar(close: float, high: float = None, low: float = None,
                  open_: float = None, volume: float = 0) -> Bar:
        c = close
        h = high or c * 1.001
        lv = low or c * 0.999
        o = open_ or c * 0.9995
        return Bar(timestamp=time.time(), open=o, high=h, low=lv, close=c, volume=volume)

    def _validate_config(self):
        errors = []
        if not self.config.products:
            errors.append("No products configured")
        if self.config.equity <= 0:
            errors.append(f"Equity must be positive, got {self.config.equity}")
        if self.config.poll_interval_secs <= 0:
            errors.append(f"Poll interval must be positive, got {self.config.poll_interval_secs}")
        if self.config.max_positions <= 0:
            errors.append(f"Max positions must be positive, got {self.config.max_positions}")
        if self.config.execution_mode != ExecutionMode.PAPER and not shutil.which(self.cb_client.cli):
            errors.append(f"Coinbase CLI not found for live mode: {self.cb_client.cli}")
        if self.config.execution_mode == ExecutionMode.FUTURES and not self.config.dry_run:
            if not os.getenv("COINBASE_API_KEY") or not os.getenv("COINBASE_API_SECRET"):
                errors.append("COINBASE_API_KEY and COINBASE_API_SECRET are required for futures mode")
        if errors:
            raise ValueError("; ".join(errors))

    def start(self):
        try:
            self._validate_config()
        except ValueError as e:
            log.error("Startup validation failed: %s", e)
            self._health["status"] = "failed"
            self._health["last_error"] = str(e)
            return

        log.info("Trader starting — mode=%s products=%s equity=%.0f dry_run=%s",
                 self.config.execution_mode.value, self.config.products,
                 self.config.equity, self.config.dry_run)
        self._health["status"] = "running"

        self.feed.start()

        def _signal_handler(signum, frame):
            log.info("Received signal %d, shutting down...", signum)
            self._shutdown_requested = True
        signal.signal(signal.SIGTERM, _signal_handler)
        signal.signal(signal.SIGINT, _signal_handler)

        try:
            while not self._shutdown_requested:
                tick_start = time.time()
                with self._lock:
                    self._tick()
                elapsed = time.time() - tick_start
                sleep_time = max(0.01, self.config.poll_interval_secs - elapsed)
                if self._shutdown_requested:
                    break
                time.sleep(sleep_time)
        except KeyboardInterrupt:
            log.info("Shutdown requested")
        except Exception as e:
            log.error("Fatal error in main loop: %s", e)
            self._health["status"] = "crashed"
            self._health["last_error"] = str(e)
        finally:
            self._shutdown()

    def _shutdown(self):
        log.info("Shutting down trader...")
        try:
            self.strategy_ranking.save()
            log.info("Ranking state saved")
        except Exception as e:
            log.warning("Failed to save ranking state: %s", e)
        try:
            self.feed.stop()
        except Exception as e:
            log.warning("Feed stop error: %s", e)
        self._health["status"] = "stopped"
        log.info("Trader stopped")

    def _tick(self):
        self._tick_count += 1
        ticker_map = self._fetch_all_tickers()
        if not ticker_map:
            try:
                self.feed._poll_once()
            except Exception as e:
                log.debug("Feed priming retry failed: %s", e)
            ticker_map = self._fetch_all_tickers(allow_synthetic=self.config.dry_run)

        if not ticker_map:
            log.warning("No prices available, skipping tick")
            return

        prices = {pid: t.price for pid, t in ticker_map.items()}
        bars, history_bars = self._build_bar_history(ticker_map)
        try:
            self.orchestrator.bucket_ledger.mark_prices(prices)
        except Exception:
            pass

        prices_list = list(prices.values())
        if len(prices_list) >= 30:
            try:
                regime, regime_features = self.regime.detect(prices_list)
                self._last_regime_str = regime.value if regime else "unknown"
                self._last_regime_features = regime_features
            except Exception as e:
                log.warning("Regime detection failed: %s", e)
                regime_features = self._last_regime_features
        else:
            regime_features = self._last_regime_features

        if regime_features:
            try:
                self._update_from_regime(regime_features, prices)
            except Exception as e:
                log.warning("Regime update failed: %s", e)

        equity_now = self.config.equity
        if abs(equity_now - self._equity_last) > 1.0:
            self.orchestrator.update_state(
                equity=equity_now, cash=equity_now, open_positions={},
            )
            self._equity_last = equity_now

        news_pulse = 0.0
        breaking_topics = []
        try:
            if hasattr(self.orchestrator.news_risk, 'kg') and self.orchestrator.news_risk.kg is not None:
                news_pulse = self.orchestrator.news_risk.kg.global_sentiment_pulse()
                breaking_topics = self.orchestrator.news_risk.kg.get_breaking_topics()
        except Exception:
            pass

        market_profile = self._build_market_profile(
            prices, regime_features, news_pulse, breaking_topics
        )
        self._last_market_profile = market_profile
        self.orchestrator.update_market_profile(market_profile)

        try:
            opportunities = self._gather_opportunities(prices, bars, history_bars)
        except Exception as e:
            log.warning("Opportunity gathering failed: %s", e)
            opportunities = []

        if opportunities:
            try:
                opportunities = self._filter_by_regime(opportunities)
            except Exception as e:
                log.warning("Regime filter failed: %s", e)
            try:
                opportunities = self._apply_confluence(opportunities, prices)
            except Exception as e:
                log.warning("Confluence failed: %s", e)
            try:
                opportunities = self._apply_product_rotation(opportunities, prices)
            except Exception as e:
                log.warning("Product rotation failed: %s", e)

        try:
            fg_opps = self._apply_fear_greed(prices, bars)
            opportunities.extend(fg_opps)
        except Exception as e:
            log.warning("Fear/greed failed: %s", e)
        try:
            mm_opps = self._apply_market_making(prices, bars)
            opportunities.extend(mm_opps)
        except Exception as e:
            log.warning("Market making failed: %s", e)

        if opportunities:
            try:
                opportunities = self._blend_signals(opportunities)
            except Exception as e:
                log.warning("Signal blend failed: %s", e)
            try:
                opportunities = self._apply_graph_overlay(opportunities)
            except Exception as e:
                log.warning("Graph overlay failed: %s", e)
            try:
                opportunities = self._apply_risk_parity(opportunities, prices)
            except Exception as e:
                log.warning("Risk parity failed: %s", e)
            try:
                if hasattr(self.orchestrator.state, 'open_positions') and self.orchestrator.state.open_positions:
                    opportunities = self._apply_correlation_sizing(opportunities)
            except Exception as e:
                log.warning("Correlation sizing failed: %s", e)
            try:
                opportunities = self._apply_kelly_sizing(opportunities)
            except Exception as e:
                log.warning("Kelly sizing failed: %s", e)

        signals: List = []
        try:
            atr_map = {pid: 0.02 * prices.get(pid, 1) for pid in prices}
            signals = self.orchestrator.process_opportunities(opportunities, atr_map)
            for pid in prices:
                price = prices[pid]
                if price > 0:
                    fee_sig = self.orchestrator.generate_fee_volume(pid, price, atr_map.get(pid, 0))
                    if fee_sig:
                        signals.append(fee_sig)
        except Exception as e:
            log.warning("Signal processing failed: %s", e)

        if signals:
            try:
                results = self.orchestrator.execute_signals(signals)
                for sig, result in zip(signals, results):
                    if result.get("success"):
                        self.performance.record_trade(
                            strategy=sig.strategy_name, product_id=sig.product_id,
                            direction=sig.direction, entry=sig.entry_price,
                            exit=None, pnl=0.0, r_multiple=0.0, fees=0.0,
                        )
                    self.orchestrator.strategy_ranking.record_trade(
                        sig.strategy_name,
                        result.get("notional", 0) * (1 if result.get("success") else -1),
                        sig.confidence,
                    )
            except Exception as e:
                log.warning("Trade execution failed: %s", e)

        try:
            self._log_summary(opportunities, signals, prices)
        except Exception as e:
            log.warning("Log summary failed: %s", e)

        self._health["tick_count"] = self._tick_count
        self._health["last_tick_ok"] = True
        if self._health.get("status") in {"starting", "running"}:
            self._health["status"] = "running"

    def _fetch_all_tickers(self, allow_synthetic: bool = False) -> Dict[str, Any]:
        tickers = {}
        for pid in self.config.products:
            t = self.ticker_cache.get_ticker(pid)
            if t:
                tickers[pid] = t
        if tickers or not allow_synthetic:
            return tickers

        # In dry-run mode we keep the loop alive with seeded prices if the live
        # feed is temporarily empty.
        now = time.time()
        for pid, buf in self._price_buffer.items():
            if not buf:
                continue
            price = float(buf[-1])
            tickers[pid] = Ticker(
                product_id=pid,
                price=price,
                bid=price * 0.999,
                ask=price * 1.001,
                volume_24h=0.0,
                timestamp=now,
                source=FeedSource.SYNTHETIC,
            )
        return tickers

    def _build_bar_history(self, ticker_map: Dict[str, Any]) -> Tuple[Dict[str, Bar], Dict[str, List[Bar]]]:
        bars = {}
        history_bars = {}
        for pid, ticker in ticker_map.items():
            price = ticker.price
            buf = self._price_buffer.setdefault(pid, [])
            buf.append(price)
            if len(buf) > 200:
                buf.pop(0)

            bar = self._make_bar(price)
            bars[pid] = bar
            history_bars[pid] = [self._make_bar(p) for p in buf[:-1]]
        return bars, history_bars

    def _update_from_regime(self, rf, prices: Dict[str, float]):
        vol_bps = (rf.volatility * 10000) if hasattr(rf, 'volatility') and rf.volatility else 30
        adx_val = rf.adx if hasattr(rf, 'adx') else 25.0
        trend_st = rf.trend_strength if hasattr(rf, 'trend_strength') else 0.0
        regime_label = getattr(rf.regime, "value", rf.regime)
        regime_label = str(regime_label)

        self.selector.set_regime(rf.regime)
        self.risk_appetite.update_regime(regime_label, 0.02)
        fg_val = float(self.fear_greed._cache.value) if hasattr(self.fear_greed, '_cache') else 50.0
        self.mode_selector.update(
            regime=regime_label, volatility_bps=vol_bps,
            fear_greed_value=fg_val, adx=adx_val,
            trend_strength=trend_st,
        )

    def _build_market_profile(self, prices: Dict[str, float], rf, news_pulse: float,
                               breaking_topics: List[str]) -> MarketConditionProfile:
        if rf:
            vol_bps = (rf.volatility * 10000) if hasattr(rf, 'volatility') and rf.volatility else 30
            adx_val = rf.adx if hasattr(rf, 'adx') else 25.0
            trend_st = rf.trend_strength if hasattr(rf, 'trend_strength') else 0.0
            hurst_v = rf.hurst_exponent if hasattr(rf, 'hurst_exponent') else 0.5
            ser_corr = rf.serial_correlation if hasattr(rf, 'serial_correlation') else 0.0
            vol_trend = rf.volume_trend if hasattr(rf, 'volume_trend') else 0.0
        else:
            vol_bps, adx_val, trend_st = 30, 25, 0.0
            hurst_v, ser_corr, vol_trend = 0.5, 0.0, 0.0

        fg_val = float(self.fear_greed._cache.value) if hasattr(self.fear_greed, '_cache') else 50.0

        return MarketConditionProfile(
            regime=self._last_regime_str,
            fear_greed=fg_val,
            news_sentiment_pulse=news_pulse,
            trend_strength=trend_st,
            volatility_bps=vol_bps,
            adx=adx_val,
            hurst=hurst_v,
            serial_correlation=ser_corr,
            volume_trend=vol_trend,
            breaking_news_ratio=0.0,
            has_hacks="hacks_security" in breaking_topics,
            has_regulation="regulation" in breaking_topics,
        )

    def _gather_opportunities(self, prices: Dict[str, float],
                                bars: Dict[str, Bar],
                                history_bars: Dict[str, List[Bar]]) -> List[Opportunity]:
        all_opps = []
        for pid in self.config.products:
            if pid in self._static_products:
                continue
            price = prices.get(pid)
            bar = bars.get(pid)
            history = history_bars.get(pid, [])
            if price is None or bar is None:
                continue
            try:
                opps = self.scanner.scan(
                    product_id=pid, bar=bar, history=history,
                    atr=0.02 * price,
                )
                all_opps.extend(opps)
            except Exception as e:
                log.warning("Scan failed for %s: %s", pid, e)
        return all_opps

    def _filter_by_regime(self, opportunities: List[Opportunity]) -> List[Opportunity]:
        if not self._last_regime_features:
            return opportunities
        filtered = []
        for opp in opportunities:
            mapping = self.selector.select(opp.strategy_name)
            if mapping.get("enabled", True):
                filtered.append(opp)
        return filtered

    def _apply_confluence(self, opportunities: List[Opportunity],
                           prices: Dict[str, float]) -> List[Opportunity]:
        if not opportunities:
            return opportunities
        for opp in opportunities:
            try:
                price = prices.get(opp.product_id, opp.entry_price)
                signal = TimeframeSignal(
                    timeframe="1h", direction=opp.direction,
                    confidence=opp.confidence, price=price, reason=opp.reason,
                )
                cr = ConfluenceResult(
                    product_id=opp.product_id,
                    overall_direction=opp.direction,
                    confidence=opp.confidence,
                    agreement_pct=0.5,
                    timeframe_signals=[signal],
                    dominant_timeframe="1h",
                    divergence_detected=False,
                )
                result = self.confluence.boost_opportunity(opp, cr)
                if result:
                    opp.confidence = result.confidence
            except Exception:
                continue
        return opportunities

    def _blend_signals(self, opportunities: List[Opportunity]) -> List[Opportunity]:
        if len(opportunities) < 2:
            return opportunities
        try:
            return self.blender.blend_signals(opportunities, {})
        except Exception:
            return opportunities

    def _apply_risk_parity(self, opportunities: List[Opportunity],
                            prices: Dict[str, float]) -> List[Opportunity]:
        vols = {pid: 0.02 for pid in prices}
        return self.risk_parity.risk_budget_sizing(opportunities, self.config.equity, vols)

    def _apply_correlation_sizing(self, opportunities: List[Opportunity]) -> List[Opportunity]:
        for opp in opportunities:
            adjustment = self.corr_sizer.adjust_size(opp.product_id, opp.base_size, {})
            opp.base_size = adjustment.adjusted_size
        return opportunities

    def _apply_kelly_sizing(self, opportunities: List[Opportunity]) -> List[Opportunity]:
        for opp in opportunities:
            win_rate = self.performance.get_win_rate(opp.strategy_name)
            avg_win, avg_loss = 0.02, 0.01
            kelly_pct = self.kelly.fractional_kelly(win_rate, avg_win, avg_loss)
            notional = kelly_pct * self.config.equity
            price = opp.entry_price or 1.0
            opp.base_size = notional / max(price, 1e-9)
            try:
                capped_size, bucket_id = self.orchestrator.bucket_ledger.apply_opportunity_limits(
                    opp.strategy_name, opp.product_id, price, opp.base_size, opp.quote_size,
                )
                if capped_size <= 0:
                    opp.base_size = 0.0
                    continue
                opp.base_size = min(opp.base_size, capped_size)
                opp.quote_size = opp.base_size * price
                opp.meta["bucket_id"] = bucket_id or "challenge"
                opp.meta["challenge_bucket"] = bucket_id or "challenge"
            except Exception:
                pass
        return opportunities

    def _apply_product_rotation(self, opportunities: List[Opportunity],
                                 prices: Dict[str, float]) -> List[Opportunity]:
        for pid in prices:
            price = prices[pid]
            bar = self._make_bar(price)
            self.product_rotator.record_bar(pid, bar.close, 0)
        self.product_rotator.rebalance()
        top = set(self.product_rotator.ranked_products)
        return [o for o in opportunities if o.product_id in top and o.product_id not in self._static_products]

    def _apply_graph_overlay(self, opportunities: List[Opportunity]) -> List[Opportunity]:
        if not opportunities:
            return opportunities
        now = time.time()
        overlays = self._graph_overlay_cache
        if not overlays or (now - self._graph_overlay_cache_ts) > self._graph_overlay_ttl:
            try:
                from .graph.portfolio_overlay import fetch_graph_weight_overlays
                product_ids = sorted({opp.product_id for opp in opportunities})
                overlays = fetch_graph_weight_overlays(product_ids, max_boost=0.25)
                self._graph_overlay_cache = overlays
                self._graph_overlay_cache_ts = now
            except Exception as e:
                log.debug("Graph overlay unavailable: %s", e)
                return opportunities

        for opp in opportunities:
            overlay = overlays.get(opp.product_id, 1.0)
            opp.confidence = min(0.99, opp.confidence * overlay)
            opp.score = opp.score * overlay
        return opportunities

    def _apply_fear_greed(self, prices: Dict[str, float],
                           bars: Dict[str, Bar]) -> List[Opportunity]:
        closes_dict = {pid: self._price_buffer.get(pid, []) for pid in prices}
        if all(len(c) < 30 for c in closes_dict.values()):
            return []
        try:
            fg_snapshot = self.fear_greed.compute(closes_dict)
        except Exception:
            return []
        opps = []
        for pid in prices:
            bar = bars.get(pid)
            if bar is None:
                continue
            try:
                history = [self._make_bar(p) for p in self._price_buffer.get(pid, [])[:-1]]
                setup = self.fg_adapter.on_bar(bar, history)
                if setup:
                    opp = Opportunity(
                        product_id=pid, direction=setup.direction,
                        instrument_type=InstrumentType.SPOT,
                        entry_price=setup.entry_price,
                        stop_price=setup.stop_price,
                        target_price=setup.target_price,
                        risk_reward=setup.risk_reward,
                        confidence=setup.confidence,
                        reason=setup.reason,
                        strategy_name=setup.strategy_name,
                        atr=setup.atr,
                        score=setup.confidence * 10,
                        meta={"fear_greed": fg_snapshot.value},
                    )
                    opps.append(opp)
            except Exception:
                continue
        return opps

    def _apply_market_making(self, prices: Dict[str, float],
                              bars: Dict[str, Bar]) -> List[Opportunity]:
        opps = []
        for pid in prices:
            bar = bars.get(pid)
            if bar is None:
                continue
            try:
                history = [self._make_bar(p) for p in self._price_buffer.get(pid, [])[:-1]]
                self.mm_strategy.set_product_id(pid)
                setup = self.mm_strategy.on_bar(bar, history)
                if setup:
                    opp = Opportunity(
                        product_id=pid, direction=setup.direction,
                        instrument_type=InstrumentType.SPOT,
                        entry_price=setup.entry_price,
                        stop_price=setup.stop_price,
                        target_price=setup.target_price,
                        risk_reward=setup.risk_reward,
                        confidence=setup.confidence,
                        reason=setup.reason,
                        strategy_name=setup.strategy_name,
                        atr=setup.atr,
                        score=setup.confidence * 8,
                        meta=setup.metadata or {},
                    )
                    opps.append(opp)
            except Exception:
                continue
        return opps

    def _log_summary(self, opportunities: List[Opportunity],
                     signals: List, prices: Dict[str, float]):
        appetite = self.risk_appetite.snapshot()
        status = self.orchestrator.status()
        market_str = ""
        if self._last_market_profile:
            try:
                market_str = self.orchestrator.market_selector.market_summary()
            except Exception:
                pass
        log.info(
            "Tick #%d — regime=%s appetite=%.2f(%s) opps=%d signals=%d "
            "fee_tier_vol=$%.0f prices=%s",
            self._tick_count, self._last_regime_str,
            appetite.score, appetite.profile_label,
            len(opportunities), len(signals),
            status.get("fee_tier_volume_30d", 0),
            {k: f"${v:.2f}" for k, v in prices.items()},
        )
        if market_str:
            log.info("  market: %s", market_str)
        if appetite.gating_reasons:
            log.info("  risk gates: %s", ", ".join(appetite.gating_reasons))
        try:
            bucket_summary = self.orchestrator.bucket_ledger.summary(prices)
            log.info("  buckets: %s", bucket_summary)
        except Exception:
            pass
        for s in signals[:5]:
            log.info("  %s %s %s @ %.2f sz=%.4f conf=%.3f",
                     s.product_id, s.strategy_name, s.direction.value,
                     s.entry_price, s.size, s.confidence)


    def health(self) -> Dict[str, Any]:
        with self._lock:
            h = dict(self._health)
        h["uptime_seconds"] = time.time() - h.pop("_started_at", time.time())
        h["products"] = self.config.products
        h["mode"] = self.config.execution_mode.value
        h["equity"] = self.config.equity
        h["regime"] = self._last_regime_str
        try:
            h["open_positions"] = len(self.orchestrator.state.open_positions)
            h["ranked_strategies"] = self.strategy_ranking.summary().get("total_tracked", 0)
        except Exception:
            pass
        return h


def start_health_server(trader: UnifiedTrader, port: int = 9090):
    import http.server
    import json

    class HealthHandler(http.server.BaseHTTPRequestHandler):
        def do_GET(self):
            if self.path == "/health":
                data = json.dumps(trader.health(), indent=2).encode()
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Access-Control-Allow-Origin", "*")
                self.end_headers()
                self.wfile.write(data)
            else:
                self.send_response(404)
                self.end_headers()

        def log_message(self, fmt, *args):
            pass

    server = http.server.HTTPServer(("", port), HealthHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    log.info("Health server on http://0.0.0.0:%d/health", port)
    return server


def main():
    parser = argparse.ArgumentParser(description="Unified Trader v2")
    parser.add_argument("--mode", choices=["paper", "approval", "live", "futures"], default=None)
    parser.add_argument("--products", nargs="+", default=None)
    parser.add_argument("--equity", type=float, default=None)
    parser.add_argument("--no-dry-run", action="store_true")
    parser.add_argument("--poll-interval", type=float, default=None)
    parser.add_argument("--health-port", type=int, default=0)
    parser.add_argument("--futures-portfolio-uuid", default=os.environ.get("COINBASE_FUTURES_PORTFOLIO_UUID", ""))
    parser.add_argument("--futures-margin-type", default=os.environ.get("COINBASE_FUTURES_MARGIN_TYPE", "CROSS"))
    parser.add_argument("--futures-default-leverage", type=float, default=float(os.environ.get("COINBASE_FUTURES_DEFAULT_LEVERAGE", "2.0")))
    args = parser.parse_args()

    config = TraderConfig.from_env()
    if args.mode:
        mode_map = {
            "paper": ExecutionMode.PAPER,
            "approval": ExecutionMode.LIVE_APPROVAL,
            "live": ExecutionMode.LIVE,
            "futures": ExecutionMode.FUTURES,
        }
        config.execution_mode = mode_map[args.mode]
    if args.products:
        config.products = args.products
    if args.equity:
        config.equity = args.equity
    if args.no_dry_run:
        config.dry_run = False
    elif args.mode in {"live", "futures"} and os.environ.get("COINBASE_DRY_RUN") is None:
        config.dry_run = False
    if args.poll_interval:
        config.poll_interval_secs = args.poll_interval

    if args.futures_portfolio_uuid:
        os.environ["COINBASE_FUTURES_PORTFOLIO_UUID"] = args.futures_portfolio_uuid
    if args.futures_margin_type:
        os.environ["COINBASE_FUTURES_MARGIN_TYPE"] = args.futures_margin_type
    if args.futures_default_leverage:
        os.environ["COINBASE_FUTURES_DEFAULT_LEVERAGE"] = str(args.futures_default_leverage)

    trader = UnifiedTrader(config)
    if args.health_port:
        start_health_server(trader, args.health_port)
    trader.start()


if __name__ == "__main__":
    main()
