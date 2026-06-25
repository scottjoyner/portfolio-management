from __future__ import annotations
import fcntl
import os
import time
import uuid
import logging
from dataclasses import dataclass, field
from typing import List, Dict, Any, Optional, Callable
from enum import Enum

from .cb_client import CBClient
from .execution_v2 import NativeExecutionEngine, BracketManager, OrderIntent, OrderType
from .data import fetch_candles_df, compute_atr
from .protocols import Direction, Opportunity, BaseStrategy, InstrumentType
from .risk_manager import RiskManager, RiskProfile, RiskLimit, PositionRisk, PortfolioRisk, KellySizer
from .risk_appetite import DynamicRiskController, RiskAppetiteSnapshot
from .fee_optimizer import FeeTracker, FeeAwareSizer, VolumeGenerator
from .liquidity_sizer import LiquidityAwareSizer, OrderBookDepthEstimator, MarketImpactModel
from .fill_model import AdaptiveFillModel
from .ranking import StrategyRanking, StrategyRankingFilter
from .dual_mm import DualMarketMaker, MarketMakingStrategy
from .adaptive_mode import AdaptiveModeSelector
from .fear_greed import FearGreedSignalAdapter
from .news_risk import NewsRiskAdjuster
from .market_condition import MarketConditionStrategySelector, MarketConditionProfile
from .capital_buckets import CapitalBucketLedger
log = logging.getLogger(__name__)


class TradeMode(Enum):
    PAPER = "paper"
    LIVE_APPROVAL = "live_approval"
    LIVE = "live"
    FUTURES = "futures"


@dataclass
class TradeSignal:
    product_id: str
    direction: Direction
    entry_price: float
    stop_price: float
    target_price: float
    size: float
    confidence: float
    reason: str
    strategy_name: str
    instrument_type: InstrumentType = InstrumentType.SPOT
    leverage: float = 1.0
    opportunity_score: float = 0.0
    bucket_id: str = ""


@dataclass
class ExecutionState:
    mode: TradeMode
    equity: float = 0.0
    cash: float = 0.0
    open_positions: Dict[str, Any] = field(default_factory=dict)
    pending_approvals: List[Dict[str, Any]] = field(default_factory=list)
    daily_trades: int = 0
    daily_volume: float = 0.0
    timestamp: float = 0.0


class ExecutionOrchestrator:
    def __init__(
        self,
        cb: Optional[CBClient] = None,
        mode: TradeMode = TradeMode.PAPER,
        risk_profile: Optional[RiskProfile] = None,
        risk_limit: RiskLimit = RiskLimit.MODERATE,
        dry_run: bool = True,
        futures_portfolio_uuid: str = "",
        futures_margin_type: str = "CROSS",
        futures_default_leverage: float = 2.0,
    ):
        self.cb = cb
        if self.cb is None and mode != TradeMode.PAPER:
            try:
                self.cb = CBClient()
            except Exception as e:
                log.warning("Failed to create CBClient (%s), falling back to paper mode", e)
                mode = TradeMode.PAPER
        elif self.cb is None:
            pass
        self.mode = mode
        self.dry_run = dry_run
        self.exec_engine = NativeExecutionEngine(self.cb, dry_run) if self.cb else None
        self.bracket_mgr = BracketManager(self.exec_engine)
        self.futures_exec = None
        if self.mode == TradeMode.FUTURES and not self.dry_run:
            try:
                from .futures_execution import CoinbaseFuturesExecutor
                api_key = os.getenv("COINBASE_API_KEY", "")
                api_secret = os.getenv("COINBASE_API_SECRET", "")
                base_url = os.getenv("COINBASE_API_BASE_URL", "api.coinbase.com")
                timeout = int(os.getenv("CB_TIMEOUT_S", "30"))
                portfolio_uuid = futures_portfolio_uuid or os.getenv("COINBASE_FUTURES_PORTFOLIO_UUID", "")
                margin_type = futures_margin_type or os.getenv("COINBASE_FUTURES_MARGIN_TYPE", "CROSS")
                default_leverage = float(os.getenv("COINBASE_FUTURES_DEFAULT_LEVERAGE", str(futures_default_leverage)))
                self.futures_exec = CoinbaseFuturesExecutor(
                    api_key=api_key,
                    api_secret=api_secret,
                    base_url=base_url,
                    timeout=timeout,
                    portfolio_uuid=portfolio_uuid,
                    margin_type=margin_type,
                    default_leverage=default_leverage,
                )
                try:
                    self.futures_exec.validate()
                    log.info("Futures execution engine enabled (portfolio_uuid=%s)", bool(portfolio_uuid))
                except Exception as exc:
                    raise RuntimeError(f"Futures validation failed: {exc}") from exc
            except Exception as e:
                raise RuntimeError(f"Failed to enable futures executor: {e}") from e
        self.risk_mgr = RiskManager(risk_profile, risk_limit)
        self.kelly = KellySizer()
        self.fill_model = AdaptiveFillModel()
        self.state = ExecutionState(mode=mode)
        self._strategy_performance: Dict[str, Dict[str, float]] = {}
        self._listeners: List[Callable] = []
        self.risk_appetite: Optional[DynamicRiskController] = None
        self.fee_tracker = FeeTracker()
        self.fee_sizer = FeeAwareSizer(self.fee_tracker)
        self.volume_generator = VolumeGenerator(self.fee_tracker)
        self.liquidity_sizer = LiquidityAwareSizer()
        self.strategy_ranking = StrategyRanking()
        self.ranking_filter = StrategyRankingFilter(self.strategy_ranking)
        self.dual_mm = DualMarketMaker()
        self.mm_strategy = MarketMakingStrategy(self.dual_mm)
        self.mode_selector = AdaptiveModeSelector()
        self.news_risk = NewsRiskAdjuster()
        self.market_selector = MarketConditionStrategySelector()
        self.bucket_ledger = CapitalBucketLedger.from_env()
        if self.mode == TradeMode.PAPER:
            seed_cash = float(self.bucket_ledger.summary().get("total_value_usd", 0.0))
            self.state.cash = seed_cash
            self.state.equity = seed_cash

    def set_risk_appetite(self, controller: DynamicRiskController):
        self.risk_appetite = controller

    def set_liquidity_24h(self, product_id: str, volume_24h: float):
        self.liquidity_sizer.set_volume_24h(product_id, volume_24h)

    def register_listener(self, fn: Callable):
        self._listeners.append(fn)

    def update_state(self, equity: float, cash: float,
                     open_positions: Dict[str, Any]):
        self.state.equity = equity
        self.state.cash = cash
        self.state.open_positions = open_positions
        self.state.timestamp = time.time()
        if self.risk_appetite:
            self.risk_appetite.update_equity(equity)

    def _apply_risk_appetite_profile(self):
        if self.risk_appetite:
            dynamic_profile = self.risk_appetite.get_profile()
            self.news_risk.adjust_profile(dynamic_profile)
            self.risk_mgr.profile = dynamic_profile

    def update_market_profile(self, profile: MarketConditionProfile):
        self.market_selector.evaluate(profile)

    def process_opportunities(self, opportunities: List[Opportunity],
                                atr_map: Optional[Dict[str, float]] = None) -> List[TradeSignal]:
        self._apply_risk_appetite_profile()
        signals: List[TradeSignal] = []
        position_risks = self._build_position_risks()

        portfolio_check = self.risk_mgr.check_portfolio(position_risks, self.state.equity)
        if not portfolio_check.passed_checks:
            log.warning(f"Portfolio risk check failed: {portfolio_check.failures}")
            return signals

        ranking_weights = {}
        try:
            ranking_weights = self.ranking_filter._ranking.rebalance_weights()
        except Exception:
            pass
        top_ranked = set()
        try:
            top_ranked = set(self.ranking_filter._ranking.top_strategies())
        except Exception:
            pass

        has_ranking = bool(top_ranked)
        conf_mode_map = {"hold": 0.3, "scalp": 0.6}
        mode_profile = self.mode_selector.profile()
        conf_cap = conf_mode_map.get(self.mode_selector.current_mode.value, 1.0)

        _news_cache: Dict[str, Any] = {}

        for opp in opportunities:
            try:
                if not self.market_selector.is_enabled(opp.strategy_name):
                    continue
            except Exception:
                pass

            try:
                if has_ranking and opp.strategy_name not in top_ranked:
                    continue
                w = ranking_weights.get(opp.strategy_name, 0.0)
                opp.confidence = min(0.99, opp.confidence * (0.5 + w))
            except Exception:
                pass

            risk_amount = abs(opp.entry_price - opp.stop_price) * opp.base_size
            risk_pct = risk_amount / max(self.state.equity, 1e-9)

            opp = self.fee_sizer.size_with_fee_boost(opp)

            atr = (atr_map or {}).get(opp.product_id, opp.atr or 0.0)
            if atr > 0:
                opp = self.liquidity_sizer.size_with_liquidity(opp, atr)

            opp.confidence = min(opp.confidence, conf_cap)

            try:
                if opp.product_id not in _news_cache:
                    _news_cache[opp.product_id] = self.news_risk.assess_product(opp.product_id)
                assessment = _news_cache[opp.product_id]
                if assessment.article_count > 0:
                    opp = self.news_risk.adjust_opportunity(opp)
            except Exception:
                pass

            perf = self._strategy_performance.get(opp.strategy_name, {})
            win_rate = perf.get("win_rate", 0.5)
            avg_win = perf.get("avg_win", 1.0)
            avg_loss = perf.get("avg_loss", 1.0)
            kelly_frac = self.kelly.fraction(win_rate, avg_win, avg_loss)
            kelly_size = self.kelly.size_for_risk(
                self.state.equity, kelly_frac * opp.total_risk_pct,
                opp.entry_price, opp.stop_price, opp.leverage,
            )
            final_size = min(opp.base_size, kelly_size) if opp.base_size > 0 else kelly_size
            final_size = max(final_size, 0.0)

            passed, reason = self.risk_mgr.check_trade(
                opp.product_id, opp.direction.value, final_size,
                opp.entry_price, opp.stop_price, opp.target_price,
                self.state.equity, position_risks,
            )
            if not passed:
                log.info(f"[SKIP] {opp.product_id} {opp.direction.value}: {reason}")
                continue

            signal = TradeSignal(
                product_id=opp.product_id,
                direction=opp.direction,
                entry_price=opp.entry_price,
                stop_price=opp.stop_price,
                target_price=opp.target_price,
                size=final_size,
                confidence=opp.confidence,
                reason=opp.reason,
                strategy_name=opp.strategy_name,
                instrument_type=opp.instrument_type,
                leverage=opp.leverage,
                opportunity_score=opp.score,
                bucket_id=str(opp.meta.get("bucket_id", "")),
            )
            signals.append(signal)

        signals.sort(key=lambda s: s.opportunity_score, reverse=True)
        return signals

    def execute_signals(self, signals: List[TradeSignal]) -> List[Dict[str, Any]]:
        results = []
        for sig in signals:
            if self.mode == TradeMode.PAPER:
                result = self._paper_execute(sig)
            elif self.mode == TradeMode.LIVE_APPROVAL:
                result = self._approval_execute(sig)
            elif self.mode == TradeMode.FUTURES:
                result = self._futures_execute(sig)
            else:
                result = self._live_execute(sig)
            results.append(result)
            for listener in self._listeners:
                try:
                    listener(sig, result)
                except Exception:
                    pass
        self.state.daily_trades += len(results)
        self.state.daily_volume += sum(
            r.get("notional", 0) for r in results if r.get("success")
        )
        return results

    def _futures_execute(self, sig: TradeSignal) -> Dict[str, Any]:
        if self.dry_run or not self.futures_exec:
            bucket_id = sig.bucket_id or self.bucket_ledger.allocate(sig.strategy_name, sig.product_id, sig.size * sig.entry_price) or "challenge"
            return {
                "success": True,
                "product_id": sig.product_id,
                "side": sig.direction.value,
                "size": sig.size,
                "entry": sig.entry_price,
                "notional": sig.size * sig.entry_price,
                "mode": "futures",
                "status": "dry_run",
                "leverage": sig.leverage,
                "bucket_id": bucket_id,
            }

        result = self.futures_exec.place_bracket(
            symbol=sig.product_id,
            side=sig.direction.value,
            base_size=sig.size,
            stop_price=sig.stop_price,
            target_price=sig.target_price,
            leverage=sig.leverage,
        )
        notional = sig.size * sig.entry_price
        self.fee_tracker.record_trade(notional)
        self.volume_generator.record_generated(notional)
        bucket_id = sig.bucket_id or self.bucket_ledger.allocate(sig.strategy_name, sig.product_id, notional) or "challenge"
        sig.bucket_id = bucket_id
        if result.success:
            self.bucket_ledger.open_position(bucket_id, sig.product_id, sig.direction.value, sig.size, sig.entry_price, sig.strategy_name)
            self.state.open_positions[sig.product_id] = {
                "direction": sig.direction.value,
                "size": sig.size,
                "entry": sig.entry_price,
                "stop": sig.stop_price,
                "target": sig.target_price,
                "strategy": sig.strategy_name,
                "bucket_id": bucket_id,
                "timestamp": time.time(),
                "leverage": sig.leverage,
            }
        return {
            "success": result.success,
            "product_id": sig.product_id,
            "side": sig.direction.value,
            "size": sig.size,
            "entry": sig.entry_price,
            "order_id": result.order_id,
            "notional": notional,
            "mode": "futures",
            "status": "open" if result.success else "failed",
            "leverage": sig.leverage,
            "raw": result.raw,
            "error": result.error,
            "bucket_id": bucket_id,
        }

    def _live_execute(self, sig: TradeSignal) -> Dict[str, Any]:
        if not self.cb or not self.exec_engine:
            return {"success": False, "reason": "no CB client", "product_id": sig.product_id}
        bracket = self.bracket_mgr.place_bracket(
            product_id=sig.product_id,
            side=sig.direction.value,
            base_size=sig.size,
            entry_price=sig.entry_price,
            stop_price=sig.stop_price,
            target_price=sig.target_price,
            strategy_id=sig.strategy_name,
        )
        notional = sig.size * sig.entry_price
        self.fee_tracker.record_trade(notional)
        self.volume_generator.record_generated(notional)
        bucket_id = sig.bucket_id or self.bucket_ledger.allocate(sig.strategy_name, sig.product_id, notional) or "challenge"
        sig.bucket_id = bucket_id
        entry_order = bracket.get("entry_order")
        if getattr(entry_order, "success", False):
            self.bucket_ledger.open_position(bucket_id, sig.product_id, sig.direction.value, sig.size, sig.entry_price, sig.strategy_name)
            self.state.open_positions[sig.product_id] = {
                "direction": sig.direction.value,
                "size": sig.size,
                "entry": sig.entry_price,
                "stop": sig.stop_price,
                "target": sig.target_price,
                "strategy": sig.strategy_name,
                "bucket_id": bucket_id,
                "timestamp": time.time(),
            }
        return {
            "success": getattr(entry_order, "success", False) if entry_order is not None else False,
            "product_id": sig.product_id,
            "side": sig.direction.value,
            "size": sig.size,
            "entry": sig.entry_price,
            "order_id": getattr(entry_order, "order_id", "") if entry_order is not None else "",
            "notional": notional,
            "mode": "live",
            "bracket_id": getattr(entry_order, "client_order_id", "") if entry_order is not None else "",
            "bucket_id": bucket_id,
        }

    def _approval_execute(self, sig: TradeSignal) -> Dict[str, Any]:
        import json, os
        token = str(uuid.uuid4())
        approval = {
            "product_id": sig.product_id,
            "direction": sig.direction.value,
            "size": sig.size,
            "entry": sig.entry_price,
            "stop": sig.stop_price,
            "target": sig.target_price,
            "strategy": sig.strategy_name,
            "confidence": sig.confidence,
            "reason": sig.reason,
            "timestamp": time.time(),
            "token": token,
            "status": "pending",
        }
        pending = {}
        try:
            if os.path.exists("pending_approvals.json"):
                with open("pending_approvals.json") as f:
                    fcntl.flock(f, fcntl.LOCK_SH)
                    pending = json.load(f)
        except Exception:
            pass
        pending[token] = approval
        with open("pending_approvals.json", "w") as f:
            fcntl.flock(f, fcntl.LOCK_EX)
            json.dump(pending, f, indent=2, default=str)
        self.state.pending_approvals.append(approval)
        return {
            "success": True,
            "product_id": sig.product_id,
            "side": sig.direction.value,
            "size": sig.size,
            "notional": sig.size * sig.entry_price,
            "mode": "approval",
            "status": "pending",
            "token": approval["token"],
        }

    def _paper_execute(self, sig: TradeSignal) -> Dict[str, Any]:
        notional = sig.size * sig.entry_price
        bucket_id = sig.bucket_id or self.bucket_ledger.allocate(sig.strategy_name, sig.product_id, notional) or "challenge"
        sig.bucket_id = bucket_id
        if sig.direction == Direction.LONG:
            if notional > self.state.cash:
                return {"success": False, "reason": "insufficient cash", "product_id": sig.product_id}
            self.state.cash -= notional
        else:
            self.state.cash += notional

        self.bucket_ledger.open_position(bucket_id, sig.product_id, sig.direction.value, sig.size, sig.entry_price, sig.strategy_name)

        self.state.open_positions[sig.product_id] = {
            "direction": sig.direction.value,
            "size": sig.size,
            "entry": sig.entry_price,
            "stop": sig.stop_price,
            "target": sig.target_price,
            "strategy": sig.strategy_name,
            "bucket_id": bucket_id,
            "timestamp": time.time(),
        }
        self.fee_tracker.record_trade(notional)
        self.volume_generator.record_generated(notional)
        log.info(f"[PAPER] {sig.direction.value} {sig.product_id} {sig.size:.4f} @ {sig.entry_price:.2f}")
        return {
            "success": True,
            "product_id": sig.product_id,
            "side": sig.direction.value,
            "size": sig.size,
            "entry": sig.entry_price,
            "notional": notional,
            "mode": "paper",
            "cash_remaining": self.state.cash,
            "bucket_id": bucket_id,
        }

    def generate_fee_volume(self, product_id: str, current_price: float,
                             atr: float) -> Optional[TradeSignal]:
        opp = self.volume_generator.generate_volume_opportunities(
            product_id, current_price, atr
        )
        if opp is None:
            return None
        opp = self.liquidity_sizer.size_with_liquidity(opp, atr)
        sig = TradeSignal(
            product_id=opp.product_id,
            direction=opp.direction,
            entry_price=opp.entry_price,
            stop_price=opp.stop_price,
            target_price=opp.target_price,
            size=opp.base_size,
            confidence=opp.confidence,
            reason=opp.reason,
            strategy_name=opp.strategy_name,
            opportunity_score=opp.score or 0.5,
        )
        return sig

    def record_trade_result(self, strategy: str, won: bool, r_multiple: float,
                            confidence: float = 0.5):
        if self.risk_appetite:
            self.risk_appetite.record_trade(won, r_multiple)
        pnl = r_multiple * 0.01 * self.state.equity
        self.strategy_ranking.record_trade(strategy, pnl, confidence)
        self.update_strategy_performance(strategy, won, r_multiple)

    def update_strategy_performance(self, strategy: str, win: bool,
                                    r_multiple: float):
        perf = self._strategy_performance.setdefault(strategy, {
            "trades": 0, "wins": 0, "losses": 0,
            "total_r": 0.0, "win_rate": 0.5,
            "avg_win": 1.0, "avg_loss": 1.0,
        })
        perf["trades"] += 1
        perf["total_r"] += r_multiple
        if win:
            perf["wins"] += 1
        else:
            perf["losses"] += 1
        perf["win_rate"] = perf["wins"] / max(perf["trades"], 1)
        if perf["wins"] > 0:
            perf["avg_win"] = perf["total_r"] / max(perf["wins"], 1)
        if perf["losses"] > 0:
            perf["avg_loss"] = abs(perf["total_r"] - perf["wins"] * perf["avg_win"]) / max(perf["losses"], 1)

    def _build_position_risks(self) -> List[PositionRisk]:
        risks = []
        for pid, pos in self.state.open_positions.items():
            risk = PositionRisk(
                product_id=pid,
                side=pos.get("direction", "long"),
                size=pos.get("size", 0.0),
                entry_price=pos.get("entry", 0.0),
                current_price=pos.get("entry", 0.0),
                stop_price=pos.get("stop", None),
                leverage=pos.get("leverage", 1.0),
            )
            risk.var_95 = RiskManager.compute_var(risk, 0.95)
            risks.append(risk)
        return risks

    def close_position(self, product_id: str, exit_price: float,
                       reason: str = "signal"):
        pos = self.state.open_positions.pop(product_id, None)
        if pos is None:
            return None
        side = pos["direction"]
        size = pos["size"]
        entry = pos["entry"]
        if side == "long":
            pnl = (exit_price - entry) * size
            r = (exit_price - entry) / max(abs(entry - pos.get("stop", entry)), 1e-9)
            self.state.cash += size * exit_price
        else:
            pnl = (entry - exit_price) * size
            r = (entry - exit_price) / max(abs(pos.get("stop", entry) - entry), 1e-9)
            self.state.cash -= size * exit_price
        bucket_id = pos.get("bucket_id", "challenge")
        try:
            self.bucket_ledger.close_position(bucket_id, product_id, exit_price)
        except Exception:
            pass
        self.record_trade_result(pos.get("strategy", "unknown"), pnl > 0, r)
        log.info(f"[CLOSE] {side} {product_id} PnL=${pnl:.2f} R={r:.2f} ({reason})")
        return {"pnl": pnl, "r_multiple": r, "product_id": product_id}

    def daily_reset(self):
        self.risk_mgr.update_daily_reset(self.state.equity)
        self.state.daily_trades = 0
        self.state.daily_volume = 0.0

    def status(self) -> Dict[str, Any]:
        return {
            "mode": self.mode.value,
            "equity": self.state.equity,
            "cash": self.state.cash,
            "open_positions": len(self.state.open_positions),
            "pending_approvals": len(self.state.pending_approvals),
            "daily_trades": self.state.daily_trades,
            "daily_volume": self.state.daily_volume,
            "strategy_performance": self._strategy_performance,
            "risk_limit": self.risk_mgr.limit.value,
            "fee_tier": self.fee_tracker.tier_display(),
            "capital_buckets": self.bucket_ledger.summary(),
            "fee_tier_volume_30d": round(self.fee_tracker.rolling_30d_volume, 2),
            "volume_to_next_tier": round(self.fee_tracker.volume_to_next_tier(), 2),
        }
