from __future__ import annotations
import fcntl
import json
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
from trading_system.core.performance_model import LatencyProfile, expected_fill_delay_ms
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
        pending_file: str = "data/pending_approvals.json",
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
        self.pending_file = pending_file
        self.live_min_cash_reserve_usd = self._env_float("TRADER_LIVE_MIN_CASH_RESERVE_USD", 100.0)
        self.live_max_order_usd = self._env_float("TRADER_LIVE_MAX_ORDER_USD", 50.0)
        self.live_max_total_notional_usd = self._env_float("TRADER_LIVE_MAX_TOTAL_NOTIONAL_USD", 50.0)
        self.live_max_open_positions = self._env_int("TRADER_LIVE_MAX_OPEN_POSITIONS", 1)
        self.live_allow_short = self._env_bool("TRADER_LIVE_ALLOW_SHORT", False)
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
        self.latency_profile = LatencyProfile()
        self.min_live_edge_bps = self._env_float("TRADER_MIN_LIVE_EDGE_BPS", 25.0)
        self.min_live_confidence = self._env_float("TRADER_MIN_LIVE_CONFIDENCE", 0.95)
        if self.mode == TradeMode.PAPER:
            seed_cash = float(self.bucket_ledger.summary().get("total_value_usd", 0.0))
            self.state.cash = seed_cash
            self.state.equity = seed_cash

    @staticmethod
    def _env_bool(name: str, default: bool = False) -> bool:
        raw = os.getenv(name)
        if raw is None:
            return default
        return raw.strip().lower() in {"1", "true", "yes", "on"}

    @staticmethod
    def _env_float(name: str, default: float) -> float:
        try:
            return float(os.getenv(name, str(default)))
        except (TypeError, ValueError):
            return default

    @staticmethod
    def _env_int(name: str, default: int) -> int:
        try:
            return int(float(os.getenv(name, str(default))))
        except (TypeError, ValueError):
            return default

    def _kill_switch_active(self) -> bool:
        from .config import is_kill_switch_active
        return is_kill_switch_active()

    def _blocked_result(self, sig: TradeSignal, reason: str, status: str = "blocked") -> Dict[str, Any]:
        return {
            "success": False,
            "status": status,
            "reason": reason,
            "product_id": sig.product_id,
            "side": sig.direction.value,
            "size": sig.size,
            "entry": sig.entry_price,
            "notional": sig.size * sig.entry_price,
            "mode": self.mode.value,
            "bucket_id": sig.bucket_id,
        }

    def _estimated_live_edge_bps(self, sig: TradeSignal) -> float:
        """Estimate whether a live trade can overcome fees and latency."""

        horizon_ms = 60.0 * 60.0 * 1000.0
        if sig.strategy_name:
            name = sig.strategy_name.lower()
            if name in {"arb", "cross_exchange_arb"}:
                horizon_ms = 5.0 * 60.0 * 1000.0
            elif name in {"mean_reversion", "rsi_revert", "zscore_revert"}:
                horizon_ms = 120.0 * 60.0 * 1000.0
            elif name in {"prediction_market", "kalshi", "polymarket"}:
                horizon_ms = 240.0 * 60.0 * 1000.0

        confidence = max(0.0, min(1.0, sig.confidence))
        rr = abs(sig.target_price - sig.entry_price) / max(abs(sig.entry_price - sig.stop_price), 1e-9)
        gross_bps = (0.55 * confidence + 0.25 * min(rr / 3.0, 1.0) + 0.20 * min(sig.opportunity_score, 1.0)) * 70.0
        fill_ms = expected_fill_delay_ms(self.latency_profile, liquidity_score=1.0, crossing_spread=True)
        latency_bps = min(20.0, fill_ms / 100.0)
        fee_bps = 10.0 + 5.0
        return gross_bps - fee_bps - latency_bps

    def _pre_execution_block_reason(self, sig: TradeSignal) -> Optional[str]:
        if self._kill_switch_active():
            return "kill_switch"
        notional = sig.size * sig.entry_price
        max_order = self._env_float(
            "TRADER_CHALLENGE_MAX_ORDER_USD",
            self._env_float("MAX_NOTIONAL_PER_TRADE_USD", 10000.0),
        )
        if max_order > 0 and notional > max_order:
            return "max_order_notional"
        if self._env_bool("TRADER_LIVE_CHALLENGE_ONLY", False):
            bucket_id = (sig.bucket_id or "challenge").strip()
            if bucket_id != "challenge":
                return "bucket_not_allowed"
        if self.mode != TradeMode.PAPER:
            if sig.direction != Direction.LONG and not self.live_allow_short:
                return "shorts_disabled_for_live_spot"
            if sig.confidence < self.min_live_confidence:
                return "min_live_confidence"
            if self._estimated_live_edge_bps(sig) < self.min_live_edge_bps:
                return "insufficient_live_edge"
            if sig.direction == Direction.LONG:
                live_cash = self._available_live_cash_usd()
                if live_cash is not None and notional > max(0.0, live_cash - self.live_min_cash_reserve_usd):
                    return "insufficient_live_cash"
            if len(self.state.open_positions) >= self.live_max_open_positions:
                return "live_max_open_positions"
            if self.live_max_order_usd > 0 and notional > self.live_max_order_usd:
                return "live_max_order_usd"
            if self.live_max_total_notional_usd > 0 and self._live_open_notional() + notional > self.live_max_total_notional_usd:
                return "live_max_total_notional_usd"
        return None

    def _available_live_cash_usd(self) -> Optional[float]:
        if not self.cb:
            return None
        try:
            accounts = self.cb.list_accounts()
        except Exception as exc:
            log.debug("Unable to fetch live balances: %s", exc)
            return None

        items = accounts.get("accounts") or accounts.get("data") or accounts
        if not isinstance(items, list):
            return None
        for acct in items:
            if not isinstance(acct, dict):
                continue
            currency = str(acct.get("currency") or acct.get("asset") or "").upper()
            if currency != "USD":
                continue
            avail = acct.get("available_balance") or acct.get("available") or acct.get("balance") or 0
            if isinstance(avail, dict):
                avail = avail.get("value", 0)
            try:
                return float(avail or 0.0)
            except (TypeError, ValueError):
                return None
        return None

    def _live_open_notional(self) -> float:
        total = 0.0
        for pos in self.state.open_positions.values():
            try:
                size = float(pos.get("size", 0.0))
                entry = float(pos.get("entry", 0.0))
                total += max(0.0, size * entry)
            except Exception:
                continue
        return total

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
        executed_count = 0
        tick_notional = 0.0
        default_max_orders = 3 if self.mode == TradeMode.PAPER else 1
        max_orders = max(0, self._env_int("TRADER_MAX_ORDERS_PER_TICK", default_max_orders))
        max_tick_notional = self._env_float("TRADER_MAX_NOTIONAL_PER_TICK", 0.0)

        for sig in sorted(signals, key=lambda s: s.opportunity_score, reverse=True):
            block_reason = self._pre_execution_block_reason(sig)
            if block_reason:
                result = self._blocked_result(sig, block_reason)
                results.append(result)
                continue

            notional = sig.size * sig.entry_price
            if max_orders > 0 and executed_count >= max_orders:
                result = self._blocked_result(sig, "max_orders_per_tick", status="deferred")
                results.append(result)
                continue
            if max_tick_notional > 0 and tick_notional + notional > max_tick_notional:
                result = self._blocked_result(sig, "max_notional_per_tick", status="deferred")
                results.append(result)
                continue

            if self.mode == TradeMode.PAPER:
                result = self._paper_execute(sig)
            elif self.mode == TradeMode.LIVE_APPROVAL:
                result = self._approval_execute(sig)
            elif self.mode == TradeMode.FUTURES:
                result = self._futures_execute(sig)
            else:
                result = self._live_execute(sig)
            results.append(result)
            if result.get("success"):
                executed_count += 1
                tick_notional += notional
            for listener in self._listeners:
                try:
                    listener(sig, result)
                except Exception:
                    pass
        self.state.daily_trades += len([r for r in results if r.get("success")])
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
        if sig.direction != Direction.LONG and not self.live_allow_short:
            return {"success": False, "reason": "shorts_disabled_for_live_spot", "product_id": sig.product_id}
        side = "BUY" if sig.direction == Direction.LONG else "SELL"
        bracket = self.bracket_mgr.place_bracket(
            product_id=sig.product_id,
            side=side,
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

    def _persist_pending_approval(self, token: str, approval: Dict[str, Any]) -> None:
        """Durably append an approval using a process-safe lock and atomic replace."""
        pending_path = os.path.abspath(self.pending_file)
        parent = os.path.dirname(pending_path)
        if parent:
            os.makedirs(parent, exist_ok=True)

        lock_path = f"{pending_path}.lock"
        temp_path = f"{pending_path}.tmp-{os.getpid()}-{uuid.uuid4().hex}"
        with open(lock_path, "a+") as lock_file:
            fcntl.flock(lock_file, fcntl.LOCK_EX)
            pending: Dict[str, Any] = {}
            try:
                with open(pending_path, encoding="utf-8") as existing:
                    loaded = json.load(existing)
                    if isinstance(loaded, dict):
                        pending = loaded
            except FileNotFoundError:
                pass
            except (OSError, ValueError, TypeError) as exc:
                log.warning("Unable to read pending approvals from %s: %s", pending_path, exc)

            pending[token] = approval
            try:
                with open(temp_path, "w", encoding="utf-8") as staged:
                    json.dump(pending, staged, indent=2, default=str, sort_keys=True)
                    staged.write("\n")
                    staged.flush()
                    os.fsync(staged.fileno())
                os.replace(temp_path, pending_path)
                if parent and hasattr(os, "O_DIRECTORY"):
                    directory_fd = os.open(parent, os.O_RDONLY | os.O_DIRECTORY)
                    try:
                        os.fsync(directory_fd)
                    finally:
                        os.close(directory_fd)
            finally:
                try:
                    if os.path.exists(temp_path):
                        os.unlink(temp_path)
                except OSError:
                    pass

    def _approval_execute(self, sig: TradeSignal) -> Dict[str, Any]:
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
        self._persist_pending_approval(token, approval)
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

        self.state.open_positions[sig.product_id] = self._merged_position_state(sig, bucket_id)
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

    def _merged_position_state(self, sig: TradeSignal, bucket_id: str) -> Dict[str, Any]:
        existing = self.state.open_positions.get(sig.product_id) or {}
        if existing.get("direction") == sig.direction.value:
            old_size = float(existing.get("size", 0.0))
            old_entry = float(existing.get("entry", sig.entry_price))
            combined_size = old_size + sig.size
            if combined_size > 0:
                entry = ((old_size * old_entry) + (sig.size * sig.entry_price)) / combined_size
                size = combined_size
            else:
                entry = sig.entry_price
                size = sig.size
        else:
            entry = sig.entry_price
            size = sig.size
        return {
            "direction": sig.direction.value,
            "size": size,
            "entry": entry,
            "stop": sig.stop_price,
            "target": sig.target_price,
            "strategy": sig.strategy_name,
            "bucket_id": bucket_id,
            "timestamp": time.time(),
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

    def execution_guard_status(self) -> Dict[str, Any]:
        default_max_orders = 3 if self.mode == TradeMode.PAPER else 1
        return {
            "kill_switch_active": self._kill_switch_active(),
            "kill_switch_path": os.getenv("TRADER_KILL_SWITCH_PATH", "data/trading_kill_switch"),
            "challenge_max_order_usd": self._env_float(
                "TRADER_CHALLENGE_MAX_ORDER_USD",
                self._env_float("MAX_NOTIONAL_PER_TRADE_USD", 10000.0),
            ),
            "max_orders_per_tick": max(0, self._env_int("TRADER_MAX_ORDERS_PER_TICK", default_max_orders)),
            "max_notional_per_tick": self._env_float("TRADER_MAX_NOTIONAL_PER_TICK", 0.0),
            "live_challenge_only": self._env_bool("TRADER_LIVE_CHALLENGE_ONLY", False),
            "live_min_cash_reserve_usd": self.live_min_cash_reserve_usd,
            "live_max_order_usd": self.live_max_order_usd,
            "live_max_total_notional_usd": self.live_max_total_notional_usd,
            "live_max_open_positions": self.live_max_open_positions,
            "live_allow_short": self.live_allow_short,
            "bucket_state_path": self.bucket_ledger.state_path,
        }

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
            "execution_guards": self.execution_guard_status(),
            "fee_tier_volume_30d": round(self.fee_tracker.rolling_30d_volume, 2),
            "volume_to_next_tier": round(self.fee_tracker.volume_to_next_tier(), 2),
        }
