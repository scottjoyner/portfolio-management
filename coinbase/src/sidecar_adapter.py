from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping, Optional

from .protocols import BracketSetup, Opportunity


COINBASE_PRODUCT_SUFFIXES = ("-USD", "-USDC", "-EUR", "-BTC", "-ETH")


@dataclass(frozen=True)
class SidecarResearchRecord:
    strategy_name: str
    product_id: str
    ticker: str
    total_return_pct: float = 0.0
    max_drawdown_pct: float = 0.0
    sharpe: float = 0.0
    profit_factor: float = 0.0
    win_rate_pct: float = 0.0
    num_trades: int = 0
    manifest_path: Optional[str] = None

    @property
    def is_coinbase_product(self) -> bool:
        return self.product_id.upper().endswith(COINBASE_PRODUCT_SUFFIXES)

    @property
    def research_score(self) -> float:
        pf_component = min(max(self.profit_factor, 0.0), 3.0) / 3.0
        sharpe_component = min(max(self.sharpe, -1.0), 3.0) / 4.0
        win_component = min(max(self.win_rate_pct, 0.0), 100.0) / 100.0
        dd_component = 1.0 - min(abs(self.max_drawdown_pct), 50.0) / 50.0
        trade_component = min(max(self.num_trades, 0), 100) / 100.0
        score = 0.30 * pf_component + 0.25 * sharpe_component + 0.20 * win_component + 0.15 * dd_component + 0.10 * trade_component
        return max(0.0, min(1.0, score))


def _as_float(value: Any, default: float = 0.0) -> float:
    try:
        return default if value is None else float(value)
    except (TypeError, ValueError):
        return default


def _as_int(value: Any, default: int = 0) -> int:
    try:
        return default if value is None else int(value)
    except (TypeError, ValueError):
        return default


def product_id_from_manifest(manifest: Mapping[str, Any]) -> str:
    config = dict(manifest.get("config") or {})
    value = config.get("product_id") or config.get("coinbase_product_id") or config.get("ticker") or manifest.get("product_id") or ""
    return str(value).upper()


def strategy_name_from_manifest(manifest: Mapping[str, Any], default: str = "sidecar_rsi_cross") -> str:
    config = dict(manifest.get("config") or {})
    return str(config.get("strategy_name") or manifest.get("strategy_name") or default)


def research_record_from_manifest(manifest: Mapping[str, Any], manifest_path: Optional[str] = None) -> SidecarResearchRecord:
    summary = dict(manifest.get("summary") or {})
    config = dict(manifest.get("config") or {})
    product_id = product_id_from_manifest(manifest)
    ticker = str(config.get("ticker") or summary.get("ticker") or product_id).upper()
    return SidecarResearchRecord(
        strategy_name=strategy_name_from_manifest(manifest),
        product_id=product_id,
        ticker=ticker,
        total_return_pct=_as_float(summary.get("total_return_pct")),
        max_drawdown_pct=_as_float(summary.get("max_drawdown_pct")),
        sharpe=_as_float(summary.get("sharpe")),
        profit_factor=_as_float(summary.get("profit_factor")),
        win_rate_pct=_as_float(summary.get("win_rate_pct")),
        num_trades=_as_int(summary.get("num_trades")),
        manifest_path=manifest_path,
    )


def bracket_to_opportunity(product_id: str, setup: BracketSetup, *, research: Optional[SidecarResearchRecord] = None) -> Opportunity:
    confidence = setup.confidence
    meta = dict(setup.metadata or {})
    if research:
        confidence = min(0.99, confidence * (0.75 + 0.50 * research.research_score))
        meta.update({
            "sidecar_manifest": research.manifest_path,
            "sidecar_research_score": research.research_score,
            "sidecar_total_return_pct": research.total_return_pct,
            "sidecar_max_drawdown_pct": research.max_drawdown_pct,
            "sidecar_profit_factor": research.profit_factor,
            "sidecar_num_trades": research.num_trades,
        })
    return Opportunity(
        product_id=product_id,
        direction=setup.direction,
        instrument_type=setup.instrument_type,
        entry_price=setup.entry_price,
        stop_price=setup.stop_price,
        target_price=setup.target_price,
        risk_reward=setup.risk_reward,
        confidence=confidence,
        reason=setup.reason,
        strategy_name=setup.strategy_name,
        atr=setup.atr,
        leverage=setup.leverage,
        score=confidence * setup.risk_reward,
        meta=meta,
    )
