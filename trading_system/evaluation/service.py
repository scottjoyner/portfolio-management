from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from .agents import (
    ApprovalDrafter,
    BacktestCritic,
    CryptoOnchainAnalyst,
    FundamentalAnalyst,
    MarketAnalyst,
    PositionAuditor,
    RiskAnalyst,
    StrategyResearcher,
)
from .base import Action, AgentResult, BaseAgent
from trading_system.storage.postgres.models import AnalystRating, PriceEstimate, SentimentAnalysis
from trading_system.storage.postgres.repository import OpsRepository


def _action_to_rating(action: Action) -> str:
    return {"strong_buy": "BUY", "buy": "BUY", "hold": "HOLD", "reduce": "SELL", "sell": "SELL", "exit": "SELL", "watch": "HOLD"}.get(action.value, "HOLD")


class EvaluationService:
    def __init__(self, repo: OpsRepository) -> None:
        self.repo = repo
        self._agents: list[BaseAgent] = [
            PositionAuditor(),
            MarketAnalyst(),
            FundamentalAnalyst(),
            CryptoOnchainAnalyst(),
            RiskAnalyst(),
            StrategyResearcher(),
            BacktestCritic(),
        ]
        self._drafter = ApprovalDrafter()

    def evaluate_instrument(self, instrument: str, market_data: dict[str, Any]) -> dict[str, Any]:
        results: list[AgentResult] = []
        for agent in self._agents:
            result = agent.evaluate(instrument, market_data)
            results.append(result)

            self.repo.db.add(AnalystRating(
                instrument=instrument, analyst=agent.agent_name,
                rating_text=_action_to_rating(result.action),
                price_target=float(market_data.get("current_price", 0)),
            ))

        consensus = self._drafter.evaluate(instrument, {**market_data, "agent_results": results})

        dcf_val = float(market_data.get("dcf_intrinsic_value", 0))
        technical = float(market_data.get("technical_score", 0))
        if dcf_val > 0 or technical > 0:
            current = float(market_data.get("current_price", 1))
            self.repo.db.add(PriceEstimate(
                instrument=instrument,
                current_market_price=current,
                dcf_intrinsic_value=dcf_val if dcf_val > 0 else None,
                technical_score=technical if technical > 0 else None,
                consensus_vs_current_pct=round(((dcf_val - current) / current * 100) if dcf_val > 0 else 0, 2),
                confidence_score=consensus.confidence,
            ))

        sentiment = float(market_data.get("sentiment_score", 0))
        self.repo.db.add(SentimentAnalysis(
            product_id=instrument,
            regime="BULLISH" if sentiment > 0.3 else "BEARISH" if sentiment < -0.3 else "NEUTRAL",
            bullish_pct=max(0, (sentiment + 1) * 50),
            bearish_pct=max(0, (1 - sentiment) * 50),
            sentiment_score=sentiment,
        ))

        self.repo.db.commit()

        return {
            "instrument": instrument,
            "consensus": {
                "action": consensus.action.value,
                "confidence": round(consensus.confidence, 4),
                "rationale": consensus.rationale,
                "risk_score": round(consensus.risk_score, 4),
                "philosophy": consensus.philosophy.value,
                "holding_period_hint": consensus.holding_period_hint,
                "dissenting": consensus.dissenting,
            },
            "agents": [
                {
                    "agent": r.agent_name,
                    "action": r.action.value,
                    "confidence": round(r.confidence, 4),
                    "rationale": r.rationale,
                    "philosophy": r.philosophy.value,
                }
                for r in results
            ],
            "evidence": [
                {"source": e.source, "metric": e.metric, "value": e.value}
                for r in results for e in r.evidence
            ],
            "evaluated_at": datetime.now(timezone.utc).isoformat(),
        }

    def evaluate_portfolio(self, portfolio_id: str) -> dict[str, Any]:
        portfolio = self.repo.get_portfolio(portfolio_id)
        if not portfolio:
            return {"portfolio_id": portfolio_id, "error": "not found"}

        products = ["BTC-USD", "ETH-USD", "SOL-USD"]
        results = {}
        for product in products:
            market_data = {
                "current_price": 60000.0,
                "entry_price": 55000.0,
                "volatility_1h": 0.025,
                "volume_24h": 1_500_000,
                "value_at_risk": 0.08,
                "current_drawdown": 0.05,
                "correlation_to_index": 0.75,
                "spread_bps": 8.0,
                "backtest_sharpe": 1.2,
                "backtest_max_drawdown": 0.15,
                "dcf_intrinsic_value": 65000.0,
                "technical_score": 72.0,
                "sentiment_score": 0.25,
            }
            results[product] = self.evaluate_instrument(product, market_data)

        return {
            "portfolio_id": portfolio_id,
            "total_instruments": len(products),
            "results": results,
            "evaluated_at": datetime.now(timezone.utc).isoformat(),
        }
