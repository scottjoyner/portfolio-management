from __future__ import annotations

from typing import Any

from .base import Action, AgentResult, BaseAgent, Evidence, Philosophy


class PositionAuditor(BaseAgent):
    agent_name = "position_auditor"

    def evaluate(self, instrument: str, market_data: dict[str, Any]) -> AgentResult:
        entry = float(market_data.get("entry_price", 0))
        current = float(market_data.get("current_price", 0))
        pnl_pct = ((current - entry) / entry * 100) if entry else 0.0

        if pnl_pct > 20:
            action = Action.REDUCE
            rationale = f"Position up {pnl_pct:.1f}%, above 20% profit threshold; consider taking partial profits"
        elif pnl_pct < -15:
            action = Action.EXIT
            rationale = f"Position down {pnl_pct:.1f}%, below -15% stop-loss threshold; exit recommended"
        else:
            action = Action.HOLD
            rationale = f"Position within normal range ({pnl_pct:.1f}% P&L); no action required"

        return AgentResult(
            agent_name=self.agent_name,
            instrument=instrument,
            action=action,
            confidence=0.85,
            rationale=rationale,
            risk_score=min(abs(pnl_pct) / 50, 1.0),
            philosophy=Philosophy.VALUE,
            holding_period_hint="medium_term",
            evidence=[
                Evidence(source=self.agent_name, metric="pnl_pct", value=round(pnl_pct, 2)),
                Evidence(source=self.agent_name, metric="entry_price", value=entry),
                Evidence(source=self.agent_name, metric="current_price", value=current),
            ],
        )


class MarketAnalyst(BaseAgent):
    agent_name = "market_analyst"

    def evaluate(self, instrument: str, market_data: dict[str, Any]) -> AgentResult:
        volatility = float(market_data.get("volatility_1h", 0))
        volume = float(market_data.get("volume_24h", 0))

        if volatility > 0.05:
            action = Action.WATCH
            rationale = f"High volatility ({volatility:.1%}) detected; recommend watching before entry"
        elif volume > 1_000_000:
            action = Action.BUY
            rationale = f"Strong volume ({volume:,.0f}) with moderate volatility; favorable for entry"
        else:
            action = Action.HOLD
            rationale = "Low volatility and volume; maintain current positions"

        return AgentResult(
            agent_name=self.agent_name,
            instrument=instrument,
            action=action,
            confidence=0.75,
            rationale=rationale,
            risk_score=min(volatility * 10, 1.0),
            philosophy=Philosophy.MOMENTUM,
            holding_period_hint="short_term",
            evidence=[
                Evidence(source=self.agent_name, metric="volatility_1h", value=volatility),
                Evidence(source=self.agent_name, metric="volume_24h", value=volume),
            ],
        )


class FundamentalAnalyst(BaseAgent):
    agent_name = "fundamental_analyst"

    def evaluate(self, instrument: str, market_data: dict[str, Any]) -> AgentResult:
        current = float(market_data.get("current_price", 0))
        dcf_value = float(market_data.get("dcf_intrinsic_value", 0))

        if dcf_value > 0:
            discount = (dcf_value - current) / dcf_value
            if discount > 0.15:
                action = Action.STRONG_BUY
                rationale = f"Trading at {discount:.1%} discount to DCF intrinsic value ({dcf_value:.2f})"
            elif discount > 0.05:
                action = Action.BUY
                rationale = f"Moderate discount ({discount:.1%}) to intrinsic value"
            elif discount < -0.15:
                action = Action.SELL
                rationale = f"Trading at {abs(discount):.1%} premium above intrinsic value"
            else:
                action = Action.HOLD
                rationale = f"Price near intrinsic value ({dcf_value:.2f}); hold"
        else:
            action = Action.HOLD
            rationale = "No DCF data available; holding pending fair-value analysis"

        return AgentResult(
            agent_name=self.agent_name,
            instrument=instrument,
            action=action,
            confidence=0.80 if dcf_value > 0 else 0.40,
            rationale=rationale,
            risk_score=0.30,
            philosophy=Philosophy.VALUE,
            holding_period_hint="long_term",
            evidence=[
                Evidence(source=self.agent_name, metric="current_price", value=current),
                Evidence(source=self.agent_name, metric="dcf_intrinsic_value", value=dcf_value),
                Evidence(source=self.agent_name, metric="discount_pct", value=round(((dcf_value - current) / dcf_value * 100) if dcf_value else 0, 2)),
            ],
        )


class CryptoOnchainAnalyst(BaseAgent):
    agent_name = "crypto_onchain_analyst"

    def evaluate(self, instrument: str, market_data: dict[str, Any]) -> AgentResult:
        tvl = float(market_data.get("tvl_usd", 0))
        volume = float(market_data.get("onchain_volume_24h", 0))
        active_users = int(market_data.get("active_users_24h", 0))

        if tvl > 10_000_000 and volume > 1_000_000:
            action = Action.BUY
            rationale = f"Strong onchain fundamentals: ${tvl:,.0f} TVL, ${volume:,.0f} daily volume"
        elif active_users > 1000:
            action = Action.HOLD
            rationale = f"Growing user base ({active_users:,} active users) but needs higher TVL"
        else:
            action = Action.WATCH
            rationale = "Low onchain activity; monitoring for growth signals"

        return AgentResult(
            agent_name=self.agent_name,
            instrument=instrument,
            action=action,
            confidence=0.70,
            rationale=rationale,
            risk_score=0.50,
            philosophy=Philosophy.GROWTH,
            holding_period_hint="medium_term",
            evidence=[
                Evidence(source=self.agent_name, metric="tvl_usd", value=tvl),
                Evidence(source=self.agent_name, metric="onchain_volume_24h", value=volume),
                Evidence(source=self.agent_name, metric="active_users_24h", value=active_users),
            ],
        )


class RiskAnalyst(BaseAgent):
    agent_name = "risk_analyst"

    def evaluate(self, instrument: str, market_data: dict[str, Any]) -> AgentResult:
        var = float(market_data.get("value_at_risk", 0))
        drawdown = float(market_data.get("current_drawdown", 0))
        correlation = float(market_data.get("correlation_to_index", 0.7))

        if var > 0.15 or drawdown > 0.20:
            action = Action.EXIT
            rationale = f"Risk thresholds exceeded: VaR={var:.1%}, drawdown={drawdown:.1%}; exit recommended"
        elif var > 0.08 or drawdown > 0.10:
            action = Action.REDUCE
            rationale = f"Elevated risk: VaR={var:.1%}, drawdown={drawdown:.1%}; consider reducing position"
        elif correlation > 0.9:
            action = Action.REDUCE
            rationale = f"High correlation ({correlation:.2f}) to index; portfolio concentration risk"
        else:
            action = Action.HOLD
            rationale = f"Risk within acceptable bounds: VaR={var:.1%}, drawdown={drawdown:.1%}"

        return AgentResult(
            agent_name=self.agent_name,
            instrument=instrument,
            action=action,
            confidence=0.90,
            rationale=rationale,
            risk_score=max(var, drawdown),
            philosophy=Philosophy.HEDGE,
            holding_period_hint="short_term",
            evidence=[
                Evidence(source=self.agent_name, metric="value_at_risk", value=var),
                Evidence(source=self.agent_name, metric="current_drawdown", value=drawdown),
                Evidence(source=self.agent_name, metric="correlation_to_index", value=correlation),
            ],
        )


class StrategyResearcher(BaseAgent):
    agent_name = "strategy_researcher"

    def evaluate(self, instrument: str, market_data: dict[str, Any]) -> AgentResult:
        vol = float(market_data.get("volatility_1h", 0))
        spread = float(market_data.get("spread_bps", 0))

        if vol > 0.03 and spread < 10:
            action = Action.BUY
            rationale = f"Favorable microstructure: volatility={vol:.1%}, spread={spread:.0f}bps; suitable for active strategies"
        elif spread > 50:
            action = Action.WATCH
            rationale = f"Wide spread ({spread:.0f}bps) indicates low liquidity; unsuitable for frequent trading"
        else:
            action = Action.HOLD
            rationale = f"Standard market conditions: vol={vol:.1%}, spread={spread:.0f}bps"

        return AgentResult(
            agent_name=self.agent_name,
            instrument=instrument,
            action=action,
            confidence=0.70,
            rationale=rationale,
            risk_score=min(spread / 100, 1.0),
            philosophy=Philosophy.MARKET_MAKING,
            holding_period_hint="intraday",
            evidence=[
                Evidence(source=self.agent_name, metric="volatility_1h", value=vol),
                Evidence(source=self.agent_name, metric="spread_bps", value=spread),
            ],
        )


class BacktestCritic(BaseAgent):
    agent_name = "backtest_critic"

    def evaluate(self, instrument: str, market_data: dict[str, Any]) -> AgentResult:
        sharpe = float(market_data.get("backtest_sharpe", 0))
        max_dd = float(market_data.get("backtest_max_drawdown", 0))

        if sharpe < 0.5 or max_dd > 0.30:
            action = Action.WATCH
            rationale = f"Backtest results below threshold: Sharpe={sharpe:.2f}, max DD={max_dd:.1%}"
        elif sharpe > 2.0:
            action = Action.BUY
            rationale = f"Strong backtest results: Sharpe={sharpe:.2f} with controlled drawdown={max_dd:.1%}"
        else:
            action = Action.HOLD
            rationale = f"Adequate backtest: Sharpe={sharpe:.2f}, max DD={max_dd:.1%}"

        return AgentResult(
            agent_name=self.agent_name,
            instrument=instrument,
            action=action,
            confidence=0.80,
            rationale=rationale,
            risk_score=min(max_dd * 2, 1.0),
            philosophy=Philosophy.MEAN_REVERSION,
            holding_period_hint="medium_term",
            evidence=[
                Evidence(source=self.agent_name, metric="backtest_sharpe", value=sharpe),
                Evidence(source=self.agent_name, metric="backtest_max_drawdown", value=max_dd),
            ],
        )


class ApprovalDrafter(BaseAgent):
    agent_name = "approval_drafter"

    def evaluate(self, instrument: str, market_data: dict[str, Any]) -> AgentResult:
        results_list: list[AgentResult] = market_data.get("agent_results", [])
        if not results_list:
            return AgentResult(
                agent_name=self.agent_name,
                instrument=instrument,
                action=Action.WATCH,
                confidence=0.0,
                rationale="No agent evaluations available; cannot draft approval",
                risk_score=1.0,
                philosophy=Philosophy.VALUE,
                holding_period_hint="unknown",
            )

        avg_confidence = sum(r.confidence for r in results_list) / len(results_list)
        actions = [r.action for r in results_list]
        buy_count = sum(1 for a in actions if a in (Action.STRONG_BUY, Action.BUY))
        sell_count = sum(1 for a in actions if a in (Action.SELL, Action.EXIT))
        dissenting_agents = [r.agent_name for r in results_list if r.dissenting]

        if buy_count > sell_count and buy_count >= 3:
            action = Action.BUY
            rationale = f"{buy_count}/{len(results_list)} agents recommend BUY (avg confidence {avg_confidence:.0%})"
        elif sell_count > buy_count and sell_count >= 3:
            action = Action.SELL
            rationale = f"{sell_count}/{len(results_list)} agents recommend SELL (avg confidence {avg_confidence:.0%})"
        else:
            action = Action.HOLD
            rationale = f"Mixed signals: {buy_count} BUY / {sell_count} SELL / {len(results_list) - buy_count - sell_count} HOLD"

        dissenting_str = f"Dissent from: {', '.join(dissenting_agents)}" if dissenting_agents else None

        return AgentResult(
            agent_name=self.agent_name,
            instrument=instrument,
            action=action,
            confidence=avg_confidence * 0.9,
            rationale=rationale,
            risk_score=1.0 - (avg_confidence * 0.8),
            philosophy=Philosophy.VALUE,
            holding_period_hint="medium_term",
            evidence=[
                Evidence(source=a.agent_name, metric="action", value=a.action.value, weight=a.confidence)
                for a in results_list
            ],
            dissenting=dissenting_str,
        )
