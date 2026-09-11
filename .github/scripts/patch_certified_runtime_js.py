from pathlib import Path


def replace_once(text: str, old: str, new: str, label: str) -> str:
    if new in text:
        return text
    if old not in text:
        raise SystemExit(f"missing anchor: {label}")
    return text.replace(old, new, 1)


# Opportunity projection: preserve certification and stop calling an in-sample screen certified.
path = Path("apps/api/src/opportunityGenerator.mjs")
text = path.read_text()
text = replace_once(
    text,
    "  const recommendation = executionPurpose === 'take_profit_exit' ? 'take_profit' : (isSell ? 'review_short' : 'paper_review');\n\n  const validTradeSize",
    "  const recommendation = executionPurpose === 'take_profit_exit' ? 'take_profit' : (isSell ? 'review_short' : 'paper_review');\n  const researchCertification = signal.research_certification || signal.researchCertification || null;\n  const tournamentCertified = signal.tournament_certified === true && Boolean(researchCertification?.certification_hash);\n\n  const validTradeSize",
    "strategy certification variables",
)
text = replace_once(
    text,
    "    tradePlan,\n    marketSlug:",
    "    tradePlan,\n    researchCertification,\n    requiresResearchCertification: tradeIntent === 'entry',\n    marketSlug:",
    "strategy opportunity certification fields",
)
text = replace_once(
    text,
    "    backtestStatus: 'same_window_30d_screen_uncertified',",
    "    backtestStatus: tournamentCertified ? 'tournament_terminal_certified_runtime' : 'same_window_30d_screen_uncertified',",
    "strategy backtest status",
)
text = replace_once(
    text,
    "    notes: `${signal.reason || signal.backtest_reason || 'strategy scan'} | same-window screen only; not tournament-certified | win_rate=${(winProbability * 100).toFixed(1)}% | sentiment=${Number(signal.sentiment_score || 0).toFixed(2)} | weighted_conf=${weightedConfidence.toFixed(2)} | tp=${takeProfitPrice || 'n/a'} | sl=${stopLossPrice || 'n/a'} | source=${signal.source || 'live_cli'}`,",
    "    notes: `${signal.reason || signal.backtest_reason || 'strategy scan'} | ${tournamentCertified ? 'tournament-certified exact runtime config' : 'same-window screen only; not tournament-certified'} | win_rate=${(winProbability * 100).toFixed(1)}% | sentiment=${Number(signal.sentiment_score || 0).toFixed(2)} | weighted_conf=${weightedConfidence.toFixed(2)} | tp=${takeProfitPrice || 'n/a'} | sl=${stopLossPrice || 'n/a'} | source=${signal.source || 'live_cli'}`,",
    "strategy notes",
)
text = replace_once(
    text,
    "      validationScope: 'same_window_in_sample_screen',\n      tournamentCertified: false,",
    "      validationScope: tournamentCertified ? 'tournament_terminal_promoted_exact_runtime' : 'same_window_in_sample_screen',\n      tournamentCertified,\n      researchCertificationHash: researchCertification?.certification_hash || null,",
    "strategy evidence certification",
)
# Freshly generated entries must wait for research + economic admission; exits may auto-draft for risk reduction.
text = replace_once(
    text,
    """    const { opportunity } = opportunityResult;
    const approvalResult = decideOpportunity(state, opportunity.id, {
      status: 'approved',
      reviewer: 'system:auto-draft',
      reason: `Auto-approved from live 30d strategy scan: ${signal.strategy} win_rate=${(Number(signal.win_rate || 0) * 100).toFixed(1)}% sentiment=${Number(signal.sentiment_score || 0).toFixed(2)}`,
    });

    if (approvalResult.errors) {
      errors.push({ symbol, strategy: signal.strategy, code: 'approval_failed', errors: approvalResult.errors });
      continue;
    }

    created.push(approvalResult.opportunity || opportunity);
    if (approvalResult.execution) executions.push(approvalResult.execution);
""",
    """    const { opportunity } = opportunityResult;
    if (opportunity.tradeIntent === 'entry') {
      // Entry research and current economics are separate gates. A new scanner
      // opportunity has not yet been routed through the economic decision
      // engine, so it remains reviewable but cannot auto-create a draft.
      created.push(opportunity);
      continue;
    }

    const approvalResult = decideOpportunity(state, opportunity.id, {
      status: 'approved',
      reviewer: 'system:risk-reduction-auto-draft',
      reason: `Risk-reduction signal from strategy scan: ${signal.strategy}`,
    });

    if (approvalResult.errors) {
      errors.push({ symbol, strategy: signal.strategy, code: 'approval_failed', errors: approvalResult.errors });
      continue;
    }

    created.push(approvalResult.opportunity || opportunity);
    if (approvalResult.execution) executions.push(approvalResult.execution);
""",
    "scanner auto-draft entry guard",
)
path.write_text(text)


# Opportunity approval: scanner entries need valid research identity + current economic approval.
path = Path("apps/api/src/opportunityFlowsLegacy.mjs")
text = path.read_text()
text = replace_once(
    text,
    "import { nextId } from '../../../packages/storage/src/operatorStore.mjs';\n",
    "import { nextId } from '../../../packages/storage/src/operatorStore.mjs';\nimport { strategyScannerResearchRequired, verifyResearchCertification } from '../../../packages/execution/src/researchCertification.mjs';\n",
    "opportunity research import",
)
text = replace_once(
    text,
    "    tradePlan: body.tradePlan || null,\n    status:",
    "    tradePlan: body.tradePlan || null,\n    researchCertification: body.researchCertification || null,\n    requiresResearchCertification: body.requiresResearchCertification === true,\n    status:",
    "opportunity research fields",
)
text = replace_once(
    text,
    """  let approvedExecutionSize = null;
  if (body.status === 'approved') {
    const rawExecutionSize = opportunity.positionSizing?.recommendedSize ?? opportunity.totalMoneyRisked;
""",
    """  let approvedExecutionSize = null;
  if (body.status === 'approved') {
    if (strategyScannerResearchRequired(opportunity)) {
      const certification = verifyResearchCertification(opportunity.researchCertification, {
        strategyId: opportunity.strategyId,
        symbol: opportunity.symbol,
      });
      if (!certification.ok) return { errors: certification.reasons };
      if (!opportunity.economicDecisionId || opportunity.economicExecutionAllowed !== true) {
        return { errors: ['strategy_entry_economic_approval_required'] };
      }
      const authoritativeEdge = finiteNumber(opportunity.netExecutableEdgeUsd, NaN);
      if (!Number.isFinite(authoritativeEdge) || authoritativeEdge <= 0) {
        return { errors: ['strategy_entry_positive_executable_edge_required'] };
      }
    }
    const rawExecutionSize = opportunity.positionSizing?.recommendedSize ?? opportunity.totalMoneyRisked;
""",
    "opportunity entry admission",
)
text = replace_once(
    text,
    "      positionSide: opportunity.positionSide || null,\n      orders:",
    "      positionSide: opportunity.positionSide || null,\n      researchCertification: opportunity.researchCertification || null,\n      requiresResearchCertification: opportunity.requiresResearchCertification === true,\n      orders:",
    "execution research fields",
)
path.write_text(text)


# Overseer: hash-bind and verify certification for exposure-increasing scanner requests.
path = Path("packages/execution/src/overseer.mjs")
text = path.read_text()
text = replace_once(
    text,
    "import { createHash } from 'node:crypto';\n",
    "import { createHash } from 'node:crypto';\nimport { strategyScannerResearchRequired, verifyResearchCertification } from './researchCertification.mjs';\n",
    "overseer research import",
)
text = replace_once(
    text,
    "export const OVERSEER_POLICY_VERSION = 'execution-admission-v3';",
    "export const OVERSEER_POLICY_VERSION = 'execution-admission-v4';",
    "overseer policy version",
)
old_return = """  return {
    schemaVersion: 1,
    strategyId: input.strategyId ?? firstOrder.strategyId ?? null,
"""
new_return = """  const envelope = {
    schemaVersion: 1,
    strategyId: input.strategyId ?? firstOrder.strategyId ?? null,
"""
text = replace_once(text, old_return, new_return, "overseer envelope declaration")
text = replace_once(
    text,
    """    riskDecision: {
      approved: riskDecision?.approved === true,
      reasons: Array.isArray(riskDecision?.reasons) ? [...riskDecision.reasons] : [],
    },
  };
}
""",
    """    riskDecision: {
      approved: riskDecision?.approved === true,
      reasons: Array.isArray(riskDecision?.reasons) ? [...riskDecision.reasons] : [],
    },
  };
  const researchRequired = strategyScannerResearchRequired(envelope);
  if (researchRequired || input.researchCertification) {
    envelope.requiresResearchCertification = researchRequired;
    envelope.researchCertification = input.researchCertification && typeof input.researchCertification === 'object'
      ? input.researchCertification
      : null;
  }
  return envelope;
}
""",
    "overseer envelope research binding",
)
text = replace_once(
    text,
    """  if (Number.isFinite(confidenceScore) && confidenceScore < minConfidence) reasons.push('confidence_below_threshold');
  if (mode === 'live') reasons.push('live_execution_not_certified');
""",
    """  if (Number.isFinite(confidenceScore) && confidenceScore < minConfidence) reasons.push('confidence_below_threshold');
  const researchRequired = strategyScannerResearchRequired(envelope);
  if (researchRequired || envelope.researchCertification) {
    const research = verifyResearchCertification(envelope.researchCertification, {
      strategyId: envelope.strategyId,
      symbol: envelope.symbol,
    });
    if (!research.ok) reasons.push(...research.reasons);
  }
  if (mode === 'live') reasons.push('live_execution_not_certified');
""",
    "overseer research admission",
)
path.write_text(text)


# Execution state keeps the certification hash-bound through plan/approve/submit.
path = Path("packages/execution/src/executionEngine.mjs")
text = path.read_text()
text = replace_once(
    text,
    "      netExecutableEdgeUsd: request.netExecutableEdgeUsd ?? null,\n    };",
    "      netExecutableEdgeUsd: request.netExecutableEdgeUsd ?? null,\n      researchCertification: request.researchCertification || null,\n      requiresResearchCertification: request.requiresResearchCertification === true,\n    };",
    "execution state research fields",
)
text = replace_once(
    text,
    """  normalizeDurableExecution(execution) {
    return {
      ...execution,
      orders: Array.isArray(execution.orders) ? execution.orders : [],
""",
    """  normalizeDurableExecution(execution) {
    const metadata = execution?.metadata || {};
    return {
      ...execution,
      researchCertification: execution.researchCertification ?? metadata.researchCertification ?? null,
      requiresResearchCertification: execution.requiresResearchCertification ?? metadata.requiresResearchCertification ?? false,
      orders: Array.isArray(execution.orders) ? execution.orders : [],
""",
    "durable execution research hydration",
)
path.write_text(text)


# Durable execution metadata keeps the credential across compatibility sync / restart.
path = Path("packages/storage/src/executionRepository.mjs")
text = path.read_text()
text = replace_once(
    text,
    "      riskDecision: input.riskDecision || null,\n      tags: input.tags || {},",
    "      riskDecision: input.riskDecision || null,\n      researchCertification: input.researchCertification || input.metadata?.researchCertification || null,\n      requiresResearchCertification: input.requiresResearchCertification === true || input.metadata?.requiresResearchCertification === true,\n      tags: input.tags || {},",
    "execution repository research metadata",
)
path.write_text(text)
