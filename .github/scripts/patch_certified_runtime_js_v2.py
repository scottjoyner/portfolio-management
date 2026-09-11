from pathlib import Path


def replace_once(text: str, old: str, new: str, label: str) -> str:
    if new in text:
        return text
    if old not in text:
        raise SystemExit(f"missing anchor: {label}")
    return text.replace(old, new, 1)


# Opportunity projection: preserve research identity and stop auto-drafting entries.
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
      // Research credibility and current trade economics are separate gates.
      // Fresh scanner entries are surfaced for review/economic evaluation but
      // cannot manufacture an execution draft from the scan alone.
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


# Opportunity approval: scanner entries require research identity + authoritative economics.
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
    "    strategyId: body.strategyId || null,",
    "    strategyId: body.strategyId || body.researchCertification?.strategy_name || null,",
    "opportunity strategy identity",
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
        strategyId: opportunity.strategyId || opportunity.researchCertification?.strategy_name,
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


# Overseer: the credential itself is part of the immutable trade-intent hash.
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
text = replace_once(
    text,
    "    schemaVersion: 2,",
    "    schemaVersion: 3,",
    "trade intent schema version",
)
text = replace_once(
    text,
    "    tradePlan: input.tradePlan && typeof input.tradePlan === 'object' ? input.tradePlan : null,\n    orders,",
    "    tradePlan: input.tradePlan && typeof input.tradePlan === 'object' ? input.tradePlan : null,\n    requiresResearchCertification: strategyScannerResearchRequired(input),\n    researchCertification: input.researchCertification && typeof input.researchCertification === 'object' ? input.researchCertification : null,\n    orders,",
    "overseer envelope research binding",
)
text = replace_once(
    text,
    """  if (!riskDecisionHash) reasons.push('risk_decision_hash_required');
  if (Number.isFinite(confidenceScore) && confidenceScore < minConfidence) reasons.push('confidence_below_threshold');
  if (mode === 'live') reasons.push('live_execution_not_certified');
""",
    """  if (!riskDecisionHash) reasons.push('risk_decision_hash_required');
  if (Number.isFinite(confidenceScore) && confidenceScore < minConfidence) reasons.push('confidence_below_threshold');
  if (envelope.requiresResearchCertification || envelope.researchCertification) {
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


# Execution state keeps the certification hash-bound through plan/approve/submit/restart.
path = Path("packages/execution/src/executionEngine.mjs")
text = path.read_text()
text = replace_once(
    text,
    """function normalizeDurableExecution(execution = {}) {
  return {
    ...clone(execution),
    orders: Array.isArray(execution.orders) ? clone(execution.orders) : [],
""",
    """function normalizeDurableExecution(execution = {}) {
  const metadata = execution?.metadata || {};
  return {
    ...clone(execution),
    researchCertification: clone(execution.researchCertification ?? metadata.researchCertification ?? null),
    requiresResearchCertification: execution.requiresResearchCertification ?? metadata.requiresResearchCertification ?? false,
    orders: Array.isArray(execution.orders) ? clone(execution.orders) : [],
""",
    "durable execution research hydration",
)
text = replace_once(
    text,
    "      positionSide: request.positionSide || plan.positionSide || null,\n      entryPrice:",
    "      positionSide: request.positionSide || plan.positionSide || null,\n      researchCertification: clone(request.researchCertification || null),\n      requiresResearchCertification: request.requiresResearchCertification === true,\n      entryPrice:",
    "execution state research fields",
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
