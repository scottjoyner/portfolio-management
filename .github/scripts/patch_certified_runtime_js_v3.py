from pathlib import Path

# Apply the complete v2 patch first.
exec(compile(
    Path('.github/scripts/patch_certified_runtime_js_v2.py').read_text(),
    '.github/scripts/patch_certified_runtime_js_v2.py',
    'exec',
))

# Preserve the pre-existing fail-closed ordering: invalid/zero execution size is
# rejected before any higher-level research/economic admission checks.
path = Path('apps/api/src/opportunityFlowsLegacy.mjs')
text = path.read_text()
old = """  let approvedExecutionSize = null;
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
    approvedExecutionSize = finiteNumber(rawExecutionSize, NaN);
    if (!Number.isFinite(approvedExecutionSize) || approvedExecutionSize <= 0) {
      return { errors: ['execution_size_required'] };
    }
  }
"""
new = """  let approvedExecutionSize = null;
  if (body.status === 'approved') {
    const rawExecutionSize = opportunity.positionSizing?.recommendedSize ?? opportunity.totalMoneyRisked;
    approvedExecutionSize = finiteNumber(rawExecutionSize, NaN);
    if (!Number.isFinite(approvedExecutionSize) || approvedExecutionSize <= 0) {
      return { errors: ['execution_size_required'] };
    }
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
  }
"""
if old not in text:
    if new not in text:
        raise SystemExit('missing opportunity admission ordering anchor')
else:
    text = text.replace(old, new, 1)
path.write_text(text)
