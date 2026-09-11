from pathlib import Path

path = Path('apps/api/src/opportunityFlowsLegacy.mjs')
text = path.read_text()
marker = "execution_size_required"
if marker in text:
    print('execution size patch already present')
    raise SystemExit(0)

old = '''  if (!opportunity) return { errors: ['opportunity_not_found'] };\n  if (!['approved', 'rejected', 'deferred'].includes(body.status)) return { errors: ['invalid_opportunity_decision'] };\n\n  opportunity.status = body.status;'''
new = '''  if (!opportunity) return { errors: ['opportunity_not_found'] };\n  if (!['approved', 'rejected', 'deferred'].includes(body.status)) return { errors: ['invalid_opportunity_decision'] };\n\n  let approvedExecutionSize = null;\n  if (body.status === 'approved') {\n    const rawExecutionSize = opportunity.positionSizing?.recommendedSize ?? opportunity.totalMoneyRisked;\n    approvedExecutionSize = finiteNumber(rawExecutionSize, NaN);\n    if (!Number.isFinite(approvedExecutionSize) || approvedExecutionSize <= 0) {\n      return { errors: ['execution_size_required'] };\n    }\n  }\n\n  opportunity.status = body.status;'''
if old not in text:
    raise SystemExit('decideOpportunity validation anchor not found')
text = text.replace(old, new, 1)

old = '''    const size = opportunity.positionSizing?.recommendedSize || opportunity.totalMoneyRisked || 1000;'''
new = '''    const size = approvedExecutionSize;'''
if old not in text:
    raise SystemExit('execution size fallback anchor not found')
text = text.replace(old, new, 1)

path.write_text(text)
