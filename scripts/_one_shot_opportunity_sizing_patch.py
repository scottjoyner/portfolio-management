from pathlib import Path

path = Path('apps/api/src/opportunityFlowsLegacy.mjs')
text = path.read_text()
marker = "sizingAuthority: 'requested_notional_capped'"
if marker in text:
    print('opportunity sizing patch already present')
    raise SystemExit(0)

old = '''  const winProbability = clampPercent(body.winProbability, 0.5);\n  const lossProbability = clampPercent(body.lossProbability ?? (1 - winProbability), 1 - winProbability);\n  const averageWin = nonNegative(body.potentialUpside || 0) / Math.max(1, nonNegative(body.totalMoneyRisked || 1));\n  const averageLoss = 1;\n  const kellyFraction = winProbability > lossProbability\n    ? Math.max(0, (winProbability * averageWin - lossProbability * averageLoss) / averageWin)\n    : 0;\n  const kellyCapped = Math.min(kellyFraction, 0.25);\n  const maxPositionSize = nonNegative(state.config?.maxPositionSizeUsd || 50000);\n  const recommendedSize = Math.min(maxPositionSize, Math.round(nonNegative(body.totalMoneyRisked || 1000)));'''
# The branch still has the old *inflating* recommendedSize. Match it if needed.
old_inflating = '''  const winProbability = clampPercent(body.winProbability, 0.5);\n  const lossProbability = clampPercent(body.lossProbability ?? (1 - winProbability), 1 - winProbability);\n  const averageWin = nonNegative(body.potentialUpside || 0) / Math.max(1, nonNegative(body.totalMoneyRisked || 1));\n  const averageLoss = 1;\n  const kellyFraction = winProbability > lossProbability\n    ? Math.max(0, (winProbability * averageWin - lossProbability * averageLoss) / averageWin)\n    : 0;\n  const kellyCapped = Math.min(kellyFraction, 0.25);\n  const maxPositionSize = nonNegative(state.config?.maxPositionSizeUsd || 50000);\n  const recommendedSize = Math.min(maxPositionSize, Math.round(nonNegative(body.totalMoneyRisked || 1000) * (1 + kellyCapped)));'''
new = '''  const winProbability = clampPercent(body.winProbability, 0.5);\n  const lossProbability = clampPercent(body.lossProbability ?? (1 - winProbability), 1 - winProbability);\n  const requestedNotional = nonNegative(body.totalMoneyRisked || 0);\n  const maxLossUsd = nonNegative(body.maxLoss || body.totalMoneyRisked || 0);\n  const potentialUpsideUsd = nonNegative(body.potentialUpside || 0);\n  const notionalBase = Math.max(1, requestedNotional);\n  const averageWin = potentialUpsideUsd / notionalBase;\n  const averageLoss = maxLossUsd / notionalBase;\n  const kellyNumerator = (winProbability * averageWin) - (lossProbability * averageLoss);\n  const kellyDenominator = averageWin * averageLoss;\n  const kellyFraction = averageWin > 0 && averageLoss > 0 && kellyDenominator > 0\n    ? Math.max(0, kellyNumerator / kellyDenominator)\n    : 0;\n  const kellyCapped = Math.min(kellyFraction, 0.25);\n  const maxPositionSize = nonNegative(state.config?.maxPositionSizeUsd || 50000);\n  // Opportunity construction does not know portfolio equity or allocator budget.\n  // Preserve the requested notional (subject to the hard per-position cap) and\n  // expose Kelly only as a diagnostic; the portfolio allocator owns sizing.\n  const recommendedSize = Math.min(maxPositionSize, Math.round(requestedNotional));'''
if old_inflating in text:
    text = text.replace(old_inflating, new, 1)
elif old in text:
    text = text.replace(old, new, 1)
else:
    raise SystemExit('createOpportunity sizing anchor not found')

old = '''      recommendedSize,\n      maxPositionSize,\n      capitalAtRisk: nonNegative(body.totalMoneyRisked || 0),\n      riskPerUnit: nonNegative(body.maxLoss || 0) / Math.max(1, recommendedSize),'''
new = '''      recommendedSize,\n      maxPositionSize,\n      requestedNotional,\n      sizingAuthority: 'requested_notional_capped',\n      capitalAtRisk: maxLossUsd,\n      riskPerUnit: maxLossUsd / Math.max(1, recommendedSize),'''
if old not in text:
    raise SystemExit('position sizing artifact anchor not found')
text = text.replace(old, new, 1)

old = '''    expectedReturn: Number(((winProbability * nonNegative(body.potentialUpside || 0)) - (lossProbability * nonNegative(body.maxLoss || 0))).toFixed(2)),\n    expectedRisk: Number((nonNegative(body.totalMoneyRisked || 0) * (1 - winProbability)).toFixed(2)),'''
new = '''    expectedReturn: Number(((winProbability * potentialUpsideUsd) - (lossProbability * maxLossUsd)).toFixed(2)),\n    expectedRisk: Number((lossProbability * maxLossUsd).toFixed(2)),'''
if old not in text:
    raise SystemExit('expected return/risk anchor not found')
text = text.replace(old, new, 1)

path.write_text(text)
