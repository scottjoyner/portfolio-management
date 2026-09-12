from pathlib import Path

certified = Path('scripts/certified_strategy_signal.py')
text = certified.read_text()
if 'ROOT = Path(__file__).resolve().parents[1]' not in text:
    text = text.replace(
        'import math\nfrom types import SimpleNamespace\n',
        'import math\nimport sys\nfrom pathlib import Path\nfrom types import SimpleNamespace\n',
        1,
    )
    anchor = 'from typing import Any\n\nfrom scripts.challenger_manager import ChallengerRegistry\n'
    replacement = '''from typing import Any\n\nROOT = Path(__file__).resolve().parents[1]\nif str(ROOT) not in sys.path:\n    sys.path.insert(0, str(ROOT))\n\nfrom scripts.challenger_manager import ChallengerRegistry\n'''
    if anchor not in text:
        raise SystemExit('certified scanner import anchor missing')
    text = text.replace(anchor, replacement, 1)
    certified.write_text(text)

path = Path('apps/api/src/opportunityGenerator.mjs')
text = path.read_text()
verifier_import = "import { verifyResearchCertification } from '../../../packages/execution/src/researchCertification.mjs';\n"
if verifier_import not in text:
    anchor = "import { GraphAlphaBotAdapter } from '../../../packages/adapters/src/graphAlphaBotAdapter.mjs';\n"
    if anchor not in text:
        raise SystemExit('generator import anchor missing')
    text = text.replace(anchor, anchor + verifier_import, 1)
scanner_const = "const CERTIFIED_STRATEGY_SIGNAL_SCANNER = fileURLToPath(new URL('../../../scripts/certified_strategy_signal.py', import.meta.url));\n"
if scanner_const not in text:
    anchor = "const STRATEGY_SIGNAL_SCANNER = fileURLToPath(new URL('../../../scripts/strategy_signal_scanner.py', import.meta.url));\n"
    if anchor not in text:
        raise SystemExit('scanner const anchor missing')
    text = text.replace(anchor, anchor + scanner_const, 1)

old = '''  const expectedValueMethod = isEntry\n    ? 'tp_sl_payoff_expected_pnl_v1'\n    : 'risk_reduction_not_edge_scored_v1';\n\n  return {\n    sourceAgentId: 'strategy-comparison-scanner','''
new = '''  const expectedValueMethod = isEntry\n    ? 'tp_sl_payoff_expected_pnl_v1'\n    : 'risk_reduction_not_edge_scored_v1';\n  const certificationCheck = verifyResearchCertification(signal.research_certification, {\n    strategyId: signal.strategy || null,\n    symbol: signal.symbol || signal.product_id || null,\n  });\n  const certified = signal.tournament_certified === true && certificationCheck.ok;\n  const certificationHash = certified ? signal.research_certification.certification_hash : null;\n\n  return {\n    sourceAgentId: certified ? 'certified-strategy-runtime' : 'strategy-comparison-scanner','''
if old not in text:
    raise SystemExit('certified detection anchor missing')
text = text.replace(old, new, 1)

old = '''    dataFreshnessScore: 95,\n    backtestStatus: 'same_window_30d_screen_uncertified',\n    executionAdmission: {\n      policy: 'same_window_screen_requires_certification',\n      status: 'screen_only',\n      autoDraftEligible: false,\n      researchCertification: 'uncertified',\n      reason: 'same_window_30d_screen_uncertified',\n    },'''
new = '''    dataFreshnessScore: 95,\n    backtestStatus: certified\n      ? 'tournament_terminal_promoted_exact_runtime'\n      : 'same_window_30d_screen_uncertified',\n    researchCertification: certified ? signal.research_certification : null,\n    executionAdmission: certified ? {\n      policy: 'certified_research_requires_executable_edge_v1',\n      status: 'research_certified_pending_executable_edge',\n      autoDraftEligible: false,\n      researchCertification: 'verified_runtime_binding',\n      certificationHash,\n      reason: 'net_executable_edge_not_yet_verified',\n    } : {\n      policy: 'same_window_screen_requires_certification',\n      status: 'screen_only',\n      autoDraftEligible: false,\n      researchCertification: 'uncertified',\n      reason: 'same_window_30d_screen_uncertified',\n    },'''
if old not in text:
    raise SystemExit('admission status anchor missing')
text = text.replace(old, new, 1)

old = '''    notes: `${signal.reason || signal.backtest_reason || 'strategy scan'} | same-window screen only; not tournament-certified | win_rate=${(winProbability * 100).toFixed(1)}% | sentiment=${Number(signal.sentiment_score || 0).toFixed(2)} | weighted_conf=${weightedConfidence.toFixed(2)} | tp=${takeProfitPrice || 'n/a'} | sl=${stopLossPrice || 'n/a'} | source=${signal.source || 'live_cli'}`,\n    evidence: [{\n      type: 'strategy_live_30d_test','''
new = '''    notes: certified\n      ? `${signal.reason || 'certified strategy signal'} | exact promoted runtime binding | certification=${certificationHash} | terminal_win_rate=${(winProbability * 100).toFixed(1)}% | tp=${takeProfitPrice || 'n/a'} | sl=${stopLossPrice || 'n/a'} | source=${signal.source || 'live_cli'}`\n      : `${signal.reason || signal.backtest_reason || 'strategy scan'} | same-window screen only; not tournament-certified | win_rate=${(winProbability * 100).toFixed(1)}% | sentiment=${Number(signal.sentiment_score || 0).toFixed(2)} | weighted_conf=${weightedConfidence.toFixed(2)} | tp=${takeProfitPrice || 'n/a'} | sl=${stopLossPrice || 'n/a'} | source=${signal.source || 'live_cli'}`,\n    evidence: [{\n      type: certified ? 'strategy_certified_runtime_signal' : 'strategy_live_30d_test','''
if old not in text:
    raise SystemExit('notes/evidence anchor missing')
text = text.replace(old, new, 1)

old = '''      validationScope: 'same_window_in_sample_screen',\n      tournamentCertified: false,\n      expectedValueUnit: 'USD','''
new = '''      validationScope: certified\n        ? 'tournament_terminal_promoted_exact_runtime'\n        : 'same_window_in_sample_screen',\n      tournamentCertified: certified,\n      researchCertificationHash: certificationHash,\n      certificationVerificationReasons: certificationCheck.reasons,\n      expectedValueUnit: 'USD','''
if old not in text:
    raise SystemExit('validation evidence anchor missing')
text = text.replace(old, new, 1)

runner_anchor = 'export async function ingestConnectorSnapshots(state, options = {}) {'
runner = '''export function runCertifiedStrategySignalScanner(options = {}) {\n  const scannerPath = options.certifiedScannerPath || CERTIFIED_STRATEGY_SIGNAL_SCANNER;\n  const args = [scannerPath, '--cache-ttl', String(options.cacheTtl || options.cache_ttl || 900)];\n  if (options.refresh) args.push('--refresh');\n  const stdout = execFileSync('python3', args, { encoding: 'utf8', stdio: ['ignore', 'pipe', 'pipe'] });\n  return JSON.parse(stdout);\n}\n\n'''
if 'export function runCertifiedStrategySignalScanner' not in text:
    if runner_anchor not in text:
        raise SystemExit('certified runner insertion anchor missing')
    text = text.replace(runner_anchor, runner + runner_anchor, 1)

generate = '''export async function generateOpportunityFromCertifiedStrategySignal(state, options = {}) {\n  ensureOpportunityState(state);\n  const scan = runCertifiedStrategySignalScanner(options);\n  const signal = scan?.signal;\n  if (!signal) return { scan, opportunity: null, executions: [], errors: [] };\n  const symbol = signal.symbol || signal.product_id;\n  const certificationHash = signal.research_certification?.certification_hash || null;\n  const duplicate = state.opportunities.some(opp =>\n    opp.symbol === symbol\n    && opp.evidence?.some(e => e.type === 'strategy_certified_runtime_signal' && e.researchCertificationHash === certificationHash)\n    && ['needs_review', 'approved', 'research_requested', 'deferred'].includes(opp.status)\n  );\n  if (duplicate) return { scan, opportunity: null, executions: [], errors: [] };\n  const strategyId = state.strategies.some(strategy => strategy.id === signal.strategy) ? signal.strategy : null;\n  const result = createOpportunity(state, {\n    ...strategySignalToOpportunityInput(signal),\n    strategyId,\n    status: 'needs_review',\n    approvalStatus: 'needs_review',\n  });\n  if (result.errors) return { scan, opportunity: null, executions: [], errors: result.errors };\n  return { scan, opportunity: result.opportunity, executions: [], errors: [] };\n}\n'''
if 'export async function generateOpportunityFromCertifiedStrategySignal' not in text:
    text = text.rstrip() + '\n\n' + generate.rstrip() + '\n'
path.write_text(text)

flows = Path('apps/api/src/opportunityFlowsLegacy.mjs')
text = flows.read_text()
old = '''    tradePlan: body.tradePlan || null,\n    executionAdmission: body.executionAdmission && typeof body.executionAdmission === 'object'\n      ? { ...body.executionAdmission }\n      : null,'''
new = '''    tradePlan: body.tradePlan || null,\n    researchCertification: body.researchCertification && typeof body.researchCertification === 'object'\n      ? { ...body.researchCertification }\n      : null,\n    executionAdmission: body.executionAdmission && typeof body.executionAdmission === 'object'\n      ? { ...body.executionAdmission }\n      : null,'''
if old not in text:
    raise SystemExit('opportunity certification persistence anchor missing')
text = text.replace(old, new, 1)
old = '''      tradePlan: opportunity.tradePlan || null,\n      tradeIntent: opportunity.tradeIntent || null,'''
new = '''      tradePlan: opportunity.tradePlan || null,\n      researchCertification: opportunity.researchCertification || null,\n      tradeIntent: opportunity.tradeIntent || null,'''
if old not in text:
    raise SystemExit('execution certification carry anchor missing')
text = text.replace(old, new, 1)
flows.write_text(text)
