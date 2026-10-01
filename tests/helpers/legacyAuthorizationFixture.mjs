import { buildTradeIntentEnvelope, stableHash } from '../../packages/execution/src/overseer.mjs';
import { evaluateTradeIntent as evaluateV3 } from '../../packages/execution/src/overseerLegacy.mjs';
import { allocatePortfolioRequest } from '../../packages/execution/src/portfolioAllocator.mjs';
import { buildCapitalRiskSnapshot } from '../../packages/execution/src/capitalRiskSnapshot.mjs';
import { createInitialOperatorState } from '../../packages/storage/src/operatorStore.mjs';

/**
 * A genuinely valid authorization minted under the legacy v3 policy.
 *
 * Earlier versions of the migration test used hand-written state, which proved
 * nothing: those fixtures were not actually valid, and a stricter verifier
 * correctly rejected them. This builds the real thing, using the same sequence
 * the execution engine uses:
 *
 *   allocate -> bind the capital risk snapshot to the *allocated* intent ->
 *   evaluate
 *
 * The allocation scales quantity, so the allocated intent hash differs from the
 * requested one, and binding the snapshot to the wrong one is what produced
 * the earlier hash mismatches.
 */

export const FIXTURE_NOW = '2026-10-01T00:00:00.000Z';
/** Inside the 30s portfolio-allocation TTL, so the artifact is not expired. */
export const FIXTURE_VERIFY_AT = '2026-10-01T00:00:05.000Z';
const ACCOUNT_ID = 'acct-paper-primary';

function operatorState(now) {
  const state = createInitialOperatorState(now);
  state.accounts[0].cash = 100000;
  state.accounts[0].nav = 100000;
  state.accounts[0].updatedAt = now;
  state.killSwitch = { enabled: false, reason: null, updatedAt: now };
  state.portfolioRiskState = {
    accounts: [{
      accountId: ACCOUNT_ID,
      highWaterNavUsd: 100000,
      highWaterAt: now,
      initializedAt: now,
      updatedAt: now,
      source: 'test',
    }],
    correlationMatrix: {},
    volatilityBySymbol: {},
    updatedAt: now,
  };
  state.marketDataSnapshots = [
    { id: 'md-btc', symbol: 'BTC-USD', venue: 'coinbase-paper', bid: 99990, ask: 100010, volume24h: 1000, status: 'connected', source: 'test', timestamp: now },
    { id: 'md-eth', symbol: 'ETH-USD', venue: 'coinbase-paper', bid: 2999, ask: 3001, volume24h: 10000, status: 'connected', source: 'test', timestamp: now },
  ];
  return state;
}

function request(overrides = {}) {
  return {
    strategyId: 'strategy-target',
    accountId: ACCOUNT_ID,
    mode: 'paper',
    venue: 'coinbase-paper',
    symbol: 'BTC-USD',
    side: 'buy',
    confidenceScore: 0.9,
    entryPrice: 100000,
    orders: [{
      id: 'order-target',
      symbol: 'BTC-USD',
      venue: 'coinbase-paper',
      side: 'buy',
      quantity: 0.05,
      price: 100000,
      orderType: 'market',
      timeInForce: 'GTC',
      confidenceScore: 0.9,
    }],
    ...overrides,
  };
}

/** Build a state that genuinely verifies under the v3 policy it was minted with. */
export function buildLegacyAuthorizedState({ now = FIXTURE_NOW, requestOverrides = {} } = {}) {
  const state = operatorState(now);
  const { allocation, executionRequest } = allocatePortfolioRequest({
    state,
    source: { source: 'test-store', revision: 'r1', observedAt: now },
    request: request(requestOverrides),
    now,
    policy: {},
  });

  const tradeIntentEnvelope = buildTradeIntentEnvelope(executionRequest);
  const tradeIntentHash = stableHash(tradeIntentEnvelope);
  const snapshot = buildCapitalRiskSnapshot({
    state,
    source: { source: 'test-store', revision: 'r1', observedAt: now },
    tradeIntentEnvelope,
    tradeIntentHash,
    now,
    policy: {},
  });

  const riskDecision = {
    approved: true,
    reasons: [],
    policyVersion: snapshot.policyVersion ?? null,
    snapshotHash: snapshot.snapshotHash ?? null,
    portfolioAllocationHash: allocation.allocationHash,
    portfolioAllocationPolicyVersion: allocation.policyVersion,
    portfolioAllocationDecisionHash: allocation.allocationDecisionHash,
  };

  const evaluation = evaluateV3(
    { ...executionRequest, riskDecision },
    {
      now,
      capitalRiskSnapshotHash: snapshot.snapshotHash ?? null,
      capitalRiskPolicyVersion: snapshot.policyVersion ?? null,
    }
  );

  return {
    allocation,
    snapshot,
    evaluation,
    storedState: {
      ...executionRequest,
      // executionId is safe to add; opportunityId is not, because the trade
      // intent envelope includes it and the verifier rebuilds the envelope from
      // the stored state. Overriding it would change the intent hash and make
      // the fixture fail its own policy for the wrong reason.
      executionId: 'exec-legacy-1',
      tradeIntentEnvelope: evaluation.tradeIntentEnvelope,
      tradeIntentHash: evaluation.tradeIntentHash,
      portfolioAllocation: allocation,
      portfolioAllocationHash: allocation.allocationHash,
      portfolioAllocationDecisionHash: allocation.allocationDecisionHash,
      capitalRiskSnapshot: snapshot,
      capitalRiskSnapshotHash: snapshot.snapshotHash ?? null,
      riskDecision: evaluation.riskDecision,
      riskDecisionHash: stableHash(evaluation.riskDecision),
      overseerDecision: evaluation.overseerDecision,
    },
  };
}

/** The legacy decision hash, so a test can assert it is preserved as provenance. */
export function legacyDecisionHash(storedState) {
  return storedState.overseerDecision.decisionHash;
}

export { ACCOUNT_ID, FIXTURE_NOW as NOW };
