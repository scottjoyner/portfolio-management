import type { ExecutionState, OrderStatus, OrderIntent, OrderFill } from '@pkg/core/types.js';
import type { ExecutionEvent } from './types.js';

const VALID_TRANSITIONS: Record<OrderStatus, OrderStatus[]> = {
  draft: ['approved', 'rejected', 'cancelled'],
  approved: ['submitted', 'rejected'],
  rejected: [],
  submitted: ['partially_filled', 'filled', 'cancelled', 'failed'],
  partially_filled: ['filled', 'cancelled', 'failed'],
  filled: [],
  cancelled: [],
  expired: [],
  failed: [],
};

export function validateTransition(from: OrderStatus, to: OrderStatus): boolean {
  const allowed = VALID_TRANSITIONS[from];
  return allowed?.includes(to) ?? false;
}

export function createExecutionState(params: {
  strategyId: string;
  opportunityId?: string;
  accountId: string;
  mode: ExecutionState['mode'];
  orders: OrderIntent[];
  confidenceScore: number;
  convictionWeight: number;
  riskDecision: ExecutionState['riskDecision'];
}): ExecutionState {
  return {
    id: `exec-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`,
    strategyId: params.strategyId,
    opportunityId: params.opportunityId,
    accountId: params.accountId,
    mode: params.mode,
    status: 'draft',
    orders: params.orders,
    fills: [],
    confidenceScore: params.confidenceScore,
    convictionWeight: params.convictionWeight,
    riskDecision: params.riskDecision,
    startedAt: new Date().toISOString(),
    lastHeartbeatAt: new Date().toISOString(),
  };
}

export function transitionExecutionState(state: ExecutionState, newStatus: OrderStatus, fill?: OrderFill): { state: ExecutionState; event: ExecutionEvent } {
  if (!validateTransition(state.status, newStatus)) {
    throw new Error(`Invalid transition: ${state.status} -> ${newStatus}`);
  }

  const event: ExecutionEvent = {
    id: `evt-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`,
    executionId: state.id,
    type: newStatus as ExecutionEvent['type'],
    timestamp: new Date().toISOString(),
    payload: fill ? { fillId: fill.id, quantity: fill.quantity, price: fill.price } : undefined,
  };

  const updated: ExecutionState = {
    ...state,
    status: newStatus,
    fills: fill ? [...state.fills, fill] : state.fills,
    completedAt: ['filled', 'cancelled', 'failed', 'expired'].includes(newStatus) ? new Date().toISOString() : state.completedAt,
    lastHeartbeatAt: new Date().toISOString(),
    error: newStatus === 'failed' ? 'Execution failed' : state.error,
  };

  return { state: updated, event };
}

export function transitionToPartialFill(state: ExecutionState, fill: OrderFill): { state: ExecutionState; event: ExecutionEvent } {
  const remaining = state.orders.reduce((sum, o) => sum + o.quantity, 0) - state.fills.reduce((sum, f) => sum + f.quantity, 0) - fill.quantity;
  const newStatus: OrderStatus = remaining <= 0 ? 'filled' : 'partially_filled';
  return transitionExecutionState(state, newStatus, fill);
}

export function transitionToFailed(state: ExecutionState, error: string): { state: ExecutionState; event: ExecutionEvent } {
  const updated: ExecutionState = {
    ...state,
    status: 'failed',
    completedAt: new Date().toISOString(),
    lastHeartbeatAt: new Date().toISOString(),
    error,
  };
  const event: ExecutionEvent = {
    id: `evt-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`,
    executionId: state.id,
    type: 'failed',
    payload: { error },
    timestamp: new Date().toISOString(),
  };
  return { state: updated, event };
}
