import type { ExecutionState, OrderIntent, OrderFill, OrderStatus, ConfidenceScore, RiskDecision, ExecutionMode } from '@pkg/core/types.js';

export interface ExecutionRequest {
  strategyId: string;
  opportunityId?: string;
  accountId: string;
  mode: ExecutionMode;
  orders: OrderIntent[];
  metadata?: Record<string, unknown>;
}

export interface ExecutionResult {
  ok: boolean;
  execution: ExecutionState;
  errors?: string[];
  warnings?: string[];
}

export interface ExecutionPlan {
  id: string;
  requests: ExecutionRequest[];
  confidenceScore: number;
  convictionWeight: number;
  riskDecision: RiskDecision;
  createdAt: string;
  approved: boolean;
}

export interface ExecutionEvent {
  id: string;
  executionId: string;
  type: 'created' | 'approved' | 'rejected' | 'submitted' | 'partially_filled' | 'filled' | 'cancelled' | 'failed' | 'settled';
  payload?: Record<string, unknown>;
  timestamp: string;
}

export interface ExecutionStore {
  save(state: ExecutionState): Promise<void>;
  load(id: string): Promise<ExecutionState | null>;
  list(filter?: { strategyId?: string; status?: OrderStatus; mode?: ExecutionMode }): Promise<ExecutionState[]>;
  saveEvent(event: ExecutionEvent): Promise<void>;
  getEvents(executionId: string): Promise<ExecutionEvent[]>;
}

export type ExecutionStatusHandler = (state: ExecutionState, event: ExecutionEvent) => Promise<void>;
