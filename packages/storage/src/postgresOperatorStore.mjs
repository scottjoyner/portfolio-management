import { createInitialOperatorState, normalizeOperatorState, MemoryOperatorStore } from './operatorStore.mjs';

function rowJson(value, fallback) {
  if (value === null || value === undefined) return fallback;
  if (typeof value === 'string') return JSON.parse(value);
  return value;
}

function iso(value) {
  if (!value) return null;
  if (value instanceof Date) return value.toISOString();
  return String(value);
}

function statusFromError(error) {
  return error ? { ok: false, error: error.message || String(error) } : { ok: true };
}

function num(value, fallback = 0) {
  if (value === null || value === undefined) return fallback;
  const n = Number(value);
  return Number.isFinite(n) ? n : fallback;
}

export class PostgresOperatorStore extends MemoryOperatorStore {
  constructor(options = {}) {
    super(options.seedState || createInitialOperatorState());
    this.kind = 'postgres';
    this.durable = true;
    this.sql = true;
    this.databaseUrl = options.databaseUrl || process.env.DATABASE_URL || 'postgresql://postgres:postgres@localhost:5432/arb';
    this.client = options.client || null;
    this.bootstrap = options.bootstrap !== false;
    this.lastError = null;
    this.migrations = { ok: false, checked: false, applied: [] };
  }

  async getClient() {
    if (this.client) return this.client;
    let pg;
    try {
      pg = await import('pg');
    } catch (error) {
      throw new Error('pg_dependency_missing: install the pg package or inject a compatible test client');
    }
    const pool = new pg.Pool({ connectionString: this.databaseUrl });
    this.client = pool;
    return this.client;
  }

  async query(sql, params = []) {
    const client = await this.getClient();
    try {
      return await client.query(sql, params);
    } catch (error) {
      this.lastError = error;
      throw error;
    }
  }

  async checkMigrations() {
    try {
      const result = await this.query(
        "SELECT to_regclass('public.schema_migrations') AS schema_migrations, to_regclass('public.strategies') AS strategies, to_regclass('public.opportunities') AS opportunities"
      );
      const row = result.rows?.[0] || {};
      if (!row.strategies) {
        this.migrations = { ok: false, checked: true, applied: [], reason: 'operator_tables_missing' };
        return this.migrations;
      }
      if (!row.opportunities) {
        this.migrations = { ok: false, checked: true, applied: [], reason: 'opportunity_agent_tables_missing' };
        return this.migrations;
      }
      if (!row.schema_migrations) {
        this.migrations = { ok: true, checked: true, applied: [], reason: 'schema_migrations_table_missing_legacy_ok' };
        return this.migrations;
      }
      const versions = await this.query('SELECT version FROM schema_migrations ORDER BY version ASC');
      this.migrations = { ok: true, checked: true, applied: versions.rows.map(r => r.version) };
      return this.migrations;
    } catch (error) {
      this.lastError = error;
      this.migrations = { ok: false, checked: true, applied: [], reason: error.message || String(error) };
      return this.migrations;
    }
  }

  async load() {
    await this.checkMigrations();
    if (!this.migrations.ok) throw new Error(`postgres_migrations_not_ready: ${this.migrations.reason || 'unknown'}`);

    const [strategies, backtests, approvals, positions, audit, flags, marketData, budgets, jobs, opportunities, risks, costs] = await Promise.all([
      this.query('SELECT * FROM strategies ORDER BY created_at ASC, id ASC'),
      this.query('SELECT * FROM backtest_runs ORDER BY started_at ASC, id ASC'),
      this.query('SELECT * FROM approvals ORDER BY created_at ASC, id ASC'),
      this.query('SELECT * FROM positions ORDER BY opened_at ASC, id ASC'),
      this.query('SELECT * FROM audit_events ORDER BY at ASC, id ASC'),
      this.query("SELECT key, value_json, updated_at FROM operator_flags WHERE key = 'kill_switch'"),
      this.query('SELECT * FROM market_data_snapshots ORDER BY timestamp DESC, id ASC'),
      this.query('SELECT * FROM agent_budgets ORDER BY agent_id ASC'),
      this.query('SELECT * FROM research_jobs ORDER BY started_at ASC, id ASC'),
      this.query('SELECT * FROM opportunities ORDER BY created_at ASC, id ASC'),
      this.query('SELECT * FROM risk_breakdowns ORDER BY generated_at ASC, id ASC'),
      this.query('SELECT * FROM agent_cost_ledger ORDER BY created_at ASC, id ASC')
    ]);

    const state = normalizeOperatorState({
      schemaVersion: 3,
      strategies: strategies.rows.map(row => ({
        id: row.id,
        name: row.name,
        version: Number(row.version),
        status: row.status,
        riskLevel: row.risk_level,
        parameters: rowJson(row.parameters_json, {}),
        createdAt: iso(row.created_at),
        updatedAt: iso(row.updated_at)
      })),
      backtests: backtests.rows.map(row => ({
        id: row.id,
        strategyId: row.strategy_id,
        status: row.status,
        startedAt: iso(row.started_at),
        completedAt: iso(row.completed_at),
        assumptions: rowJson(row.assumptions_json, {}),
        metrics: rowJson(row.metrics_json, {}),
        equityCurve: rowJson(row.equity_curve_json, []),
        trades: rowJson(row.trades_json, [])
      })),
      approvals: approvals.rows.map(row => ({
        id: row.id,
        strategyId: row.strategy_id,
        status: row.status,
        tier: row.tier,
        reason: row.reason,
        createdAt: iso(row.created_at),
        reviewedAt: iso(row.reviewed_at),
        reviewer: row.reviewer
      })),
      marketDataSnapshots: marketData.rows.map(row => ({
        id: row.id,
        symbol: row.symbol,
        venue: row.venue,
        assetClass: row.asset_class,
        bid: num(row.bid, null),
        ask: num(row.ask, null),
        spreadBps: num(row.spread_bps, null),
        volume24h: num(row.volume_24h, null),
        liquidityScore: num(row.liquidity_score, null),
        volatilityScore: num(row.volatility_score, null),
        status: row.status,
        source: row.source,
        timestamp: iso(row.timestamp)
      })),
      agentBudgets: budgets.rows.map(row => ({
        agentId: row.agent_id,
        dailyTokenLimit: num(row.daily_token_limit),
        dailyCostLimit: num(row.daily_cost_limit),
        perJobTokenLimit: num(row.per_job_token_limit),
        perMarketCostLimit: num(row.per_market_cost_limit),
        requireApprovalAboveCost: num(row.require_approval_above_cost),
        enabled: Boolean(row.enabled),
        updatedAt: iso(row.updated_at)
      })),
      researchJobs: jobs.rows.map(row => ({
        id: row.id,
        agentId: row.agent_id,
        triggerType: row.trigger_type,
        marketScope: row.market_scope,
        symbolScope: row.symbol_scope,
        provider: row.provider,
        model: row.model,
        localOrRemote: row.local_or_remote,
        status: row.status,
        startedAt: iso(row.started_at),
        completedAt: iso(row.completed_at),
        promptTokens: num(row.prompt_tokens),
        completionTokens: num(row.completion_tokens),
        totalTokens: num(row.total_tokens),
        estimatedRemoteCost: num(row.estimated_remote_cost),
        estimatedLocalCost: num(row.estimated_local_cost),
        opportunityIdsCreated: rowJson(row.opportunity_ids_json, []),
        failureReason: row.failure_reason
      })),
      opportunities: opportunities.rows.map(row => ({
        id: row.id,
        sourceAgentId: row.source_agent_id,
        researchJobId: row.research_job_id,
        strategyId: row.strategy_id,
        marketType: row.market_type,
        venue: row.venue,
        symbol: row.symbol,
        marketSlug: row.market_slug,
        title: row.title,
        recommendation: row.recommendation,
        confidenceScore: num(row.confidence_score),
        winProbability: num(row.win_probability),
        lossProbability: num(row.loss_probability),
        expectedValue: num(row.expected_value),
        grossExpectedValue: num(row.gross_expected_value),
        totalMoneyRisked: num(row.total_money_risked),
        maxLoss: num(row.max_loss),
        potentialUpside: num(row.potential_upside),
        rewardRiskRatio: num(row.reward_risk_ratio),
        liquidityScore: num(row.liquidity_score),
        dataFreshnessScore: num(row.data_freshness_score),
        backtestId: row.backtest_id,
        backtestStatus: row.backtest_status,
        riskBreakdownId: row.risk_breakdown_id,
        status: row.status,
        approvalStatus: row.approval_status,
        estimatedFees: num(row.estimated_fees),
        estimatedSlippage: num(row.estimated_slippage),
        estimatedGas: num(row.estimated_gas),
        agentResearchCost: num(row.agent_research_cost),
        modelInferenceCost: num(row.model_inference_cost),
        netExpectedValue: num(row.net_expected_value),
        notes: row.notes,
        evidence: rowJson(row.evidence_json, []),
        expiresAt: iso(row.expires_at),
        reviewedAt: iso(row.reviewed_at),
        reviewer: row.reviewer,
        decisionReason: row.decision_reason,
        createdAt: iso(row.created_at),
        updatedAt: iso(row.updated_at)
      })),
      riskBreakdowns: risks.rows.map(row => ({
        id: row.id,
        scope: row.scope,
        scopeId: row.scope_id,
        aggregateScore: num(row.aggregate_score),
        capitalAtRiskScore: num(row.capital_at_risk_score),
        liquidityScore: num(row.liquidity_score),
        slippageScore: num(row.slippage_score),
        drawdownScore: num(row.drawdown_score),
        volatilityScore: num(row.volatility_score),
        correlationScore: num(row.correlation_score),
        modelConfidenceScore: num(row.model_confidence_score),
        dataFreshnessScore: num(row.data_freshness_score),
        agentCostScore: num(row.agent_cost_score),
        explanation: row.explanation,
        generatedAt: iso(row.generated_at)
      })),
      agentCostLedger: costs.rows.map(row => ({
        id: row.id,
        agentId: row.agent_id,
        jobId: row.job_id,
        model: row.model,
        provider: row.provider,
        localOrRemote: row.local_or_remote,
        promptTokens: num(row.prompt_tokens),
        completionTokens: num(row.completion_tokens),
        totalTokens: num(row.total_tokens),
        remoteApiCost: num(row.remote_api_cost),
        localComputeCost: num(row.local_compute_cost),
        allocatedOpportunityId: row.allocated_opportunity_id,
        createdAt: iso(row.created_at)
      })),
      positions: positions.rows.map(row => ({
        id: row.id,
        strategyId: row.strategy_id,
        symbol: row.symbol,
        quantity: Number(row.quantity),
        averagePrice: Number(row.average_price),
        markPrice: row.mark_price === null || row.mark_price === undefined ? null : Number(row.mark_price),
        status: row.status,
        openedAt: iso(row.opened_at),
        closedAt: iso(row.closed_at)
      })),
      audit: audit.rows.map(row => ({
        id: row.id,
        action: row.action,
        actor: row.actor,
        at: iso(row.at),
        details: row.details,
        payload: rowJson(row.payload_json, {})
      })),
      killSwitch: flags.rows[0] ? rowJson(flags.rows[0].value_json, { enabled: false, reason: null, updatedAt: null }) : { enabled: false, reason: null, updatedAt: null }
    });

    if (this.bootstrap && state.strategies.length === 0 && state.audit.length === 0) {
      const seeded = createInitialOperatorState();
      await this.save(seeded);
      this.state = seeded;
      return this.state;
    }

    this.state = state;
    return this.state;
  }

  async save(nextState) {
    const state = normalizeOperatorState(nextState);
    await this.checkMigrations();
    if (!this.migrations.ok) throw new Error(`postgres_migrations_not_ready: ${this.migrations.reason || 'unknown'}`);

    await this.query('BEGIN');
    try {
      await this.query('DELETE FROM agent_cost_ledger');
      await this.query('DELETE FROM risk_breakdowns');
      await this.query('DELETE FROM opportunities');
      await this.query('DELETE FROM research_jobs');
      await this.query('DELETE FROM agent_budgets');
      await this.query('DELETE FROM market_data_snapshots');
      await this.query('DELETE FROM operator_flags');
      await this.query('DELETE FROM audit_events');
      await this.query('DELETE FROM positions');
      await this.query('DELETE FROM approvals');
      await this.query('DELETE FROM backtest_runs');
      await this.query('DELETE FROM strategies');

      for (const strategy of state.strategies) {
        await this.query(
          'INSERT INTO strategies (id, name, version, status, risk_level, parameters_json, created_at, updated_at) VALUES ($1,$2,$3,$4,$5,$6,$7,$8)',
          [strategy.id, strategy.name, strategy.version || 1, strategy.status || 'draft', strategy.riskLevel || strategy.risk_level || 'medium', JSON.stringify(strategy.parameters || {}), strategy.createdAt || new Date().toISOString(), strategy.updatedAt || new Date().toISOString()]
        );
      }

      for (const backtest of state.backtests) {
        await this.query(
          'INSERT INTO backtest_runs (id, strategy_id, status, assumptions_json, metrics_json, equity_curve_json, trades_json, started_at, completed_at) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)',
          [backtest.id, backtest.strategyId, backtest.status || 'completed', JSON.stringify(backtest.assumptions || {}), JSON.stringify(backtest.metrics || {}), JSON.stringify(backtest.equityCurve || []), JSON.stringify(backtest.trades || []), backtest.startedAt || new Date().toISOString(), backtest.completedAt || null]
        );
      }

      for (const approval of state.approvals) {
        await this.query(
          'INSERT INTO approvals (id, strategy_id, status, tier, reason, created_at, reviewed_at, reviewer) VALUES ($1,$2,$3,$4,$5,$6,$7,$8)',
          [approval.id, approval.strategyId, approval.status || 'pending_review', approval.tier || 'canary', approval.reason || null, approval.createdAt || new Date().toISOString(), approval.reviewedAt || null, approval.reviewer || null]
        );
      }

      for (const snapshot of state.marketDataSnapshots || []) {
        await this.query(
          'INSERT INTO market_data_snapshots (id, symbol, venue, asset_class, bid, ask, spread_bps, volume_24h, liquidity_score, volatility_score, status, source, timestamp) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)',
          [snapshot.id, snapshot.symbol, snapshot.venue, snapshot.assetClass || snapshot.asset_class, snapshot.bid ?? null, snapshot.ask ?? null, snapshot.spreadBps ?? snapshot.spread_bps ?? null, snapshot.volume24h ?? snapshot.volume_24h ?? null, snapshot.liquidityScore ?? snapshot.liquidity_score ?? null, snapshot.volatilityScore ?? snapshot.volatility_score ?? null, snapshot.status || 'watching', snapshot.source || 'unknown', snapshot.timestamp || new Date().toISOString()]
        );
      }

      for (const budget of state.agentBudgets || []) {
        await this.query(
          'INSERT INTO agent_budgets (agent_id, daily_token_limit, daily_cost_limit, per_job_token_limit, per_market_cost_limit, require_approval_above_cost, enabled, updated_at) VALUES ($1,$2,$3,$4,$5,$6,$7,$8)',
          [budget.agentId || budget.agent_id, budget.dailyTokenLimit || 0, budget.dailyCostLimit || 0, budget.perJobTokenLimit || 0, budget.perMarketCostLimit || 0, budget.requireApprovalAboveCost || 0, budget.enabled !== false, budget.updatedAt || new Date().toISOString()]
        );
      }

      for (const job of state.researchJobs || []) {
        await this.query(
          'INSERT INTO research_jobs (id, agent_id, trigger_type, market_scope, symbol_scope, provider, model, local_or_remote, status, started_at, completed_at, prompt_tokens, completion_tokens, total_tokens, estimated_remote_cost, estimated_local_cost, opportunity_ids_json, failure_reason) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18)',
          [job.id, job.agentId || job.agent_id, job.triggerType || job.trigger_type || 'operator_request', job.marketScope || job.market_scope || 'general', job.symbolScope || job.symbol_scope || null, job.provider || 'unknown', job.model || 'unknown', job.localOrRemote || job.local_or_remote || 'remote', job.status || 'completed', job.startedAt || new Date().toISOString(), job.completedAt || null, job.promptTokens || 0, job.completionTokens || 0, job.totalTokens || 0, job.estimatedRemoteCost || 0, job.estimatedLocalCost || 0, JSON.stringify(job.opportunityIdsCreated || []), job.failureReason || null]
        );
      }

      for (const opportunity of state.opportunities || []) {
        await this.query(
          'INSERT INTO opportunities (id, source_agent_id, research_job_id, strategy_id, market_type, venue, symbol, market_slug, title, recommendation, confidence_score, win_probability, loss_probability, expected_value, gross_expected_value, total_money_risked, max_loss, potential_upside, reward_risk_ratio, liquidity_score, data_freshness_score, backtest_id, backtest_status, risk_breakdown_id, status, approval_status, estimated_fees, estimated_slippage, estimated_gas, agent_research_cost, model_inference_cost, net_expected_value, notes, evidence_json, expires_at, reviewed_at, reviewer, decision_reason, created_at, updated_at) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25,$26,$27,$28,$29,$30,$31,$32,$33,$34,$35,$36,$37,$38,$39,$40)',
          [opportunity.id, opportunity.sourceAgentId || opportunity.source_agent_id, opportunity.researchJobId || opportunity.research_job_id || null, opportunity.strategyId || opportunity.strategy_id || null, opportunity.marketType || opportunity.market_type, opportunity.venue, opportunity.symbol || null, opportunity.marketSlug || opportunity.market_slug || null, opportunity.title, opportunity.recommendation || 'review', opportunity.confidenceScore ?? 0.5, opportunity.winProbability ?? 0.5, opportunity.lossProbability ?? 0.5, opportunity.expectedValue ?? 0, opportunity.grossExpectedValue ?? 0, opportunity.totalMoneyRisked ?? 0, opportunity.maxLoss ?? 0, opportunity.potentialUpside ?? 0, opportunity.rewardRiskRatio ?? 0, opportunity.liquidityScore ?? 50, opportunity.dataFreshnessScore ?? 70, opportunity.backtestId || null, opportunity.backtestStatus || 'backtest_missing', opportunity.riskBreakdownId || null, opportunity.status || 'needs_review', opportunity.approvalStatus || opportunity.status || 'needs_review', opportunity.estimatedFees ?? 0, opportunity.estimatedSlippage ?? 0, opportunity.estimatedGas ?? 0, opportunity.agentResearchCost ?? 0, opportunity.modelInferenceCost ?? 0, opportunity.netExpectedValue ?? 0, opportunity.notes || null, JSON.stringify(opportunity.evidence || []), opportunity.expiresAt || null, opportunity.reviewedAt || null, opportunity.reviewer || null, opportunity.decisionReason || null, opportunity.createdAt || new Date().toISOString(), opportunity.updatedAt || new Date().toISOString()]
        );
      }

      for (const risk of state.riskBreakdowns || []) {
        await this.query(
          'INSERT INTO risk_breakdowns (id, scope, scope_id, aggregate_score, capital_at_risk_score, liquidity_score, slippage_score, drawdown_score, volatility_score, correlation_score, model_confidence_score, data_freshness_score, agent_cost_score, explanation, generated_at) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15)',
          [risk.id, risk.scope || 'opportunity', risk.scopeId || risk.scope_id, risk.aggregateScore ?? 0, risk.capitalAtRiskScore ?? 0, risk.liquidityScore ?? 0, risk.slippageScore ?? 0, risk.drawdownScore ?? 0, risk.volatilityScore ?? 0, risk.correlationScore ?? 0, risk.modelConfidenceScore ?? 0, risk.dataFreshnessScore ?? 0, risk.agentCostScore ?? 0, risk.explanation || '', risk.generatedAt || new Date().toISOString()]
        );
      }

      for (const cost of state.agentCostLedger || []) {
        await this.query(
          'INSERT INTO agent_cost_ledger (id, agent_id, job_id, model, provider, local_or_remote, prompt_tokens, completion_tokens, total_tokens, remote_api_cost, local_compute_cost, allocated_opportunity_id, created_at) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)',
          [cost.id, cost.agentId || cost.agent_id, cost.jobId || cost.job_id, cost.model || 'unknown', cost.provider || 'unknown', cost.localOrRemote || cost.local_or_remote || 'remote', cost.promptTokens || 0, cost.completionTokens || 0, cost.totalTokens || 0, cost.remoteApiCost || 0, cost.localComputeCost || 0, cost.allocatedOpportunityId || cost.allocated_opportunity_id || null, cost.createdAt || new Date().toISOString()]
        );
      }

      for (const position of state.positions) {
        await this.query(
          'INSERT INTO positions (id, strategy_id, symbol, quantity, average_price, mark_price, status, opened_at, closed_at) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)',
          [position.id, position.strategyId || null, position.symbol, position.quantity, position.averagePrice || position.average_price, position.markPrice || position.mark_price || null, position.status || 'open', position.openedAt || new Date().toISOString(), position.closedAt || null]
        );
      }

      for (const event of state.audit) {
        await this.query(
          'INSERT INTO audit_events (id, action, actor, at, details, payload_json) VALUES ($1,$2,$3,$4,$5,$6)',
          [event.id, event.action, event.actor || 'system', event.at || new Date().toISOString(), event.details || null, JSON.stringify(event.payload || {})]
        );
      }

      await this.query(
        'INSERT INTO operator_flags (key, value_json, updated_at) VALUES ($1,$2,$3)',
        ['kill_switch', JSON.stringify(state.killSwitch || { enabled: false, reason: null, updatedAt: null }), state.killSwitch?.updatedAt || new Date().toISOString()]
      );

      await this.query('COMMIT');
      this.state = state;
      return this.state;
    } catch (error) {
      await this.query('ROLLBACK').catch(() => {});
      this.lastError = error;
      throw error;
    }
  }

  async mutate(mutator) {
    const state = await this.load();
    const result = await mutator(state);
    await this.save(state);
    return result;
  }

  getStatus() {
    return {
      kind: this.kind,
      durable: this.durable,
      sql: this.sql,
      schemaVersion: this.state.schemaVersion,
      databaseUrlConfigured: Boolean(this.databaseUrl),
      migrations: this.migrations,
      connection: statusFromError(this.lastError)
    };
  }
}
