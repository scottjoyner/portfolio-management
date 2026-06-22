import type { ConfidenceScore, OrderIntent, RiskDecision, Market } from '@pkg/core/types.js';

export interface ConfidenceScorerConfig {
  minOverall: number;
  strategyConvictionWeight: number;
  marketConditionWeight: number;
  riskAssessmentWeight: number;
  historicalPerformanceWeight: number;
  dataFreshnessWeight: number;
  minDataFreshnessHours: number;
  decayFactorPerHour: number;
}

const DEFAULT_CONFIG: ConfidenceScorerConfig = {
  minOverall: 0.6,
  strategyConvictionWeight: 0.30,
  marketConditionWeight: 0.25,
  riskAssessmentWeight: 0.25,
  historicalPerformanceWeight: 0.12,
  dataFreshnessWeight: 0.08,
  minDataFreshnessHours: 24,
  decayFactorPerHour: 0.02,
};

export interface ScoringInput {
  strategySignalStrength: number;
  strategyWinRate?: number;
  strategySharpe?: number;
  strategyTotalTrades?: number;
  spreadBps: number;
  liquidityScore: number;
  volatilityScore: number;
  volume24h: number;
  marketStatus: string;
  riskDecision: RiskDecision;
  dataTimestamp: string;
  market?: Market;
  convictionOverride?: number;
}

export class ConfidenceScorer {
  private config: ConfidenceScorerConfig;

  constructor(config: Partial<ConfidenceScorerConfig> = {}) {
    this.config = { ...DEFAULT_CONFIG, ...config };
  }

  score(input: ScoringInput): ConfidenceScore {
    const strategyConviction = this.scoreStrategyConviction(input);
    const marketCondition = this.scoreMarketCondition(input);
    const riskAssessment = this.scoreRiskAssessment(input);
    const historicalPerformance = this.scoreHistoricalPerformance(input);
    const dataFreshness = this.scoreDataFreshness(input);

    const overall =
      strategyConviction * this.config.strategyConvictionWeight +
      marketCondition * this.config.marketConditionWeight +
      riskAssessment * this.config.riskAssessmentWeight +
      historicalPerformance * this.config.historicalPerformanceWeight +
      dataFreshness * this.config.dataFreshnessWeight;

    const clamped = Math.min(1, Math.max(0, overall));

    const reasons: string[] = [];
    if (strategyConviction < 0.4) reasons.push('low_strategy_conviction');
    if (marketCondition < 0.4) reasons.push('poor_market_conditions');
    if (!input.riskDecision.approved) reasons.push(...input.riskDecision.reasons);
    if (dataFreshness < 0.3) reasons.push('stale_data');
    if (historicalPerformance < 0.3) reasons.push('poor_historical_performance');

    return {
      overall: clamped,
      strategyConviction,
      marketCondition,
      riskAssessment,
      historicalPerformance,
      dataFreshness,
      explanation: reasons.length > 0 ? `Gated: ${reasons.join(', ')}` : 'All checks passed',
      components: {
        strategyConviction,
        marketCondition,
        riskAssessment,
        historicalPerformance,
        dataFreshness,
      },
    };
  }

  qualifies(scores: ConfidenceScore): { ok: boolean; reason?: string } {
    if (scores.overall < this.config.minOverall) {
      return { ok: false, reason: `confidence_below_threshold: ${scores.overall.toFixed(3)} < ${this.config.minOverall}` };
    }
    if (!scores.riskAssessment || scores.riskAssessment < 0.3) {
      return { ok: false, reason: 'risk_assessment_blocked' };
    }
    return { ok: true };
  }

  computeConvictionWeight(score: ConfidenceScore, baseWeight: number = 1.0): number {
    return baseWeight * (0.5 + score.overall * 0.5);
  }

  applyToOrder(intent: OrderIntent, score: ConfidenceScore): OrderIntent {
    return {
      ...intent,
      confidenceScore: score.overall,
      convictionWeight: this.computeConvictionWeight(score),
    };
  }

  private scoreStrategyConviction(input: ScoringInput): number {
    const signal = Math.min(1, Math.max(0, Math.abs(input.strategySignalStrength)));
    if (input.convictionOverride !== undefined) {
      return Math.min(1, Math.max(0, input.convictionOverride));
    }
    return signal;
  }

  private scoreMarketCondition(input: ScoringInput): number {
    const spreadScore = Math.max(0, 1 - input.spreadBps / 500);
    const liquidityScore = Math.min(1, input.liquidityScore / 100);
    const volatilityScore = Math.max(0, 1 - input.volatilityScore / 150);
    const volumeScore = Math.min(1, Math.log10(input.volume24h + 1) / 12);

    return spreadScore * 0.30 + liquidityScore * 0.30 + volatilityScore * 0.20 + volumeScore * 0.20;
  }

  private scoreRiskAssessment(input: ScoringInput): number {
    if (!input.riskDecision.approved) return 0;
    const totalReasons = input.riskDecision.reasons.length;
    return Math.max(0.3, 1 - totalReasons * 0.15);
  }

  private scoreHistoricalPerformance(input: ScoringInput): number {
    const winRate = input.strategyWinRate ?? 0.5;
    const sharpe = input.strategySharpe ?? 0.5;
    const trades = input.strategyTotalTrades ?? 20;

    const winRateScore = Math.min(1, winRate / 0.7);
    const sharpeScore = Math.min(1, sharpe / 3.0);
    const tradeCountScore = Math.min(1, trades / 100);

    return winRateScore * 0.4 + sharpeScore * 0.4 + tradeCountScore * 0.2;
  }

  private scoreDataFreshness(input: ScoringInput): number {
    const ageMs = Date.now() - new Date(input.dataTimestamp).getTime();
    const ageHours = ageMs / (1000 * 60 * 60);
    if (ageHours <= this.config.minDataFreshnessHours) return 1;
    const decay = Math.max(0, 1 - (ageHours - this.config.minDataFreshnessHours) * this.config.decayFactorPerHour);
    return decay;
  }
}

export function createConfidenceScorer(config?: Partial<ConfidenceScorerConfig>): ConfidenceScorer {
  return new ConfidenceScorer(config);
}
