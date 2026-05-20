import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[2]))

import argparse
import hashlib
import json

from analytics.metrics.live_transfer import BacktestRealismScorer, SimulationAssumptions
from analytics.metrics.performance import basic_metrics
from execution.queue_model.models import SimpleQueueModel
from research.experiment_tracking.tracker import ExperimentRun, ExperimentTracker
from strategies.registry.registry import load_strategies


def _horizon_from_metadata(meta: dict) -> str:
    return str(meta.get("expected_holding_horizon", "intraday"))


def _strategy_assumptions(strategy_id: str, queue_fill: float, stale_decay: float, latency_ms: float) -> SimulationAssumptions:
    sid = strategy_id.lower()
    is_micro = "micro" in sid or "queue" in sid or "latency" in sid
    is_maker = "maker" in sid or "mm" in sid
    return SimulationAssumptions(
        latency_ms=max(1.0, latency_ms - (8.0 if is_micro else 0.0)),
        queue_fill_probability=min(0.95, queue_fill + (0.08 if is_maker else 0.0)),
        stale_quote_decay=max(0.0, stale_decay - (0.08 if is_maker else 0.02)),
        maker_ratio=0.7 if is_maker else 0.45,
        cancel_ratio=0.72 if is_maker else 0.55,
        rejection_rate=0.03 if is_micro else 0.015,
        outage_rate=0.01,
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True)
    parser.add_argument("--latency-ms", type=float, default=25.0)
    parser.add_argument("--output", default="artifacts/backtest_summary.json")
    parser.add_argument("--validation-report", default="artifacts/live_transfer_validation.json")
    args = parser.parse_args()

    sample_returns = [0.01, -0.004, 0.007, 0.002, -0.001]
    metrics = basic_metrics(sample_returns)
    queue = SimpleQueueModel().estimate(queue_ahead=3.0, trade_rate=1.4, cancel_rate=0.8)

    strategies = load_strategies()
    assessments = []
    for s in strategies:
        meta = s.metadata()
        assumptions = _strategy_assumptions(
            strategy_id=s.strategy_id,
            queue_fill=queue.fill_probability,
            stale_decay=queue.stale_quote_decay,
            latency_ms=args.latency_ms,
        )
        assessments.append(
            BacktestRealismScorer.assess_strategy(
                strategy_id=s.strategy_id,
                simulated_return=metrics["return"],
                sharpe=metrics["sharpe"],
                assumptions=assumptions,
                holding_horizon_hint=_horizon_from_metadata(meta),
            )
        )

    ranked = sorted(assessments, key=lambda x: x.live_transfer_confidence, reverse=True)
    strategy_rank = [
        {
            "rank": i + 1,
            "strategy_id": a.strategy_id,
            "live_transfer_confidence": round(a.live_transfer_confidence, 4),
            "fragility_score": round(a.fragility_score, 4),
            "expected_live_return": round(a.expected_live_return, 6),
            "realism_penalty_total": round(a.breakdown.total, 4),
        }
        for i, a in enumerate(ranked)
    ]

    report = {
        "config": args.config,
        "latency_injection_ms": args.latency_ms,
        "metrics": metrics,
        "queue_model": queue.__dict__,
        "maker_taker_participation": {"maker": 0.62, "taker": 0.38},
        "fragility_score": round(sum(a.fragility_score for a in assessments) / len(assessments), 4),
        "live_portability_score": round(sum(a.live_transfer_confidence for a in assessments) / len(assessments), 4),
        "strategy_count": len(strategies),
    }

    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(report, indent=2), encoding="utf-8")

    validation = {
        "config": args.config,
        "methodology": "realism-penalized portability scoring",
        "optimism_sources_penalized": [
            "low latency assumptions",
            "high queue fill probability",
            "low stale quote decay",
            "high cancel/replace turnover",
            "order rejection/outage under-modeling",
        ],
        "strategy_rankings": strategy_rank,
    }
    validation_path = Path(args.validation_report)
    validation_path.parent.mkdir(parents=True, exist_ok=True)
    validation_path.write_text(json.dumps(validation, indent=2), encoding="utf-8")

    cfg_hash = hashlib.sha256(args.config.encode("utf-8")).hexdigest()[:16]
    ExperimentTracker().record(ExperimentRun(run_id=cfg_hash, strategy_id="portfolio", config_hash=cfg_hash, metrics=metrics))
    print(report)


if __name__ == "__main__":
    main()
