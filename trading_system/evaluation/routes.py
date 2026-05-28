from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy.orm import Session

from evaluation.service import EvaluationService
from storage.postgres.repository import OpsRepository
from storage.postgres.session import get_db

router = APIRouter(prefix="/evaluations", tags=["evaluations"])


class EvaluationResponse(BaseModel):
    instrument: str
    consensus: dict
    agents: list[dict]
    evidence: list[dict]
    evaluated_at: str


class PortfolioEvaluationResponse(BaseModel):
    portfolio_id: str
    total_instruments: int
    results: dict[str, EvaluationResponse]
    evaluated_at: str


def _svc(db: Session) -> EvaluationService:
    return EvaluationService(OpsRepository(db))


@router.post("/instruments/{instrument}/evaluate", response_model=EvaluationResponse)
def evaluate_instrument(instrument: str, db: Session = Depends(get_db)) -> EvaluationResponse:
    market_data = {
        "current_price": 60000.0,
        "entry_price": 55000.0,
        "volatility_1h": 0.025,
        "volume_24h": 1_500_000,
        "value_at_risk": 0.08,
        "current_drawdown": 0.05,
        "correlation_to_index": 0.75,
        "spread_bps": 8.0,
        "backtest_sharpe": 1.2,
        "backtest_max_drawdown": 0.15,
        "dcf_intrinsic_value": 65000.0,
        "technical_score": 72.0,
        "sentiment_score": 0.25,
        "tvl_usd": 50_000_000,
        "onchain_volume_24h": 5_000_000,
        "active_users_24h": 2500,
    }
    return _svc(db).evaluate_instrument(instrument, market_data)


@router.post("/portfolios/{portfolio_id}/evaluate", response_model=PortfolioEvaluationResponse)
def evaluate_portfolio(portfolio_id: str, db: Session = Depends(get_db)) -> PortfolioEvaluationResponse:
    result = _svc(db).evaluate_portfolio(portfolio_id)
    if "error" in result:
        raise HTTPException(status_code=404, detail=result["error"])
    return result


@router.get("/history/{instrument}")
def evaluation_history(instrument: str, db: Session = Depends(get_db)) -> list[dict]:
    from storage.postgres.models import AnalystRating, PriceEstimate, SentimentAnalysis
    ratings = db.query(AnalystRating).filter(AnalystRating.instrument == instrument).all()
    estimates = db.query(PriceEstimate).filter(PriceEstimate.instrument == instrument).order_by(PriceEstimate.timestamp.desc()).limit(10).all()
    sentiments = db.query(SentimentAnalysis).filter(SentimentAnalysis.product_id == instrument).order_by(SentimentAnalysis.timestamp.desc()).limit(10).all()

    return {
        "instrument": instrument,
        "analyst_ratings": [
            {"analyst": r.analyst, "rating": r.rating_text, "price_target": float(r.price_target) if r.price_target else None, "at": r.created_at.isoformat()}
            for r in ratings
        ],
        "price_estimates": [
            {"dcf_value": float(e.dcf_intrinsic_value) if e.dcf_intrinsic_value else None, "technical_score": float(e.technical_score) if e.technical_score else None, "confidence": float(e.confidence_score), "at": e.timestamp.isoformat()}
            for e in estimates
        ],
        "sentiment": [
            {"regime": s.regime, "score": float(s.sentiment_score), "at": s.timestamp.isoformat()}
            for s in sentiments
        ],
    }


@router.get("/stale-data-warnings")
def stale_data_warnings(db: Session = Depends(get_db)) -> list[dict]:
    from storage.postgres.models import MarketDataFeed
    stale = db.query(MarketDataFeed).filter(
        MarketDataFeed.state != "healthy"
    ).all()
    return [
        {"feed": f.feed_name, "state": f.state, "freshness_ms": f.freshness_ms}
        for f in stale
    ]
