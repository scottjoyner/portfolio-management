"""Tests for pricing models in the evaluation package."""

import asyncio

import pytest

from evaluation.pricing_models import (
    PositionQualityMetrics,
    PriceEstimationEngine,
    PriceTargetModel,
)


def test_price_estimation_engine_initialization():
    engine = PriceEstimationEngine()
    assert engine.price_source == "fundamental"

    engine = PriceEstimationEngine(
        {
            "price_source": "technical",
            "volatility_threshold_high": 0.4,
            "volatility_threshold_extreme": 0.6,
        }
    )
    assert engine.price_source == "technical"


def test_estimate_price_basic():
    engine = PriceEstimationEngine(config={"price_source": "fundamental"})
    result = engine.estimate_price(
        "ETH",
        PriceTargetModel.FUNDAMENTAL_BASED,
        {
            "current_price": "5000",
            "market_cap": "50B",
            "volume_24h": "1.2B",
        },
    )

    assert "buy_level" in result
    assert "sell_level" in result
    assert "hold_level" in result
    assert "confidence_score" in result
    assert "model_used" in result
    assert isinstance(result["buy_level"], float)
    assert isinstance(result["sell_level"], float)


def test_estimate_price_remains_await_compatible():
    engine = PriceEstimationEngine()
    immediate = engine.estimate_price(
        "ETH",
        PriceTargetModel.FUNDAMENTAL_BASED,
        {"current_price": 5000},
    )

    async def resolve():
        return await immediate

    assert asyncio.run(resolve()) is immediate


def test_calculate_position_quality_basic():
    engine = PriceEstimationEngine()
    metrics = engine.calculate_position_quality(
        {
            "quantity": 100,
            "entry_price": "4500",
            "current_price": "5000",
            "correlation_to_index": 0.85,
            "volatility_regime": "moderate",
        }
    )

    assert isinstance(metrics, PositionQualityMetrics)
    assert hasattr(metrics, "risk_score")
    assert hasattr(metrics, "alpha_score")
    assert hasattr(metrics, "beta_exposure")
    assert hasattr(metrics, "correlation_to_index")


def test_position_quality_risk_scoring():
    engine = PriceEstimationEngine()
    small_metrics = engine.calculate_position_quality(
        {
            "quantity": 10,
            "entry_price": "4500",
            "current_price": "5000",
        }
    )
    large_metrics = engine.calculate_position_quality(
        {
            "quantity": 1000,
            "entry_price": "4500",
            "current_price": "5000",
        }
    )

    assert large_metrics.risk_score >= small_metrics.risk_score


def test_volatility_regime_classification():
    engine = PriceEstimationEngine(
        {
            "volatility_threshold_high": 0.4,
            "volatility_threshold_extreme": 0.6,
        }
    )

    metrics = engine.calculate_position_quality(
        {
            "average_volatility_20d": 25,
            "correlation_to_index": 0.8,
            "volatility_regime": "low",
        }
    )
    assert metrics.volatility_regime == "low"

    metrics = engine.calculate_position_quality(
        {
            "average_volatility_20d": 55,
            "correlation_to_index": 0.7,
            "volatility_regime": "high",
        }
    )
    assert metrics.volatility_regime == "high"


def test_invalid_current_price_fails_closed():
    engine = PriceEstimationEngine()
    with pytest.raises(ValueError, match="current_price"):
        engine.estimate_price(
            "ETH",
            PriceTargetModel.FUNDAMENTAL_BASED,
            {"current_price": 0},
        )
