"""Tests for pricing models in evaluation package."""

import pytest


def test_price_estimation_engine_initialization():
    """Test PriceEstimationEngine initialization with various configs."""
    from evaluation.pricing_models import PriceEstimationEngine
    
    # Test default config
    engine = PriceEstimationEngine()
    assert engine.price_source == "fundamental"
    
    # Test custom config
    config = {
        "price_source": "technical",
        "volatility_threshold_high": 0.4,
        "volatility_threshold_extreme": 0.6,
    }
    engine = PriceEstimationEngine(config)
    assert engine.price_source == "technical"


def test_estimate_price_basic():
    """Test basic price estimation functionality."""
    from evaluation.pricing_models import PriceEstimationEngine, PriceTargetModel
    
    engine = PriceEstimationEngine(config={"price_source": "fundamental"})
    price_data = {
        "current_price": "5000",
        "market_cap": "50B",
        "volume_24h": "1.2B"
    }
    
    result = engine.estimate_price(
        "ETH", 
        PriceTargetModel.FUNDAMENTAL_BASED, 
        price_data
    )
    
    # Verify structure of result
    assert "buy_level" in result
    assert "sell_level" in result
    assert "hold_level" in result
    assert "confidence_score" in result
    assert "model_used" in result
    
    # Verify values are reasonable (placehers)
    assert isinstance(result["buy_level"], float)
    assert isinstance(result["sell_level"], float)


def test_calculate_position_quality_basic():
    """Test position quality metrics calculation."""
    from evaluation.pricing_models import (
        PriceEstimationEngine, 
        PositionQualityMetrics
    )
    
    engine = PriceEstimationEngine()
    position_data = {
        "quantity": 100,
        "entry_price": "4500",
        "current_price": "5000",
        "correlation_to_index": 0.85,
        "volatility_regime": "moderate"
    }
    
    metrics = engine.calculate_position_quality(position_data)
    
    # Verify structure of result
    assert isinstance(metrics, PositionQualityMetrics)
    assert hasattr(metrics, "risk_score")
    assert hasattr(metrics, "alpha_score")
    assert hasattr(metrics, "beta_exposure")
    assert hasattr(metrics, "correlation_to_index")


def test_position_quality_risk_scoring():
    """Test that risk scoring increases with position size."""
    
    engine = PriceEstimationEngine()
    
    # Small position - lower risk
    small_pos = {
        "quantity": 10,
        "entry_price": "4500",
        "current_price": "5000",
    }
    
    # Large position - higher risk
    large_pos = {
        "quantity": 1000,
        "entry_price": "4500",
        "current_price": "5000",
    }
    
    small_metrics = engine.calculate_position_quality(small_pos)
    large_metrics = engine.calculate_position_quality(large_pos)
    
    # Large position should have higher risk score (placeholder logic)
    assert large_metrics.risk_score >= small_metrics.risk_score


def test_volatility_regime_classification():
    """Test volatility regime classification."""
    from evaluation.pricing_models import PriceEstimationEngine
    
    engine = PriceEstimationEngine(config={
        "volatility_threshold_high": 0.4,
        "volatility_threshold_extreme": 0.6,
    })
    
    # Low volatility
    low_vol_data = {"average_volatility_20d": 25}
    metrics = engine.calculate_position_quality({
        **low_vol_data,
        "correlation_to_index": 0.8,
        "volatility_regime": "low"
    })
    assert metrics.volatility_regime == "low"
    
    # High volatility
    high_vol_data = {"average_volatility_20d": 55}
    metrics = engine.calculate_position_quality({
        **high_vol_data,
        "correlation_to_index": 0.7,
        "volatility_regime": "high"
    })
    assert metrics.volatility_regime == "high"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
