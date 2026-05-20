from strategies.base.interfaces import StrategyConfig
from strategies.registry.registry import load_strategies, strategy_metadata_index
from strategies.trend.breakout import TrendFollowingBreakoutStrategy


def test_strategy_config_rejects_unrealistic_score_ceiling():
    try:
        StrategyConfig(max_abs_score=0.1)
    except ValueError as exc:
        assert "unrealistically tight" in str(exc)
    else:
        raise AssertionError("StrategyConfig accepted unsafe max_abs_score")


def test_base_strategies_have_data_requirements_and_mode_flags():
    metadata = strategy_metadata_index()
    trend = metadata["TrendFollowingBreakoutStrategy"]
    assert "data_requirements" in trend
    assert trend["paper_mode"] is True
    assert trend["backtest_supported"] is True


def test_cooldown_blocks_immediate_resignal():
    strategy = TrendFollowingBreakoutStrategy()
    market_state = {
        "product_id": "BTC-USD",
        "score": 0.6,
        "close": 100.0,
        "high": 101.0,
        "warmup_complete": True,
    }
    first = strategy.generate_signal(market_state)
    second = strategy.generate_signal(market_state)
    assert first is not None
    assert second is None


def test_strategy_registry_has_unique_ids():
    strategies = load_strategies()
    ids = [strategy.strategy_id for strategy in strategies]
    assert len(ids) == len(set(ids))
