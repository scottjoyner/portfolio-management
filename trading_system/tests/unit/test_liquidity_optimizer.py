from portfolio.liquidity_distribution.optimizer import LiquidityInput, LiquidityOptimizer


def test_liquidity_score_bounds() -> None:
    optimizer = LiquidityOptimizer()
    score = optimizer.score(
        LiquidityInput(
            asset="USDC",
            idle_balance=500_000,
            working_balance=300_000,
            depth_score=0.8,
            spread_opportunity=0.7,
            friction_score=0.3,
            hedgeability=0.9,
            risk_budget_available=0.6,
        )
    )

    assert 0 <= score.usefulness <= 1
    assert 0 <= score.productivity <= 1
    assert 0 <= score.transfer_necessity <= 1


def test_recommend_move_amount_increases_with_idle_balance() -> None:
    optimizer = LiquidityOptimizer()
    low_idle = LiquidityInput("USDC", 10_000, 100_000, 0.7, 0.6, 0.2, 0.9, 0.7)
    high_idle = LiquidityInput("USDC", 150_000, 100_000, 0.7, 0.6, 0.2, 0.9, 0.7)

    assert optimizer.recommend_move_amount(high_idle) > optimizer.recommend_move_amount(low_idle)
