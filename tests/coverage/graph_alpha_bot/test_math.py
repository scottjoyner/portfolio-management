from app.utils.math import risk_reward


def test_risk_reward_positive():
    assert risk_reward(100.0, 95.0, 110.0) == 2.0


def test_risk_reward_negative_direction():
    # target below entry -> negative reward
    assert risk_reward(100.0, 95.0, 90.0) == -2.0


def test_risk_reward_zero_stop_returns_zero():
    assert risk_reward(100.0, 100.0, 110.0) == 0.0


def test_risk_reward_float_passthrough():
    assert risk_reward(50.0, 40.0, 60.0) == 1.0
