from core.models.domain import CapitalBucketType, ExchangeTrustScore, OrderIntent, RiskMode
from risk.engine import RiskEngine, RiskPolicy


def test_locked_reserve_blocked():
    engine = RiskEngine(RiskPolicy())
    intent = OrderIntent(
        strategy_id="s",
        product_id="BTC-USD",
        side="BUY",
        order_type="LIMIT",
        size=0.1,
        price=30000,
        bucket=CapitalBucketType.LOCKED_RESERVE,
        rationale="test",
    )
    allowed, reason = engine.evaluate(intent, mark_price=30000)
    assert not allowed
    assert "locked reserve" in reason


def test_small_order_allowed_normal_mode():
    engine = RiskEngine(RiskPolicy())
    intent = OrderIntent(
        strategy_id="s", product_id="BTC-USD", side="BUY", order_type="LIMIT", size=0.01, price=30000, rationale="test"
    )
    allowed, _ = engine.evaluate(intent, mark_price=30000)
    assert allowed


def test_untrusted_blocks_risk_increasing_orders():
    engine = RiskEngine(RiskPolicy())
    engine.set_exchange_trust(ExchangeTrustScore.UNTRUSTED)
    intent = OrderIntent(
        strategy_id="s", product_id="BTC-USD", side="BUY", order_type="LIMIT", size=0.01, price=30000, rationale="test"
    )
    allowed, _ = engine.evaluate(intent, mark_price=30000)
    assert not allowed


def test_expert_mode_requires_enablement():
    engine = RiskEngine(RiskPolicy())
    intent = OrderIntent(
        strategy_id="s",
        product_id="BTC-USD",
        side="BUY",
        order_type="LIMIT",
        size=0.01,
        price=30000,
        rationale="test",
        risk_mode=RiskMode.EXPERT_HIGH_RISK,
    )
    allowed, _ = engine.evaluate(intent, mark_price=30000)
    assert not allowed
    engine.enable_mode(RiskMode.EXPERT_HIGH_RISK)
    allowed2, _ = engine.evaluate(intent, mark_price=30000)
    assert allowed2
