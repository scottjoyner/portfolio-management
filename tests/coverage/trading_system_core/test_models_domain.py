"""Tests for trading_system.core.models.domain (pydantic models + fallback)."""

import importlib.util
import sys

from trading_system.core.models.domain import (
    CapitalBucketType,
    RiskMode,
    ExchangeTrustScore,
    CapitalBucket,
    OrderIntent,
    StructuredApprovalPayload,
    LatencyTrace,
    Bracket,
)


def test_enums():
    assert CapitalBucketType.ACTIVE_TRADING.value == "ACTIVE_TRADING"
    assert RiskMode.NORMAL.value == "NORMAL"
    assert ExchangeTrustScore.HEALTHY.value == "HEALTHY"


def test_capital_bucket_pydantic():
    cb = CapitalBucket(
        name="core",
        bucket_type=CapitalBucketType.ACTIVE_TRADING,
        target_weight=0.5,
        min_weight=0.1,
        max_weight=0.9,
        locked=True,
    )
    assert cb.target_weight == 0.5
    assert cb.locked is True
    assert cb.model_dump()["name"] == "core"


def test_order_intent_pydantic():
    oi = OrderIntent(
        strategy_id="s",
        product_id="BTC-USD",
        side="buy",
        order_type="market",
        size=1.0,
        bucket=CapitalBucketType.ACCUMULATION,
        risk_mode=RiskMode.AGGRESSIVE,
        rationale="r",
        reduce_only=True,
    )
    assert oi.side == "buy"
    assert oi.bucket == CapitalBucketType.ACCUMULATION
    assert oi.model_dump()["reduce_only"] is True


def test_structured_approval_payload():
    p = StructuredApprovalPayload(
        reason="r",
        urgency="high",
        expected_upside=0.1,
        modeled_worst_case_downside=-0.2,
        capital_affected=100.0,
        strategy_confidence=0.8,
        regime="trending",
        liquidity_state="good",
        exchange_trust_score=ExchangeTrustScore.HEALTHY,
        rollback_plan="rp",
    )
    assert p.strategy_confidence == 0.8
    assert p.model_dump()["exchange_trust_score"] == "HEALTHY"


def test_latency_trace_as_us_zero():
    t = LatencyTrace()
    us = t.as_us()
    assert us["feed_receive_latency_us"] == 0.0
    assert us["exchange_ack_latency_us"] == 0.0


def test_latency_trace_as_us_with_values():
    t = LatencyTrace(
        feed_received_ns=0,
        normalize_done_ns=1000,
        feature_done_ns=3000,
        strategy_done_ns=6000,
        risk_done_ns=10000,
        submit_done_ns=15000,
        ack_done_ns=20000,
        fill_done_ns=30000,
    )
    us = t.as_us()
    assert us["normalization_latency_us"] == 2.0
    assert us["feature_latency_us"] == 3.0
    assert us["strategy_decision_latency_us"] == 4.0
    assert us["risk_approval_latency_us"] == 5.0
    assert us["order_submit_latency_us"] == 5.0
    assert us["exchange_ack_latency_us"] == 10.0


def test_latency_trace_now_ns():
    assert isinstance(LatencyTrace.now_ns(), int)


def test_bracket_pydantic_defaults():
    b = Bracket(
        client_order_id="c1",
        product_id="BTC-USD",
        side="BUY",
        base_size=1.0,
        quote_size=100.0,
        entry_price=100.0,
        strategy_id="s",
        timestamp=1.0,
    )
    assert b.status == "OPEN"
    assert b.stop_loss is None
    assert b.metadata == {}
    assert b.model_dump()["product_id"] == "BTC-USD"


def test_bracket_full():
    b = Bracket(
        client_order_id="c2",
        product_id="ETH-USD",
        side="SELL",
        base_size=2.0,
        quote_size=200.0,
        entry_price=50.0,
        stop_loss=45.0,
        take_profit=60.0,
        status="FILLED",
        strategy_id="s2",
        timestamp=2.0,
        metadata={"k": "v"},
    )
    assert b.stop_loss == 45.0
    assert b.take_profit == 60.0
    assert b.model_dump()["metadata"] == {"k": "v"}


def test_fallback_field_when_pydantic_missing():
    """Cover the try/except ImportError fallback in domain.py (lines 5-12)."""
    path = __import__("trading_system.core.models.domain", fromlist=["__file__"]).__file__
    src = open(path).read()
    saved = sys.modules.get("pydantic")
    sys.modules["pydantic"] = None  # forces ImportError on `from pydantic import ...`
    try:
        ns = {}
        exec(compile(src, path, "exec"), ns)
        # The fallback Field() should have been defined and used.
        assert ns["BaseModel"] is object
        # Bracket uses Field(default_factory=dict) -> must not raise at def time
        assert "Bracket" in ns
    finally:
        del sys.modules["pydantic"]
        if saved is not None:
            sys.modules["pydantic"] = saved
