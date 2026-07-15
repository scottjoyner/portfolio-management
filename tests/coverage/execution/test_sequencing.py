from decimal import Decimal

from trading_system.execution.hybrid.sequencing import classify_hybrid_atomicity


def test_strong():
    assert classify_hybrid_atomicity(Decimal("1"), Decimal("1")) == "SEMI_ATOMIC_STRONG"
    assert classify_hybrid_atomicity(Decimal("0.9"), Decimal("0.95")) == "SEMI_ATOMIC_STRONG"


def test_moderate():
    assert classify_hybrid_atomicity(Decimal("0.8"), Decimal("0.8")) == "SEMI_ATOMIC_MODERATE"
    assert classify_hybrid_atomicity(Decimal("0.6"), Decimal("0.9")) == "SEMI_ATOMIC_MODERATE"


def test_non_atomic():
    assert classify_hybrid_atomicity(Decimal("0.5"), Decimal("0.5")) == "NON_ATOMIC"
    assert classify_hybrid_atomicity(Decimal("0.1"), Decimal("0.2")) == "NON_ATOMIC"
