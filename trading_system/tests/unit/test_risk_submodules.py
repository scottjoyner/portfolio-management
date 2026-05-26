from decimal import Decimal

from risk.sizing.service import fixed_fractional, kelly_criterion, fixed_risk_usd
from risk.slippage.service import estimate_slippage, slippage_adjusted_price
from risk.limits.service import LimitManager, PositionLimit
from risk.kill_switch.service import KillSwitchManager
from risk.approvals.service import RiskApprovalService


def test_fixed_fractional_sizing():
    result = fixed_fractional(Decimal("100000"), Decimal("2"), Decimal("0.05"))
    assert result.suggested_size > 0
    assert result.max_size > 0


def test_fixed_fractional_zero_stop():
    result = fixed_fractional(Decimal("100000"), Decimal("2"), Decimal("0"))
    assert result.suggested_size == 0


def test_kelly_criterion():
    kelly = kelly_criterion(Decimal("0.6"), Decimal("100"), Decimal("50"))
    assert 0 < kelly <= Decimal("0.25")


def test_fixed_risk_usd():
    result = fixed_risk_usd(Decimal("100000"), Decimal("2"), Decimal("50000"))
    assert result.suggested_size > 0


def test_estimate_slippage():
    result = estimate_slippage(Decimal("1000"), Decimal("1000000"), Decimal("5"))
    assert result.estimated_slippage_bps > 0
    assert result.within_limits


def test_estimate_slippage_zero_liquidity():
    result = estimate_slippage(Decimal("10000"), Decimal("0"))
    assert not result.within_limits


def test_slippage_adjusted_price_buy():
    adjusted = slippage_adjusted_price(Decimal("50000"), Decimal("50"), "buy")
    assert adjusted > 50000


def test_slippage_adjusted_price_sell():
    adjusted = slippage_adjusted_price(Decimal("50000"), Decimal("50"), "sell")
    assert adjusted < 50000


def test_position_limit():
    lm = LimitManager()
    lm.set_limit(PositionLimit(product_id="BTC-USD", max_size=Decimal("10"), max_notional=Decimal("500000")))
    lm.update_position("BTC-USD", Decimal("5"))
    ok, reason = lm.check_order("BTC-USD", "buy", Decimal("3"), Decimal("50000"))
    assert ok
    assert reason == ""


def test_position_limit_exceeds():
    lm = LimitManager()
    lm.set_limit(PositionLimit(product_id="BTC-USD", max_size=Decimal("10"), max_notional=Decimal("500000")))
    lm.update_position("BTC-USD", Decimal("9"))
    ok, reason = lm.check_order("BTC-USD", "buy", Decimal("2"), Decimal("50000"))
    assert not ok


def test_kill_switch():
    km = KillSwitchManager()
    assert not km.is_active()
    km.engage("global", "test", "testing")
    assert km.is_active()
    km.disengage()
    assert not km.is_active()


def test_auto_trigger():
    km = KillSwitchManager()
    km.set_auto_trigger("drawdown", 0.2)
    assert not km.check_auto_trigger("drawdown", 0.15)
    assert km.check_auto_trigger("drawdown", 0.25)
    assert km.is_active("auto:drawdown")


def test_risk_approval_service():
    svc = RiskApprovalService()
    assert svc.requires_approval("enable_aggressive")
    assert not svc.requires_approval("enable_normal")
    approval = svc.request("enable_aggressive", "operator")
    assert not approval.approved
    svc.approve(approval, "admin")
    assert approval.approved
    assert approval.approved_by == "admin"
