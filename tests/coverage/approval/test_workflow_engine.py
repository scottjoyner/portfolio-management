import unittest

from trading_system.approval.workflow_engine import (
    ApprovalRequest,
    ApprovalTier,
    WorkflowEngine,
)


# route_strategy refuses when a required validation is absent rather than
# skipped. Passing no validation_results used to auto-approve a strategy that
# had had no code review and no security scan, so these tests now supply the
# validations they depend on, and the refusal is asserted separately.
ALL_VALIDATED = {
    "code_review_passed": True,
    "security_scan_passed": True,
    "performance_benchmark_met": True,
}

class TestWorkflowEngine(unittest.IsolatedAsyncioTestCase):
    def test_tier_enum(self):
        self.assertEqual(ApprovalTier.AUTO_APPROVE.value, "auto")
        self.assertEqual(ApprovalTier.CANARY_PHASE.value, "canary")
        self.assertEqual(ApprovalTier.FULL_SCALE.value, "production")

    def test_requires_approval_auto_low(self):
        req = ApprovalRequest("k", "1.0", 0.1, 100, 10)
        self.assertFalse(req.requires_approval(ApprovalTier.AUTO_APPROVE))

    def test_requires_approval_high_risk(self):
        req = ApprovalRequest("k", "1.0", 0.5, 100, 10)
        self.assertTrue(req.requires_approval(ApprovalTier.AUTO_APPROVE))

    def test_requires_approval_high_capital(self):
        req = ApprovalRequest("k", "1.0", 0.1, 20000, 10)
        self.assertTrue(req.requires_approval(ApprovalTier.AUTO_APPROVE))

    def test_requires_approval_non_auto_tier(self):
        req = ApprovalRequest("k", "1.0", 0.1, 100, 10)
        self.assertTrue(req.requires_approval(ApprovalTier.CANARY_PHASE))
        self.assertTrue(req.requires_approval(ApprovalTier.FULL_SCALE))

    def test_get_required_tier_auto(self):
        req = ApprovalRequest("k", "1.0", 0.1, 100, 10)
        self.assertEqual(req.get_required_tier(), ApprovalTier.AUTO_APPROVE)

    def test_get_required_tier_canary(self):
        req = ApprovalRequest("k", "1.0", 0.3, 100, 10)
        self.assertEqual(req.get_required_tier(), ApprovalTier.CANARY_PHASE)

    def test_get_required_tier_full(self):
        req = ApprovalRequest("k", "1.0", 0.5, 100, 10)
        self.assertEqual(req.get_required_tier(), ApprovalTier.FULL_SCALE)

    def test_default_config(self):
        eng = WorkflowEngine()
        self.assertEqual(eng.risk_threshold_canary, 0.4)
        self.assertEqual(eng.risk_threshold_production, 0.6)
        self.assertEqual(eng.auto_approve_capital_limit, 5000)

    def test_custom_config(self):
        eng = WorkflowEngine({"risk_threshold_canary": 0.1, "risk_threshold_production": 0.2,
                              "auto_approve_capital_limit": 999})
        self.assertEqual(eng.auto_approve_capital_limit, 999)

    async def test_route_auto(self):
        eng = WorkflowEngine()
        req = ApprovalRequest("k", "1.0", 0.1, 100, 10)
        res = await eng.route_strategy(req, dict(ALL_VALIDATED))
        self.assertEqual(res["status"], "approved")
        self.assertEqual(res["tier"], "auto")

    async def test_route_full_scale(self):
        eng = WorkflowEngine()
        req = ApprovalRequest("k", "1.0", 0.7, 100, 10)
        res = await eng.route_strategy(req, dict(ALL_VALIDATED))
        self.assertEqual(res["status"], "pending_review")
        self.assertEqual(res["tier"], "production")

    async def test_route_canary(self):
        eng = WorkflowEngine()
        req = ApprovalRequest("k", "1.0", 0.5, 100, 10)
        res = await eng.route_strategy(req, dict(ALL_VALIDATED))
        self.assertEqual(res["status"], "canary_approved")
        self.assertEqual(res["tier"], "canary")

    async def test_route_with_partial_validations_is_rejected(self):
        """A partial validation set does not approve.

        Passing no validation_results auto-approved a strategy with no code review
        and no security scan, because `if name in validation_results` skipped an
        absent key instead of failing on it. Absent and false are the same answer.
        """
        eng = WorkflowEngine()
        req = ApprovalRequest("k", "1.0", 0.1, 100, 10)
        res = await eng.route_strategy(req, {"code_review_passed": True})
        self.assertEqual(res["status"], "rejected")
        self.assertTrue(res["requires_human_approval"])

    async def test_route_with_no_validations_is_rejected(self):
        eng = WorkflowEngine()
        req = ApprovalRequest("k", "1.0", 0.1, 100, 10)
        res = await eng.route_strategy(req)
        self.assertEqual(res["status"], "rejected")

    async def test_route_validation_false_is_rejected(self):
        eng = WorkflowEngine()
        req = ApprovalRequest("k", "1.0", 0.1, 100, 10)
        res = await eng.route_strategy(req, {**ALL_VALIDATED, "security_scan_passed": False})
        self.assertEqual(res["status"], "rejected")


if __name__ == "__main__":
    unittest.main()
