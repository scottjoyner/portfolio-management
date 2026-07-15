import unittest

from trading_system.approval.workflow_engine import (
    ApprovalRequest,
    ApprovalTier,
    WorkflowEngine,
)


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
        res = await eng.route_strategy(req)
        self.assertEqual(res["status"], "approved")
        self.assertEqual(res["tier"], "auto")

    async def test_route_full_scale(self):
        eng = WorkflowEngine()
        req = ApprovalRequest("k", "1.0", 0.7, 100, 10)
        res = await eng.route_strategy(req)
        self.assertEqual(res["status"], "pending_review")
        self.assertEqual(res["tier"], "production")

    async def test_route_canary(self):
        eng = WorkflowEngine()
        req = ApprovalRequest("k", "1.0", 0.5, 100, 10)
        res = await eng.route_strategy(req)
        self.assertEqual(res["status"], "canary_approved")
        self.assertEqual(res["tier"], "canary")

    async def test_route_with_validation_results(self):
        eng = WorkflowEngine()
        req = ApprovalRequest("k", "1.0", 0.1, 100, 10)
        res = await eng.route_strategy(req, {"code_review_passed": True})
        self.assertEqual(res["status"], "approved")


if __name__ == "__main__":
    unittest.main()
