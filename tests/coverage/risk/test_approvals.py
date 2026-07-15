import unittest

from trading_system.risk.approvals.service import RiskApproval, RiskApprovalService


class TestRiskApprovalService(unittest.TestCase):
    def setUp(self):
        self.svc = RiskApprovalService()

    def test_requires_approval_true(self):
        self.assertTrue(self.svc.requires_approval("enable_aggressive"))

    def test_requires_approval_false(self):
        self.assertFalse(self.svc.requires_approval("safe_action"))

    def test_request(self):
        appr = self.svc.request("increase_capital_limit", "alice")
        self.assertIsInstance(appr, RiskApproval)
        self.assertEqual(appr.action, "increase_capital_limit")
        self.assertEqual(appr.requested_by, "alice")
        self.assertIn(appr, self.svc.pending)
        self.assertFalse(appr.approved)

    def test_approve(self):
        appr = RiskApproval(action="x", requested_by="a")
        self.svc.approve(appr, "boss")
        self.assertTrue(appr.approved)
        self.assertEqual(appr.approved_by, "boss")

    def test_reject(self):
        appr = RiskApproval(action="x", requested_by="a")
        self.svc.reject(appr, "too risky")
        self.assertFalse(appr.approved)
        self.assertEqual(appr.reason, "too risky")


if __name__ == "__main__":
    unittest.main()
