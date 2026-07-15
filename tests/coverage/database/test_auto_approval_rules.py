import unittest

from db_helpers import install_fakes, make_db, QueryStub, _Row

spm, _pm, _pd = install_fakes()

from trading_system.database.queries import auto_approval_rules as aar

Portfolio = spm.Portfolio
Approval = spm.Approval


class _Meta:
    def __init__(self, counterparties):
        self.counterparties = counterparties


class TestWhitelist(unittest.TestCase):
    def setUp(self):
        self.repo = aar.AutoApprovalRulesRepository(make_db())

    def test_product_whitelist_match(self):
        out = self.repo.check_whitelist_patterns({}, product_id="BTC-USD")
        self.assertTrue(out["is_whitelisted"])
        self.assertEqual(out["tier"], 1)

    def test_counterparty_whitelist_match(self):
        order = {"metadata": _Meta(["Coinbase_Prime"])}
        out = self.repo.check_whitelist_patterns(order, product_id="ZZZ-USD")
        self.assertTrue(out["is_whitelisted"])
        self.assertEqual(out["reason"], "approved_counterparty")

    def test_no_match(self):
        out = self.repo.check_whitelist_patterns({"metadata": _Meta([])}, product_id="ZZZ-USD")
        self.assertFalse(out["is_whitelisted"])

    def test_counterparty_no_match_iterates(self):
        order = {"metadata": _Meta(["unknown_cp", "another_cp"])}
        out = self.repo.check_whitelist_patterns(order, product_id="ZZZ-USD")
        self.assertFalse(out["is_whitelisted"])

    def test_no_product_no_order(self):
        out = self.repo.check_whitelist_patterns({}, product_id=None)
        self.assertFalse(out["is_whitelisted"])


class TestCapitalLimits(unittest.TestCase):
    def test_no_portfolio(self):
        db = make_db({Portfolio: QueryStub(first=None)})
        repo = aar.AutoApprovalRulesRepository(db)
        out = repo.check_capital_allocation_limits("pf1", "BTC-USD", 1.0)
        self.assertTrue(out["is_within_limits"])
        self.assertEqual(out["remaining_capacity"], float("inf"))

    def test_within_limits(self):
        db = make_db({Portfolio: QueryStub(first=_Row(id="pf1", nav=1_000_000.0))})
        repo = aar.AutoApprovalRulesRepository(db)
        out = repo.check_capital_allocation_limits("pf1", "BTC-USD", 1.0)
        self.assertTrue(out["is_within_limits"])
        self.assertEqual(out["portfolio_nav"], 1_000_000.0)

    def test_over_limits(self):
        db = make_db({Portfolio: QueryStub(first=_Row(id="pf1", nav=100.0))})
        repo = aar.AutoApprovalRulesRepository(db)
        out = repo.check_capital_allocation_limits("pf1", "BTC-USD", 100.0)
        self.assertFalse(out["is_within_limits"])


class TestTierLogic(unittest.TestCase):
    def test_tier1_whitelisted(self):
        repo = aar.AutoApprovalRulesRepository(make_db())
        tier = repo.determine_approval_tier({"portfolio_id": "pf1", "size": 1}, product_id="BTC-USD")
        self.assertEqual(tier, 1)

    def test_tier1_within_limits(self):
        db = make_db({Portfolio: QueryStub(first=None)})
        repo = aar.AutoApprovalRulesRepository(db)
        tier = repo.determine_approval_tier({"portfolio_id": "pf1", "size": 1}, product_id="ZZZ-USD")
        self.assertEqual(tier, 1)

    def test_tier2(self):
        db = make_db({Portfolio: QueryStub(first=_Row(id="pf1", nav=100.0))})
        repo = aar.AutoApprovalRulesRepository(db)
        tier = repo.determine_approval_tier(
            {"portfolio_id": "pf1", "size": 10, "notional": 100}, product_id="ZZZ-USD")
        self.assertEqual(tier, 2)

    def test_tier3(self):
        db = make_db({Portfolio: QueryStub(first=_Row(id="pf1", nav=100.0))})
        repo = aar.AutoApprovalRulesRepository(db)
        tier = repo.determine_approval_tier(
            {"portfolio_id": "pf1", "size": 10, "notional": 1000}, product_id="ZZZ-USD")
        self.assertEqual(tier, 3)

    def test_get_tier_requirements(self):
        repo = aar.AutoApprovalRulesRepository(make_db())
        self.assertEqual(repo.get_tier_requirements(1)["name"], "Auto-Approve")
        self.assertEqual(repo.get_tier_requirements(2)["max_order_value"], 50_000)
        self.assertTrue(repo.get_tier_requirements(3)["requires_review"])
        # invalid tier -> default tier 3
        self.assertTrue(repo.get_tier_requirements(99)["requires_review"])


class TestApprovalRecords(unittest.TestCase):
    def test_create_approval_record(self):
        db = make_db()
        repo = aar.AutoApprovalRulesRepository(db)
        out = repo.create_approval_record("order", "summary text", 1000.0,
                                          status="pending", approved_by="a1")
        self.assertEqual(out["type"], "order")
        db.add.assert_called_once()

    def test_create_approval_record_defaults(self):
        db = make_db()
        repo = aar.AutoApprovalRulesRepository(db)
        out = repo.create_approval_record("", "s", 0.0)
        self.assertEqual(out["type"], "order")

    def test_get_pending_approvals(self):
        rows = [_Row(approval_id="a1", status="pending"), {"raw": 2}]
        db = make_db({Approval: QueryStub(rows=rows)})
        repo = aar.AutoApprovalRulesRepository(db)
        out = repo.get_pending_approvals()
        self.assertEqual(len(out), 2)
        self.assertEqual(out[1], {"raw": 2})

    def test_mark_approval_complete_found(self):
        appr = _Row(approval_id="a1", status="pending")
        db = make_db({Approval: QueryStub(first=appr)})
        repo = aar.AutoApprovalRulesRepository(db)
        out = repo.mark_approval_complete("a1", status="approved", approved_by="me")
        self.assertEqual(out["new_status"], "approved")

    def test_mark_approval_complete_missing(self):
        db = make_db({Approval: QueryStub(first=None)})
        repo = aar.AutoApprovalRulesRepository(db)
        out = repo.mark_approval_complete("x")
        self.assertFalse(out["success"])


class TestModuleHelpers(unittest.TestCase):
    def test_candidates_and_review(self):
        db = make_db()
        self.assertEqual(aar.get_auto_approve_candidates(db), [])
        self.assertEqual(aar.get_orders_needing_review(db), [])

    def test_approvals_summary_nonzero(self):
        db = make_db({Approval: QueryStub(count=10)})
        out = aar.get_approvals_summary(db)
        self.assertEqual(out["total_approvals"], 10)
        self.assertGreater(out["pending_rate"], 0)

    def test_approvals_summary_zero(self):
        db = make_db({Approval: QueryStub(count=0)})
        out = aar.get_approvals_summary(db)
        self.assertEqual(out["pending_rate"], 0)


if __name__ == "__main__":
    unittest.main()
