import unittest

from trading_system.plaid import database_models as dm


class TestPlaidDatabaseModels(unittest.TestCase):
    # ---- enums ----
    def test_consent_from_plaid(self):
        self.assertEqual(dm.ConsentState.from_plaid(None), dm.ConsentState.GRANTED)
        self.assertEqual(dm.ConsentState.from_plaid("pending"), dm.ConsentState.PENDING)
        self.assertEqual(dm.ConsentState.from_plaid("granted"), dm.ConsentState.GRANTED)
        self.assertEqual(dm.ConsentState.from_plaid("x"), dm.ConsentState.REVOKED)

    def test_status_from_plaid(self):
        self.assertEqual(dm.InstitutionStatus.from_plaid(None), dm.InstitutionStatus.ACTIVE)
        self.assertEqual(dm.InstitutionStatus.from_plaid("read_only"), dm.InstitutionStatus.READ_ONLY)
        self.assertEqual(dm.InstitutionStatus.from_plaid("a"), dm.InstitutionStatus.ACTIVE)
        self.assertEqual(dm.InstitutionStatus.from_plaid("x"), dm.InstitutionStatus.INACTIVE)

    # ---- PlaidItem.from_plaid_item ----
    def test_from_plaid_item_explicit_id(self):
        item = dm.PlaidItem.from_plaid_item(
            id="fixed", item_id="i1", institution_name="Bank",
            status="active", consent_state="granted",
            inactive_reason="manual",
        )
        self.assertEqual(item.id, "fixed")
        self.assertEqual(item.institution_name, "Bank")
        self.assertEqual(item.status, dm.InstitutionStatus.ACTIVE)
        self.assertEqual(item.inactive_reason, "manual")

    def test_from_plaid_item_generated_id(self):
        item = dm.PlaidItem.from_plaid_item(item_id="i1", status="active")
        self.assertTrue(item.id)

    def test_from_plaid_item_inactive(self):
        item = dm.PlaidItem.from_plaid_item(item_id="i1", status="inactive")
        self.assertTrue(item.inactive_reason)

    def test_from_plaid_item_active_no_reason(self):
        item = dm.PlaidItem.from_plaid_item(item_id="i1", status="active")
        self.assertFalse(item.inactive_reason)

    def test_plaid_item_is_active(self):
        item = dm.PlaidItem(item_id="i1", status=dm.InstitutionStatus.ACTIVE)
        self.assertTrue(item.is_active)
        item.status = dm.InstitutionStatus.INACTIVE
        self.assertFalse(item.is_active)

    # ---- PlaidAccount ----
    def test_account_balance(self):
        acc = dm.PlaidAccount(id="x", item_id="i", account_id="a",
                              account_type="checking", available_balance_cents=20000)
        self.assertEqual(acc.balance, 200)

    def test_account_balance_none(self):
        acc = dm.PlaidAccount(id="x", item_id="i", account_id="a",
                              account_type="checking")
        self.assertIsNone(acc.balance)

    def test_account_is_checking(self):
        acc = dm.PlaidAccount(id="x", item_id="i", account_id="a",
                              account_type="checking")
        self.assertTrue(acc.is_checking)

    def test_account_is_checking_subtype(self):
        acc = dm.PlaidAccount(id="x", item_id="i", account_id="a",
                              account_type="x", sub_type="my_checking_acct")
        self.assertTrue(acc.is_checking)

    def test_account_not_checking(self):
        acc = dm.PlaidAccount(id="x", item_id="i", account_id="a",
                              account_type="savings")
        self.assertFalse(acc.is_checking)

    # ---- PlaidTransaction ----
    def test_transaction_amount(self):
        t = dm.PlaidTransaction(id="x", item_id="i", account_id="a",
                                amount_cents=3000)
        self.assertEqual(t.amount, 30)

    def test_transaction_amount_none(self):
        t = dm.PlaidTransaction(id="x", item_id="i", account_id="a")
        self.assertIsNone(t.amount)


if __name__ == "__main__":
    unittest.main()
