import unittest

from trading_system.plaid import models as m


class TestPlaidModels(unittest.TestCase):
    # ---- InstitutionStatus.from_plaid ----
    def test_inst_active_none(self):
        self.assertEqual(m.InstitutionStatus.from_plaid(None), m.InstitutionStatus.ACTIVE)

    def test_inst_active(self):
        self.assertEqual(m.InstitutionStatus.from_plaid("active"), m.InstitutionStatus.ACTIVE)
        self.assertEqual(m.InstitutionStatus.from_plaid("a"), m.InstitutionStatus.ACTIVE)

    def test_inst_read_only(self):
        self.assertEqual(m.InstitutionStatus.from_plaid("read_only"), m.InstitutionStatus.READ_ONLY)
        self.assertEqual(m.InstitutionStatus.from_plaid("r"), m.InstitutionStatus.READ_ONLY)

    def test_inst_inactive(self):
        self.assertEqual(m.InstitutionStatus.from_plaid("foo"), m.InstitutionStatus.INACTIVE)

    # ---- ConsentState.from_plaid ----
    def test_consent_granted_none(self):
        self.assertEqual(m.ConsentState.from_plaid(None), m.ConsentState.GRANTED)

    def test_consent_pending(self):
        self.assertEqual(m.ConsentState.from_plaid("pending"), m.ConsentState.PENDING)
        self.assertEqual(m.ConsentState.from_plaid("p"), m.ConsentState.PENDING)

    def test_consent_granted(self):
        self.assertEqual(m.ConsentState.from_plaid("granted"), m.ConsentState.GRANTED)
        self.assertEqual(m.ConsentState.from_plaid("g"), m.ConsentState.GRANTED)

    def test_consent_revoked(self):
        self.assertEqual(m.ConsentState.from_plaid("revoked"), m.ConsentState.REVOKED)

    # ---- PlaidCredentials.validate ----
    def test_credentials_validate_ok(self):
        c = m.PlaidCredentials("cid", "sandbox", "secret")
        c.validate()

    def test_credentials_validate_no_id(self):
        c = m.PlaidCredentials("", "sandbox", "secret")
        self.assertRaises(ValueError, c.validate)

    def test_credentials_validate_bad_env(self):
        c = m.PlaidCredentials("cid", "prod", "secret")
        self.assertRaises(ValueError, c.validate)

    # ---- PlaidItem ----
    def test_plaid_item_active(self):
        item = m.PlaidItem(item_id="i1", status=m.InstitutionStatus.ACTIVE)
        self.assertTrue(item.is_active)

    def test_plaid_item_inactive(self):
        item = m.PlaidItem(item_id="i1", status=m.InstitutionStatus.INACTIVE)
        self.assertFalse(item.is_active)

    def test_plaid_item_consent_granted(self):
        item = m.PlaidItem(item_id="i1", consent_state=m.ConsentState.GRANTED)
        self.assertTrue(item.consent_granted)

    def test_plaid_item_consent_pending(self):
        item = m.PlaidItem(item_id="i1", consent_state=m.ConsentState.PENDING)
        self.assertTrue(item.consent_granted)

    def test_plaid_item_consent_revoked(self):
        item = m.PlaidItem(item_id="i1", consent_state=m.ConsentState.REVOKED)
        self.assertFalse(item.consent_granted)

    # ---- PlaidAccount ----
    def test_account_balance_cents(self):
        acc = m.PlaidAccount(account_id="a1", account_type="checking",
                             available_balance_cents=15000)
        self.assertEqual(acc.balance, 150)

    def test_account_balance_none(self):
        acc = m.PlaidAccount(account_id="a1", account_type="checking")
        self.assertIsNone(acc.balance)

    def test_account_is_checking_type(self):
        acc = m.PlaidAccount(account_id="a1", account_type="checking")
        self.assertTrue(acc.is_checking)

    def test_account_is_checking_subtype(self):
        acc = m.PlaidAccount(account_id="a1", account_type="x",
                             sub_type="interest_checking")
        self.assertTrue(acc.is_checking)

    def test_account_demand_not_checking(self):
        acc = m.PlaidAccount(account_id="a1", account_type="demand")
        self.assertFalse(acc.is_checking)

    def test_account_not_checking(self):
        acc = m.PlaidAccount(account_id="a1", account_type="savings")
        self.assertFalse(acc.is_checking)

    # ---- PlaidTransaction ----
    def test_transaction_amount_cents(self):
        t = m.PlaidTransaction(transaction_id="t1", amount_cents=5000)
        self.assertEqual(t.amount, 50)

    def test_transaction_amount_none(self):
        t = m.PlaidTransaction(transaction_id="t1")
        self.assertIsNone(t.amount)


if __name__ == "__main__":
    unittest.main()
