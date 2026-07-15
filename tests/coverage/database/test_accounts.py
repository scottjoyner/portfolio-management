import unittest
from unittest import mock

from db_helpers import install_fakes, make_db, QueryStub, _Row

spm, _plaid_models, _plaid_db = install_fakes()

from trading_system.database.queries import accounts as acc


class TestAccountsRepository(unittest.TestCase):
    def test_get_portfolio(self):
        p = _Row(id="p1")
        db = make_db({spm.Portfolio: QueryStub(first=p)})
        repo = acc.AccountsRepository(db)
        self.assertIs(repo.get_portfolio("p1"), p)

    def test_list_portfolios_no_filter(self):
        rows = [_Row(id="a"), _Row(id="b")]
        db = make_db({spm.Portfolio: QueryStub(rows=rows)})
        repo = acc.AccountsRepository(db)
        self.assertEqual(repo.list_portfolios(), rows)

    def test_list_portfolios_with_objective(self):
        rows = [_Row(id="a")]
        db = make_db({spm.Portfolio: QueryStub(rows=rows)})
        repo = acc.AccountsRepository(db)
        self.assertEqual(repo.list_portfolios(objective="hedge"), rows)

    def test_update_portfolio_nav_found(self):
        p = _Row(id="p1", nav=0)
        db = make_db({spm.Portfolio: QueryStub(first=p)})
        repo = acc.AccountsRepository(db)
        out = repo.update_portfolio_nav("p1", 123.0)
        self.assertEqual(out.nav, 123.0)
        db.commit.assert_called_once()
        db.refresh.assert_called_once()

    def test_update_portfolio_nav_missing(self):
        db = make_db({spm.Portfolio: QueryStub(first=None)})
        repo = acc.AccountsRepository(db)
        self.assertIsNone(repo.update_portfolio_nav("x", 1.0))

    def test_update_portfolio_metrics(self):
        p = _Row(id="p1", nav=1.0, realized_pnl=0.0)
        db = make_db({spm.Portfolio: QueryStub(first=p)})
        repo = acc.AccountsRepository(db)
        out = repo.update_portfolio_metrics("p1", realized_pnl=5.0, not_a_field=9.0)
        self.assertEqual(out.realized_pnl, 5.0)
        self.assertFalse(hasattr(out, "not_a_field"))

    def test_update_portfolio_metrics_missing(self):
        db = make_db({spm.Portfolio: QueryStub(first=None)})
        repo = acc.AccountsRepository(db)
        self.assertIsNone(repo.update_portfolio_metrics("x", nav=1.0))

    def test_seed_default_portfolios_existing(self):
        db = make_db({spm.Portfolio: QueryStub(count=3)})
        repo = acc.AccountsRepository(db)
        self.assertEqual(repo.seed_default_portfolios(), [])

    def test_seed_default_portfolios_new(self):
        db = make_db({spm.Portfolio: QueryStub(count=0)})
        repo = acc.AccountsRepository(db)
        out = repo.seed_default_portfolios()
        self.assertEqual(len(out), 2)
        db.add_all.assert_called_once()
        db.flush.assert_called_once()
        # sleeves added: 3 for core-mm + 2 for hedge
        self.assertEqual(db.add.call_count, 5)

    def test_capital_bucket_ops(self):
        b = _Row(id="b1")
        db = make_db({spm.CapitalBucket: QueryStub(first=b, rows=[b])})
        repo = acc.AccountsRepository(db)
        self.assertIs(repo.get_capital_bucket("b1"), b)
        self.assertEqual(repo.list_capital_buckets(), [b])
        self.assertEqual(repo.list_capital_buckets(portfolio_id="p1"), [b])

    def test_create_capital_bucket_defaults(self):
        db = make_db()
        repo = acc.AccountsRepository(db)
        out = repo.create_capital_bucket({"portfolio_id": "p1", "name": "n", "bucket_type": "t"})
        self.assertEqual(out.portfolio_id, "p1")
        self.assertEqual(out.max_weight, 1.0)
        db.add.assert_called_once()
        db.commit.assert_called_once()

    def test_create_capital_bucket_with_id(self):
        db = make_db()
        repo = acc.AccountsRepository(db)
        out = repo.create_capital_bucket({"id": "myid", "amount": 10, "locked": True, "status": "on"})
        self.assertEqual(out.id, "myid")

    def test_get_plaid_item_table_path(self):
        item = _Row(id="i1")
        db = make_db({spm.plaid_items_table: QueryStub(first=item)})
        repo = acc.AccountsRepository(db)
        self.assertIs(repo.get_plaid_item("i1"), item)

    def test_get_plaid_item_fallback(self):
        import sys as _sys; pdb = _sys.modules["plaid.database_models"]

        item = _Row(id="i2")

        def _query(model, *a, **k):
            if model is spm.plaid_items_table:
                raise RuntimeError("no table")
            if model is pdb.PlaidItem:
                return QueryStub(first=item)
            return QueryStub()

        db = mock.MagicMock()
        db.query.side_effect = _query
        repo = acc.AccountsRepository(db)
        self.assertIs(repo.get_plaid_item("i2"), item)

    def test_list_plaid_items(self):
        import sys as _sys; pdb = _sys.modules["plaid.database_models"]

        rows = [_Row(id="a")]
        db = make_db({pdb.PlaidItem: QueryStub(rows=rows)})
        repo = acc.AccountsRepository(db)
        self.assertEqual(repo.list_plaid_items(), rows)
        self.assertEqual(repo.list_plaid_items(access_token_active_only=True), rows)

    def test_upsert_plaid_item_new(self):
        db = make_db({spm.plaid_items_table: QueryStub(first=None)})
        repo = acc.AccountsRepository(db)
        item = _Row(item_id="i1", access_token="t")
        out = repo.upsert_plaid_item(item)
        self.assertIs(out, item)
        db.add.assert_called_once_with(item)

    def test_upsert_plaid_item_existing(self):
        existing = _Row(item_id="i1", access_token="old", updated_at=None)
        db = make_db({spm.plaid_items_table: QueryStub(first=existing)})
        repo = acc.AccountsRepository(db)
        item = _Row(item_id="i1", access_token="new", _private="skip")
        repo.upsert_plaid_item(item)
        self.assertEqual(existing.access_token, "new")

    def test_sync_methods(self):
        repo = acc.AccountsRepository(make_db())
        self.assertEqual(repo.sync_account_balances("tok")["status"], "complete")
        self.assertEqual(repo.sync_account_holdings("tok")["status"], "complete")

    def test_check_token_expiration_no_item(self):
        db = make_db({spm.plaid_items_table: QueryStub(first=None)})
        repo = acc.AccountsRepository(db)
        self.assertEqual(repo.check_token_expiration("x"), (False, None))

    def test_check_token_expiration_granted(self):
        item = _Row(id="i1", consent_state="GRANTED")
        db = make_db({spm.plaid_items_table: QueryStub(first=item)})
        repo = acc.AccountsRepository(db)
        expiring, delta = repo.check_token_expiration("i1")
        self.assertFalse(expiring)
        self.assertIsNotNone(delta)


class TestModuleHelpers(unittest.TestCase):
    def test_get_account_overview(self):
        p = _Row(id="p", nav=100.0)
        db = make_db({spm.Portfolio: QueryStub(rows=[p]), spm.plaid_items_table: QueryStub(rows=[])})
        # list_plaid_items uses plaid.database_models.PlaidItem
        import sys as _sys; pdb = _sys.modules["plaid.database_models"]
        db2 = make_db({spm.Portfolio: QueryStub(rows=[p]), pdb.PlaidItem: QueryStub(rows=[_Row()])})
        out = acc.get_account_overview(db2)
        self.assertEqual(out["total_nav"], 100.0)
        self.assertEqual(out["items_count"], 1)

    def test_get_portfolio_summary_found(self):
        p = _Row(id="p", name="n", objective="o", nav=1.0,
                 available_capital=2.0, locked_capital=3.0)
        b = _Row(id="b")
        db = make_db({spm.Portfolio: QueryStub(first=p), spm.CapitalBucket: QueryStub(rows=[b])})
        out = acc.get_portfolio_summary(db, "p")
        self.assertEqual(out["portfolio"]["nav"], 1.0)
        self.assertEqual(len(out["capital_buckets"]), 1)

    def test_get_portfolio_summary_missing(self):
        db = make_db({spm.Portfolio: QueryStub(first=None)})
        self.assertIsNone(acc.get_portfolio_summary(db, "x"))


if __name__ == "__main__":
    unittest.main()
