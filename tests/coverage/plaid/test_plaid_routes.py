import unittest

from trading_system.plaid.api import plaid_routes as pr


class TestPlaidRoutes(unittest.IsolatedAsyncioTestCase):
    async def test_create_link_token(self):
        res = await pr.create_link_token("cid", "sandbox")
        self.assertEqual(res["status"], "success")

    async def test_link_item(self):
        res = await pr.link_item("i1", "lt", "pt", "secret")
        self.assertEqual(res["item_id"], "i1")

    async def test_get_item(self):
        res = await pr.get_item("i1")
        self.assertEqual(res["item_id"], "i1")

    async def test_get_accounts(self):
        res = await pr.get_accounts("i1")
        self.assertEqual(res["total_accounts"], 2)

    async def test_refresh_item(self):
        res = await pr.refresh_item("i1")
        self.assertEqual(res["status"], "success")

    async def test_revoke_item(self):
        res = await pr.revoke_item("i1")
        self.assertEqual(res["status"], "success")

    async def test_webhook_created(self):
        res = await pr.handle_plaid_webhook("sync", "i1",
                                            {"event": {"type": "ITEM_CREATED"}})
        self.assertEqual(res["action"], "item_created_or_refreshed")

    async def test_webhook_refreshed(self):
        res = await pr.handle_plaid_webhook("sync", "i1",
                                            {"event": {"type": "TRANSACTIONS_REFRESHED"}})
        self.assertEqual(res["action"], "item_created_or_refreshed")

    async def test_webhook_revoked(self):
        res = await pr.handle_plaid_webhook("sync", "i1",
                                            {"event": {"type": "ITEM_REVOKED"}})
        self.assertEqual(res["action"], "item_revoked")

    async def test_webhook_unknown(self):
        res = await pr.handle_plaid_webhook("sync", "i1",
                                            {"event": {"type": "SOMETHING_ELSE"}})
        self.assertEqual(res["action"], "unknown_event_type")


if __name__ == "__main__":
    unittest.main()
