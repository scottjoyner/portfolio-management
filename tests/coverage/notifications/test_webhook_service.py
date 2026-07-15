import unittest

from trading_system.notifications.webhook.service import (
    WebhookEndpoint,
    WebhookService,
)


class TestWebhookService(unittest.TestCase):
    def setUp(self):
        self.svc = WebhookService()

    def test_add_and_remove(self):
        ep = WebhookEndpoint(url="https://hook", secret="s", headers={"h": "1"})
        self.svc.add_endpoint(ep)
        self.assertEqual(len(self.svc.endpoints), 1)
        self.svc.remove_endpoint("https://hook")
        self.assertEqual(len(self.svc.endpoints), 0)
        # Removing a non-existent endpoint is a no-op
        self.svc.remove_endpoint("https://missing")

    def test_dispatch_enabled_and_disabled(self):
        enabled = WebhookEndpoint(url="https://a", enabled=True)
        disabled = WebhookEndpoint(url="https://b", enabled=False)
        self.svc.add_endpoint(enabled)
        self.svc.add_endpoint(disabled)
        results = self.svc.dispatch("trade", {"x": 1})
        # Only the enabled endpoint produces a result
        self.assertEqual(results, [True])
        # Empty dispatch
        empty = WebhookService()
        self.assertEqual(empty.dispatch("e", {}), [])


if __name__ == "__main__":
    unittest.main()
