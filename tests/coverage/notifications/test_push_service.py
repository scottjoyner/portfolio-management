import unittest

from trading_system.notifications.push.service import PushNotification, PushService


class TestPushService(unittest.TestCase):
    def test_send(self):
        svc = PushService()
        note = PushNotification(title="t", body="b", data={"k": "v"}, device_tokens=["tok1", "tok2"])
        self.assertTrue(svc.send(note))
        note2 = PushNotification(title="t2", body="b2")
        self.assertTrue(svc.send(note2))


if __name__ == "__main__":
    unittest.main()
