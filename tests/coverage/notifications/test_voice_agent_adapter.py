import unittest

from trading_system.notifications.voice_agent.adapter import (
    ApprovalRequest,
    VoiceAgentAdapter,
)


class TestVoiceAgentAdapter(unittest.TestCase):
    def test_build_payload(self):
        req = ApprovalRequest(
            action="EXECUTE",
            summary="buy BTC",
            rationale="momentum",
            risk_notes=["vol"],
        )
        adapter = VoiceAgentAdapter()
        payload = adapter.build_payload(req)
        self.assertEqual(payload["type"], "approval_required")
        self.assertEqual(payload["payload"]["action"], "EXECUTE")
        self.assertEqual(payload["payload"]["risk_notes"], ["vol"])


if __name__ == "__main__":
    unittest.main()
