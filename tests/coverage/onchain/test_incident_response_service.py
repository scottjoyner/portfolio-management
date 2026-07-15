from __future__ import annotations

from unittest import TestCase
from datetime import datetime, timezone

from onchain.security.incident_response.service import (
    IncidentAction,
    IncidentResponse,
    IncidentResponseService,
)


class TestIncidentResponse(TestCase):
    def test_add_action(self):
        resp = IncidentResponse(incident_id="i1", severity="high", summary="s")
        action = resp.add_action("freeze", {"k": "v"})
        self.assertIsInstance(action, IncidentAction)
        self.assertEqual(action.action_type, "freeze")
        self.assertEqual(action.params, {"k": "v"})
        self.assertIn(action, resp.actions)

    def test_execute_all(self):
        resp = IncidentResponse(incident_id="i1", severity="high", summary="s")
        resp.add_action("freeze")
        resp.add_action("notify")
        resp.execute_all()
        self.assertTrue(resp.resolved)
        for a in resp.actions:
            self.assertTrue(a.executed)
            self.assertIsInstance(a.executed_at, datetime)

    def test_execute_all_no_actions(self):
        resp = IncidentResponse(incident_id="i1", severity="low", summary="s")
        resp.execute_all()
        self.assertTrue(resp.resolved)

    def test_service_create_and_get(self):
        svc = IncidentResponseService()
        resp = svc.create_response("i2", "critical", "breach")
        self.assertIs(svc.get_response("i2"), resp)
        self.assertIsNone(svc.get_response("missing"))
