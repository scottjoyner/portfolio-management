from trading_system.apps.api import main


class FakeRecorder:
    def tail(self, limit=100, strategy_id=None, source=None, event_type=None):
        return [
            {
                "timestamp": "2026-06-06T00:00:00Z",
                "source": source or "worker",
                "event_type": event_type or "strategy_tick",
                "strategy_id": strategy_id or "triplema",
                "payload": {"ok": True},
            }
        ]


def test_events_endpoint_shape(monkeypatch):
    monkeypatch.setattr(main, "get_event_recorder", lambda: FakeRecorder())

    data = main.events(limit=1, strategy_id="triplema", source="worker", event_type="strategy_tick")

    assert data["count"] == 1
    assert data["events"][0]["strategy_id"] == "triplema"
