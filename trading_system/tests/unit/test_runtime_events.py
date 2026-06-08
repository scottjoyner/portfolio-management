from pathlib import Path

from trading_system.core.runtime.events import EventRecorder
from trading_system.core.runtime.models import TradingEvent


def test_event_recorder_appends_events_to_jsonl(tmp_path: Path):
    path = tmp_path / "events.jsonl"
    recorder = EventRecorder(path=path)

    recorder.record(TradingEvent(source="test", event_type="started", payload={"ok": True}))

    lines = path.read_text().strip().splitlines()
    assert len(lines) == 1
    assert '"event_type": "started"' in lines[0]


def test_event_recorder_tail_returns_newest_events(tmp_path: Path):
    recorder = EventRecorder(path=tmp_path / "events.jsonl")
    recorder.record(TradingEvent(source="test", event_type="one"))
    recorder.record(TradingEvent(source="test", event_type="two"))
    recorder.record(TradingEvent(source="test", event_type="three"))

    events = recorder.tail(limit=2)

    assert [event["event_type"] for event in events] == ["two", "three"]


def test_event_recorder_filters_by_strategy_source_and_type(tmp_path: Path):
    recorder = EventRecorder(path=tmp_path / "events.jsonl")
    recorder.record(TradingEvent(source="worker", event_type="strategy_tick", strategy_id="triplema"))
    recorder.record(TradingEvent(source="api", event_type="coinbase_balance", strategy_id=None))
    recorder.record(TradingEvent(source="worker", event_type="strategy_tick", strategy_id="zscore"))

    events = recorder.tail(limit=10, strategy_id="triplema", source="worker", event_type="strategy_tick")

    assert len(events) == 1
    assert events[0]["strategy_id"] == "triplema"
