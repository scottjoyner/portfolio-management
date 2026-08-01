from __future__ import annotations

import json
from pathlib import Path

from coinbase.src.orchestrator import ExecutionOrchestrator, TradeMode, TradeSignal
from coinbase.src.protocols import Direction


def _signal(product_id: str = "BTC-USD") -> TradeSignal:
    return TradeSignal(
        product_id=product_id,
        direction=Direction.LONG,
        entry_price=50_000.0,
        stop_price=49_000.0,
        target_price=52_000.0,
        size=0.001,
        confidence=0.9,
        reason="approval persistence regression",
        strategy_name="test_strategy",
    )


def test_approval_persistence_creates_nested_parent_and_appends(tmp_path: Path):
    pending_path = tmp_path / "nested" / "state" / "pending_approvals.json"
    orchestrator = ExecutionOrchestrator(
        mode=TradeMode.PAPER,
        dry_run=True,
        pending_file=str(pending_path),
    )

    first = orchestrator._approval_execute(_signal("BTC-USD"))
    second = orchestrator._approval_execute(_signal("ETH-USD"))

    assert pending_path.exists()
    payload = json.loads(pending_path.read_text())
    assert isinstance(payload, dict)
    assert set(payload) == {first["token"], second["token"]}
    assert payload[first["token"]]["product_id"] == "BTC-USD"
    assert payload[second["token"]]["product_id"] == "ETH-USD"
    assert len(orchestrator.state.pending_approvals) == 2
    assert not list(pending_path.parent.glob("pending_approvals.json.tmp-*"))


def test_approval_persistence_recovers_from_invalid_existing_payload(tmp_path: Path):
    pending_path = tmp_path / "pending" / "pending_approvals.json"
    pending_path.parent.mkdir(parents=True)
    pending_path.write_text("not-json")

    orchestrator = ExecutionOrchestrator(
        mode=TradeMode.PAPER,
        dry_run=True,
        pending_file=str(pending_path),
    )
    result = orchestrator._approval_execute(_signal())

    payload = json.loads(pending_path.read_text())
    assert list(payload) == [result["token"]]
    assert payload[result["token"]]["status"] == "pending"
    assert pending_path.with_suffix(pending_path.suffix + ".lock").exists()
