from __future__ import annotations

import multiprocessing as mp
from pathlib import Path

import pytest

from scripts.learning_lineage import LineageStore


def _append_worker(path: str, worker: int, count: int) -> None:
    store = LineageStore(path)
    for index in range(count):
        store.append(
            "error",
            {"worker": worker, "index": index},
            actor=f"worker-{worker}",
        )


def test_parallel_lineage_writers_preserve_every_event_and_hash_chain(tmp_path: Path):
    lineage_path = tmp_path / "lineage.jsonl"
    workers = 4
    events_per_worker = 8
    ctx = mp.get_context("spawn")
    processes = [
        ctx.Process(
            target=_append_worker,
            args=(str(lineage_path), worker, events_per_worker),
        )
        for worker in range(workers)
    ]

    for process in processes:
        process.start()
    for process in processes:
        process.join(timeout=30)
        assert process.exitcode == 0

    store = LineageStore(lineage_path)
    rows = store.events()
    assert len(rows) == workers * events_per_worker
    assert [row["sequence"] for row in rows] == list(range(1, len(rows) + 1))
    assert store.verify()["ok"] is True
    observed = {
        (row["payload"]["worker"], row["payload"]["index"])
        for row in rows
    }
    assert len(observed) == workers * events_per_worker


def test_nonfinite_payload_is_rejected_before_lineage_mutation(tmp_path: Path):
    lineage_path = tmp_path / "lineage.jsonl"
    store = LineageStore(lineage_path)
    first = store.append("error", {"ok": 1.0})

    with pytest.raises(ValueError):
        store.append("error", {"bad": float("nan")})

    rows = store.events()
    assert len(rows) == 1
    assert rows[0]["id"] == first["id"]
    assert store.verify()["ok"] is True
