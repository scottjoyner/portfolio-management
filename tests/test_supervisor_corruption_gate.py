from pathlib import Path

import run_production as supervisor


class FakePopen:
    calls = []

    def __init__(self, argv, **kwargs):
        self.argv = argv
        self.pid = 4242
        self.returncode = None
        FakePopen.calls.append((argv, kwargs))

    def poll(self):
        return self.returncode


def _config(tmp_path: Path, name: str):
    return {
        "script": f"{name}.py",
        "args": [],
        "pidfile": tmp_path / f"{name}.pid",
        "logfile": tmp_path / f"{name}.log",
        "proc": None,
    }


def test_corruption_sentinel_blocks_only_trader_start(tmp_path, monkeypatch):
    monkeypatch.setattr(supervisor, "ROOT", tmp_path)
    monkeypatch.setattr(
        supervisor,
        "PROCESSES",
        {
            "trader-v4": _config(tmp_path, "trader"),
            "dashboard": _config(tmp_path, "dashboard"),
        },
    )
    monkeypatch.setattr(supervisor.subprocess, "Popen", FakePopen)
    monkeypatch.setattr(supervisor, "_restart_counts", {})
    monkeypatch.setattr(supervisor, "_last_restart_ts", {})
    (tmp_path / "data").mkdir()
    (tmp_path / "data" / "trader_state_corrupt").write_text("blocked")
    FakePopen.calls.clear()

    assert supervisor._start("trader-v4") is None
    dashboard = supervisor._start("dashboard")

    assert dashboard is not None
    assert len(FakePopen.calls) == 1
    assert FakePopen.calls[0][0][-1].endswith("dashboard.py")
    assert not (tmp_path / "trader.pid").exists()
    assert (tmp_path / "dashboard.pid").exists()


def test_trader_can_start_after_operator_clears_sentinel(tmp_path, monkeypatch):
    monkeypatch.setattr(supervisor, "ROOT", tmp_path)
    monkeypatch.setattr(
        supervisor, "PROCESSES", {"trader-v4": _config(tmp_path, "trader")}
    )
    monkeypatch.setattr(supervisor.subprocess, "Popen", FakePopen)
    monkeypatch.setattr(supervisor, "_restart_counts", {})
    monkeypatch.setattr(supervisor, "_last_restart_ts", {})
    (tmp_path / "data").mkdir()
    FakePopen.calls.clear()

    assert supervisor._start("trader-v4") is not None
    assert len(FakePopen.calls) == 1
