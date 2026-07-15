import os
import sys
import time

import pytest

import run_production as rp
from run_production import (
    _signal_handler, _running_under_systemd, _pidfile_path,
    _write_supervisor_pid, _remove_supervisor_pid, _backoff_delay,
    _start, _stop, _shutdown_all, supervise, run_foreground,
    status, stop, main,
)


class FakeProc:
    def __init__(self, pid=12345, poll_code=None):
        self.pid = pid
        self._poll_code = poll_code

    def poll(self):
        return self._poll_code

    def terminate(self):
        self._terminated = True

    def wait(self, timeout=None):
        return 0

    def kill(self):
        self._killed = True


@pytest.fixture
def fresh_state(tmp_path, monkeypatch):
    rp._shutdown_requested = False
    rp._restart_counts = {}
    rp._last_restart_ts = {}
    procs = {}
    for name in ("daemon", "dashboard", "trader-v4", "llm-watchdog"):
        procs[name] = {
            "script": "x.py",
            "args": [],
            "pidfile": tmp_path / f"{name}.pid",
            "logfile": tmp_path / f"{name}.log",
            "proc": None,
        }
    monkeypatch.setattr(rp, "PROCESSES", procs)
    monkeypatch.setattr(rp.subprocess, "Popen", lambda *a, **k: FakeProc())
    return procs


def test_signal_handler():
    rp._shutdown_requested = False
    _signal_handler(15, None)
    assert rp._shutdown_requested is True
    rp._shutdown_requested = False


def test_running_under_systemd(monkeypatch):
    monkeypatch.setenv("INVOCATION_ID", "abc")
    assert _running_under_systemd() is True
    monkeypatch.delenv("INVOCATION_ID", raising=False)
    monkeypatch.setenv("NOTIFY_SOCKET", "/x")
    assert _running_under_systemd() is True
    monkeypatch.delenv("NOTIFY_SOCKET", raising=False)
    monkeypatch.setenv("JOURNAL_STREAM", "1")
    assert _running_under_systemd() is True
    monkeypatch.delenv("JOURNAL_STREAM", raising=False)
    assert _running_under_systemd() is False


def test_pidfile_path(tmp_path, monkeypatch):
    monkeypatch.setattr(rp, "LOGDIR", tmp_path)
    assert _pidfile_path() == tmp_path / "supervisor.pid"


def test_write_remove_pidfile(tmp_path, monkeypatch):
    pidfile = tmp_path / "sup.pid"
    _write_supervisor_pid(pidfile)
    assert pidfile.read_text() == str(os.getpid())
    _remove_supervisor_pid(pidfile)
    assert not pidfile.exists()


def test_backoff_delay():
    rp._restart_counts = {}
    assert _backoff_delay("x", 0.0) == 0.0
    rp._restart_counts = {"x": 1}
    assert _backoff_delay("x", 0.0) == 5.0
    rp._restart_counts = {"x": 3}
    assert _backoff_delay("x", 0.0) == 20.0
    rp._restart_counts = {"x": 50}
    assert _backoff_delay("x", 0.0) == 300.0


def test_start(fresh_state):
    proc = _start("daemon")
    assert isinstance(proc, FakeProc)
    assert fresh_state["daemon"]["proc"] is proc
    assert fresh_state["daemon"]["pidfile"].read_text() == str(proc.pid)
    assert rp._restart_counts["daemon"] == 1
    assert rp._last_restart_ts["daemon"] > 0


def test_start_backoff_no_wait(fresh_state, monkeypatch):
    rp._restart_counts = {"daemon": 1}
    rp._last_restart_ts = {"daemon": time.time() - 100}
    monkeypatch.setattr(rp, "time", time)
    proc = _start("daemon")
    assert isinstance(proc, FakeProc)


def test_start_backoff_waits(fresh_state, monkeypatch):
    sleeps = []
    monkeypatch.setattr(rp, "time", time)
    monkeypatch.setattr(rp.time, "sleep", lambda s: sleeps.append(s))
    rp._restart_counts = {"daemon": 1}
    rp._last_restart_ts = {"daemon": time.time()}  # elapsed ~0 < delay 5 -> sleep
    proc = _start("daemon")
    assert isinstance(proc, FakeProc)
    assert sum(sleeps) >= 4


def test_start_backoff_shutdown_exit(fresh_state, monkeypatch):
    monkeypatch.setattr(rp, "time", time)
    monkeypatch.setattr(rp.time, "sleep", lambda s: None)
    rp._shutdown_requested = True
    rp._restart_counts = {"daemon": 1}
    rp._last_restart_ts = {"daemon": time.time()}
    proc = _start("daemon")
    assert proc is fresh_state["daemon"].get("proc")
    rp._shutdown_requested = False


def test_stop_proc(fresh_state, monkeypatch):
    fake = FakeProc()
    fresh_state["daemon"]["proc"] = fake
    assert _stop("daemon") is True
    assert fresh_state["daemon"]["proc"] is None


def test_stop_no_proc(fresh_state):
    fresh_state["daemon"]["proc"] = None
    assert _stop("daemon") is True


def test_stop_proc_timeout(fresh_state, monkeypatch):
    class StuckProc:
        def terminate(self):
            pass
        def wait(self, timeout=None):
            if getattr(self, "_killed", False):
                return 0
            raise rp.subprocess.TimeoutExpired("p", timeout)
        def kill(self):
            self._killed = True
        def poll(self):
            return 1
    fresh_state["daemon"]["proc"] = StuckProc()
    assert _stop("daemon") is True


def test_stop_proc_generic_error(fresh_state):
    class BadProc:
        def terminate(self):
            pass
        def wait(self, timeout=None):
            raise ValueError("boom")
        def kill(self):
            pass
        def poll(self):
            return 1
    fresh_state["daemon"]["proc"] = BadProc()
    assert _stop("daemon") is True


def test_shutdown_all(fresh_state):
    for name in fresh_state:
        fresh_state[name]["proc"] = FakeProc()
    _shutdown_all()
    for name in fresh_state:
        assert fresh_state[name]["proc"] is None


def test_supervise_healthy(fresh_state, monkeypatch):
    monkeypatch.setattr(rp.signal, "signal", lambda *a, **k: None)
    names = list(fresh_state.keys())
    started = []

    # fake _start that populates procs WITHOUT touching restart bookkeeping,
    # so the in-loop healthy-reset branch can trigger.
    def fake_start(name):
        p = FakeProc()
        fresh_state[name]["proc"] = p
        started.append(name)
        return p

    monkeypatch.setattr(rp, "_start", fake_start)
    rp._last_restart_ts = {n: time.time() - 100 for n in names}
    # one process has a non-zero restart count (print path) and one has zero
    rp._restart_counts = {names[0]: 2}
    monkeypatch.setattr(rp, "time", time)

    sleep_calls = {"n": 0}

    def slow_sleep(s):
        sleep_calls["n"] += 1
        if sleep_calls["n"] >= 6:
            rp._shutdown_requested = True

    monkeypatch.setattr(rp.time, "sleep", slow_sleep)
    supervise()
    assert len(started) == len(names)
    assert rp._shutdown_requested is True
    rp._shutdown_requested = False


def test_supervise_restart(fresh_state, monkeypatch):
    monkeypatch.setattr(rp.signal, "signal", lambda *a, **k: None)
    rp._shutdown_requested = False
    # first pass: procs return code -> restart path taken
    for name in fresh_state:
        fresh_state[name]["proc"] = FakeProc(poll_code=1)

    def fake_start(name):
        p = FakeProc(poll_code=1)
        fresh_state[name]["proc"] = p
        return p

    monkeypatch.setattr(rp, "_start", fake_start)

    def exiting_sleep(s):
        rp._shutdown_requested = True

    monkeypatch.setattr(rp, "time", time)
    monkeypatch.setattr(rp.time, "sleep", exiting_sleep)
    supervise()
    assert rp._shutdown_requested is True
    rp._shutdown_requested = False


def test_run_foreground(fresh_state, monkeypatch):
    monkeypatch.setattr(rp, "supervise", lambda: None)
    pidfile = fresh_state["daemon"]["pidfile"].with_name("supervisor.pid")
    monkeypatch.setattr(rp, "_pidfile_path", lambda: pidfile)
    run_foreground()
    rp._remove_supervisor_pid(pidfile)
    assert not pidfile.exists()


def test_status_running(tmp_path, monkeypatch, capsys):
    # supervisor running
    sup_pid = tmp_path / "supervisor.pid"
    sup_pid.write_text("99999")
    monkeypatch.setattr(rp, "_pidfile_path", lambda: sup_pid)
    monkeypatch.setattr(rp, "PROCESSES", {"daemon": {"pidfile": tmp_path / "daemon.pid"}})
    (tmp_path / "daemon.pid").write_text("88888")
    monkeypatch.setattr(rp.os, "kill", lambda p, s: None)
    monkeypatch.setattr(rp, "ROOT", tmp_path)
    status()
    out = capsys.readouterr().out
    assert "RUNNING" in out


def test_status_stopped(tmp_path, monkeypatch, capsys):
    monkeypatch.setattr(rp, "_pidfile_path", lambda: tmp_path / "nope.pid")
    monkeypatch.setattr(rp, "PROCESSES", {})
    status()
    out = capsys.readouterr().out
    assert "STOPPED" in out


def test_status_stale_pid(tmp_path, monkeypatch, capsys):
    sup_pid = tmp_path / "supervisor.pid"
    sup_pid.write_text("99999")
    monkeypatch.setattr(rp, "_pidfile_path", lambda: sup_pid)
    monkeypatch.setattr(rp, "PROCESSES", {"daemon": {"pidfile": tmp_path / "daemon.pid"}})
    (tmp_path / "daemon.pid").write_text("88888")
    def kill_raise(pid, sig):
        raise OSError()
    monkeypatch.setattr(rp.os, "kill", kill_raise)
    monkeypatch.setattr(rp, "ROOT", tmp_path)
    status()
    out = capsys.readouterr().out
    assert "STOPPED" in out
    assert "STALE" in out


def test_status_heartbeat(tmp_path, monkeypatch, capsys):
    sup_pid = tmp_path / "supervisor.pid"
    sup_pid.write_text("99999")
    monkeypatch.setattr(rp, "_pidfile_path", lambda: sup_pid)
    monkeypatch.setattr(rp, "PROCESSES", {})
    monkeypatch.setattr(rp.os, "kill", lambda p, s: None)
    hb = tmp_path / "data" / ".daemon_heartbeat"
    hb.parent.mkdir(parents=True, exist_ok=True)
    monkeypatch.setattr(rp, "ROOT", tmp_path)
    hb.write_text(str(time.time() - 5))
    status()
    out = capsys.readouterr().out
    assert "Heartbeat" in out


def test_stop_command(tmp_path, monkeypatch, capsys):
    sup_pid = tmp_path / "supervisor.pid"
    sup_pid.write_text("99999")
    monkeypatch.setattr(rp, "_pidfile_path", lambda: sup_pid)
    procs = {}
    for name in ("daemon", "dashboard"):
        p = tmp_path / f"{name}.pid"
        p.write_text("88888")
        procs[name] = {"pidfile": p}
    monkeypatch.setattr(rp, "PROCESSES", procs)

    def kill_selective(pid, sig):
        if pid == 99999:
            return None
        raise OSError()
    monkeypatch.setattr(rp.os, "kill", kill_selective)
    monkeypatch.setattr(rp, "time", time)
    monkeypatch.setattr(rp.time, "sleep", lambda s: None)
    stop()
    assert not sup_pid.exists()
    out = capsys.readouterr().out
    assert "Stopped" in out


def test_remove_pidfile_oserror(tmp_path, monkeypatch):
    class FakePid:
        def unlink(self, missing_ok=True):
            raise OSError()
    rp._remove_supervisor_pid(FakePid())


def test_status_child_stopped(tmp_path, monkeypatch, capsys):
    monkeypatch.setattr(rp, "_pidfile_path", lambda: tmp_path / "nope.pid")
    monkeypatch.setattr(rp, "PROCESSES", {"daemon": {"pidfile": tmp_path / "missing.pid"}})
    monkeypatch.setattr(rp.os, "kill", lambda p, s: None)
    monkeypatch.setattr(rp, "ROOT", tmp_path)
    status()
    out = capsys.readouterr().out
    assert "STOPPED" in out


def test_status_heartbeat_invalid(tmp_path, monkeypatch, capsys):
    sup_pid = tmp_path / "supervisor.pid"
    sup_pid.write_text("99999")
    monkeypatch.setattr(rp, "_pidfile_path", lambda: sup_pid)
    monkeypatch.setattr(rp, "PROCESSES", {})
    monkeypatch.setattr(rp.os, "kill", lambda p, s: None)
    hb = tmp_path / "data" / ".daemon_heartbeat"
    hb.parent.mkdir(parents=True, exist_ok=True)
    monkeypatch.setattr(rp, "ROOT", tmp_path)
    hb.write_text("not-a-float")
    status()
    out = capsys.readouterr().out
    assert "Heartbeat" not in out


def test_stop_supervisor_bad_pid(tmp_path, monkeypatch, capsys):
    sup_pid = tmp_path / "supervisor.pid"
    sup_pid.write_text("not-an-int")
    monkeypatch.setattr(rp, "_pidfile_path", lambda: sup_pid)
    monkeypatch.setattr(rp, "PROCESSES", {})
    monkeypatch.setattr(rp.time, "sleep", lambda s: None)
    stop()
    out = capsys.readouterr().out
    assert "Stopped" in out


def test_stop_no_supervisor(tmp_path, monkeypatch, capsys):
    monkeypatch.setattr(rp, "_pidfile_path", lambda: tmp_path / "nope.pid")
    procs = {}
    for name in ("daemon", "dashboard"):
        p = tmp_path / f"{name}.pid"
        p.write_text("88888")
        procs[name] = {"pidfile": p}
    monkeypatch.setattr(rp, "PROCESSES", procs)

    def kill_raise(pid, sig):
        raise OSError()
    monkeypatch.setattr(rp.os, "kill", kill_raise)
    monkeypatch.setattr(rp.time, "sleep", lambda s: None)
    stop()
    assert "Stopped" in capsys.readouterr().out


def test_stop_command_full(tmp_path, monkeypatch, capsys):
    sup_pid = tmp_path / "supervisor.pid"
    sup_pid.write_text("99999")
    monkeypatch.setattr(rp, "_pidfile_path", lambda: sup_pid)
    procs = {}
    for name in ("daemon", "dashboard"):
        p = tmp_path / f"{name}.pid"
        p.write_text("88888")
        procs[name] = {"pidfile": p}
    monkeypatch.setattr(rp, "PROCESSES", procs)

    def kill_all(pid, sig):
        return None
    monkeypatch.setattr(rp.os, "kill", kill_all)
    monkeypatch.setattr(rp.time, "sleep", lambda s: None)
    stop()
    out = capsys.readouterr().out
    assert "Sent SIGTERM to supervisor" in out
    assert "Sent SIGTERM to daemon" in out
    assert "Force killed PID" in out


class _FakePidFile:
    def exists(self):
        return True
    def read_text(self):
        return "88888"
    def unlink(self, missing_ok=True):
        raise OSError()


def test_stop_pidfile_unlink_oserror(tmp_path, monkeypatch, capsys):
    sup_pid = tmp_path / "supervisor.pid"
    sup_pid.write_text("99999")
    monkeypatch.setattr(rp, "_pidfile_path", lambda: sup_pid)
    procs = {"daemon": {"pidfile": _FakePidFile()}}
    monkeypatch.setattr(rp, "PROCESSES", procs)

    def kill_raise(pid, sig):
        if pid == 99999:
            return None
        raise OSError()
    monkeypatch.setattr(rp.os, "kill", kill_raise)
    monkeypatch.setattr(rp.time, "sleep", lambda s: None)
    stop()
    assert "Stopped" in capsys.readouterr().out


def test_main_start_stale_pidfile(fresh_state, tmp_path, monkeypatch):
    pidfile = tmp_path / "supervisor.pid"
    pidfile.write_text("99999")
    monkeypatch.setattr(rp, "_pidfile_path", lambda: pidfile)
    # stale pid -> os.kill raises -> falls through to run_foreground
    monkeypatch.setattr(rp.os, "kill", lambda p, s: (_ for _ in ()).throw(OSError()))
    called = {}
    monkeypatch.setattr(rp, "run_foreground", lambda: called.setdefault("run", True))
    monkeypatch.setattr(sys, "argv", ["run_production.py", "start"])
    monkeypatch.setattr(rp, "ROOT", tmp_path)
    main()
    assert called.get("run")
    rp._remove_supervisor_pid(pidfile)


def test_stop_child_missing_pidfile(tmp_path, monkeypatch, capsys):
    sup_pid = tmp_path / "supervisor.pid"
    sup_pid.write_text("99999")
    monkeypatch.setattr(rp, "_pidfile_path", lambda: sup_pid)
    existing = tmp_path / "daemon.pid"
    existing.write_text("88888")
    procs = {
        "daemon": {"pidfile": existing},
        "dashboard": {"pidfile": tmp_path / "ghost.pid"},  # does not exist
    }
    monkeypatch.setattr(rp, "PROCESSES", procs)

    def kill_all(pid, sig):
        return None
    monkeypatch.setattr(rp.os, "kill", kill_all)
    monkeypatch.setattr(rp.time, "sleep", lambda s: None)
    stop()
    assert "Stopped" in capsys.readouterr().out


def test_main_status(fresh_state, monkeypatch, capsys):
    monkeypatch.setattr(rp, "status", lambda: capsys.readouterr())
    monkeypatch.setattr(sys, "argv", ["run_production.py", "status"])
    main()
    # status prints; just ensure no error
    assert True


def test_main_stop(fresh_state, monkeypatch):
    called = {}
    monkeypatch.setattr(rp, "stop", lambda: called.setdefault("stop", True))
    monkeypatch.setattr(sys, "argv", ["run_production.py", "stop"])
    main()
    assert called.get("stop")


def test_main_run(fresh_state, monkeypatch):
    called = {}
    monkeypatch.setattr(rp, "run_foreground", lambda: called.setdefault("run", True))
    monkeypatch.setattr(sys, "argv", ["run_production.py", "run"])
    main()
    assert called.get("run")


def test_main_restart(fresh_state, monkeypatch):
    calls = []
    monkeypatch.setattr(rp, "stop", lambda: calls.append("stop"))
    monkeypatch.setattr(rp, "main", lambda: calls.append("main"))
    monkeypatch.setattr(rp, "time", time)
    monkeypatch.setattr(rp.time, "sleep", lambda s: None)
    monkeypatch.setattr(sys, "argv", ["run_production.py", "restart"])
    main()
    assert "stop" in calls
    assert "main" in calls


def test_main_start_fresh(fresh_state, tmp_path, monkeypatch):
    called = {}
    monkeypatch.setattr(rp, "run_foreground", lambda: called.setdefault("run", True))
    monkeypatch.setattr(sys, "argv", ["run_production.py", "start"])
    pidfile = tmp_path / "supervisor.pid"
    monkeypatch.setattr(rp, "_pidfile_path", lambda: pidfile)
    monkeypatch.setattr(rp, "ROOT", tmp_path)
    main()
    assert called.get("run")
    # cleanup
    rp._remove_supervisor_pid(pidfile)


def test_main_start_already_running(fresh_state, tmp_path, monkeypatch):
    pidfile = tmp_path / "supervisor.pid"
    pidfile.write_text("99999")
    monkeypatch.setattr(rp, "_pidfile_path", lambda: pidfile)
    monkeypatch.setattr(rp.os, "kill", lambda p, s: None)
    monkeypatch.setattr(sys, "argv", ["run_production.py", "start"])
    with pytest.raises(SystemExit):
        main()


def test_main_unknown(fresh_state, monkeypatch):
    monkeypatch.setattr(sys, "argv", ["run_production.py", "bogus"])
    with pytest.raises(SystemExit):
        main()
