import os

import pytest

from coinbase.src import trader_host_guard as guard


def test_optional_hostname_allowlist_fails_closed_on_mismatch(tmp_path, monkeypatch):
    monkeypatch.setattr(guard, "_is_local_ext4", lambda fd: True)

    with pytest.raises(guard.HostGuardError, match="not in TRADER_ACTIVE_HOST"):
        guard.acquire_writer_guard(
            tmp_path / "trader.lock", active_hosts="primary-a,primary-b", hostname="other"
        )

    assert not (tmp_path / "trader.lock").exists()


def test_hostname_allowlist_accepts_exact_host(tmp_path, monkeypatch):
    monkeypatch.setattr(guard, "_is_local_ext4", lambda fd: True)
    held = guard.acquire_writer_guard(
        tmp_path / "trader.lock", active_hosts="primary-a, primary-b", hostname="primary-b"
    )
    try:
        assert held.handle.fileno() >= 0
    finally:
        held.close()


def test_non_ext4_lock_path_fails_closed(tmp_path, monkeypatch):
    monkeypatch.setattr(guard, "_is_local_ext4", lambda fd: False)

    with pytest.raises(guard.HostGuardError, match="local ext4"):
        guard.acquire_writer_guard(tmp_path / "trader.lock", active_hosts="")


def test_flock_is_single_writer_and_held_until_guard_close(tmp_path, monkeypatch):
    monkeypatch.setattr(guard, "_is_local_ext4", lambda fd: True)
    path = tmp_path / "trader.lock"
    first = guard.acquire_writer_guard(path, active_hosts="")
    try:
        with pytest.raises(guard.HostGuardError, match="already holds"):
            guard.acquire_writer_guard(path, active_hosts="")
    finally:
        first.close()

    second = guard.acquire_writer_guard(path, active_hosts="")
    second.close()


def test_environment_constructor_uses_declared_guard_settings(tmp_path, monkeypatch):
    monkeypatch.setenv("TRADER_LOCK_PATH", str(tmp_path / "custom.lock"))
    monkeypatch.setenv("TRADER_ACTIVE_HOST", "node-a")
    monkeypatch.setattr(guard.socket, "gethostname", lambda: "node-a")
    monkeypatch.setattr(guard, "_is_local_ext4", lambda fd: True)

    held = guard.acquire_writer_guard_from_environment()
    try:
        assert held.path == tmp_path / "custom.lock"
        assert held.hostname == "node-a"
    finally:
        held.close()
