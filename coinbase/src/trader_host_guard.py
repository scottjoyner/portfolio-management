"""Local-host trader process guard.

This is deliberately a single-machine control: a Linux ``flock`` on a verified
local ext4 mount plus an optional hostname allowlist. It is not distributed
fencing and must not be represented as such.
"""

from __future__ import annotations

import fcntl
import json
import os
import socket
import time
from dataclasses import dataclass
from pathlib import Path
from typing import TextIO


class HostGuardError(RuntimeError):
    """The local host is not authorized to become the trader writer."""


@dataclass
class WriterGuard:
    path: Path
    handle: TextIO
    hostname: str

    def close(self) -> None:
        if not self.handle.closed:
            fcntl.flock(self.handle.fileno(), fcntl.LOCK_UN)
            self.handle.close()

    def __enter__(self) -> "WriterGuard":
        return self

    def __exit__(self, *_exc: object) -> None:
        self.close()


def _is_local_ext4(fd: int) -> bool:
    """Identify the fd's mounted filesystem from Linux mountinfo and st_dev."""
    stat = os.fstat(fd)
    device = f"{os.major(stat.st_dev)}:{os.minor(stat.st_dev)}"
    try:
        with open("/proc/self/mountinfo", encoding="utf-8") as mounts:
            for line in mounts:
                left, separator, right = line.partition(" - ")
                if not separator:
                    continue
                fields = left.split()
                fs_fields = right.split()
                if len(fields) >= 3 and fields[2] == device and fs_fields:
                    return fs_fields[0] == "ext4"
    except OSError:
        return False
    return False


def _allowed_hosts(raw: str) -> set[str]:
    return {item.strip() for item in raw.split(",") if item.strip()}


def acquire_writer_guard(
    lock_path: Path,
    *,
    active_hosts: str | None = None,
    hostname: str | None = None,
) -> WriterGuard:
    """Acquire and return the process-lifetime local writer guard.

    The caller must retain the returned object for its entire trading lifetime.
    An unset/empty ``active_hosts`` disables the hostname restriction; when set,
    the current hostname must be an exact item in the comma-separated allowlist.
    """
    host = hostname if hostname is not None else socket.gethostname()
    allowlist = _allowed_hosts(active_hosts or "")
    if allowlist and host not in allowlist:
        raise HostGuardError(
            f"hostname {host!r} is not in TRADER_ACTIVE_HOST allowlist; refusing to start"
        )

    path = Path(lock_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    handle = path.open("a+", encoding="utf-8")
    try:
        if not _is_local_ext4(handle.fileno()):
            raise HostGuardError(
                f"writer lock {path} is not on a local ext4 filesystem; refusing to start"
            )
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise HostGuardError(
                f"another trader process already holds writer lock {path}"
            ) from exc
        handle.seek(0)
        handle.truncate()
        json.dump(
            {"pid": os.getpid(), "hostname": host, "acquired_unix": time.time()},
            handle,
            sort_keys=True,
        )
        handle.write("\n")
        handle.flush()
        os.fsync(handle.fileno())
        return WriterGuard(path=path, handle=handle, hostname=host)
    except BaseException:
        handle.close()
        raise


def acquire_writer_guard_from_environment() -> WriterGuard:
    return acquire_writer_guard(
        Path(os.environ.get("TRADER_LOCK_PATH", "data/trader-v4.lock")),
        active_hosts=os.environ.get("TRADER_ACTIVE_HOST", ""),
    )
