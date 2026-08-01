from __future__ import annotations
import json
import os
import tempfile
import threading
import time
import fcntl
import logging
from typing import Dict, List, Any, Optional
from trading_system.core.models.domain import Bracket

logger = logging.getLogger(__name__)

_STATE_DIR_ENV = "TRADING_SYSTEM_STATE_DIR"
_DEFAULT_STATE_DIR = os.path.abspath(
    os.path.join(os.path.dirname(os.path.dirname(__file__)), "state")
)


class FileLock:
    """Cross-process advisory file lock using fcntl flock.

    Lightweight lockfile context manager so multiple daemons writing the
    same JSON state files do not clobber each other.
    """

    def __init__(self, lock_path: str, timeout: float = 30.0):
        self.lock_path = lock_path
        self.timeout = timeout
        self._fd = None

    def acquire(self) -> None:
        self._fd = open(self.lock_path, "a+")
        deadline = time.monotonic() + self.timeout
        while True:
            try:
                fcntl.flock(self._fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                return
            except OSError:
                if time.monotonic() >= deadline:
                    self._fd.close()
                    self._fd = None
                    raise TimeoutError(
                        f"Could not acquire lock {self.lock_path} within {self.timeout}s"
                    )
                time.sleep(0.05)

    def release(self) -> None:
        if self._fd is not None:
            try:
                fcntl.flock(self._fd, fcntl.LOCK_UN)
            finally:
                self._fd.close()
                self._fd = None

    def __enter__(self) -> "FileLock":
        self.acquire()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.release()


class StateManager:
    """Centralized, process-safe file persistence for portfolio entities."""

    def __init__(self, state_dir: Optional[str] = None):
        configured = state_dir or os.getenv(_STATE_DIR_ENV) or _DEFAULT_STATE_DIR
        self.state_dir = os.path.abspath(os.path.expanduser(configured))
        self.brackets_path = os.path.join(self.state_dir, "brackets.json")
        self.brackets_lock_path = self.brackets_path + ".lock"
        self.brackets_backup_path = self.brackets_path + ".bak"
        self.lock = threading.RLock()
        self._ensure_dir()

    def _ensure_dir(self) -> None:
        os.makedirs(self.state_dir, exist_ok=True)

    def _atomic_write(self, path: str, payload: Any) -> None:
        directory = os.path.dirname(path)
        fd, tmp_path = tempfile.mkstemp(dir=directory, prefix=".tmp-", suffix=".json")
        try:
            with os.fdopen(fd, "w") as f:
                json.dump(payload, f, indent=2)
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp_path, path)
        except BaseException:
            if os.path.exists(tmp_path):
                try:
                    os.remove(tmp_path)
                except OSError:
                    pass
            raise

    def _deserialize(self, raw: str) -> List[Bracket]:
        data = json.loads(raw)
        brackets: List[Bracket] = []
        for idx, b in enumerate(data.get("brackets", [])):
            try:
                brackets.append(Bracket(**b))
            except Exception as exc:
                logger.warning(
                    "Skipping invalid bracket record at index %d: %s", idx, exc
                )
        return brackets

    def load_brackets(self) -> List[Bracket]:
        with self.lock:
            if not os.path.exists(self.brackets_path):
                return []
            try:
                with FileLock(self.brackets_lock_path):
                    with open(self.brackets_path, "r") as f:
                        return self._deserialize(f.read())
            except Exception as exc:
                logger.error(
                    "Failed to load brackets from %s: %s", self.brackets_path, exc
                )
                if os.path.exists(self.brackets_backup_path):
                    try:
                        with open(self.brackets_backup_path, "r") as f:
                            brackets = self._deserialize(f.read())
                        logger.warning(
                            "Recovered brackets from backup %s (%d entries)",
                            self.brackets_backup_path,
                            len(brackets),
                        )
                        return brackets
                    except Exception as bexc:
                        logger.error(
                            "Backup recovery failed from %s: %s",
                            self.brackets_backup_path,
                            bexc,
                        )
                logger.error(
                    "No usable brackets state or backup; raising to avoid silent data loss"
                )
                raise

    def save_brackets(self, brackets: List[Bracket]) -> None:
        with self.lock:
            payload = [b.model_dump() if hasattr(b, "model_dump") else b.dict() for b in brackets]
            serializable = {"brackets": payload}
            with FileLock(self.brackets_lock_path):
                self._atomic_write(self.brackets_path, serializable)
                try:
                    import shutil
                    shutil.copy2(self.brackets_path, self.brackets_backup_path)
                except OSError as exc:
                    logger.warning("Could not write backup after save: %s", exc)

    def add_bracket(self, bracket: Bracket) -> None:
        with self.lock:
            brackets = self.load_brackets()
            brackets.append(bracket)
            self.save_brackets(brackets)

    def remove_bracket_by_id(self, client_order_id: str) -> None:
        with self.lock:
            brackets = self.load_brackets()
            filtered = [b for b in brackets if b.client_order_id != client_order_id]
            self.save_brackets(filtered)


# Singleton instance for the system. The path is portable and may be overridden
# by TRADING_SYSTEM_STATE_DIR on deployment hosts and in tests.
state_manager = StateManager()
