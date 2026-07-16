from __future__ import annotations
import json
import os
import threading
import tempfile
import fcntl
import logging
from typing import Dict, List, Any, Optional
from trading_system.core.models.domain import Bracket

logger = logging.getLogger(__name__)


class FileLock:
    """Cross-process advisory lock using fcntl flock on a sidecar file."""

    def __init__(self, path: str, timeout: float = 30.0):
        self.lock_path = f"{path}.lock"
        self.timeout = timeout
        self._fh = None

    def __enter__(self) -> "FileLock":
        self._fh = open(self.lock_path, "w")
        fcntl.flock(self._fh.fileno(), fcntl.LOCK_EX)
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        try:
            if self._fh is not None:
                fcntl.flock(self._fh.fileno(), fcntl.LOCK_UN)
                self._fh.close()
        except Exception:
            pass


class StateManager:
    """
    Centralized state manager for portfolio entities.
    Thread- and cross-process-safe file-based persistence with atomic writes
    and corruption recovery via a rolling backup.
    """
    def __init__(self, state_dir: str = "/home/scott/git/portfolio-management/trading_system/state"):
        self.state_dir = state_dir
        self.brackets_path = os.path.join(self.state_dir, "brackets.json")
        self.brackets_bak = os.path.join(self.state_dir, "brackets.json.bak")
        self.lock = threading.RLock()
        self._ensure_dir()

    def _ensure_dir(self) -> None:
        if not os.path.exists(self.state_dir):
            os.makedirs(self.state_dir, exist_ok=True)

    def _atomic_write(self, payload: Dict[str, Any]) -> None:
        self._ensure_dir()
        fd, tmp = tempfile.mkstemp(dir=self.state_dir, suffix=".tmp")
        try:
            with os.fdopen(fd, "w") as f:
                json.dump(payload, f, indent=2)
                f.flush()
                os.fsync(f.fileno())
            if os.path.exists(self.brackets_path):
                if os.path.exists(self.brackets_bak):
                    os.remove(self.brackets_bak)
                os.rename(self.brackets_path, self.brackets_bak)
            else:
                import shutil
                shutil.copy2(tmp, self.brackets_bak)
            os.rename(tmp, self.brackets_path)
        except Exception:
            if os.path.exists(tmp):
                os.remove(tmp)
            raise

    def load_brackets(self) -> List[Bracket]:
        with self.lock:
            if not os.path.exists(self.brackets_path):
                return []
            try:
                with open(self.brackets_path, "r") as f:
                    data = json.load(f)
                return self._deserialize(data)
            except Exception as exc:
                logger.warning("brackets load failed: %s; attempting backup", exc)
                if os.path.exists(self.brackets_bak):
                    try:
                        with open(self.brackets_bak, "r") as f:
                            data = json.load(f)
                        return self._deserialize(data)
                    except Exception as bak_exc:
                        logger.error("brackets backup load failed: %s", bak_exc)
                raise

    def _deserialize(self, data: Dict[str, Any]) -> List[Bracket]:
        brackets: List[Bracket] = []
        for b in data.get("brackets", []):
            try:
                brackets.append(Bracket(**b))
            except Exception as exc:
                logger.warning("skipping invalid bracket record: %s", exc)
        return brackets

    def save_brackets(self, brackets: List[Bracket]) -> None:
        with self.lock:
            with FileLock(self.brackets_path):
                payload = [b.model_dump() if hasattr(b, 'model_dump') else b.dict() for b in brackets]
                self._atomic_write({"brackets": payload})

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

# Singleton instance for the system
state_manager = StateManager()
