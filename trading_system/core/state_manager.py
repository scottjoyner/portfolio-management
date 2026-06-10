from __future__ import annotations
import json
import os
import threading
from typing import Dict, List, Any, Optional
from trading_system.core.models.domain import Bracket

class StateManager:
    """
    Centralized state manager for portfolio entities.
    Thread-safe file-based persistence for initial implementation.
    """
    def __init__(self, state_dir: str = "/home/scott/git/portfolio-management/trading_system/state"):
        self.state_dir = state_dir
        self.brackets_path = os.path.join(self.state_dir, "brackets.json")
        self.lock = threading.Lock()
        self._ensure_dir()

    def _ensure_dir(self) -> None:
        if not os.path.exists(self.state_dir):
            os.makedirs(self.state_dir, exist_ok=True)

    def load_brackets(self) -> List[Bracket]:
        with self.lock:
            if not os.path.exists(self.brackets_path):
                return []
            try:
                with open(self.brackets_path, "r") as f:
                    data = json.load(f)
                    return [Bracket(**b) for b in data.get("brackets", [])]
            except Exception:
                return []

    def save_brackets(self, brackets: List[Bracket]) -> None:
        with self.lock:
            with open(self.brackets_path, "w") as f:
                json.dump([b.dict() for b in brackets], f, indent=2)

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
