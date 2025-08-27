from __future__ import annotations
import json, os, time
from typing import Dict, Any, List

STATE_FILE = os.path.join(os.path.dirname(__file__), "..", "state", "brackets.json")

def load_state() -> Dict[str, Any]:
    try:
        with open(STATE_FILE, "r") as f:
            return json.load(f)
    except Exception:
        return {"brackets": []}

def save_state(state: Dict[str, Any]) -> None:
    os.makedirs(os.path.dirname(STATE_FILE), exist_ok=True)
    with open(STATE_FILE, "w") as f:
        json.dump(state, f, indent=2)

def add_bracket(record: Dict[str, Any]) -> None:
    st = load_state()
    st["brackets"].append(record)
    save_state(st)

def remove_bracket_by_id(cid: str) -> None:
    st = load_state()
    st["brackets"] = [b for b in st["brackets"] if b.get("client_order_id") != cid]
    save_state(st)
