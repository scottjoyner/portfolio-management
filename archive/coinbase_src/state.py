from __future__ import annotations
from typing import List, Dict, Any
from trading_system.core.state_manager import state_manager
from trading_system.core.models.domain import Bracket

def load_state() -> List[Bracket]:
    """
    Loads all brackets from the centralized state manager.
    """
    return state_manager.load_brackets()

def save_state(brackets: List[Bracket]) -> None:
    """
    Saves the list of brackets to the centralized state manager.
    """
    state_manager.save_brackets(brackets)

def add_bracket(record: Dict[str, Any]) -> None:
    """
    Adds a new bracket to the centralized state manager.
    """
    # Convert dict to Bracket model
    bracket = Bracket(**record)
    state_manager.add_bracket(bracket)

def remove_bracket_by_id(cid: str) -> None:
    """
    Removes a bracket by its client_order_id from the centralized state manager.
    """
    state_manager.remove_bracket_by_id(cid)
