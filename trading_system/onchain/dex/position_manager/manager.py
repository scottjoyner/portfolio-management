from __future__ import annotations

from .actions import PositionAction
from .validators import validate_action_limits


class PositionManager:
    def __init__(self, max_action_usd):
        self.max_action_usd = max_action_usd

    def propose(self, action: PositionAction) -> PositionAction:
        validate_action_limits(action.amount_usd, self.max_action_usd)
        return action
