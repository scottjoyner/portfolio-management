from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone


@dataclass
class KillSwitch:
    active: bool = False
    triggered_by: str = ""
    reason: str = ""
    triggered_at: datetime | None = None

    def engage(self, triggered_by: str, reason: str = "manual") -> None:
        self.active = True
        self.triggered_by = triggered_by
        self.reason = reason
        self.triggered_at = datetime.now(timezone.utc)

    def disengage(self) -> None:
        self.active = False
        self.triggered_by = ""
        self.reason = ""
        self.triggered_at = None


@dataclass
class KillSwitchManager:
    switches: dict[str, KillSwitch] = field(default_factory=dict)
    auto_triggers: dict[str, float] = field(default_factory=dict)

    def get_switch(self, name: str = "global") -> KillSwitch:
        return self.switches.setdefault(name, KillSwitch())

    def engage(self, name: str, triggered_by: str, reason: str = "manual") -> None:
        self.get_switch(name).engage(triggered_by, reason)

    def disengage(self, name: str = "global") -> None:
        self.get_switch(name).disengage()

    def is_active(self, name: str = "global") -> bool:
        return self.get_switch(name).active

    def set_auto_trigger(self, condition: str, threshold: float) -> None:
        self.auto_triggers[condition] = threshold

    def check_auto_trigger(self, condition: str, current_value: float) -> bool:
        threshold = self.auto_triggers.get(condition)
        if threshold is None:
            return False
        if current_value >= threshold:
            self.engage(f"auto:{condition}", f"{condition} exceeded threshold {threshold}")
            return True
        return False
