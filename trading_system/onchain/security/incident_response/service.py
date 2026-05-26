from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone


@dataclass
class IncidentAction:
    action_type: str
    params: dict = field(default_factory=dict)
    executed: bool = False
    executed_at: datetime | None = None


@dataclass
class IncidentResponse:
    incident_id: str
    severity: str
    summary: str
    actions: list[IncidentAction] = field(default_factory=list)
    resolved: bool = False

    def add_action(self, action_type: str, params: dict | None = None) -> IncidentAction:
        action = IncidentAction(action_type=action_type, params=params or {})
        self.actions.append(action)
        return action

    def execute_all(self) -> None:
        for action in self.actions:
            action.executed = True
            action.executed_at = datetime.now(timezone.utc)
        self.resolved = True


@dataclass
class IncidentResponseService:
    responses: dict[str, IncidentResponse] = field(default_factory=dict)

    def create_response(self, incident_id: str, severity: str, summary: str) -> IncidentResponse:
        response = IncidentResponse(incident_id=incident_id, severity=severity, summary=summary)
        self.responses[incident_id] = response
        return response

    def get_response(self, incident_id: str) -> IncidentResponse | None:
        return self.responses.get(incident_id)
