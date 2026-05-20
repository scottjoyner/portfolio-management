from pydantic import BaseModel


class ApprovalRequest(BaseModel):
    action: str
    summary: str
    rationale: str
    risk_notes: list[str]


class VoiceAgentAdapter:
    def build_payload(self, request: ApprovalRequest) -> dict:
        return {"type": "approval_required", "payload": request.model_dump()}
