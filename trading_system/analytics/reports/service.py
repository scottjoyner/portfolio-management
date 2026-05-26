from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class Report:
    title: str
    sections: dict[str, Any] = field(default_factory=dict)

    def add_section(self, name: str, data: Any) -> None:
        self.sections[name] = data

    def to_dict(self) -> dict[str, Any]:
        return {"title": self.title, "sections": self.sections}


@dataclass
class ReportService:
    reports: dict[str, Report] = field(default_factory=dict)

    def generate(self, report_id: str, title: str) -> Report:
        report = Report(title=title)
        self.reports[report_id] = report
        return report
