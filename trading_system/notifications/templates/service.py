from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class NotificationTemplate:
    name: str
    subject_template: str
    body_template: str
    html_template: str = ""


@dataclass
class TemplateService:
    templates: dict[str, NotificationTemplate] = field(default_factory=dict)

    def register(self, template: NotificationTemplate) -> None:
        self.templates[template.name] = template

    def get(self, name: str) -> NotificationTemplate | None:
        return self.templates.get(name)

    def render_subject(self, name: str, vars: dict[str, Any]) -> str:
        template = self.templates.get(name)
        if template is None:
            return ""
        return template.subject_template.format(**vars)

    def render_body(self, name: str, vars: dict[str, Any]) -> str:
        template = self.templates.get(name)
        if template is None:
            return ""
        return template.body_template.format(**vars)
