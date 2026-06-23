from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass
from typing import Any, Literal

import requests
from pydantic import BaseModel, Field, ValidationError

from .tools import ToolRuntime, tool_spec_json


@dataclass
class LLMConfig:
    base_url: str = os.getenv("LTA_LLM_BASE_URL", "http://127.0.0.1:8080/v1")
    model: str = os.getenv("LTA_LLM_MODEL", "local-model")
    temperature: float = 0.1
    max_tokens: int = 2048
    timeout: int = 120


class AgentAction(BaseModel):
    action: Literal["run_shell", "run_backtest", "write_file", "read_file", "finish"]
    args: dict[str, Any] = Field(default_factory=dict)


class LocalLLMClient:
    def __init__(self, cfg: LLMConfig) -> None:
        self.cfg = cfg

    def chat(self, messages: list[dict[str, str]]) -> str:
        url = self.cfg.base_url.rstrip("/") + "/chat/completions"
        payload = {
            "model": self.cfg.model,
            "messages": messages,
            "temperature": self.cfg.temperature,
            "max_tokens": self.cfg.max_tokens,
            "stream": False,
            "response_format": {"type": "json_object"},
        }
        response = requests.post(url, json=payload, timeout=self.cfg.timeout)
        response.raise_for_status()
        data = response.json()
        return data["choices"][0]["message"]["content"]


def _extract_json(text: str) -> dict[str, Any]:
    text = text.strip()
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        match = re.search(r"\{.*\}", text, flags=re.DOTALL)
        if not match:
            raise
        return json.loads(match.group(0))


def _parse_action(raw: str) -> AgentAction:
    return AgentAction.model_validate(_extract_json(raw))


SYSTEM_PROMPT = """You are Local Trader Agent, a local-only research assistant for trading backtests.

You can use tools, but you must obey these constraints:
- Only backtest and report. Do not place live trades or call brokerage APIs.
- Prefer deterministic tool calls over inventing results.
- Use yfinance only for market data research.
- Produce one JSON object per turn, no markdown outside JSON.
- JSON schema: {"action": "run_shell|run_backtest|write_file|read_file|finish", "args": {...}}
- When the requested report is generated, call finish with the report path, manifest path, and summary.

Available tools:
TOOLS_JSON
"""


class ResearchAgent:
    def __init__(self, llm: LocalLLMClient, runtime: ToolRuntime, max_steps: int = 12) -> None:
        self.llm = llm
        self.runtime = runtime
        self.max_steps = max_steps

    def _persist_transcript(self, transcript: list[dict[str, Any]]) -> str:
        path = self.runtime.run_dir / "transcript.json"
        path.write_text(json.dumps(transcript, indent=2, default=str), encoding="utf-8")
        return str(path)

    def run(self, task: str) -> dict[str, Any]:
        (self.runtime.run_dir / "task.txt").write_text(task, encoding="utf-8")
        messages = [
            {"role": "system", "content": SYSTEM_PROMPT.replace("TOOLS_JSON", tool_spec_json())},
            {"role": "user", "content": task},
        ]
        transcript: list[dict[str, Any]] = []

        for step in range(1, self.max_steps + 1):
            raw = self.llm.chat(messages)
            try:
                parsed = _parse_action(raw)
            except (ValidationError, json.JSONDecodeError, ValueError) as exc:
                observation = {"error": "InvalidAgentAction", "message": str(exc), "raw": raw[-2000:]}
                messages.append({"role": "assistant", "content": raw})
                messages.append({"role": "user", "content": "Observation: " + json.dumps(observation)})
                transcript.append({"step": step, "raw": raw, "observation": observation})
                continue

            action = parsed.action
            args = parsed.args
            transcript.append({"step": step, "action": action, "args": args})

            if action == "finish":
                transcript_path = self._persist_transcript(transcript)
                return {
                    "status": "finished",
                    "message": args.get("message", ""),
                    "run_dir": str(self.runtime.run_dir),
                    "transcript_path": transcript_path,
                    "transcript": transcript,
                }

            observation = self.runtime.dispatch(action, args)
            transcript[-1]["observation"] = observation
            messages.append({"role": "assistant", "content": parsed.model_dump_json()})
            messages.append({"role": "user", "content": "Observation: " + json.dumps(observation, default=str)})

        transcript_path = self._persist_transcript(transcript)
        return {
            "status": "max_steps_reached",
            "message": "Agent reached max_steps before finish.",
            "run_dir": str(self.runtime.run_dir),
            "transcript_path": transcript_path,
            "transcript": transcript,
        }
