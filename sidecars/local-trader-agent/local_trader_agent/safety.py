from __future__ import annotations

from pathlib import Path


class SafetyError(RuntimeError):
    pass


BLOCKED_SHELL_TOKENS = {
    "rm", "rmdir", "del", "erase", "format", "mkfs", "dd", "shutdown", "reboot",
    "sudo", "su", "chmod", "chown", "curl", "wget", "ssh", "scp", "ftp", "nc", "netcat",
    "docker", "kubectl", "aws", "gcloud", "az", "coinbase", "alpaca", "ib_insync",
}

ALLOWED_SHELL_PREFIXES = (
    "python --version",
    "python -V",
    "python -m pip list",
    "python -m pip show",
    "pip list",
    "pip show",
    "where python",
    "which python",
    "pwd",
    "ls",
    "dir",
)


def ensure_safe_shell(command: str) -> None:
    normalized = command.strip().lower()
    if not normalized:
        raise SafetyError("Empty shell command")
    if not any(normalized.startswith(prefix) for prefix in ALLOWED_SHELL_PREFIXES):
        raise SafetyError(
            "Command blocked by allowlist. Allowed examples: python --version, python -m pip list, pip show PACKAGE, where python, which python, pwd, ls, dir."
        )
    tokens = {part.strip(";&|()[]{}<>`$\\\"'") for part in normalized.split()}
    dangerous = sorted(tokens & BLOCKED_SHELL_TOKENS)
    if dangerous:
        raise SafetyError(f"Command contains blocked token(s): {dangerous}")


def resolve_workspace_path(workspace: Path, requested: str | Path) -> Path:
    workspace = workspace.resolve()
    path = Path(requested)
    if not path.is_absolute():
        path = workspace / path
    path = path.resolve()
    if workspace != path and workspace not in path.parents:
        raise SafetyError(f"Path escapes workspace: {requested}")
    return path
