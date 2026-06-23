from __future__ import annotations

import shlex
from pathlib import Path


class SafetyError(RuntimeError):
    pass


BLOCKED_SHELL_TOKENS = {
    "rm", "rmdir", "del", "erase", "format", "mkfs", "dd", "shutdown", "reboot",
    "sudo", "su", "chmod", "chown", "curl", "wget", "ssh", "scp", "ftp", "nc", "netcat",
    "docker", "kubectl", "aws", "gcloud", "az", "coinbase", "alpaca", "ib_insync",
}

EXACT_COMMANDS = {
    ("python", "--version"),
    ("python", "-V"),
    ("python", "-m", "pip", "list"),
    ("pip", "list"),
    ("where", "python"),
    ("which", "python"),
    ("pwd",),
    ("ls",),
    ("dir",),
}

PREFIX_COMMANDS = {
    ("python", "-m", "pip", "show"),
    ("pip", "show"),
    ("ls",),
    ("dir",),
}


def parse_safe_shell(command: str) -> list[str]:
    """Parse and validate an environment-audit command without using a shell."""
    if not command.strip():
        raise SafetyError("Empty shell command")
    try:
        parts = shlex.split(command, posix=True)
    except ValueError as exc:
        raise SafetyError(f"Could not parse command: {exc}") from exc
    if not parts:
        raise SafetyError("Empty shell command")

    lowered = tuple(part.lower() for part in parts)
    dangerous = sorted(set(lowered) & BLOCKED_SHELL_TOKENS)
    if dangerous:
        raise SafetyError(f"Command contains blocked token(s): {dangerous}")

    if lowered in EXACT_COMMANDS:
        return parts

    for prefix in PREFIX_COMMANDS:
        if lowered[: len(prefix)] == prefix:
            if prefix in {("ls",), ("dir",)} and len(parts) > 2:
                raise SafetyError("ls/dir allow at most one path argument")
            if prefix in {("python", "-m", "pip", "show"), ("pip", "show")} and len(parts) < len(prefix) + 1:
                raise SafetyError("pip show requires a package name")
            return parts

    raise SafetyError(
        "Command blocked by allowlist. Allowed examples: python --version, python -m pip list, "
        "python -m pip show PACKAGE, where python, which python, pwd, ls, dir."
    )


def resolve_workspace_path(workspace: Path, requested: str | Path) -> Path:
    workspace = workspace.resolve()
    path = Path(requested)
    if not path.is_absolute():
        path = workspace / path
    path = path.resolve()
    if workspace != path and workspace not in path.parents:
        raise SafetyError(f"Path escapes workspace: {requested}")
    return path
