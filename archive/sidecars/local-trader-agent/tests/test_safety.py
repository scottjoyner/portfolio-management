from __future__ import annotations

import pytest

from local_trader_agent.safety import SafetyError, parse_safe_shell


def test_parse_safe_shell_allows_exact_command():
    assert parse_safe_shell("python --version") == ["python", "--version"]


def test_parse_safe_shell_blocks_shell_chaining():
    with pytest.raises(SafetyError):
        parse_safe_shell("python --version && rm -rf /")


def test_parse_safe_shell_allows_pip_show_package():
    assert parse_safe_shell("python -m pip show pandas") == ["python", "-m", "pip", "show", "pandas"]
