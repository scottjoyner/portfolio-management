"""Coinbase CDP transfers wrapper.

Supports preview, create, execute, and listing of transfers using the CDP CLI.
External sends stay behind the global `COINBASE_ENABLE_TRANSFERS` gate.
"""

from __future__ import annotations

import json
import os
import subprocess
from dataclasses import dataclass
from typing import Any, Dict, Optional


class CDPTransferError(Exception):
    pass


def _transfers_enabled() -> bool:
    return os.getenv("COINBASE_ENABLE_TRANSFERS", "false").lower() == "true"


def _cli() -> str:
    return os.getenv("COINBASE_CLI_PATH", "cdp")


def _run(*args: str, timeout: int = 60) -> Dict[str, Any]:
    out = subprocess.run([_cli(), *args], capture_output=True, text=True, timeout=timeout)
    if out.returncode != 0:
        raise CDPTransferError((out.stderr or out.stdout or "cdp transfer command failed").strip())
    return json.loads(out.stdout) if out.stdout else {}


def _transfer_auth_hint(message: str) -> Optional[str]:
    lowered = message.lower()
    if "must use a cdp entity scoped api key" in lowered:
        return (
            "CDP rejected the transfer with an entity-scope error. "
            "Use an entity-scoped API key from the Portal, and confirm a wallet secret is configured via `cdp env`."
        )
    if "wallet authentication error" in lowered or "x-wallet-auth" in lowered:
        return (
            "CDP rejected the transfer because wallet auth is missing or invalid. "
            "Add a wallet secret with `cdp env live --wallet-secret-file ...` and retry."
        )
    return None


def list_accounts() -> Dict[str, Any]:
    return _run("accounts", "list", "--jq", ".")


def list_balances(account_id: str) -> Dict[str, Any]:
    return _run("accounts", "balances", account_id, "--jq", ".")


def _field_arg(key: str, value: Any) -> str:
    if isinstance(value, bool):
        return f"{key}:={'true' if value else 'false'}"
    if isinstance(value, (dict, list)):
        return f"{key}:={json.dumps(value, separators=(',', ':'))}"
    return f"{key}={value}"


def create_transfer(request: Dict[str, Any] | str, *, dry_run: bool = True) -> Dict[str, Any]:
    if not _transfers_enabled():
        return {"ok": False, "enabled": False, "error": "coinbase transfers disabled; set COINBASE_ENABLE_TRANSFERS=true to enable"}
    if isinstance(request, str):
        request = json.loads(request)
    args = ["transfers", "create"]
    if dry_run:
        args.append("--dry-run")
    for key, value in request.items():
        args.append(_field_arg(key, value))
    return _run(*args, timeout=120)


def execute_transfer(transfer_id: str) -> Dict[str, Any]:
    if not _transfers_enabled():
        return {"ok": False, "enabled": False, "error": "coinbase transfers disabled; set COINBASE_ENABLE_TRANSFERS=true to enable"}
    return _run("transfers", "execute", transfer_id, timeout=120)


def get_transfer(transfer_id: str) -> Dict[str, Any]:
    return _run("transfers", "get", transfer_id, timeout=60)


def list_transfers(*filters: str) -> Dict[str, Any]:
    args = ["transfers", "list", *filters, "--jq", "."]
    return _run(*args, timeout=120)


def build_crypto_transfer(
    *,
    source_account_id: str,
    source_asset: str,
    target_address: str,
    target_network: str,
    target_asset: str,
    amount: str,
    amount_type: str = "source",
    execute: bool = False,
    metadata: Optional[Dict[str, str]] = None,
    validate_only: bool = False,
) -> Dict[str, Any]:
    payload: Dict[str, Any] = {
        "amount": amount,
        "amountType": amount_type,
        "asset": source_asset,
        "execute": execute,
        "source": {"accountId": source_account_id, "asset": source_asset},
        "target": {"address": target_address, "network": target_network, "asset": target_asset},
    }
    if metadata:
        payload["metadata"] = metadata
    if validate_only:
        payload["validateOnly"] = True
    return payload


def preview_crypto_transfer(payload: Dict[str, Any]) -> Dict[str, Any]:
    return create_transfer(payload, dry_run=True)


def validate_crypto_transfer(payload: Dict[str, Any]) -> Dict[str, Any]:
    payload = dict(payload)
    payload["validateOnly"] = True
    payload["execute"] = False
    try:
        return create_transfer(payload, dry_run=False)
    except CDPTransferError as exc:
        message = str(exc)
        return {
            "ok": False,
            "enabled": True,
            "validated": False,
            "error": message,
            "hint": _transfer_auth_hint(message),
            "payload": payload,
        }


def submit_crypto_transfer(payload: Dict[str, Any]) -> Dict[str, Any]:
    payload = dict(payload)
    payload["execute"] = True
    try:
        return create_transfer(payload, dry_run=False)
    except CDPTransferError as exc:
        message = str(exc)
        return {
            "ok": False,
            "enabled": True,
            "submitted": False,
            "error": message,
            "hint": _transfer_auth_hint(message),
            "payload": payload,
        }
