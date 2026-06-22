"""Polymarket relayer client.

Supports relayer API key authentication for read-only relayer endpoints.
This does not sign or submit wallet batches by itself; it only loads the
current credential set and can query relayer-managed metadata.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Optional
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


DEFAULT_RELAYER_URL = "https://relayer-v2.polymarket.com"


@dataclass
class PolymarketRelayerCredentials:
    api_key: str = ""
    api_key_address: str = ""

    @classmethod
    def from_file(cls, path: str) -> "PolymarketRelayerCredentials":
        data = Path(path).read_text(encoding="utf-8", errors="ignore").splitlines()
        parsed: Dict[str, str] = {}
        for line in data:
            if ":" not in line:
                continue
            key, value = line.split(":", 1)
            parsed[key.strip().upper().replace(" ", "_")] = value.strip()
        return cls(
            api_key=parsed.get("RELAYER_API_KEY", ""),
            api_key_address=parsed.get("RELAYER_API_KEY_ADDRESS", parsed.get("SIGNER_ADRESS", parsed.get("SIGNER_ADDRESS", ""))),
        )


@dataclass
class PolymarketBuilderCredentials:
    api_key: str = ""
    secret: str = ""
    passphrase: str = ""

    @classmethod
    def from_file(cls, path: str) -> "PolymarketBuilderCredentials":
        data = Path(path).read_text(encoding="utf-8", errors="ignore").splitlines()
        parsed: Dict[str, str] = {}
        for line in data:
            if ":" not in line:
                continue
            key, value = line.split(":", 1)
            parsed[key.strip().upper().replace(" ", "_")] = value.strip()
        return cls(
            api_key=parsed.get("APIKEY", parsed.get("API_KEY", "")),
            secret=parsed.get("SECRET", ""),
            passphrase=parsed.get("PASSPHRASE", ""),
        )


class PolymarketRelayerClient:
    def __init__(
        self,
        api_key: str = "",
        api_key_address: str = "",
        relayer_url: str = "",
        credentials_path: str = "",
        timeout: int = 15,
    ):
        if credentials_path and (not api_key or not api_key_address):
            creds = PolymarketRelayerCredentials.from_file(credentials_path)
            api_key = api_key or creds.api_key
            api_key_address = api_key_address or creds.api_key_address
        self.api_key = api_key or os.environ.get("RELAYER_API_KEY", "")
        self.api_key_address = api_key_address or os.environ.get("RELAYER_API_KEY_ADDRESS", "")
        self.relayer_url = (relayer_url or os.environ.get("POLYMARKET_RELAYER_URL", DEFAULT_RELAYER_URL)).rstrip("/")
        self.timeout = timeout

    def _headers(self) -> Dict[str, str]:
        if not self.api_key or not self.api_key_address:
            raise RuntimeError("Polymarket relayer credentials not configured")
        return {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "RELAYER_API_KEY": self.api_key,
            "RELAYER_API_KEY_ADDRESS": self.api_key_address,
        }

    def _request_json(self, path: str) -> Dict[str, Any]:
        req = Request(f"{self.relayer_url}{path}", headers=self._headers())
        try:
            with urlopen(req, timeout=self.timeout) as resp:
                import json

                return json.loads(resp.read().decode())
        except HTTPError as exc:
            raise RuntimeError(f"HTTP {exc.code}: {exc.reason}") from exc
        except URLError as exc:
            raise RuntimeError(f"connection error: {exc.reason}") from exc

    def list_api_keys(self) -> list[dict[str, Any]]:
        data = self._request_json("/relayer/api/keys")
        if isinstance(data, list):
            return data
        if isinstance(data, dict):
            return data.get("data", []) if isinstance(data.get("data", []), list) else []
        return []

    def ping(self) -> dict[str, Any]:
        return {"ok": True, "relayer_url": self.relayer_url, "address": self.api_key_address}

    def builder_auth_headers(self, builder_creds: PolymarketBuilderCredentials) -> Dict[str, str]:
        raise NotImplementedError(
            "Builder request signing is not implemented here; use the official Polymarket SDK or add the exact HMAC signing scheme before calling relayer builder endpoints."
        )


def load_relayer_credentials(path: str) -> PolymarketRelayerCredentials:
    return PolymarketRelayerCredentials.from_file(path)


def load_builder_credentials(path: str) -> PolymarketBuilderCredentials:
    return PolymarketBuilderCredentials.from_file(path)
