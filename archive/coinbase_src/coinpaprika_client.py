from __future__ import annotations
import httpx

BASE="https://api.coinpaprika.com/v1"

def list_coins() -> list[dict]:
    with httpx.Client(timeout=20) as c:
        r = c.get(f"{BASE}/coins")
        r.raise_for_status()
        return r.json()
