from __future__ import annotations
import httpx

def fetch_chainlist(chains_url: str) -> list[dict]:
    with httpx.Client(timeout=20) as c:
        r = c.get(chains_url)
        r.raise_for_status()
        return r.json()
