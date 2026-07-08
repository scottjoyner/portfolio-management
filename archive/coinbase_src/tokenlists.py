from __future__ import annotations
import httpx
from typing import List, Dict

def fetch_tokenlists(urls: List[str]) -> Dict[str, dict]:
    out = {}
    headers = {"accept": "application/json"}
    with httpx.Client(timeout=20, headers=headers) as c:
        for u in urls:
            if not u.strip(): continue
            r = c.get(u.strip())
            r.raise_for_status()
            out[u] = r.json()
    return out
