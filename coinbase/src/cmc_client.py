from __future__ import annotations
import os, time, math, json, pathlib, typing as t
import httpx
from dotenv import load_dotenv

load_dotenv(override=False)

CMC_API_KEY = os.getenv("CMC_API_KEY", "")
CACHE_DIR = pathlib.Path(os.getenv("CMC_CACHE_DIR", "data/cmc/json"))
CACHE_DIR.mkdir(parents=True, exist_ok=True)

BASE = "https://pro-api.coinmarketcap.com"

class CMCClient:
    def __init__(self, api_key: str | None = None, timeout: int = 20, rate_per_min: int = 25):
        self.api_key = api_key or CMC_API_KEY
        self.client = httpx.Client(timeout=timeout, headers={"X-CMC_PRO_API_KEY": self.api_key})
        self.rate_interval = max(1, int(60 / max(1, rate_per_min)))

    def _get(self, path: str, params: dict) -> dict:
        r = self.client.get(BASE + path, params=params)
        if r.status_code == 429:
            time.sleep(self.rate_interval + 1)
            r = self.client.get(BASE + path, params=params)
        r.raise_for_status()
        return r.json()

    # Listings for top N (supports start & limit, documented max limit=5000)
    def listings(self, start: int = 1, limit: int = 5000, convert: str = "USD") -> dict:
        params = {"start": start, "limit": min(limit, 5000), "convert": convert, "sort": "market_cap"}
        return self._get("/v1/cryptocurrency/listings/latest", params)

    # Info/metadata for ids (max 500 ids per call per docs)
    def info(self, ids: list[int]) -> dict:
        chunks = [ids[i:i+400] for i in range(0, len(ids), 400)]  # be conservative
        out = {"data": {}}
        for ch in chunks:
            params = {"id": ",".join(map(str, ch))}
            js = self._get("/v2/cryptocurrency/info", params)
            out["data"].update(js.get("data", {}))
            time.sleep(self.rate_interval)
        return out

    def cache_json(self, name: str, payload: dict) -> pathlib.Path:
        p = CACHE_DIR / f"{name}.json"
        p.write_text(json.dumps(payload, indent=2))
        return p
