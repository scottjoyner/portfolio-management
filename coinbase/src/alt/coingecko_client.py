from __future__ import annotations
import os, time, math, json, pathlib, typing as t
import httpx
from dotenv import load_dotenv

load_dotenv(override=False)

CG_API_KEY = os.getenv("COINGECKO_API_KEY", "")
BASE = "https://pro-api.coingecko.com/api/v3"
CACHE_DIR = pathlib.Path(os.getenv("CG_CACHE_DIR", "data/alt/json"))
CACHE_DIR.mkdir(parents=True, exist_ok=True)

def _client():
    headers = {"accept": "application/json"}
    if CG_API_KEY:
        headers["x-cg-pro-api-key"] = CG_API_KEY
    return httpx.Client(timeout=30, headers=headers)

def cache_json(name: str, payload: dict) -> pathlib.Path:
    p = CACHE_DIR / f"{name}.json"
    p.write_text(json.dumps(payload, indent=2))
    return p

def coins_markets(vs="usd", page=1, per_page=250) -> dict:
    params = {"vs_currency": vs, "order": "market_cap_desc", "page": page, "per_page": per_page, "sparkline": "false"}
    with _client() as c:
        r = c.get(f"{BASE}/coins/markets", params=params)
        r.raise_for_status()
        return {"data": r.json()}

def coin_meta(id_: str) -> dict:
    params = {"localization": "false", "tickers": "false", "market_data": "false", "community_data": "false", "developer_data": "false", "sparkline": "false"}
    with _client() as c:
        r = c.get(f"{BASE}/coins/{id_}", params=params)
        r.raise_for_status()
        return r.json()
