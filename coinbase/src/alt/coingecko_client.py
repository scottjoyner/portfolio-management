# src/alt/coingecko_client.py
from __future__ import annotations
import os, json, pathlib, time, random, typing as t
import httpx
from dotenv import load_dotenv

load_dotenv(override=False)

CG_API_KEY = os.getenv("COINGECKO_API_KEY", "").strip()
BASE = os.getenv("COINGECKO_BASE", "https://pro-api.coingecko.com/api/v3").strip()
PUBLIC_BASE = "https://api.coingecko.com/api/v3"

# Tunables (env overrides welcome)
CG_TIMEOUT_S   = float(os.getenv("CG_TIMEOUT_S",   "30"))
CG_MAX_RETRIES = int(os.getenv("CG_MAX_RETRIES",   "8"))
CG_BACKOFF_BASE= float(os.getenv("CG_BACKOFF_BASE","2.0"))  # seconds
CG_BACKOFF_CAP = float(os.getenv("CG_BACKOFF_CAP", "60.0"))

CACHE_DIR = pathlib.Path(os.getenv("CG_CACHE_DIR", "data/alt/json"))
CACHE_DIR.mkdir(parents=True, exist_ok=True)

def _client(base: str):
    headers = {
        "accept": "application/json",
        "user-agent": "coinbase-quant-bot/1.0 (+local)"
    }
    if "pro-api.coingecko.com" in base and CG_API_KEY:
        headers["x-cg-pro-api-key"] = CG_API_KEY
    return httpx.Client(base_url=base, timeout=CG_TIMEOUT_S, headers=headers)

def _sleep_with_jitter(base_s: float) -> None:
    jitter = random.uniform(0.25, 0.75) * base_s
    time.sleep(base_s + jitter)

def _get_json(path: str, params: dict, prefer_pro: bool = True) -> dict:
    """
    Hit PRO first (if allowed), then PUBLIC on error. On 429/5xx apply
    exponential backoff + jitter and honor Retry-After.
    """
    bases: list[str] = []
    if prefer_pro:
        bases.append(BASE)
        if BASE != PUBLIC_BASE:  # make sure PUBLIC is distinct fallback
            bases.append(PUBLIC_BASE)
    else:
        bases = [PUBLIC_BASE]

    last_err: Exception | None = None
    for base in bases:
        backoff = CG_BACKOFF_BASE
        for attempt in range(CG_MAX_RETRIES):
            try:
                with _client(base) as c:
                    r = c.get(path, params=params)
                # Fast success
                if r.status_code < 400:
                    return r.json()

                # Honor Retry-After if present on rate-limit
                if r.status_code == 429:
                    ra = r.headers.get("Retry-After")
                    if ra:
                        try:
                            wait_s = min(CG_BACKOFF_CAP, float(ra))
                            time.sleep(wait_s)
                        except Exception:
                            _sleep_with_jitter(backoff)
                    else:
                        _sleep_with_jitter(min(CG_BACKOFF_CAP, backoff))
                        backoff = min(CG_BACKOFF_CAP, backoff * 2.0)
                    continue

                # 5xx → backoff and retry
                if 500 <= r.status_code < 600:
                    _sleep_with_jitter(min(CG_BACKOFF_CAP, backoff))
                    backoff = min(CG_BACKOFF_CAP, backoff * 2.0)
                    continue

                # Other client errors → give up on this base and try the next
                r.raise_for_status()

            except Exception as e:
                last_err = e
                # Backoff between attempts on network errors too
                _sleep_with_jitter(min(CG_BACKOFF_CAP, backoff))
                backoff = min(CG_BACKOFF_CAP, backoff * 2.0)
                continue

    raise RuntimeError(
        f"CoinGecko request failed for {path} params={params}. Last error: {last_err}"
    )

def cache_json(name: str, payload: dict) -> pathlib.Path:
    p = CACHE_DIR / f"{name}.json"
    p.write_text(json.dumps(payload, indent=2))
    return p

def coins_markets(vs="usd", page=1, per_page=250) -> dict:
    vs = (vs or "usd").lower()
    per_page = min(int(per_page), 250)
    params = {"vs_currency": vs, "order": "market_cap_desc", "page": int(page), "per_page": per_page, "sparkline": "false"}
    js = _get_json("/coins/markets", params, prefer_pro=True)
    return {"data": js}

def coin_meta(id_: str) -> dict:
    params = {
        "localization": "false",
        "tickers": "false",
        "market_data": "false",
        "community_data": "false",
        "developer_data": "false",
        "sparkline": "false",
    }
    return _get_json(f"/coins/{id_}", params, prefer_pro=True)
