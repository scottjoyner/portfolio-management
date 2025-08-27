from __future__ import annotations
import os, json, time, math, pathlib
from dotenv import load_dotenv
from .coingecko_client import coins_markets, coin_meta, cache_json
from .tokenlists import fetch_tokenlists
from .chain_registry import fetch_chainlist
from .coinpaprika_client import list_coins as paprika_list

load_dotenv(override=False)

DATA_DIR = pathlib.Path(os.getenv("ALT_CACHE_DIR", "data/alt/json"))
DATA_DIR.mkdir(parents=True, exist_ok=True)

def fetch_top_by_marketcap(limit: int = 5000, vs="usd"):
    per_page = 250
    pages = math.ceil(limit / per_page)
    all_rows = []
    for page in range(1, pages+1):
        js = coins_markets(vs=vs, page=page, per_page=per_page)
        rows = js.get("data", [])
        if not rows:
            break
        all_rows.extend(rows)
        time.sleep(1.0)
    cache_json(f"coingecko_markets_top_{limit}", {"data": all_rows})
    return all_rows

def fetch_assets_meta(ids: list[str], throttle: float = 1.2):
    out = {}
    for id_ in ids:
        try:
            js = coin_meta(id_)
            out[id_] = {
                "id": js.get("id"),
                "symbol": js.get("symbol"),
                "name": js.get("name"),
                "image": (js.get("image") or {}).get("large"),
                "links": js.get("links") or {},
                "categories": js.get("categories") or [],
                "asset_platform_id": js.get("asset_platform_id"),
                "platforms": js.get("platforms") or {},  # { network_slug: contract_address }
                "contract_address": js.get("contract_address"),
                "hashing_algorithm": js.get("hashing_algorithm"),
            }
        except Exception as e:
            out[id_] = {"error": str(e)}
        time.sleep(throttle)
    cache_json("coingecko_assets_meta", {"data": out})
    return out

def fetch_tokenlists_and_chains():
    urls = os.getenv("TOKENLIST_URLS", "").split(",")
    lists = fetch_tokenlists([u for u in urls if u.strip()])
    cache_json("tokenlists", {"data": lists})
    chains = fetch_chainlist(os.getenv("CHAINLIST_URL", "https://chainid.network/chains.json"))
    cache_json("evm_chains", {"data": chains})
    return lists, chains

def fetch_paprika_basics():
    rows = paprika_list()
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    path = DATA_DIR / "coinpaprika_coins.json"
    path.write_text(json.dumps({"data": rows}, indent=2))
    return rows
