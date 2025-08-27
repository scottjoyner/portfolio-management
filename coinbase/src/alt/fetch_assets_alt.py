# src/alt/fetch_assets_alt.py
from __future__ import annotations
import os, json, time, math, pathlib, sys
from dotenv import load_dotenv
from .coingecko_client import coins_markets, coin_meta, cache_json
from .tokenlists import fetch_tokenlists
from .chain_registry import fetch_chainlist
from .coinpaprika_client import list_coins as paprika_list

load_dotenv(override=False)

DATA_DIR = pathlib.Path(os.getenv("ALT_CACHE_DIR", "data/alt/json"))
DATA_DIR.mkdir(parents=True, exist_ok=True)

PAGE_PAUSE_S = float(os.getenv("CG_PAGE_PAUSE_S", "2.5"))  # polite spacing between pages (public API)
META_THROTTLE_S = float(os.getenv("CG_META_THROTTLE_S", "2.5"))  # per-asset sleep (public API)

def _dedupe_by_id(rows):
    seen, out = set(), []
    for r in rows:
        cid = r.get("id")
        if cid in seen: continue
        seen.add(cid); out.append(r)
    return out

def fetch_top_by_marketcap(limit: int = 5000, vs="usd", resume: bool = True, out_name: str | None = None):
    """
    Fetch up to `limit` assets ordered by market cap. Writes a checkpoint file every page so you can resume.
    """
    limit = int(limit)
    per_page = 250
    pages = math.ceil(limit / per_page)
    out_name = out_name or f"coingecko_markets_top_{limit}"
    out_path = DATA_DIR / f"{out_name}.json"

    # Resume support
    all_rows = []
    if resume and out_path.exists():
        try:
            all_rows = (json.loads(out_path.read_text()) or {}).get("data", [])
        except Exception:
            all_rows = []
        all_rows = _dedupe_by_id(all_rows)
        start_page = max(1, len(all_rows)//per_page + 1)
    else:
        start_page = 1

    for page in range(start_page, pages + 1):
        try:
            js = coins_markets(vs=vs, page=page, per_page=per_page)
        except Exception as e:
            print(f"[fetch_top_by_marketcap] page={page} failed: {e}", file=sys.stderr)
            break
        rows = js.get("data", [])
        if not rows:
            break
        all_rows.extend(rows)
        all_rows = _dedupe_by_id(all_rows)
        if len(all_rows) >= limit:
            all_rows = all_rows[:limit]
        cache_json(out_name, {"data": all_rows})  # checkpoint after each page
        if len(all_rows) >= limit:
            break
        time.sleep(PAGE_PAUSE_S)  # be nice to public API

    return all_rows

def fetch_assets_meta(ids: list[str], throttle: float | None = None, resume: bool = True, out_name: str = "coingecko_assets_meta"):
    """
    Fetch per-asset metadata. Respects rate limits and writes incrementally every N items.
    """
    throttle = META_THROTTLE_S if throttle is None else float(throttle)
    out_path = DATA_DIR / f"{out_name}.json"

    # Resume: load existing and skip already-done IDs
    done: dict = {}
    if resume and out_path.exists():
        try:
            done = (json.loads(out_path.read_text()) or {}).get("data", {})
        except Exception:
            done = {}

    out = dict(done)  # continue building
    total = len(ids)
    for i, id_ in enumerate(ids, 1):
        if id_ in out and "error" not in out[id_]:
            # already OK
            if i % 50 == 0: print(f"[fetch_assets_meta] {i}/{total} (skipping cached)")
            continue
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
                "platforms": js.get("platforms") or {},
                "contract_address": js.get("contract_address"),
                "hashing_algorithm": js.get("hashing_algorithm"),
            }
        except Exception as e:
            out[id_] = {"error": str(e)}
        # Incremental write every 20 items to survive interruptions
        if i % 20 == 0 or i == total:
            cache_json(out_name, {"data": out})
            print(f"[fetch_assets_meta] checkpoint {i}/{total} saved")
        time.sleep(throttle)

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
    path = DATA_DIR / "coinpaprika_coins.json"
    path.write_text(json.dumps({"data": rows}, indent=2))
    return rows
