from __future__ import annotations
import os, time, pathlib, json, math
from dotenv import load_dotenv
from .cmc_client import CMCClient

load_dotenv(override=False)

def fetch_top5000(cache_prefix: str = "listings_top5000"):
    cmc = CMCClient()
    js = cmc.listings(start=1, limit=5000)
    path = cmc.cache_json(cache_prefix, js)
    print(f"Saved listings to {path}")

    # Extract IDs and fetch detailed info in batches
    ids = [int(x["id"]) for x in js.get("data", [])]
    info = cmc.info(ids)
    ipath = cmc.cache_json(cache_prefix + "_info", info)
    print(f"Saved info to {ipath}")

if __name__ == "__main__":
    fetch_top5000()
