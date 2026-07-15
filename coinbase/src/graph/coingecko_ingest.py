from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Iterable

from .models import GraphAsset, TokenContract
from .neo4j_graph import CryptoGraphStore


def assets_from_market_rows(rows: Iterable[dict[str, Any]], *, coinbase_symbols: set[str] | None = None) -> list[GraphAsset]:
    if isinstance(rows, dict):
        rows = rows.get("data", rows)
    coinbase_symbols = {s.upper() for s in (coinbase_symbols or set())}
    assets: list[GraphAsset] = []
    for row in rows:
        symbol = str(row.get("symbol") or "").upper()
        if not symbol:
            continue
        assets.append(GraphAsset(
            cg_id=str(row.get("id") or symbol.lower()),
            symbol=symbol,
            name=str(row.get("name") or symbol),
            product_id=f"{symbol}-USD",
            market_cap_rank=row.get("market_cap_rank"),
            market_cap=row.get("market_cap"),
            available_on_coinbase=symbol in coinbase_symbols,
        ))
    return assets


def assets_and_tokens_from_coin_meta(meta: dict[str, Any], *, coinbase_symbols: set[str] | None = None) -> tuple[list[GraphAsset], list[TokenContract]]:
    coinbase_symbols = {s.upper() for s in (coinbase_symbols or set())}
    assets: list[GraphAsset] = []
    tokens: list[TokenContract] = []
    for cg_id, payload in meta.items():
        if not isinstance(payload, dict) or payload.get("error"):
            continue
        symbol = str(payload.get("symbol") or "").upper()
        if not symbol:
            continue
        categories = [str(c) for c in (payload.get("categories") or []) if c]
        platforms = payload.get("platforms") or {}
        networks = [str(n) for n, addr in platforms.items() if addr]
        assets.append(GraphAsset(
            cg_id=str(payload.get("id") or cg_id),
            symbol=symbol,
            name=str(payload.get("name") or symbol),
            product_id=f"{symbol}-USD",
            available_on_coinbase=symbol in coinbase_symbols,
            categories=categories,
            networks=networks,
        ))
        for network, address in platforms.items():
            if not address:
                continue
            tokens.append(TokenContract(
                key=f"{cg_id}:{network}:{str(address).lower()}",
                asset_id=str(payload.get("id") or cg_id),
                network=str(network),
                address=str(address).lower(),
                symbol=symbol,
                name=str(payload.get("name") or symbol),
            ))
    return assets, tokens


def load_json_payload(path: str | Path) -> Any:
    return json.loads(Path(path).read_text(encoding="utf-8"))


def ingest_markets_file(path: str | Path, store: CryptoGraphStore, *, coinbase_symbols: set[str] | None = None) -> int:
    payload = load_json_payload(path)
    rows = payload.get("data", payload) if isinstance(payload, dict) else payload
    assets = assets_from_market_rows(rows or [], coinbase_symbols=coinbase_symbols)
    return store.upsert_assets(assets)


def ingest_meta_file(path: str | Path, store: CryptoGraphStore, *, coinbase_symbols: set[str] | None = None) -> dict[str, int]:
    payload = load_json_payload(path)
    meta = payload.get("data", payload) if isinstance(payload, dict) else {}
    assets, tokens = assets_and_tokens_from_coin_meta(meta or {}, coinbase_symbols=coinbase_symbols)
    return {"assets": store.upsert_assets(assets), "tokens": store.upsert_tokens(tokens)}
