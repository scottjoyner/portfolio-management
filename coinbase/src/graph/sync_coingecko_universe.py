from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
from typing import Any, Iterable

from .coingecko_ingest import assets_from_market_rows, assets_and_tokens_from_coin_meta, load_json_payload
from .neo4j_graph import CryptoGraphStore

try:
    from ..alt.coingecko_client import coins_markets, cache_json
except Exception:  # pragma: no cover - optional dependency path
    coins_markets = None
    cache_json = None


REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_MARKETS_PATH = REPO_ROOT / "coinbase" / "data" / "alt" / "json" / "coingecko_markets_top_5000.json"
DEFAULT_META_PATH = REPO_ROOT / "coinbase" / "data" / "alt" / "json" / "coingecko_assets_meta.json"


def _load_coinbase_symbols(extra: Iterable[str] | None = None) -> set[str]:
    symbols = {s.strip().upper() for s in (extra or []) if s and str(s).strip()}
    env = os.getenv("COINBASE_SYMBOLS", "")
    symbols.update({s.strip().upper() for s in env.split(",") if s.strip()})

    try:
        from ..cb_client import CBClient

        client = CBClient()
        for row in client.get_products():
            pid = str(row.get("product_id") or row.get("id") or "")
            if "-" in pid:
                symbols.add(pid.split("-", 1)[0].upper())
    except Exception:
        pass

    return symbols


def _fetch_live_markets(*, pages: int = 20, per_page: int = 250, save_to: Path | None = None) -> dict[str, Any]:
    if coins_markets is None:
        raise RuntimeError("CoinGecko client is unavailable")

    rows: list[dict[str, Any]] = []
    for page in range(1, max(1, pages) + 1):
        payload = coins_markets(page=page, per_page=per_page)
        page_rows = payload.get("data", []) if isinstance(payload, dict) else []
        if not page_rows:
            break
        rows.extend(page_rows)
        if len(page_rows) < per_page:
            break

    payload = {"data": rows}
    if save_to is not None and cache_json is not None:
        save_to.parent.mkdir(parents=True, exist_ok=True)
        save_to.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return payload


def sync_coingecko_universe(
    *,
    store: CryptoGraphStore | None = None,
    markets_path: str | Path | None = None,
    meta_path: str | Path | None = None,
    fetch_live: bool = False,
    pages: int = 20,
    per_page: int = 250,
    coinbase_symbols: Iterable[str] | None = None,
) -> dict[str, int]:
    store = store or CryptoGraphStore()
    store.apply_schema()

    markets_file = Path(markets_path) if markets_path else DEFAULT_MARKETS_PATH
    meta_file = Path(meta_path) if meta_path else DEFAULT_META_PATH
    symbols = _load_coinbase_symbols(coinbase_symbols)

    if fetch_live or not markets_file.exists():
        payload = _fetch_live_markets(pages=pages, per_page=per_page, save_to=markets_file)
    else:
        payload = load_json_payload(markets_file)

    rows = payload.get("data", payload) if isinstance(payload, dict) else payload
    assets = assets_from_market_rows(rows or [], coinbase_symbols=symbols)
    asset_count = store.upsert_assets(assets)

    meta_count = 0
    token_count = 0
    if meta_file.exists():
        meta_payload = load_json_payload(meta_file)
        meta = meta_payload.get("data", meta_payload) if isinstance(meta_payload, dict) else {}
        meta_assets, tokens = assets_and_tokens_from_coin_meta(meta or {}, coinbase_symbols=symbols)
        meta_count = store.upsert_assets(meta_assets)
        token_count = store.upsert_tokens(tokens)

    return {
        "assets": asset_count,
        "meta_assets": meta_count,
        "tokens": token_count,
        "source_assets": len(assets),
        "coinbase_symbols": len(symbols),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Sync CoinGecko universe into Neo4j")
    parser.add_argument("--markets-path", default=str(DEFAULT_MARKETS_PATH))
    parser.add_argument("--meta-path", default=str(DEFAULT_META_PATH))
    parser.add_argument("--fetch-live", action="store_true")
    parser.add_argument("--pages", type=int, default=20)
    parser.add_argument("--per-page", type=int, default=250)
    args = parser.parse_args()

    summary = sync_coingecko_universe(
        markets_path=args.markets_path,
        meta_path=args.meta_path,
        fetch_live=args.fetch_live,
        pages=args.pages,
        per_page=args.per_page,
    )
    print(json.dumps(summary, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
