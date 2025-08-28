#!/usr/bin/env python3
from __future__ import annotations
import os, json, math, datetime as dt
from typing import Dict, Any, List, Tuple
from dotenv import load_dotenv
from neo4j import GraphDatabase
import httpx
from pathlib import Path
from urllib.parse import urlparse
from datetime import datetime, timezone
# -------------------- Config --------------------
load_dotenv(override=False)

NEO4J_URI      = os.getenv("NEO4J_URI", "bolt://localhost:7687")
NEO4J_USER     = os.getenv("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "please_change_me")
NEO4J_DATABASE = os.getenv("NEO4J_DATABASE", "neo4j")

ASSETS_META_PATH = "/home/deathstar/git/portfolio-management/coinbase/data/alt/json/coingecko_assets_meta.json"
MARKETS_TOP_PATH = "/home/deathstar/git/portfolio-management/coinbase/data/alt/json/coingecko_markets_top_5000.json"

COINBASE_PRODUCTS_URL = "https://api.coinbase.com/api/v3/brokerage/market/products"
COINBASE_PAGE_LIMIT   = 250

# portfolio config
PORTFOLIO_TOPN            = 12
PORTFOLIO_MAX_WEIGHT      = 0.25
UNIVERSE_MAX_RANK         = 300
MIN_VOLUME_USD            = 5_000_000
CATEGORY_DIVERSITY_TARGET = 6

# -------------------- Neo4j helpers --------------------
def driver():
    return GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))

SCHEMA = """
CREATE CONSTRAINT asset_cg IF NOT EXISTS FOR (a:Asset) REQUIRE a.cg_id IS UNIQUE;
CREATE CONSTRAINT category_name IF NOT EXISTS FOR (c:Category) REQUIRE c.name IS UNIQUE;
CREATE CONSTRAINT tag_name IF NOT EXISTS FOR (t:Tag) REQUIRE t.name IS UNIQUE;
CREATE CONSTRAINT network_slug IF NOT EXISTS FOR (n:Network) REQUIRE n.slug IS UNIQUE;
CREATE CONSTRAINT token_key IF NOT EXISTS FOR (t:Token) REQUIRE t.key IS UNIQUE;
CREATE CONSTRAINT link_url IF NOT EXISTS FOR (l:Link) REQUIRE l.url IS UNIQUE;

CREATE INDEX asset_symbol IF NOT EXISTS FOR (a:Asset) ON (a.symbol);
CREATE INDEX asset_rank IF NOT EXISTS FOR (a:Asset) ON (a.market_cap_rank);
CREATE INDEX link_domain IF NOT EXISTS FOR (l:Link) ON (l.domain);
"""

def apply_schema(sess):
    for stmt in [s.strip() for s in SCHEMA.strip().split(";") if s.strip()]:
        sess.run(stmt)

# -------------------- Coinbase products (public) --------------------
def fetch_coinbase_products() -> List[Dict[str, Any]]:
    items: List[Dict[str, Any]] = []
    url = f"{COINBASE_PRODUCTS_URL}?limit={COINBASE_PAGE_LIMIT}"
    with httpx.Client(timeout=30) as c:
        while True:
            r = c.get(url)
            r.raise_for_status()
            js = r.json()
            products = js.get("products", [])
            items.extend(products)
            next_cursor = (js.get("cursor") or {}).get("next")
            if not next_cursor:
                break
            url = f"{COINBASE_PRODUCTS_URL}?cursor={next_cursor}&limit={COINBASE_PAGE_LIMIT}"
    return items

def coinbase_tradeable_symbols(products: List[Dict[str, Any]], quote="USD") -> set[str]:
    ok = set()
    for p in products:
        base = str(p.get("base_currency_id", "")).upper()
        quote_ccy = str(p.get("quote_currency_id", "")).upper()
        status = (p.get("status") or p.get("product_status") or "").upper()
        if quote_ccy == quote.upper() and status in ("ONLINE", "ACTIVE", "TRADING", "OPEN"):
            if base:
                ok.add(base)
    return ok

# -------------------- Load JSONs --------------------
def load_assets_meta(path: str) -> Dict[str, Any]:
    js = json.loads(Path(path).read_text())
    return js.get("data", {})

def load_markets_top(path: str) -> List[Dict[str, Any]]:
    js = json.loads(Path(path).read_text())
    return js.get("data", [])

# -------------------- Link normalization --------------------
def _norm_url(u: str | None) -> str | None:
    if not u:
        return None
    u = u.strip()
    if not u:
        return None
    if u.startswith("http://") or u.startswith("https://"):
        return u
    # some providers give bare usernames or protocol-less urls
    if u.startswith("www."):
        return "https://" + u
    # if it's likely a domain, add https
    if "." in u and " " not in u and "/" not in u:
        return "https://" + u
    return None  # ignore unknown formats

def _domain(url: str) -> str:
    try:
        return urlparse(url).netloc.lower()
    except Exception:
        return ""

def extract_links(aid: str, links: Dict[str, Any]) -> List[Tuple[str, str]]:
    """
    Return list of (url, kind) pairs.
    """
    out: List[Tuple[str, str]] = []
    if not isinstance(links, dict):
        return out

    def add_many(kind: str, arr):
        if not arr:
            return
        for u in arr:
            url = _norm_url(u)
            if url:
                out.append((url, kind))

    # lists of urls
    add_many("homepage", links.get("homepage"))
    add_many("blockchain_site", links.get("blockchain_site"))
    add_many("official_forum", links.get("official_forum_url"))
    add_many("chat", links.get("chat_url"))
    add_many("announcement", links.get("announcement_url"))

    # single url-ish fields
    url = _norm_url(links.get("whitepaper"))
    if url: out.append((url, "whitepaper"))
    url = _norm_url(links.get("subreddit_url"))
    if url: out.append((url, "subreddit"))
    url = _norm_url(links.get("snapshot_url"))
    if url: out.append((url, "snapshot"))

    # social handles → full urls
    tw = (links.get("twitter_screen_name") or "").strip().lstrip("@")
    if tw:
        out.append((f"https://twitter.com/{tw}", "twitter"))
    fb = (links.get("facebook_username") or "").strip()
    if fb:
        out.append((f"https://facebook.com/{fb}", "facebook"))
    tg = (links.get("telegram_channel_identifier") or "").strip().lstrip("@")
    if tg:
        out.append((f"https://t.me/{tg}", "telegram"))

    # repos_url maps
    repos = links.get("repos_url") or {}
    add_many("github_repo", repos.get("github"))
    add_many("bitbucket_repo", repos.get("bitbucket"))

    # dedupe
    seen = set()
    deduped = []
    for url, kind in out:
        key = (url, kind)
        if key in seen:
            continue
        seen.add(key)
        deduped.append((url, kind))
    return deduped

# -------------------- Ingestion --------------------
def ingest_assets_meta(sess, meta: Dict[str, Any]):
    """
    Upsert Asset + categories, hashing algo, platforms. Links as separate nodes.
    """
    q_asset = """
    MERGE (a:Asset {cg_id:$id})
    SET a.symbol = coalesce($symbol, a.symbol),
        a.name   = coalesce($name,   a.name),
        a.image  = coalesce($image,  a.image),
        a.categories = coalesce($categories, a.categories),
        a.hashing_algorithm = coalesce($hashing_algorithm, a.hashing_algorithm)
    """
    q_cat = """
    UNWIND coalesce($categories, []) AS cat
    MERGE (c:Category {name:cat})
    WITH c
    MATCH (a:Asset {cg_id:$id})
    MERGE (a)-[:HAS_CATEGORY]->(c)
    """
    q_algo_similarity = """
    MATCH (a:Asset {cg_id:$id}) WHERE a.hashing_algorithm IS NOT NULL
    MATCH (b:Asset) WHERE b.hashing_algorithm = a.hashing_algorithm AND b.cg_id <> a.cg_id
    MERGE (a)-[:SIMILAR_TO {kind:'algo'}]->(b)
    """
    q_platforms = """
    UNWIND [k IN keys($platforms) | {slug:k, addr:$platforms[k]}] AS p
    WITH p WHERE coalesce(p.addr,'') <> ''
    MERGE (n:Network {slug:p.slug})
    WITH n, p
    MERGE (a:Asset {cg_id:$id})
    WITH a, n, p
    MERGE (t:Token {key: toLower($id)+':'+toLower(n.slug)+':'+toLower(p.addr)})
    SET t.address = toLower(p.addr), t.symbol = a.symbol, t.name = a.name
    MERGE (t)-[:IMPLEMENTS]->(a)
    MERGE (t)-[:ON_NETWORK]->(n)
    """
    q_link = """
    MERGE (l:Link {url:$url})
    SET l.domain = $domain
    WITH l
    MATCH (a:Asset {cg_id:$id})
    MERGE (a)-[:HAS_LINK {kind:$kind}]->(l)
    """
    for aid, js in meta.items():
        if "error" in js:
            continue
        params = {
            "id": js.get("id") or aid,
            "symbol": (js.get("symbol") or "").upper(),
            "name": js.get("name"),
            "image": js.get("image"),
            "categories": js.get("categories"),
            "hashing_algorithm": js.get("hashing_algorithm"),
            "platforms": js.get("platforms") or {},
        }
        sess.run(q_asset, **params)
        if params["categories"]:
            sess.run(q_cat, **params)
        if params["hashing_algorithm"]:
            sess.run(q_algo_similarity, **params)
        if params["platforms"]:
            sess.run(q_platforms, **params)

        # links → separate nodes/rels
        links = js.get("links") or {}
        pairs = extract_links(params["id"], links)
        for url, kind in pairs:
            sess.run(q_link, id=params["id"], url=url, kind=kind, domain=_domain(url))

def ingest_markets(sess, markets: List[Dict[str, Any]]):
    """
    Create immutable MarketSnapshot nodes and mirror latest fields on Asset.
    """
    q_snap = """
    MERGE (a:Asset {cg_id:$id})
    SET a.symbol = coalesce($symbol, a.symbol),
        a.name   = coalesce($name,   a.name),
        a.image  = coalesce($image,  a.image),
        a.current_price = $price,
        a.market_cap = $market_cap,
        a.market_cap_rank = $rank,
        a.volume_24h = $vol,
        a.last_updated = datetime($last_updated)
    MERGE (s:MarketSnapshot {as_of: datetime($last_updated), id:$id})
    SET s.price = $price, s.market_cap=$market_cap, s.market_cap_rank=$rank, s.volume_24h=$vol
    MERGE (a)-[:HAS_SNAPSHOT]->(s)
    """
    for row in markets:
        params = {
            "id": row.get("id"),
            "symbol": (row.get("symbol") or "").upper(),
            "name": row.get("name"),
            "image": row.get("image"),
            "price": float(row.get("current_price") or 0.0),
            "market_cap": float(row.get("market_cap") or 0.0),
            "rank": int(row.get("market_cap_rank") or 0),
            "vol": float(row.get("total_volume") or 0.0),
            "last_updated": row.get("last_updated") or dt.datetime.utcnow().isoformat()+"Z"
        }
        sess.run(q_snap, **params)

def mark_available_on_coinbase(sess, symbols_ok: set[str]):
    q = """
    MATCH (a:Asset)
    WITH a, toUpper(a.symbol) AS sym
    SET a.available_on_coinbase = sym IN $ok
    """
    sess.run(q, ok=list(symbols_ok))

# -------------------- Similarity edges (category Jaccard) --------------------
def build_category_similarity(sess, min_jaccard: float = 0.25):
    # APOC version (preferred)
    q_apoc = """
    MATCH (c:Category)<-[:HAS_CATEGORY]-(a:Asset)
    WITH c, collect({id:a.cg_id, cats:coalesce(a.categories,[]), node:a}) AS aa
    UNWIND aa AS x
    UNWIND aa AS y
    WITH x, y
    WHERE x.id < y.id
    WITH x.node AS x, y.node AS y,
        apoc.coll.toSet(x.cats) AS cx,
        apoc.coll.toSet(y.cats) AS cy
    WITH x, y, apoc.coll.intersection(cx,cy) AS inter, apoc.coll.union(cx,cy) AS uni
    WITH x, y, toFloat(size(inter)) / toFloat(size(uni)) AS j
    WHERE j >= $min_j
    MERGE (x)-[s:SIMILAR_TO {kind:'category'}]->(y)
    SET s.jaccard = j
    """
    # Fallback (no APOC): connect if any category overlaps (jaccard=1.0 placeholder)
    q_simple = """
    MATCH (c:Category)<-[:HAS_CATEGORY]-(a:Asset)
    WITH c, collect(a) AS aa
    UNWIND aa AS x
    UNWIND aa AS y
    WITH x, y
    WHERE id(x) < id(y)
      AND any(cat IN coalesce(x.categories,[]) WHERE cat IN coalesce(y.categories,[]))
    MERGE (x)-[:SIMILAR_TO {kind:'category', jaccard:1.0}]->(y)
    """
    try:
        sess.run("RETURN apoc.version()").single()
        sess.run(q_apoc, min_j=min_jaccard)
    except Exception:
        sess.run(q_simple)

# -------------------- Portfolio selection --------------------
def normalize(vals: List[float]) -> List[float]:
    s = sum(max(v, 0.0) for v in vals)
    if s <= 0:
        return [0.0]*len(vals)
    return [max(v,0.0)/s for v in vals]

def propose_portfolio(sess,
                      topn: int = PORTFOLIO_TOPN,
                      max_weight: float = PORTFOLIO_MAX_WEIGHT,
                      universe_rank: int = UNIVERSE_MAX_RANK,
                      min_vol_usd: float = MIN_VOLUME_USD,
                      diversity_target: int = CATEGORY_DIVERSITY_TARGET
                      ) -> List[Dict[str, Any]]:
    q = """
    MATCH (a:Asset)
    WHERE a.available_on_coinbase = true
      AND a.market_cap_rank IS NOT NULL AND a.market_cap_rank > 0 AND a.market_cap_rank <= $max_rank
      AND coalesce(a.volume_24h, 0) >= $min_vol
    WITH a, coalesce(a.categories, []) AS cats
    RETURN a.cg_id AS id, a.symbol AS symbol, a.name AS name,
           a.market_cap AS cap, a.market_cap_rank AS rank,
           a.volume_24h AS vol, cats AS categories
    ORDER BY rank ASC
    LIMIT 1000
    """
    rows = [r.data() for r in sess.run(q, max_rank=universe_rank, min_vol=min_vol_usd)]
    if not rows:
        return []

    caps  = [float(r["cap"] or 0.0) for r in rows]
    vols  = [float(r["vol"] or 0.0) for r in rows]
    max_cap = max(caps) if caps else 1.0
    cap_score = [(c/max_cap) if max_cap>0 else 0.0 for c in caps]
    import math as _math
    log_vols = [_math.log1p(max(v, 0.0)) for v in vols]
    max_lv   = max(log_vols) if log_vols else 1.0
    vol_score= [(lv/max_lv) if max_lv>0 else 0.0 for lv in log_vols]

    index_terms = {"Coinbase 50 Index","GMCI 30 Index","GMCI Index","GMCI Layer 1 Index"}
    index_bonus = [1.0 if any(ix in set((r["categories"] or [])) for ix in index_terms) else 0.0 for r in rows]

    scores = []
    for i in range(len(rows)):
        s = 0.6*cap_score[i] + 0.4*vol_score[i] + 0.1*index_bonus[i]
        scores.append(min(1.0, max(0.0, s)))

    order = sorted(range(len(rows)), key=lambda i: scores[i], reverse=True)
    selected, seen_cats = [], set()
    for i in order:
        r = rows[i]
        cats = [c for c in (r["categories"] or [])]
        new_cats = [c for c in cats if c not in seen_cats]
        if len(selected) < topn:
            selected.append((r, scores[i]))
            for c in new_cats:
                seen_cats.add(c)
            if len(seen_cats) >= diversity_target and len(selected) >= topn:
                break

    j = 0
    while len(selected) < topn and j < len(order):
        idx = order[j]
        cand = (rows[idx], scores[idx])
        if cand not in selected:
            selected.append(cand)
        j += 1

    w_raw = normalize([s for _, s in selected])
    w_capped = [min(max_weight, w) for w in w_raw]
    w = normalize(w_capped)

    out = []
    for (r, s), wi in zip(selected, w):
        out.append({
            "id": r["id"], "symbol": r["symbol"], "name": r["name"],
            "score": round(float(s), 4),
            "weight": round(float(wi), 4),
            "rank": int(r["rank"] or 0),
            "market_cap": float(r["cap"] or 0.0),
            "volume_24h": float(r["vol"] or 0.0),
            "categories": r["categories"] or []
        })
    return out

# -------------------- Main --------------------
def main():
    meta = load_assets_meta(ASSETS_META_PATH)
    markets = load_markets_top(MARKETS_TOP_PATH)

    products = fetch_coinbase_products()
    sym_ok = coinbase_tradeable_symbols(products, quote="USD")

    with driver() as drv, drv.session(database=NEO4J_DATABASE) as sess:
        apply_schema(sess)
        ingest_assets_meta(sess, meta)
        ingest_markets(sess, markets)
        mark_available_on_coinbase(sess, sym_ok)
        build_category_similarity(sess, min_jaccard=0.25)

        portfolio = propose_portfolio(sess)
        print("\n=== Proposed portfolio (graph-diversified, Coinbase-available) ===")
        for p in portfolio:
            print(f"{p['symbol']:>6}  w={p['weight']:.3f}  score={p['score']:.3f}  "
                  f"rank={p['rank']:>3}  cap=${p['market_cap']:.0f}  vol24h=${p['volume_24h']:.0f}")
        print("\nWeights sum:", round(sum(x['weight'] for x in portfolio), 6))

        ts = datetime.now(timezone.utc).isoformat()
        for p in portfolio:
            sess.run("""
                MATCH (a:Asset {cg_id:$id})
                SET a.last_recommended_weight = $w, a.last_recommended_at = datetime($ts)
            """, id=p["id"], w=p["weight"], ts=ts)

if __name__ == "__main__":
    main()
