from __future__ import annotations
import os, json, pathlib
from neo4j import GraphDatabase
from dotenv import load_dotenv
load_dotenv(override=False)

NEO4J_URI=os.getenv("NEO4J_URI","bolt://localhost:7687")
NEO4J_USER=os.getenv("NEO4J_USER","neo4j")
NEO4J_PASSWORD=os.getenv("NEO4J_PASSWORD","please_change_me")
NEO4J_DATABASE=os.getenv("NEO4J_DATABASE","neo4j")

def _driver():
    return GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))

SCHEMA = """
CREATE CONSTRAINT asset_id IF NOT EXISTS FOR (a:Asset) REQUIRE a.cg_id IS UNIQUE;
CREATE CONSTRAINT token_key IF NOT EXISTS FOR (t:Token) REQUIRE t.key IS UNIQUE;
CREATE CONSTRAINT network_id IF NOT EXISTS FOR (n:Network) REQUIRE n.chain_id IS UNIQUE;
CREATE CONSTRAINT tag_name IF NOT EXISTS FOR (t:Tag) REQUIRE t.name IS UNIQUE;
CREATE CONSTRAINT category_name IF NOT EXISTS FOR (c:Category) REQUIRE c.name IS UNIQUE;
"""

def apply_schema():
    with _driver() as drv, drv.session(database=NEO4J_DATABASE) as s:
        for stmt in SCHEMA.strip().split(";"):
            st=stmt.strip()
            if st: s.run(st)

def ingest_assets_markets(markets_path):
    data=json.loads(pathlib.Path(markets_path).read_text()).get("data", [])
    with _driver() as drv, drv.session(database=NEO4J_DATABASE) as s:
        apply_schema()
        for row in data:
            s.run("MERGE (a:Asset {cg_id:$id}) SET a.symbol=$symbol, a.name=$name, a.image=$image, a.market_cap=$market_cap, a.cg_rank=$market_cap_rank",
                  id=row.get("id"), symbol=row.get("symbol"), name=row.get("name"), image=row.get("image"),
                  market_cap=row.get("market_cap"), market_cap_rank=row.get("market_cap_rank"))

def ingest_assets_meta(meta_path):
    data=json.loads(pathlib.Path(meta_path).read_text()).get("data", {})
    with _driver() as drv, drv.session(database=NEO4J_DATABASE) as s:
        apply_schema()
        for id_, js in data.items():
            if "error" in js: continue
            s.run("MERGE (a:Asset {cg_id:$id}) SET a.symbol=$symbol, a.name=$name, a.image=$image, a.hashing_algorithm=$hashing, a.links=$links, a.categories=$categories, a.asset_platform_id=$asset_platform_id",
                  id=js.get("id"), symbol=js.get("symbol"), name=js.get("name"), image=js.get("image"), hashing=js.get("hashing_algorithm"),
                  links=js.get("links"), categories=js.get("categories"), asset_platform_id=js.get("asset_platform_id"))
            for cat in js.get("categories") or []:
                s.run("MERGE (c:Category {name:$name})", name=cat)
                s.run("MATCH (a:Asset {cg_id:$id}),(c:Category {name:$name}) MERGE (a)-[:HAS_CATEGORY]->(c)", id=id_, name=cat)
            platforms = js.get("platforms") or {}
            for net_slug, addr in platforms.items():
                if not addr: continue
                s.run("MERGE (n:Network {name:$net_slug})", net_slug=net_slug)
                token_key=f"{id_}:{net_slug}:{addr.lower()}"
                s.run("MERGE (t:Token {key:$key}) SET t.address=$addr, t.symbol=$symbol, t.name=$name",
                      key=token_key, addr=addr, symbol=js.get("symbol"), name=js.get("name"))
                s.run("MATCH (a:Asset {cg_id:$id}),(t:Token {key:$key}),(n:Network {name:$net_slug}) MERGE (t)-[:IMPLEMENTS]->(a) MERGE (t)-[:ON_NETWORK]->(n)",
                      id=id_, key=token_key, net_slug=net_slug)

def ingest_tokenlists(tokenlists_path):
    data=json.loads(pathlib.Path(tokenlists_path).read_text()).get("data", {})
    with _driver() as drv, drv.session(database=NEO4J_DATABASE) as s:
        apply_schema()
        for url, tl in data.items():
            for t in tl.get("tokens", []):
                chain_id=t.get("chainId"); addr=(t.get("address") or "").lower(); symbol=t.get("symbol"); name=t.get("name"); decimals=t.get("decimals"); logo=t.get("logoURI")
                s.run("MERGE (n:Network {chain_id:toInteger($chain_id)}) SET n.name=coalesce(n.name, $name)",
                      chain_id=chain_id, name=str(chain_id))
                tkey=f"{symbol}:{chain_id}:{addr}"
                s.run("MERGE (tok:Token {key:$key}) SET tok.address=$addr, tok.symbol=$symbol, tok.name=$name, tok.decimals=toInteger($decimals), tok.logo=$logo",
                      key=tkey, addr=addr, symbol=symbol, name=name, decimals=decimals, logo=logo)
                s.run("MATCH (tok:Token {key:$key}),(n:Network {chain_id:toInteger($chain_id)}) MERGE (tok)-[:ON_NETWORK]->(n)",
                      key=tkey, chain_id=chain_id)
