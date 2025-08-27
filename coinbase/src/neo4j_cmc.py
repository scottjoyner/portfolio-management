from __future__ import annotations
import os, json, pathlib, typing as t
from neo4j import GraphDatabase
from dotenv import load_dotenv

load_dotenv(override=False)

NEO4J_URI = os.getenv("NEO4J_URI", "bolt://localhost:7687")
NEO4J_USER = os.getenv("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "please_change_me")
NEO4J_DATABASE = os.getenv("NEO4J_DATABASE", "neo4j")

def _driver():
    return GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))

SCHEMA_CYPHER = """
CREATE CONSTRAINT asset_id IF NOT EXISTS FOR (a:Asset) REQUIRE a.cmc_id IS UNIQUE;
CREATE CONSTRAINT network_name IF NOT EXISTS FOR (n:Network) REQUIRE n.name IS UNIQUE;
CREATE CONSTRAINT tag_name IF NOT EXISTS FOR (t:Tag) REQUIRE t.name IS UNIQUE;
CREATE CONSTRAINT category_name IF NOT EXISTS FOR (c:Category) REQUIRE c.name IS UNIQUE;
"""

def apply_schema():
    with _driver() as drv, drv.session(database=NEO4J_DATABASE) as s:
        for stmt in SCHEMA_CYPHER.strip().split(";"):
            st = stmt.strip()
            if not st: continue
            s.run(st)

def _merge_asset(tx, asset: dict):
    tx.run(
        """
        MERGE (a:Asset {cmc_id:$cmc_id})
        SET a.name=$name, a.symbol=$symbol, a.slug=$slug, a.cmc_rank=$cmc_rank,
            a.date_added=$date_added, a.max_supply=$max_supply, a.circulating_supply=$circ_supply,
            a.total_supply=$total_supply, a.platform_symbol=$platform_symbol, a.platform_token_address=$platform_address,
            a.logo=$logo, a.urls=$urls, a.category=$category
        """,
        **asset
    )

def _merge_network(tx, name: str):
    tx.run("MERGE (n:Network {name:$name})", name=name)

def _rel_on_network(tx, cmc_id: int, network_name: str):
    tx.run(
        "MATCH (a:Asset {cmc_id:$cmc_id}), (n:Network {name:$network_name}) MERGE (a)-[:ON_NETWORK]->(n)",
        cmc_id=cmc_id, network_name=network_name
    )

def _merge_tag(tx, name: str):
    tx.run("MERGE (t:Tag {name:$name})", name=name)

def _rel_has_tag(tx, cmc_id: int, tag: str):
    tx.run("MATCH (a:Asset {cmc_id:$cmc_id}), (t:Tag {name:$tag}) MERGE (a)-[:HAS_TAG]->(t)", cmc_id=cmc_id, tag=tag)

def _merge_category(tx, name: str):
    tx.run("MERGE (c:Category {name:$name})", name=name)

def _rel_has_category(tx, cmc_id: int, cat: str):
    tx.run("MATCH (a:Asset {cmc_id:$cmc_id}), (c:Category {name:$cat}) MERGE (a)-[:HAS_CATEGORY]->(c)", cmc_id=cmc_id, cat=cat)

def ingest_from_files(json_dir: str | pathlib.Path):
    json_dir = pathlib.Path(json_dir)
    files = sorted(json_dir.glob("*.json"))
    with _driver() as drv, drv.session(database=NEO4J_DATABASE) as s:
        apply_schema()
        for f in files:
            data = json.loads(f.read_text())
            lst = data.get("data", {}).get("listings", data.get("data"))
            # Support both cache styles
            if isinstance(lst, dict) and "cryptoCurrencyList" in lst:
                entries = lst["cryptoCurrencyList"]
            elif isinstance(lst, list):
                entries = lst
            else:
                # maybe this is the info() dump
                entries = data.get("data", {})
            # If dict of id->info (from info endpoint)
            if isinstance(entries, dict):
                for sid, meta in entries.items():
                    cmc_id = int(meta.get("id", sid))
                    _ingest_entry(s, cmc_id, meta, info_mode=True)
            else:
                for e in entries:
                    cmc_id = int(e.get("id"))
                    _ingest_entry(s, cmc_id, e, info_mode=False)

def _ingest_entry(session, cmc_id: int, obj: dict, info_mode: bool):
    # Normalize fields
    if info_mode:
        name = obj.get("name"); symbol = obj.get("symbol"); slug = obj.get("slug")
        # urls
        urls = obj.get("urls", {})
        logo = obj.get("logo")
        tags = obj.get("tags", [])
        category = obj.get("category") or obj.get("type") or ("token" if obj.get("platform") else "coin")
        platform = obj.get("platform") or {}
        platform_symbol = platform.get("symbol")
        platform_address = None
        if "contract_address" in obj and obj["contract_address"]:
            # choose primary contract (first)
            platform_address = obj["contract_address"][0].get("contract_address")
    else:
        name = obj.get("name"); symbol = obj.get("symbol"); slug = obj.get("slug")
        cmc_rank = obj.get("cmc_rank")
        supply = obj.get("total_supply"); circ_supply = obj.get("circulating_supply"); max_supply = obj.get("max_supply")
        date_added = obj.get("date_added")
        platform = obj.get("platform") or {}
        platform_symbol = platform.get("symbol")
        platform_address = platform.get("token_address")
        category = obj.get("category") or ("token" if platform_symbol else "coin")
        logo = None; urls = {}; tags = obj.get("tags", [])

    asset = {
        "cmc_id": cmc_id,
        "name": name, "symbol": symbol, "slug": slug,
        "cmc_rank": obj.get("cmc_rank"),
        "date_added": obj.get("date_added"),
        "max_supply": obj.get("max_supply"),
        "circ_supply": obj.get("circulating_supply"),
        "total_supply": obj.get("total_supply"),
        "platform_symbol": platform_symbol,
        "platform_address": platform_address,
        "logo": logo,
        "urls": urls,
        "category": category,
    }
    session.execute_write(_merge_asset, asset)

    # Network
    network_name = platform_symbol or ("NATIVE" if category=="coin" else None)
    if network_name:
        session.execute_write(_merge_network, network_name)
        session.execute_write(_rel_on_network, cmc_id, network_name)

    # Category
    if category:
        session.execute_write(_merge_category, category)
        session.execute_write(_rel_has_category, cmc_id, category)

    # Tags
    for t in (tags or []):
        session.execute_write(_merge_tag, t)
        session.execute_write(_rel_has_tag, cmc_id, t)
