from __future__ import annotations
import os
from typing import Dict, List
from dotenv import load_dotenv
from neo4j import GraphDatabase

load_dotenv(override=False)

NEO4J_URI      = os.getenv("NEO4J_URI", "bolt://localhost:7687")
NEO4J_USER     = os.getenv("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "please_change_me")
NEO4J_DATABASE = os.getenv("NEO4J_DATABASE", "neo4j")

def fetch_allocations(min_weight: float = 0.005) -> List[Dict]:
    """Return assets with last_recommended_weight from Neo4j."""
    drv = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
    q = """
    MATCH (a:Asset)
    WHERE a.available_on_coinbase = true
      AND a.last_recommended_weight IS NOT NULL
      AND a.last_recommended_weight >= $minw
    RETURN a.cg_id AS id, toUpper(a.symbol) AS symbol, a.name AS name,
           a.last_recommended_weight AS weight,
           a.market_cap_rank AS rank
    ORDER BY weight DESC
    """
    with drv.session(database=NEO4J_DATABASE) as sess:
        rows = [r.data() for r in sess.run(q, minw=float(min_weight))]
    drv.close()
    # force weight renorm in case they don't sum to 1
    s = sum(r["weight"] for r in rows) or 1.0
    for r in rows:
        r["weight"] = r["weight"] / s
        r["product_id"] = f"{r['symbol']}-USD"
    return rows
