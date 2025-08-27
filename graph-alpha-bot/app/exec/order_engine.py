import uuid
from typing import List, Dict, Any
from neo4j import GraphDatabase
from app.settings import NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD

def fetch_today_signals(limit=50):
    drv = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
    out=[]
    with drv.session() as s:
        q = """
        MATCH (g:Signal)-[:FOR]->(t:Ticker)
        WHERE date(g.ts) = date() AND g.score > 0
        WITH t.symbol AS sym, sum(g.score) AS s
        ORDER BY s DESC LIMIT $limit
        RETURN sym, s
        """
        for rec in s.run(q, limit=limit):
            out.append({"symbol": rec["sym"], "score": float(rec["s"])})
    drv.close()
    return out

def create_orders(candidates: List[Dict[str,Any]], cash: float=100000.0) -> List[Dict[str,Any]]:
    if not candidates: return []
    total_score = sum(max(0.0, c["score"]) for c in candidates)
    if total_score <= 0: return []
    per_name = []
    for c in candidates:
        w = max(0.0, c["score"]) / total_score
        alloc = cash * w
        per_name.append({
            "id": str(uuid.uuid4()),
            "symbol": c["symbol"],
            "alloc": alloc,
            "qty": int(alloc // 100),  # rough: assume $100/share for demo
            "type": "market",
            "side": "buy",
        })
    return per_name
