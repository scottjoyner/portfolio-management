# Scaffold for Insider Cluster Drift (ICD)
# Requires (:Insider)-[:OFFICER_OF]->(:Ticker) and Form 4 data
import uuid
from typing import List
from app.strategies.base import Strategy

class InsiderClusterDrift(Strategy):
    name = "InsiderClusterDrift"

    def generate(self, symbols: List[str]) -> int:
        # Placeholder: slight positive for symbols starting with 'A', negative small otherwise
        n=0
        with self.session() as sess:
            for sym in symbols:
                score = 0.1 if sym.startswith("A") else -0.02
                sess.run("""
                MERGE (s:Strategy {name:$strategy})
                MERGE (t:Ticker {symbol:$symbol})
                MERGE (g:Signal {id:$id})
                SET g.ts=date(), g.score=$score, g.meta=$meta
                MERGE (s)-[:GENERATED]->(g)-[:FOR]->(t)
                """, strategy=self.name, symbol=sym, id=str(uuid.uuid4()),
                      score=float(score), meta={"note":"placeholder ICD"})
                n+=1
        return n
