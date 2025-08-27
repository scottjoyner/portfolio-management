# Simplified scaffold for News Centrality Momentum (NCM)
# In production, ingest real news with sentiment -> (:News)<-[:MENTIONED_IN]-(:Ticker)
import uuid
from typing import List
from app.strategies.base import Strategy

class NewsCentralityMomentum(Strategy):
    name = "NewsCentralityMomentum"

    def generate(self, symbols: List[str]) -> int:
        # Placeholder that writes a small positive score
        n=0
        with self.session() as sess:
            for sym in symbols:
                sess.run("""
                MERGE (s:Strategy {name:$strategy})
                MERGE (t:Ticker {symbol:$symbol})
                MERGE (g:Signal {id:$id})
                SET g.ts=date(), g.score=$score, g.meta=$meta
                MERGE (s)-[:GENERATED]->(g)-[:FOR]->(t)
                """, strategy=self.name, symbol=sym, id=str(uuid.uuid4()),
                      score=0.05, meta={"note":"placeholder NCM score"})
                n+=1
        return n
