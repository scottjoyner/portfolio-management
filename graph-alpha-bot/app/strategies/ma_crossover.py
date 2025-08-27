import uuid, numpy as np, pandas as pd
from typing import List
from app.strategies.base import Strategy

class MovingAverageCrossover(Strategy):
    name = "MA_Crossover"

    def _get_prices(self, sess, symbol, lookback=260):
        q = """
        MATCH (b:PriceBar {symbol:$symbol})
        WITH b ORDER BY b.date ASC
        RETURN collect({d:b.date, c:b.c}) AS bars
        """
        bars = sess.run(q, symbol=symbol).single().value()
        df = pd.DataFrame(bars).tail(lookback)
        if df.empty:
            return df
        df['ret'] = np.log(df['c']).diff()
        return df

    def _write_signal(self, sess, symbol, ts, score, meta):
        sess.run("""
        MERGE (s:Strategy {name:$strategy})
        MERGE (t:Ticker {symbol:$symbol})
        MERGE (g:Signal {id:$id})
        SET g.ts=$ts, g.score=$score, g.meta=$meta
        MERGE (s)-[:GENERATED]->(g)-[:FOR]->(t)
        """, strategy=self.name, symbol=symbol, id=str(uuid.uuid4()),
              ts=ts, score=float(score), meta=meta)

    def generate(self, symbols: List[str]) -> int:
        n = 0
        with self.session() as sess:
            for sym in symbols:
                df = self._get_prices(sess, sym)
                if len(df) < 100:
                    continue
                df['ma20'] = df['c'].rolling(20).mean()
                df['ma100'] = df['c'].rolling(100).mean()
                last = df.iloc[-1]
                sig = 1 if last['ma20'] > last['ma100'] else -1
                entry = last['c']
                stop = entry*0.95
                target = entry*1.10
                rr = float((target-entry)/(entry-stop)) if entry != stop else 0.0
                score = sig*rr
                self._write_signal(sess, sym, ts=str(df.index[-1]), score=score,
                                   meta={"rr": rr, "stop": float(stop), "target": float(target)})
                n += 1
        return n
