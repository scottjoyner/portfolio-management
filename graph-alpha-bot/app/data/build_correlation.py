#!/usr/bin/env python3
import argparse, numpy as np, pandas as pd
from neo4j import GraphDatabase
from app.settings import NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD
from app.utils.logger import get_logger
log = get_logger("build_correlation")

def get_prices(session, symbol, window):
    q = """
    MATCH (b:PriceBar {symbol:$symbol})
    WITH b ORDER BY b.date ASC
    RETURN collect(b.c) AS closes
    """
    closes = session.run(q, symbol=symbol).single().value()
    if not closes:
        return pd.Series(dtype=float)
    s = pd.Series(closes).astype(float).tail(window+1)
    return s

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--symbols", type=str, required=True)
    ap.add_argument("--window", type=int, default=90)
    args = ap.parse_args()
    symbols = [s.strip().upper() for s in args.symbols.split(",") if s.strip()]
    drv = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
    with drv.session() as s:
        rets = {}
        for sym in symbols:
            cls = get_prices(s, sym, args.window)
            if len(cls) < args.window//2:
                log.warning(f"Not enough data for {sym}")
                continue
            rets[sym] = np.log(cls).diff().dropna()
        if len(rets) < 2:
            log.warning("Insufficient series for correlation.")
            return
        df = pd.DataFrame(rets).dropna()
        corr = df.corr()
        edges = []
        for a in corr.columns:
            for b in corr.columns:
                if a >= b:
                    continue
                rho = float(corr.loc[a,b])
                if abs(rho) < 0.5:
                    continue
                edges.append({"a": a, "b": b, "rho": rho, "win": args.window})
        s.run("""
        UNWIND $edges AS e
        MATCH (a:Ticker {symbol:e.a}),(b:Ticker {symbol:e.b})
        MERGE (a)-[r:CORRELATED {window:e.win}]->(b)
        SET r.rho=e.rho, r.updated=date()
        """, edges=edges)
    drv.close()
    log.info("Correlation edges updated.")

if __name__ == "__main__":
    main()
