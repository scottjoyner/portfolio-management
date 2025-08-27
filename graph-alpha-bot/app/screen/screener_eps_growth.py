#!/usr/bin/env python3
"""
Return top N S&P500 tickers by EPS YoY growth (from SEC XBRL).
"""
import argparse, pandas as pd
from neo4j import GraphDatabase
from app.settings import NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD

def query_top(n):
    drv = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
    rows=[]
    with drv.session() as s:
        q = """
        MATCH (t:Ticker)
        WHERE t.sp500 = true AND exists(t.eps_yoy)
        RETURN t.symbol AS symbol, t.name AS name, t.sector AS sector,
               t.eps_yoy AS eps_yoy, t.eps_diluted_fy AS eps, t.eps_fy AS fy,
               t.eps_diluted_fy_prev AS eps_prev, t.eps_fy_prev AS fy_prev
        ORDER BY t.eps_yoy DESC
        LIMIT $n
        """
        for rec in s.run(q, n=n):
            rows.append(dict(rec))
    drv.close()
    return pd.DataFrame(rows)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--top", type=int, default=25)
    ap.add_argument("--csv", type=str, default=None, help="Optional path to save CSV")
    args = ap.parse_args()

    df = query_top(args.top)
    if df.empty:
        print("No EPS YoY data found. Did you run app/data/ingest_eps_sec.py?")
        return
    if args.csv:
        df.to_csv(args.csv, index=False)
        print(f"Saved {len(df)} rows to {args.csv}")
    else:
        # Pretty print
        with pd.option_context('display.max_rows', None, 'display.max_columns', None):
            print(df)

if __name__ == "__main__":
    main()
