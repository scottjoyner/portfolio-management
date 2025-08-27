#!/usr/bin/env python3
"""
Fetch current S&P 500 constituents from Wikipedia and map to CIK via SEC JSON.
Writes:
  - config/symbols_sp500.csv (symbol,name,sector)
  - config/sp500_cik.csv (symbol,CIK,company)
And MERGEs (:Ticker) nodes with properties (symbol,name,sector,sp500=true, cik).
"""
import argparse, pandas as pd, requests, time, sys
from neo4j import GraphDatabase
from app.settings import NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD
from app.utils.logger import get_logger
import os

log = get_logger("refresh_sp500")

WIKI_URL = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
SEC_TICKERS_URL = "https://www.sec.gov/files/company_tickers_exchange.json"
UA = os.getenv("SEC_USER_AGENT", "youremail@example.com GraphAlphaBot/1.0")

def fetch_sp500_table():
    log.info("Loading S&P 500 table from Wikipedia...")
    tables = pd.read_html(WIKI_URL, flavor="bs4")
    # Find table with Ticker symbol column
    for df in tables:
        cols = [c.lower() for c in df.columns.astype(str)]
        if any("symbol" in c or "ticker" in c for c in cols) and any("gics" in c for c in cols):
            df.columns = [str(c).strip() for c in df.columns]
            # Standardize
            if "Symbol" in df.columns:
                df.rename(columns={"Symbol":"symbol"}, inplace=True)
            elif "Ticker symbol" in df.columns:
                df.rename(columns={"Ticker symbol":"symbol"}, inplace=True)
            name_col = "Security" if "Security" in df.columns else "Company"
            sector_col = "GICS Sector" if "GICS Sector" in df.columns else "GICS sector"
            df = df[["symbol", name_col, sector_col]].copy()
            df.rename(columns={name_col:"name", sector_col:"sector"}, inplace=True)
            df["symbol"] = df["symbol"].astype(str).str.upper().str.replace(".", "-", regex=False)
            return df
    raise RuntimeError("Could not locate S&P 500 constituents table.")

def fetch_sec_ticker_map():
    log.info("Fetching SEC ticker/CIK map...")
    r = requests.get(SEC_TICKERS_URL, headers={"User-Agent": UA}, timeout=60)
    r.raise_for_status()
    data = r.json()
    # Build map: symbol -> {cik, title}
    mp = {}
    for row in data:
        sym = str(row.get("ticker","")).upper()
        if not sym: continue
        mp[sym] = {"cik": int(row.get("cik", 0)), "company": row.get("title","")}
    return mp

def upsert_tickers(df):
    drv = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
    with drv.session() as s:
        for _, r in df.iterrows():
            s.run("""
            MERGE (t:Ticker {symbol:$symbol})
            SET t.name=$name, t.sector=$sector, t.sp500=true, t.cik=$cik
            """, symbol=r["symbol"], name=r["name"], sector=r["sector"], cik=int(r.get("cik") or 0))
    drv.close()

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--no-write", action="store_true", help="Skip Neo4j writes, just dump CSVs")
    args = ap.parse_args()

    sp = fetch_sp500_table()
    mp = fetch_sec_ticker_map()
    sp["cik"] = sp["symbol"].map(lambda s: mp.get(s, {}).get("cik", 0))
    sp["company_sec"] = sp["symbol"].map(lambda s: mp.get(s, {}).get("company", ""))

    sp.to_csv("config/symbols_sp500.csv", index=False)
    sp[["symbol","cik","company_sec"]].to_csv("config/sp500_cik.csv", index=False)
    log.info(f"Wrote {len(sp)} S&P 500 rows.")

    if not args.no_write:
        upsert_tickers(sp)
        log.info("Upserted :Ticker nodes to Neo4j.")

if __name__ == "__main__":
    main()
