#!/usr/bin/env python3
"""
Fetch current S&P 500 constituents from Wikipedia with a proper User-Agent
and map to CIK via SEC JSON.

Writes:
  - config/symbols_sp500.csv (symbol,name,sector)
  - config/sp500_cik.csv (symbol,CIK,company)
MERGE (:Ticker) nodes with (symbol,name,sector,sp500=true,cik)
"""
import argparse, os, time
import pandas as pd
import requests
from bs4 import BeautifulSoup
from neo4j import GraphDatabase
from app.settings import NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD
from app.utils.logger import get_logger

log = get_logger("refresh_sp500")

WIKI_URL = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
# Public fallback CSV (mirrors Wikipedia; not official S&P)
FALLBACK_CSV = "https://raw.githubusercontent.com/datasets/s-and-p-500-companies/master/data/constituents.csv"
SEC_TICKERS_URL = "https://www.sec.gov/files/company_tickers_exchange.json"

UA = (os.getenv("WIKI_USER_AGENT")
      or os.getenv("SEC_USER_AGENT")
      or "youremail@example.com GraphAlphaBot/1.0")

def fetch_html(url: str) -> str:
    log.info(f"GET {url}")
    r = requests.get(
        url,
        headers={
            "User-Agent": UA,
            "Accept": "text/html,application/xhtml+xml",
            "Accept-Language": "en-US,en;q=0.9",
        },
        timeout=60,
    )
    r.raise_for_status()
    return r.text

def parse_wiki_table(html: str) -> pd.DataFrame:
    # Use BeautifulSoup to isolate the main constituents table, then let pandas parse it.
    soup = BeautifulSoup(html, "lxml")
    tables = soup.select("table.wikitable")
    if not tables:
        raise RuntimeError("No wikitable elements found on page.")
    # The first wikitable on that page is the constituents table
    html_str = str(tables[0])
    dfs = pd.read_html(html_str)  # uses lxml backend (already installed)
    if not dfs:
        raise RuntimeError("pandas could not parse table HTML.")
    df = dfs[0]
    # Normalize columns
    cols = {c: c.strip() for c in df.columns}
    df.rename(columns=cols, inplace=True)
    # Handle name/symbol/sector columns across page variants
    if "Symbol" in df.columns:
        df.rename(columns={"Symbol": "symbol"}, inplace=True)
    if "Security" in df.columns:
        df.rename(columns={"Security": "name"}, inplace=True)
    if "GICS Sector" in df.columns:
        df.rename(columns={"GICS Sector": "sector"}, inplace=True)
    if not {"symbol", "name", "sector"}.issubset(df.columns):
        raise RuntimeError(f"Unexpected table columns: {list(df.columns)}")
    df = df[["symbol", "name", "sector"]].copy()
    # Standardize symbols (yfinance prefers '-' instead of '.' for tickers like BRK.B)
    df["symbol"] = df["symbol"].astype(str).str.upper().str.replace(".", "-", regex=False)
    return df

def fetch_sec_ticker_map() -> dict:
    """
    Returns a dict: { "AAPL": {"cik": 320193, "company": "Apple Inc."}, ... }
    Handles multiple SEC JSON formats:
      - list[dict]
      - dict with "data" + "fields"
      - dict keyed by ints -> dict
    """
    log.info("Fetching SEC ticker/CIK map...")
    r = requests.get(SEC_TICKERS_URL, headers={"User-Agent": UA}, timeout=60)
    r.raise_for_status()
    j = r.json()

    rows = []

    # Case 1: already a list of dicts
    if isinstance(j, list):
        rows = [x for x in j if isinstance(x, dict)]

    # Case 2: dict with "data" + "fields" (tabular)
    elif isinstance(j, dict) and "data" in j:
        fields = j.get("fields", [])
        data = j.get("data", [])
        for item in data:
            if isinstance(item, list) and fields:
                d = {}
                for i, f in enumerate(fields):
                    if i < len(item):
                        d[f] = item[i]
                rows.append(d)
            elif isinstance(item, dict):
                rows.append(item)

    # Case 3: dict of { "0": {...}, "1": {...}, ... }
    elif isinstance(j, dict):
        for _, v in j.items():
            if isinstance(v, dict):
                rows.append(v)

    mp = {}
    for row in rows:
        # Field name variants across SEC files
        ticker = (
            row.get("ticker")
            or row.get("symbol")
            or row.get("Ticker")
            or row.get("Symbol")
        )
        cik = row.get("cik") or row.get("CIK")
        title = row.get("title") or row.get("name") or row.get("company") or row.get("Company")

        if not ticker:
            continue
        try:
            cik_int = int(cik) if cik is not None and str(cik).strip() != "" else 0
        except Exception:
            cik_int = 0

        sym = str(ticker).upper().replace(".", "-")
        mp[sym] = {"cik": cik_int, "company": title or ""}

    if not mp:
        raise RuntimeError("Could not parse SEC ticker map; structure unrecognized.")

    return mp


def upsert_tickers(df: pd.DataFrame):
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

    # Try Wikipedia with a polite UA; fall back to public CSV if blocked
    try:
        html = fetch_html(WIKI_URL)
        sp = parse_wiki_table(html)
    except Exception as e:
        log.warning(f"Wikipedia fetch/parse failed ({e}); using fallback CSV.")
        sp = pd.read_csv(FALLBACK_CSV)
        # Normalize fallback columns to match expected schema
        # Fallback CSV columns: Symbol,Name,Sector
        sp.rename(columns={"Symbol":"symbol","Name":"name","Sector":"sector"}, inplace=True)
        sp["symbol"] = sp["symbol"].astype(str).str.upper().str.replace(".", "-", regex=False)

    mp = fetch_sec_ticker_map()
    sp["cik"] = sp["symbol"].map(lambda s: mp.get(s, {}).get("cik", 0))
    sp["company_sec"] = sp["symbol"].map(lambda s: mp.get(s, {}).get("company", ""))

    os.makedirs("config", exist_ok=True)
    sp[["symbol","name","sector"]].to_csv("config/symbols_sp500.csv", index=False)
    sp[["symbol","cik","company_sec"]].to_csv("config/sp500_cik.csv", index=False)
    log.info(f"Wrote {len(sp)} S&P 500 rows to config/.")

    if not args.no_write:
        upsert_tickers(sp)
        log.info("Upserted :Ticker nodes to Neo4j.")

if __name__ == "__main__":
    main()
