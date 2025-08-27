#!/usr/bin/env python3
import argparse, pandas as pd, yfinance as yf
from neo4j import GraphDatabase
from app.settings import NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD
from app.utils.logger import get_logger

log = get_logger("ingest_prices")

def upsert_bars(tx, symbol, df, source):
    for dt, row in df.iterrows():
        tx.run(
            """
            MERGE (t:Ticker {symbol:$symbol})
            MERGE (b:PriceBar {symbol:$symbol, date:$date})
            SET b.o=$o, b.h=$h, b.l=$l, b.c=$c, b.v=$v, b.adj=$adj, b.source=$source
            """,
            symbol=symbol, date=dt.date().isoformat(),
            o=float(row["Open"]), h=float(row["High"]), l=float(row["Low"]),
            c=float(row["Close"]), v=int(row.get("Volume") or 0),
            adj=float(row.get("Adj Close", row["Close"])), source=source
        )

def load_prices(symbols, period="5y", interval="1d", source="yfinance"):
    drv = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
    with drv.session() as sess:
        for sym in symbols:
            log.info(f"Downloading {sym} ({period}, {interval})")
            df = yf.download(sym, period=period, interval=interval, auto_adjust=False, progress=False)
            if df.empty:
                log.warning(f"No data for {sym}")
                continue
            df = df.tz_localize(None)
            sess.execute_write(upsert_bars, sym, df, source)
    drv.close()

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--symbols", type=str, required=True, help="Comma-separated symbols")
    ap.add_argument("--period", type=str, default="5y")
    ap.add_argument("--interval", type=str, default="1d")
    args = ap.parse_args()
    symbols = [s.strip().upper() for s in args.symbols.split(",") if s.strip()]
    load_prices(symbols, period=args.period, interval=args.interval)

if __name__ == "__main__":
    main()
