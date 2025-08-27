#!/usr/bin/env python3
import argparse, os, time, io, logging
import pandas as pd
import numpy as np
import requests
import yfinance as yf
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter
from neo4j import GraphDatabase
from app.settings import NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD
from app.utils.logger import get_logger

log = get_logger("ingest_prices")

# ---------- Networking helpers ----------
def make_session():
    ua = (os.getenv("YF_USER_AGENT")
          or os.getenv("WIKI_USER_AGENT")
          or os.getenv("SEC_USER_AGENT")
          or "youremail@example.com GraphAlphaBot/1.0")
    s = requests.Session()
    s.headers.update({
        "User-Agent": ua,
        "Accept": "application/json,text/csv;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Connection": "keep-alive",
    })
    # Optional proxy: export YF_PROXY="http://host:port"
    proxy = os.getenv("YF_PROXY")
    if proxy:
        s.proxies.update({"http": proxy, "https": proxy})
    retry = Retry(
        total=5,
        backoff_factor=0.6,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET", "POST"]
    )
    s.mount("https://", HTTPAdapter(max_retries=retry))
    s.mount("http://", HTTPAdapter(max_retries=retry))
    return s

# ---------- Symbol normalization ----------
ALIASES = {
    # Known tricky ones:
    "RR": "RR.L",      # Rolls-Royce (LSE) — US ADR is RYCEY
    # add more as needed
}
# Prefixes we know are invalid or usually not listed on Yahoo
KNOWN_INVALID = set(["BLSH", "CRCL"])

def normalize_symbols(symbols):
    out = []
    skipped = []
    for s in symbols:
        s0 = s.strip().upper()
        if not s0:
            continue
        if s0 in KNOWN_INVALID:
            skipped.append((s0, "invalid/unknown symbol"))
            continue
        out.append(ALIASES.get(s0, s0))
    if skipped:
        for sym, why in skipped:
            log.warning(f"Skipping {sym}: {why}")
    # De-dupe while preserving order
    seen = set(); dedup = []
    for s in out:
        if s not in seen:
            seen.add(s); dedup.append(s)
    return dedup

# ---------- Fallback: Stooq CSV ----------
def fetch_stooq(symbol, session):
    """
    Try Stooq daily CSV as a fallback.
    US symbols are usually lower-case + '.us' (e.g., aapl.us).
    LSE uses '.uk' (e.g., rr.uk).
    """
    sym = symbol.lower()
    # Heuristic for market suffix
    if symbol.endswith(".L"):
        s = f"{sym.replace('.l','')}.uk"
    else:
        s = f"{sym}.us"
    url = f"https://stooq.com/q/d/l/?s={s}&i=d"
    try:
        r = session.get(url, timeout=30)
        r.raise_for_status()
        if r.text.strip().lower().startswith("error"):
            return pd.DataFrame()
        df = pd.read_csv(io.StringIO(r.text))
        if df.empty or "Date" not in df.columns:
            return pd.DataFrame()
        df.rename(columns={
            "Open":"Open","High":"High","Low":"Low","Close":"Close","Volume":"Volume"
        }, inplace=True)
        df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
        df = df.dropna(subset=["Date"]).set_index("Date").sort_index()
        return df[["Open","High","Low","Close","Volume"]]
    except Exception as e:
        log.warning(f"Stooq fallback failed for {symbol}: {e}")
        return pd.DataFrame()

# ---------- Neo4j upsert ----------
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

def write_symbol(session, symbol, df, source):
    if df is None or df.empty:
        return 0
    # Ensure required columns exist
    cols = {c.lower(): c for c in df.columns}
    need = ["open","high","low","close"]
    if not all(k in cols for k in need):
        return 0
    # Build a normalized frame
    out = pd.DataFrame({
        "Open": df[cols["open"]],
        "High": df[cols["high"]],
        "Low":  df[cols["low"]],
        "Close":df[cols["close"]],
        "Volume": df[cols["volume"]] if "volume" in cols else 0,
        "Adj Close": df[cols.get("adj close", cols["close"])],
    }, index=pd.to_datetime(df.index)).sort_index()
    session.execute_write(upsert_bars, symbol, out, source)
    return len(out)

# ---------- Main load ----------
def load_prices(symbols, period="5y", interval="1d", source="yfinance"):
    sess = make_session()
    drv = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))

    symbols = normalize_symbols(symbols)
    if not symbols:
        log.error("No valid symbols after normalization.")
        return

    # Try batch download first (reduces Yahoo calls)
    batch = " ".join(symbols)
    log.info(f"Batch downloading: {batch} ({period}, {interval})")
    try:
        df_all = yf.download(
            tickers=batch,
            period=period,
            interval=interval,
            auto_adjust=False,
            progress=False,
            threads=False,
            session=sess,
        )
    except Exception as e:
        log.warning(f"Batch download failed: {e}")
        df_all = pd.DataFrame()

    # Determine if MultiIndex (per-ticker) or single
    per_ticker_frames = {}
    if isinstance(df_all.columns, pd.MultiIndex):
        # df_all has columns like ('AAPL','Open'), etc.
        for sym in symbols:
            if sym in df_all.columns.get_level_values(0):
                sub = df_all[sym].dropna(how="all")
                per_ticker_frames[sym] = sub
    else:
        # single ticker or failure
        if len(symbols) == 1:
            per_ticker_frames[symbols[0]] = df_all

    inserted = 0
    with drv.session() as s:
        # For each symbol, either take batch result, or fetch individually (and fallback to Stooq)
        for sym in symbols:
            df_sym = per_ticker_frames.get(sym, None)
            if df_sym is None or df_sym.empty:
                log.info(f"Downloading individually: {sym} ({period}, {interval})")
                try:
                    t = yf.Ticker(sym, session=sess)
                    df_sym = t.history(period=period, interval=interval, auto_adjust=False)
                except Exception as e:
                    log.warning(f"yfinance individual failed for {sym}: {e}")
                    df_sym = pd.DataFrame()

            if df_sym is None or df_sym.empty:
                log.warning(f"No Yahoo data for {sym}; trying Stooq fallback.")
                df_stooq = fetch_stooq(sym, sess)
                if not df_stooq.empty:
                    # make Adj Close same as Close (stooq has no adj)
                    df_stooq["Adj Close"] = df_stooq["Close"]
                    n = write_symbol(s, sym, df_stooq, source="stooq")
                    log.info(f"{sym}: inserted {n} bars from Stooq.")
                    inserted += n
                    time.sleep(0.2)
                    continue
                else:
                    log.error(f"{sym}: no data from Yahoo or Stooq; skipping.")
                    continue

            # Yahoo path
            n = write_symbol(s, sym, df_sym, source=source)
            log.info(f"{sym}: inserted {n} bars from Yahoo.")
            inserted += n
            time.sleep(0.2)  # be polite

    drv.close()
    log.info(f"Done. Inserted {inserted} bars total.")

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
