#!/usr/bin/env python3
"""
Compute EPS (diluted) YoY from SEC XBRL company-concept API and store on :Ticker.
- Prefers us-gaap:EarningsPerShareDiluted, falls back to us-gaap:EarningsPerShareBasic.
- Uses last two fiscal years (fy fields) from 10-K facts.
Writes properties:
  t.eps_diluted_fy, t.eps_diluted_fy_prev, t.eps_yoy, t.eps_source="sec_xbrl"
"""
import argparse, requests, os, pandas as pd, math
from neo4j import GraphDatabase
from app.settings import NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD
from app.utils.logger import get_logger

log = get_logger("ingest_eps_sec")
UA = os.getenv("SEC_USER_AGENT", "youremail@example.com GraphAlphaBot/1.0")

CONCEPTS = [
    ("us-gaap","EarningsPerShareDiluted"),
    ("us-gaap","EarningsPerShareBasic"),
]

def fetch_company_concept(cik: int, taxonomy: str, tag: str):
    url = f"https://data.sec.gov/api/xbrl/companyconcept/CIK{int(cik):010d}/{taxonomy}/{tag}.json"
    r = requests.get(url, headers={"User-Agent": UA}, timeout=60)
    if r.status_code == 404:
        return None
    r.raise_for_status()
    return r.json()

def extract_annual_eps(concept_json):
    if not concept_json: return pd.DataFrame(columns=["fy","form","val"])
    # JSON has 'units' dict keyed by unit, values arrays with fields incl. fy, form, val
    out = []
    for unit, arr in concept_json.get("units", {}).items():
        for it in arr:
            fy = it.get("fy")
            form = it.get("form","")
            val = it.get("val")
            if fy and form and val is not None and "10-K" in form:
                out.append({"fy": str(fy), "form": form, "val": float(val)})
    if not out:
        return pd.DataFrame(columns=["fy","form","val"])
    df = pd.DataFrame(out)
    # Keep last value per FY in case of duplicates
    df = df.sort_values(["fy"]).groupby("fy", as_index=False).last()
    return df

def compute_yoy(df):
    if len(df) < 2:
        return None
    df = df.sort_values("fy")
    last = df.iloc[-1]
    prev = df.iloc[-2]
    if prev["val"] == 0:
        return None
    # If prev negative, growth is noisy; return None to avoid misleading ranks
    if prev["val"] < 0:
        return None
    return float((last["val"] - prev["val"]) / prev["val"]), float(last["val"]), float(prev["val"]), str(last["fy"]), str(prev["fy"])

def upsert_eps(symbol, eps, eps_prev, fy, fy_prev, yoy):
    drv = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
    with drv.session() as s:
        s.run("""
        MERGE (t:Ticker {symbol:$symbol})
        SET t.eps_diluted_fy=$eps, t.eps_diluted_fy_prev=$eps_prev,
            t.eps_yoy=$yoy, t.eps_fy=$fy, t.eps_fy_prev=$fy_prev,
            t.eps_source='sec_xbrl'
        """, symbol=symbol, eps=eps, eps_prev=eps_prev, yoy=yoy, fy=fy, fy_prev=fy_prev)
    drv.close()

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--mapping", type=str, default="config/sp500_cik.csv", help="CSV with symbol,cik")
    ap.add_argument("--limit", type=int, default=None, help="Process first N symbols")
    args = ap.parse_args()

    m = pd.read_csv(args.mapping)
    if args.limit:
        m = m.head(args.limit)

    for _, r in m.iterrows():
        sym = str(r["symbol"]).upper()
        cik = int(r.get("cik") or 0)
        if cik == 0:
            log.warning(f"{sym}: missing CIK")
            continue

        df_all = pd.DataFrame()
        for tx, tag in CONCEPTS:
            try:
                js = fetch_company_concept(cik, tx, tag)
            except Exception as e:
                log.warning(f"{sym}: fetch error for {tx}:{tag} - {e}")
                continue
            df = extract_annual_eps(js)
            if not df.empty:
                df_all = df
                break  # prefer diluted if available

        if df_all.empty:
            log.warning(f"{sym}: no annual EPS facts")
            continue

        comp = compute_yoy(df_all)
        if not comp:
            log.info(f"{sym}: insufficient/unsuitable EPS series for YoY")
            continue

        yoy, eps, eps_prev, fy, fy_prev = comp
        upsert_eps(sym, eps, eps_prev, fy, fy_prev, yoy)
        log.info(f"{sym}: EPS YoY={yoy:.2%} ({fy}:{eps} vs {fy_prev}:{eps_prev})")

if __name__ == "__main__":
    main()
