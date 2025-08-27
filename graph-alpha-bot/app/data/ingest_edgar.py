#!/usr/bin/env python3
import argparse, time, os, requests
from typing import List, Dict
from neo4j import GraphDatabase
from app.settings import NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD
from app.utils.logger import get_logger

log = get_logger("ingest_edgar")

UA = os.getenv("SEC_USER_AGENT", "youremail@example.com GraphAlphaBot/1.0")
BASE = "https://data.sec.gov/submissions/"

def company_submissions(cik: int) -> dict:
    url = f"{BASE}CIK{int(cik):010d}.json"
    r = requests.get(url, headers={"User-Agent": UA}, timeout=60)
    r.raise_for_status()
    return r.json()

def _normalize_recent(recent) -> List[Dict]:
    """
    Normalize the 'recent' filings into a list of dicts:
    [{form, accessionNumber, filingDate}, ...]
    Supports two shapes:
      1) dict of arrays: { 'form': [...], 'accessionNumber': [...], 'filingDate': [...] }
      2) list of dicts:  [ {'form': '10-K', 'accessionNumber': '...', 'filingDate': '...'}, ... ]
    Also tolerates alternate keys: filingType vs form, filed vs filingDate, accession_number vs accessionNumber.
    """
    out = []

    # Case 2: already a list of dicts
    if isinstance(recent, list):
        for row in recent:
            if not isinstance(row, dict):
                continue
            form = row.get("form") or row.get("filingType")
            acc  = row.get("accessionNumber") or row.get("accession_number")
            date = row.get("filingDate") or row.get("filed")
            if form and acc and date:
                out.append({"form": form, "accessionNumber": acc, "filingDate": date})
        return out

    # Case 1: dict of arrays
    if isinstance(recent, dict):
        forms = recent.get("form") or recent.get("filingType") or []
        accs  = recent.get("accessionNumber") or recent.get("accession_number") or []
        dates = recent.get("filingDate") or recent.get("filed") or []
        n = min(len(forms), len(accs), len(dates))
        for i in range(n):
            f, a, d = forms[i], accs[i], dates[i]
            if f and a and d:
                out.append({"form": f, "accessionNumber": a, "filingDate": d})
        return out

    # Unknown shape
    return out

def upsert_filings(tx, cik: int, filings: List[Dict]):
    for row in filings:
        tx.run(
            """
            MERGE (f:Filing {accession:$acc})
            SET f.type=$form, f.filed=$filed, f.cik=$cik
            """,
            acc=row["accessionNumber"], form=row["form"], filed=row["filingDate"], cik=int(cik)
        )

def load_company(cik: int):
    data = company_submissions(cik)
    recent = (data.get("filings") or {}).get("recent")
    if recent is None:
        log.warning(f"CIK {cik}: no 'recent' filings block present.")
        return

    rows = _normalize_recent(recent)
    if not rows:
        log.warning(f"CIK {cik}: could not parse any recent filings.")
        return

    # Write to Neo4j
    drv = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
    with drv.session() as s:
        s.execute_write(upsert_filings, cik, rows)
    drv.close()

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--cik", type=int, required=True, help="Company CIK (e.g., 320193 for Apple)")
    args = ap.parse_args()

    log.info(f"Fetching SEC submissions for CIK={args.cik}")
    try:
        load_company(args.cik)
        # polite pause if running in loops
        time.sleep(0.2)
    except requests.HTTPError as e:
        log.error(f"HTTP error from SEC for CIK {args.cik}: {e}")
    except Exception as e:
        log.error(f"Unhandled error for CIK {args.cik}: {e}")

if __name__ == "__main__":
    main()
