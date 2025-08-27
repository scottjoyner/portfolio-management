#!/usr/bin/env python3
import argparse, time, requests
from neo4j import GraphDatabase
from app.settings import NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD
from app.utils.logger import get_logger
log = get_logger("ingest_edgar")

HEADERS = {"User-Agent": "research@example.com"}  # Replace with your contact info

def company_submissions(cik: int):
    url = f"https://data.sec.gov/submissions/CIK{int(cik):010d}.json"
    r = requests.get(url, headers=HEADERS, timeout=30); r.raise_for_status()
    return r.json()

def upsert_filings(tx, cik, filings):
    for ftype, acc, filed in zip(filings["filingType"], filings["accessionNumber"], filings["filingDate"]):
        tx.run(
            """
            MERGE (f:Filing {accession:$acc})
            SET f.type=$ftype, f.filed=$filed, f.cik=$cik
            """, acc=acc, ftype=ftype, filed=filed, cik=int(cik)
        )

def load_company(cik):
    data = company_submissions(cik)
    filings = data.get("filings", {}).get("recent", {})
    drv = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
    with drv.session() as s:
        s.execute_write(upsert_filings, cik, filings)
    drv.close()

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--cik", type=int, required=True)
    args = ap.parse_args()
    log.info(f"Fetching SEC submissions for CIK={args.cik}")
    load_company(args.cik)
    time.sleep(0.2)

if __name__ == "__main__":
    main()
