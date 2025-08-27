#!/usr/bin/env python3
import argparse
from neo4j import GraphDatabase
from app.settings import NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD

SCHEMA = open("app/graph/schema.cypher","r",encoding="utf-8").read()

INIT_CYPHER = """
MERGE (:Strategy {name:'MA_Crossover', type:'trend'});
MERGE (:Strategy {name:'Graph_Community_MR', type:'mean_reversion'});
MERGE (:Strategy {name:'NewsCentralityMomentum', type:'novel'});
MERGE (:Strategy {name:'SupplyChainShockDiffusion', type:'novel'});
MERGE (:Strategy {name:'InsiderClusterDrift', type:'novel'});
"""

def run_cypher(cypher: str):
    drv = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
    with drv.session() as s:
        for stmt in [x.strip() for x in cypher.split(';') if x.strip()]:
            s.run(stmt)
    drv.close()

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--init", action="store_true")
    args = ap.parse_args()
    if args.init:
        run_cypher(SCHEMA)
        run_cypher(INIT_CYPHER)
        print("Neo4j schema and base strategies initialized.")

if __name__ == "__main__":
    main()
