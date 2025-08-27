#!/usr/bin/env bash
set -euo pipefail

NEO4J_URI=${NEO4J_URI:-bolt://localhost:7687}
NEO4J_USER=${NEO4J_USER:-neo4j}
NEO4J_PASSWORD=${NEO4J_PASSWORD:-neo4j_password}

echo "Seeding constraints and base strategies into Neo4j..."
python app/graph/queries.py --init
