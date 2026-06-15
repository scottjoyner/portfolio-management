#!/usr/bin/env python3
"""MCP Server Main - Starts Neo4j connection and graph query endpoints."""

import sys, os, json, argparse
from datetime import datetime
sys.path.insert(0, '/home/scott/git/portfolio-management/graph-alpha-bot')

from mcp_server_http import create_graph_database, Config

def main():
    parser = argparse.ArgumentParser(description="Neo4j MCP Server for graph queries")
    parser.add_argument("--uri", default=None, help="Neo4j Bolt URI")
    parser.add_argument("--user", default=None, help="Neo4j username")
    parser.add_argument("--password", default=None, help="Neo4j password")
    parser.add_argument("--port", type=int, default=8080, help="HTTP server port")
    
    args = parser.parse_args()
    
    # Apply overrides from command line
    if args.uri: os.environ["NEO4J_URI"] = args.uri
    if args.user: os.environ["NEO4J_USER"] = args.user
    if args.password: os.environ["NEO4J_PASSWORD"] = args.password
    
    print(f"Starting MCP Server...")
    print(f"Neo4j URI: {os.getenv('NEO4J_URI', 'bolt://x1-370:7687')}")
    print(f"HTTP Port: {args.port}")
    print("Press Ctrl+C to stop.")
    
    # Test Neo4j connection
    try:
        db = create_graph_database(
            os.getenv("NEO4J_URI", "bolt://x1-370.tailcb8954.ts.net:7687"),
            os.getenv("NEO4J_USER", "neo4j"),
            os.getenv("NEO4J_PASSWORD", "gluhlaf8")
        )
        
        # Test basic query
        session = db.session()
        result = session.run("RETURN 1 as test", {})
        print(f"Neo4j connection successful! Test query returned: {result.fetch_all()}")
        session.close()
        
    except Exception as e:
        print(f"Warning: Could not connect to Neo4j: {e}")
        print("Server will start but graph queries may fail.")
    
    # For now, just exit - the actual HTTP server would need more code
    # This is a placeholder that demonstrates the connection works


if __name__ == "__main__":
    main()
