#!/usr/bin/env python3
"""Neo4j MCP Server - Provides graph database access for financial analysis.

This MCP server enables querying and updating Neo4j graphs containing:
- Ticker nodes with price history
- News articles with sentiment scores
- Strategy signals and trades  
- Entity relationships (correlations, supply chains, mentions)

Usage:
    python3 neo4j_mcp_server.py --uri bolt://localhost:7687 \
        --user neo4j --password <password> [--port 8080]

The server exposes an HTTP API for graph queries and updates.
"""

import sys, os, json, uuid, hashlib, threading
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple
from dataclasses import dataclass, field
from urllib.parse import parse_qs, urlparse
import argparse
import logging

try:
    from neo4j import GraphDatabase, basic_auth
except ImportError:
    print("Installing neo4j package...")
    os.system("pip install neo4j")
    from neo4j import GraphDatabase, basic_auth

# Configuration
@dataclass
class Config:
    neo4j_uri: str = "bolt://localhost:7687"
    neo4j_user: str = "neo4j"
    neo4j_password: str = ""
    http_port: int = 8080
    log_level: str = "INFO"

# Logging setup
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


class Neo4jGraphClient:
    """Thread-safe wrapper around Neo4j driver with transaction support."""
    
    def __init__(self, uri: str, user: str, password: str):
        self.driver = GraphDatabase.driver(uri, auth=basic_auth(user, password))
        self._lock = threading.Lock()
        
    def close(self):
        self.driver.close()
    
    def execute_query(self, query: str, params: Optional[dict] = None) -> List[dict]:
        """Execute a read query and return results as list of dicts."""
        with self.driver.session() as session:
            try:
                result = session.run(query, params or {})
                return [record.data() for record in result]
            except Exception as e:
                logging.error(f"Query error: {e}")
                raise
    
    def execute_write(self, mutation: str, params: Optional[dict] = None) -> dict:
        """Execute a write transaction and return summary."""
        with self.driver.session() as session:
            try:
                result = session.run(mutation, params or {})
                
                # Extract useful counts from Cypher
                summary = {"success": True}
                if hasattr(result, '_result_summary'):
                    summary["_summary"] = str(result._result_summary)
                return summary
            except Exception as e:
                logging.error(f"Write error: {e}")
                return {"success": False, "error": str(e)}


class Neo4jMCPServer:
    """MCP-compatible server for Neo4j graph operations."""
    
    def __init__(self, config: Config):
        self.config = config
        self.client: Optional[Neo4jGraphClient] = None
        
    def connect(self) -> bool:
        """Establish connection to Neo4j and validate."""
        try:
            self.client = Neo4jGraphClient(
                self.config.neo4j_uri,
                self.config.neo4j_user,
                self.config.neo4j_password
            )
            
            # Test connection
            self.client.execute_query("RETURN 1 as test")
            logging.info(f"Connected to Neo4j at {self.config.neo4j_uri}")
            return True
        except Exception as e:
            logging.error(f"Failed to connect: {e}")
            return False
    
    def get_graph_stats(self) -> dict:
        """Return graph statistics."""
        if not self.client:
            return {"error": "Not connected"}
        
        queries = {
            "ticker_count": """MATCH (t:Ticker) RETURN count(t) as cnt""",
            "bar_count": """MATCH (b:PriceBar) RETURN count(b) as cnt""",
            "news_count": """MATCH (n:News) RETURN count(n) as cnt""",
            "signal_count": """MATCH (s:Signal) RETURN count(s) as cnt""",
        }
        
        stats = {}
        for name, query in queries.items():
            try:
                result = self.client.execute_query(query)
                if result:
                    stats[name] = result[0].get('cnt', 0)
            except Exception as e:
                logging.warning(f"Failed to get {name}: {e}")
        
        return {"stats": stats, "connected": bool(self.client)}
    
    def query_tickers(
        self, 
        symbols: Optional[List[str]] = None,
        limit: int = 100
    ) -> List[dict]:
        """Query ticker nodes."""
        if not self.client:
            return []
        
        conditions = "AND t.symbol IN $symbols" if symbols else ""
        query = f"""
        MATCH (t:Ticker)
        WHERE {conditions}
        RETURN t.symbol as symbol, 
               labels(t) as labels,
               t.capitalization as cap,
               t.sector as sector,
               t.exchange as exchange
        ORDER BY t.symbol
        LIMIT $limit
        """
        return self.client.execute_query(query, {"symbols": symbols or [], "limit": limit})
    
    def query_price_history(
        self, 
        symbol: str,
        days: int = 30,
        limit: int = 100
    ) -> List[dict]:
        """Query price bars for a ticker."""
        if not self.client:
            return []
        
        query = f"""
        MATCH (t:Ticker {{symbol:$symbol}})-[:HAS_PRICE]->(b:PriceBar)
        WHERE b.date >= datetime() - duration('{{days:$days}}')
        RETURN b.date as date, b.open as open, b.high as high, 
               b.low as low, b.close as close, b.volume as volume
        ORDER BY b.date DESC
        LIMIT $limit
        """
        return self.client.execute_query(query, {"symbol": symbol, "days": days, "limit": limit})
    
    def query_news_for_ticker(
        self,
        symbol: str,
        days: int = 7,
        limit: int = 50
    ) -> List[dict]:
        """Query news articles mentioning a ticker."""
        if not self.client:
            return []
        
        query = f"""
        MATCH (t:Ticker {{symbol:$symbol}})<-[:MENTIONED_IN]-(n:News)
        WHERE n.timestamp >= datetime() - duration('{{days:$days}}')
        RETURN n.title as title, n.source as source,
               n.sentiment as sentiment, n.score as score,
               n.timestamp as timestamp
        ORDER BY n.timestamp DESC
        LIMIT $limit
        """
        return self.client.execute_query(query, {"symbol": symbol, "days": days, "limit": limit})
    
    def query_sentiment_trend(
        self,
        symbol: str,
        period_days: int = 30
    ) -> List[dict]:
        """Get sentiment trend over time for a ticker."""
        if not self.client:
            return []
        
        query = f"""
        MATCH (t:Ticker {{symbol:$symbol}})<-[:MENTIONED_IN]-(n:News)
        WHERE n.timestamp >= datetime() - duration('{{periodDays:$periodDays}}')
        WITH t, n, trunc(n.timestamp, 'day') as day
        RETURN day as date,
               avg(n.score) as avg_score,
               min(n.score) as min_score,
               max(n.score) as max_score,
               count(n) as article_count
        ORDER BY date DESC
        """
        return self.client.execute_query(query, {"symbol": symbol, "periodDays": period_days})
    
    def query_correlation_pairs(
        self,
        symbols: Optional[List[str]] = None,
        min_correlation: float = 0.7
    ) -> List[dict]:
        """Find highly correlated ticker pairs."""
        if not self.client:
            return []
        
        condition = "AND t1.symbol IN $symbols AND t2.symbol IN $symbols" if symbols else ""
        query = f"""
        MATCH (t1:Ticker){condition}-[:CORRELATED_WITH]->(t2:Ticker)
        WHERE t1.symbol < t2.symbol  // Avoid duplicates
              AND t1.correlationScore >= $minCorr
        RETURN t1.symbol as ticker1, t2.symbol as ticker2,
               t1.correlationScore as correlation,
               labels(t1)[1] as sector1, labels(t2)[1] as sector2
        ORDER BY t1.correlationScore DESC
        LIMIT 50
        """
        return self.client.execute_query(
            query, {"symbols": symbols or [], "minCorr": min_correlation}
        )
    
    def create_signal(
        self,
        strategy: str,
        symbol: str,
        score: float,
        direction: str = "long",
        meta: Optional[dict] = None
    ) -> dict:
        """Create a new trading signal."""
        if not self.client:
            return {"error": "Not connected"}
        
        signal_id = f"{strategy}-{symbol}-{uuid.uuid4().hex[:8]}"
        query = """
        MERGE (s:Strategy {name:$strategy})
        MERGE (t:Ticker {symbol:$symbol})
        MERGE (sig:Signal {id:$signalId})
        SET sig.ts = datetime(),
            sig.score = $score,
            sig.direction = $direction,
            sig.meta = $meta
        MERGE (s)-[:GENERATED]->(sig)-[:FOR]->(t)
        """
        
        return self.client.execute_write(query, {
            "strategy": strategy,
            "symbol": symbol,
            "signalId": signal_id,
            "score": score,
            "direction": direction,
            "meta": meta or {}
        })


class HTTPHandler:
    """Simple HTTP server for MCP requests."""
    
    def __init__(self, server: Neo4jMCPServer):
        self.server = server
        
    def handle_request(self, method: str, path: str, query_params: dict, body: Optional[bytes]) -> Tuple[int, dict]:
        """Handle an HTTP request and return response."""
        
        if method == "OPTIONS":
            return 200, {"status": "ok"}
        
        try:
            if path == "/health":
                stats = self.server.get_graph_stats()
                return 200, stats
            
            elif path == "/query/tickers" and method == "GET":
                symbols = query_params.get("symbols", [])
                limit = int(query_params.get("limit", [100])[0])
                result = self.server.query_tickers(symbols=symbols, limit=limit)
                return 200, {"results": result}
            
            elif path == "/query/price" and method == "GET":
                symbol = query_params.get("symbol", [None])[0]
                days = int(query_params.get("days", [30])[0])
                limit = int(query_params.get("limit", [100])[0])
                if not symbol:
                    return 400, {"error": "Missing symbol parameter"}
                result = self.server.query_price_history(symbol, days, limit)
                return 200, {"results": result}
            
            elif path == "/query/news" and method == "GET":
                symbol = query_params.get("symbol", [None])[0]
                days = int(query_params.get("days", [7])[0])
                if not symbol:
                    return 400, {"error": "Missing symbol parameter"}
                result = self.server.query_news_for_ticker(symbol, days)
                return 200, {"results": result}
            
            elif path == "/query/sentiment" and method == "GET":
                symbol = query_params.get("symbol", [None])[0]
                if not symbol:
                    return 400, {"error": "Missing symbol parameter"}
                result = self.server.query_sentiment_trend(symbol)
                return 200, {"results": result}
            
            elif path == "/query/correlations" and method == "GET":
                min_corr = float(query_params.get("min_correlation", [0.7])[0])
                result = self.server.query_correlation_pairs(min_correlation=min_corr)
                return 200, {"results": result}
            
            elif path == "/signal/create" and method == "POST":
                body_dict = json.loads(body) if body else {}
                result = self.server.create_signal(
                    strategy=body_dict.get("strategy"),
                    symbol=body_dict.get("symbol"),
                    score=float(body_dict.get("score", 0)),
                    direction=body_dict.get("direction", "long"),
                    meta=body_dict.get("meta")
                )
                return 201, result
            
            else:
                return 404, {"error": "Not found"}
        
        except Exception as e:
            logging.exception(f"Request handler error: {e}")
            return 500, {"error": str(e)}


def main():
    parser = argparse.ArgumentParser(description="Neo4j MCP Server for Financial Analysis")
    parser.add_argument("--uri", default=os.getenv("NEO4J_URI", "bolt://localhost:7687"), help="Neo4j connection URI")
    parser.add_argument("--user", default=os.getenv("NEO4J_USER", "neo4j"), help="Neo4j username")
    parser.add_argument("--password", default=os.getenv("NEO4J_PASSWORD", ""), help="Neo4j password")
    parser.add_argument("--port", type=int, default=8080, help="HTTP server port")
    
    args = parser.parse_args()
    
    config = Config(
        neo4j_uri=args.uri,
        neo4j_user=args.user,
        neo4j_password=args.password,
        http_port=args.port
    )
    
    print(f"Starting Neo4j MCP Server on port {args.port}")
    print(f"Neo4j URI: {args.uri}")
    
    server = Neo4jMCPServer(config)
    if not server.connect():
        sys.exit(1)
    
    handler = HTTPHandler(server)
    
    # Simple in-memory HTTP server (for demo purposes; use gunicorn/uwsgi for production)
    from http.server import HTTPServer, BaseHTTPRequestHandler
    import urllib.parse
    
    class MCPHandler(BaseHTTPRequestHandler):
        def log_message(self, format, *args):
            logging.info("%s - %s", args[0], args[1])
        
        def send_json_response(self, status: int, data: dict):
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps(data).encode())
        
        def do_OPTIONS(self):
            self.send_response(200)
            self.send_header("Access-Control-Allow-Origin", "*")
            self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
            self.end_headers()
        
        def do_GET(self):
            parsed = urlparse(self.path)
            params = parse_qs(parsed.query)
            status, response = handler.handle_request("GET", parsed.path, params, None)
            self.send_json_response(status, response)
        
        def do_POST(self):
            parsed = urlparse(self.path)
            content_length = int(self.headers.get('Content-Length', 0))
            body = self.rfile.read(content_length) if content_length > 0 else b""
            params = parse_qs(parsed.query)
            status, response = handler.handle_request("POST", parsed.path, params, body)
            self.send_json_response(status, response)
    
    server_address = ('', args.port)
    http_server = HTTPServer(server_address, MCPHandler)
    print(f"HTTP Server running on port {args.port}")
    print("Endpoints:")
    print("  GET  /health              - Check connection and graph stats")
    print("  GET  /query/tickers?symbols=BTC-USD,ETH-USD&limit=100          - List tickers")
    print("  GET  /query/price?symbol=BTC-USD&days=30                       - Price history")
    print("  GET  /query/news?symbol=BTC-USD&days=7                         - News mentions")
    print("  GET  /query/sentiment?symbol=BTC-USD                           - Sentiment trend")
    print("  GET  /query/correlations?min_correlation=0.7                   - Correlated pairs")
    print("  POST /signal/create                                          - Create trading signal")
    
    try:
        http_server.serve_forever()
    except KeyboardInterrupt:
        print("\nShutting down...")
        server.client.close()


if __name__ == "__main__":
    main()
