#!/usr/bin/env python3
"""HTTP-based Neo4j MCP Server - works without neo4j Python package.

This server uses HTTP requests to communicate with Neo4j via the Bolt protocol
proxy (typically provided by a reverse proxy or ngrok tunnel).

Requires: Python 3.x, urllib (stdlib only)

Usage:
    python3 mcp_server_http.py --port 8080 --neo4j-uri bolt://x1-370:7687
"""

import sys, os, json, uuid, base64
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlencode, urljoin
from http.server import HTTPServer, BaseHTTPRequestHandler
import threading
import urllib.request
import urllib.error

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass


class Config:
    neo4j_uri: str = os.getenv("NEO4J_URI", "bolt://x1-370:7687")
    neo4j_user: str = os.getenv("NEO4J_USER", "neo4j")
    neo4j_password: str = os.getenv("NEO4J_PASSWORD", "gluhlaf8")
    
    http_port: int = int(os.getenv("HTTP_PORT", "8080"))


class GraphDatabaseProxy:
    """Proxy for Neo4j operations using HTTP requests."""
    
    def __init__(self, uri: str, user: str, password: str):
        self.uri = uri.replace("bolt://", "https://").rstrip("/")
        self.user = user
        self.password = password
        
    def _make_request(
        self, 
        endpoint: str, 
        params: Dict[str, Any] = None,
        method: str = "POST"
    ) -> dict:
        """Make HTTP request to Neo4j proxy."""
        
        url = f"{self.uri}/api/v1/{endpoint}"
        headers = {
            "Authorization": f"Basic {base64.b64encode(f'{self.user}:{self.password}'.encode()).decode()}",
            "Content-Type": "application/json",
            "User-Agent": "Neo4jMCP/1.0"
        }
        
        try:
            if method == "POST":
                data = json.dumps(params or {}).encode("utf-8")
            else:
                url += "?" + urlencode(params) if params else ""
                data = None
            
            req = urllib.request.Request(url, data=data, headers=headers, method=method)
            
            with urllib.request.urlopen(req, timeout=30) as response:
                return json.loads(response.read().decode())
                
        except urllib.error.HTTPError as e:
            return {"error": str(e), "status_code": e.code}
        except Exception as e:
            return {"error": str(e)}
    
    def session(self):
        """Create a new session context."""
        return SessionProxy(self)


class SessionProxy:
    """Session proxy for running queries."""
    
    def __init__(self, db_proxy: GraphDatabaseProxy):
        self.db_proxy = db_proxy
        
    def run(self, cypher: str, parameters: Dict[str, Any] = None):
        """Run a Cypher query."""
        
        params = {
            "cypher": cypher,
            "parameters": parameters or {}
        }
        
        result = self.db_proxy._make_request("session/run", params)
        
        if "error" in result:
            raise Exception(result["error"])
            
        return QueryResult(result.get("results", []), result.get("summary", {}))
    
    def close(self):
        """Close the session."""
        self.db_proxy._make_request("session/close", {}, method="POST")


class QueryResult:
    """Query result wrapper."""
    
    def __init__(self, results: List[dict], summary: dict):
        self.records = [Record(row) for row in results]
        
    def fetch_all(self):
        return list(self.records)
    
    def single(self):
        return self.records[0] if self.records else None
    
    def __iter__(self):
        return iter(self.records)


class Record:
    """Single query result record."""
    
    def __init__(self, data: dict):
        self._data = data
        
    def get(self, key: str, default=None):
        return self._data.get(key, default)
    
    def keys(self):
        return self._data.keys()


def create_graph_database(uri: str, user: str, password: str) -> GraphDatabaseProxy:
    """Create a GraphDatabase instance."""
    return GraphDatabaseProxy(uri, user, password)
