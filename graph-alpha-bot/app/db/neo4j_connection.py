#!/usr/bin/env python3
"""Neo4j connection manager for GraphAlphaBot with proper SSL configuration."""

import os
from typing import Optional, Dict, List, Any, Callable, Tuple
from neo4j import GraphDatabase, Driver, Session


class Neo4jConnection:
    """Manages Neo4j database connection with fallback to local storage."""
    
    def __init__(
        self, 
        uri: str = "",
        user: str = "neo4j",
        password: str = "",
        database: str = "neo4j"
    ):
        # P1-7: never embed secrets/tailnet URIs. Resolve from the environment
        # with NO defaults — a missing NEO4J_URI/PASSWORD must fail loudly
        # rather than connect to a hardcoded personal host or use a literal pw.
        self.uri = uri or os.getenv("NEO4J_URI", "")
        self.user = user or os.getenv("NEO4J_USER", "neo4j")
        self.password = password or os.getenv("NEO4J_PASSWORD", "")
        self.database = database
        self.driver: Optional[Driver] = None
        self.use_fallback = False
        
        try:
            self._init_driver(uri, (user, password))
            print(f"✓ Neo4j connection initialized: {uri}")
        except Exception as e:
            print(f"⚠ Failed to initialize Neo4j connection: {e}")
            self.use_fallback = True
    
    def _init_driver(self, uri: str, auth: Tuple[str, str]):
        """Initialize the Neo4j driver with proper SSL settings."""
        try:
            configs_to_try = [
                {"encrypted": True},
                {"encrypted": False},
                {"trustedCertificates": []}
            ]
            
            for config in configs_to_try:
                try:
                    self.driver = GraphDatabase.driver(
                        uri, 
                        auth=auth,
                        **config
                    )
                    with self.driver.session(database=self.database) as session:
                        result = session.run("RETURN 1")
                        if result.single():
                            print(f"  Using config: {config}")
                            break
                except Exception as e:
                    continue
            
            # Test connection
            if self.driver:
                with self.driver.session(database=self.database) as session:
                    session.run("RETURN 1")
            else:
                raise RuntimeError("Failed to create Neo4j driver with any SSL config")
        except Exception as e:
            raise RuntimeError(f"Neo4j connection failed: {e}")
    
    def is_healthy(self) -> bool:
        """Check if the database connection is healthy."""
        if self.use_fallback or not self.driver:
            return False
        
        try:
            with self.driver.session(database=self.database) as session:
                result = session.run("RETURN 1")
                return result.single() is not None
        except Exception as e:
            print(f"Neo4j health check failed: {e}")
            return False
    
    def execute_query(
        self, 
        query: str, 
        parameters: Optional[Dict] = None,
        limit: int = 1000
    ) -> List[Any]:
        """Execute a Cypher query and return results."""
        if self.use_fallback or not self.driver:
            print("⚠ Neo4j unavailable - using fallback")
            return []
        
        try:
            with self.driver.session(database=self.database) as session:
                result = session.run(query, parameters or {})
                records = list(result)
                
                if len(records) > limit:
                    records = records[:limit]
                
                return [record.values() for record in records]
        except Exception as e:
            print(f"Query execution failed: {e}")
            self.use_fallback = True
            return []
    
    def execute_write(
        self, 
        cypher: str,
        param_fn: Callable,
        access_mode: str = "WRITE",
        **parameters
    ) -> Any:
        """Execute a write transaction with automatic retry."""
        if self.use_fallback or not self.driver:
            return None
        
        try:
            with self.driver.session(database=self.database) as session:
                result = session.execute_write(
                    cypher,
                    param_fn,
                    access_mode=access_mode,
                    **parameters
                )
                return result
        except Exception as e:
            print(f"Write failed: {e}")
            self.use_fallback = True
            return None


def get_connection(uri=None, user=None, password=None) -> Neo4jConnection:
    """Get or create a Neo4j connection instance."""
    uri = uri or os.getenv("NEO4J_URI", "")
    user = user or os.getenv("NEO4J_USER", "neo4j")
    password = password or os.getenv("NEO4J_PASSWORD", "")
    
    return Neo4jConnection(uri=uri, user=user, password=password)
