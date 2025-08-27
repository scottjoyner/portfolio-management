from abc import ABC, abstractmethod
from typing import List
from neo4j import GraphDatabase
from app.settings import NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD

class Strategy(ABC):
    name: str = "Base"
    def __init__(self):
        self.driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))

    def session(self):
        return self.driver.session()

    @abstractmethod
    def generate(self, symbols: List[str]) -> int:
        """Return number of signals generated."""

    def close(self):
        self.driver.close()
