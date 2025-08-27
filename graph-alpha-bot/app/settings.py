import os
from dotenv import load_dotenv
load_dotenv()

NEO4J_URI = os.getenv("NEO4J_URI", "bolt://localhost:7687")
NEO4J_USER = os.getenv("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "livelongandprosper")

SNAPTRADE_CLIENT_ID = os.getenv("SNAPTRADE_CLIENT_ID", "KIPNERTER-TEST-RITEZ")
SNAPTRADE_CONSUMER_KEY = os.getenv("SNAPTRADE_CONSUMER_KEY", "6f3wzzZPt5eUHOc1q8yMacWhed1PjB6BydeYBZZsc0EWfmWBoy")
SNAPTRADE_SIGNATURE = os.getenv("SNAPTRADE_SIGNATURE", "")
SNAPTRADE_CONNECTION_ID = os.getenv("SNAPTRADE_CONNECTION_ID", "")

PLAID_CLIENT_ID = os.getenv("PLAID_CLIENT_ID", "")
PLAID_SECRET = os.getenv("PLAID_SECRET", "")
PLAID_ACCESS_TOKEN = os.getenv("PLAID_ACCESS_TOKEN", "")
