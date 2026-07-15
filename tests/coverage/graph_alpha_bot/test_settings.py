from app import settings as s


def test_settings_values():
    assert s.NEO4J_USER == "neo4j"
    assert isinstance(s.NEO4J_URI, str) and s.NEO4J_URI
    assert isinstance(s.NEO4J_PASSWORD, str)
    assert isinstance(s.SNAPTRADE_CLIENT_ID, str)
    assert isinstance(s.SNAPTRADE_CONSUMER_KEY, str)
    assert isinstance(s.SNAPTRADE_SIGNATURE, str)
    assert isinstance(s.SNAPTRADE_CONNECTION_ID, str)
    assert isinstance(s.PLAID_CLIENT_ID, str)
    assert isinstance(s.PLAID_SECRET, str)
    assert isinstance(s.PLAID_ACCESS_TOKEN, str)
