from unittest.mock import MagicMock, patch
from app.db.neo4j_connection import Neo4jConnection, get_connection


def _make_driver(healthy=True, raises=False):
    driver = MagicMock()
    if raises:
        driver.session.side_effect = RuntimeError("conn fail")
    else:
        sess = MagicMock()
        sess.run.return_value.single.return_value = (1,) if healthy else None
        driver.session.return_value.__enter__.return_value = sess
    return driver


def test_init_success():
    with patch("app.db.neo4j_connection.GraphDatabase.driver", return_value=_make_driver(True)):
        conn = Neo4jConnection(uri="bolt://x", user="u", password="p")
    assert conn.use_fallback is False
    assert conn.driver is not None


def test_init_failure_fallback():
    driver = MagicMock()
    driver.session.side_effect = RuntimeError("conn bad")
    with patch("app.db.neo4j_connection.GraphDatabase.driver", return_value=driver):
        conn = Neo4jConnection(uri="bolt://x", user="u", password="p")
    assert conn.use_fallback is True
    assert conn.driver is not None


def test_is_healthy_fallback():
    conn = Neo4jConnection.__new__(Neo4jConnection)
    conn.use_fallback = True
    conn.driver = None
    assert conn.is_healthy() is False


def test_is_healthy_true():
    drv = _make_driver(True)
    conn = Neo4jConnection.__new__(Neo4jConnection)
    conn.use_fallback = False
    conn.driver = drv
    conn.database = "neo4j"
    assert conn.is_healthy() is True


def test_is_healthy_false_on_error():
    drv = _make_driver(False)
    conn = Neo4jConnection.__new__(Neo4jConnection)
    conn.use_fallback = False
    conn.driver = drv
    conn.database = "neo4j"
    assert conn.is_healthy() is False


def test_execute_query_fallback():
    conn = Neo4jConnection.__new__(Neo4jConnection)
    conn.use_fallback = True
    conn.driver = None
    assert conn.execute_query("MATCH (n) RETURN n") == []


def test_execute_query_success():
    drv = _make_driver(True)
    rec = MagicMock()
    rec.values.return_value = ["BTC-USD", 0.9]
    sess = MagicMock()
    sess.run.return_value = [rec]
    drv.session.return_value.__enter__.return_value = sess
    conn = Neo4jConnection.__new__(Neo4jConnection)
    conn.use_fallback = False
    conn.driver = drv
    conn.database = "neo4j"
    out = conn.execute_query("Q", {"a": 1})
    assert out == [["BTC-USD", 0.9]]


def test_execute_query_error_sets_fallback():
    drv = _make_driver(True)
    sess = MagicMock()
    sess.run.side_effect = RuntimeError("query boom")
    drv.session.return_value.__enter__.return_value = sess
    conn = Neo4jConnection.__new__(Neo4jConnection)
    conn.use_fallback = False
    conn.driver = drv
    conn.database = "neo4j"
    assert conn.execute_query("Q") == []
    assert conn.use_fallback is True


def test_execute_query_limit_truncation():
    drv = _make_driver(True)
    recs = [MagicMock() for _ in range(5)]
    for r in recs:
        r.values.return_value = [r]
    sess = MagicMock()
    sess.run.return_value = recs
    drv.session.return_value.__enter__.return_value = sess
    conn = Neo4jConnection.__new__(Neo4jConnection)
    conn.use_fallback = False
    conn.driver = drv
    conn.database = "neo4j"
    out = conn.execute_query("Q", limit=2)
    assert len(out) == 2


def test_execute_write_fallback():
    conn = Neo4jConnection.__new__(Neo4jConnection)
    conn.use_fallback = True
    conn.driver = None
    assert conn.execute_write("C", lambda: None) is None


def test_execute_write_success():
    drv = _make_driver(True)
    sess = MagicMock()
    sess.execute_write.return_value = "ok"
    drv.session.return_value.__enter__.return_value = sess
    conn = Neo4jConnection.__new__(Neo4jConnection)
    conn.use_fallback = False
    conn.driver = drv
    conn.database = "neo4j"
    assert conn.execute_write("C", lambda s, **k: "ok", x=1) == "ok"


def test_execute_write_error():
    drv = _make_driver(True)
    sess = MagicMock()
    sess.execute_write.side_effect = RuntimeError("write boom")
    drv.session.return_value.__enter__.return_value = sess
    conn = Neo4jConnection.__new__(Neo4jConnection)
    conn.use_fallback = False
    conn.driver = drv
    conn.database = "neo4j"
    assert conn.execute_write("C", lambda s: None) is None
    assert conn.use_fallback is True


def test_get_connection():
    with patch("app.db.neo4j_connection.Neo4jConnection") as NC:
        get_connection(uri="bolt://u", user="a", password="b")
        NC.assert_called_once()
