from unittest.mock import MagicMock

import neo4j

import app.db.neo4j_connection as m
from app.db.neo4j_connection import Neo4jConnection, get_connection


def _healthy_session():
    sess = MagicMock()

    def run_effect(q, *a, **k):
        if "RETURN 1" in q:
            r = MagicMock()
            r.single.return_value = ("ok",)
            return r
        r = MagicMock()
        r.__iter__.return_value = iter(
            [MagicMock(values=lambda: [1, 2]), MagicMock(values=lambda: [3, 4])]
        )
        return r

    sess.run.side_effect = run_effect
    return sess


def test_connection_healthy(monkeypatch):
    sess = _healthy_session()
    driver = MagicMock()
    driver.session.return_value.__enter__.return_value = sess
    driver.session.return_value.__exit__.return_value = False
    monkeypatch.setattr(neo4j.GraphDatabase, "driver", lambda *a, **k: driver)
    conn = Neo4jConnection()
    assert conn.use_fallback is False
    assert conn.is_healthy() is True
    recs = conn.execute_query("MATCH (n) RETURN n")
    assert recs == [[1, 2], [3, 4]]
    conn.execute_write("CYPHER", lambda tx: None)
    assert isinstance(get_connection(), Neo4jConnection)


def test_connection_fallback(monkeypatch):
    def boom(*a, **k):
        raise RuntimeError("cannot connect")

    monkeypatch.setattr(neo4j.GraphDatabase, "driver", boom)
    conn = Neo4jConnection()
    assert conn.use_fallback is True
    assert conn.is_healthy() is False
    assert conn.execute_query("MATCH (n) RETURN n") == []
    assert conn.execute_write("CYPHER", lambda tx: None) is None


def test_query_exception_falls_back(monkeypatch):
    sess = MagicMock()

    def run_effect(q, *a, **k):
        if "RETURN 1" in q:
            r = MagicMock()
            r.single.return_value = ("ok",)
            return r
        raise RuntimeError("query failed")

    sess.run.side_effect = run_effect
    driver = MagicMock()
    driver.session.return_value.__enter__.return_value = sess
    driver.session.return_value.__exit__.return_value = False
    monkeypatch.setattr(neo4j.GraphDatabase, "driver", lambda *a, **k: driver)
    conn = Neo4jConnection()
    assert conn.use_fallback is False
    # real query raises -> falls back to []
    assert conn.execute_query("MATCH (n) RETURN n") == []
    assert conn.use_fallback is True


def test_init_driver_tries_multiple_configs(monkeypatch):
    sess = MagicMock()
    calls = {"n": 0}

    def run_effect(q, *a, **k):
        if "RETURN 1" in q:
            calls["n"] += 1
            r = MagicMock()
            # first config validation fails, subsequent ones succeed
            r.single.return_value = None if calls["n"] == 1 else ("ok",)
            return r
        r = MagicMock()
        r.__iter__.return_value = iter([MagicMock(values=lambda: [1, 2])])
        return r

    sess.run.side_effect = run_effect
    driver = MagicMock()
    driver.session.return_value.__enter__.return_value = sess
    driver.session.return_value.__exit__.return_value = False
    monkeypatch.setattr(neo4j.GraphDatabase, "driver", lambda *a, **k: driver)
    conn = Neo4jConnection()
    assert conn.use_fallback is False
    assert calls["n"] >= 2  # tried more than one SSL config


def test_execute_query_truncates(monkeypatch):
    sess = MagicMock()

    def run_effect(q, *a, **k):
        if "RETURN 1" in q:
            r = MagicMock()
            r.single.return_value = ("ok",)
            return r
        r = MagicMock()
        r.__iter__.return_value = iter([MagicMock(values=lambda i=i: [i]) for i in range(1500)])
        return r

    sess.run.side_effect = run_effect
    driver = MagicMock()
    driver.session.return_value.__enter__.return_value = sess
    driver.session.return_value.__exit__.return_value = False
    monkeypatch.setattr(neo4j.GraphDatabase, "driver", lambda *a, **k: driver)
    conn = Neo4jConnection()
    recs = conn.execute_query("MATCH (n) RETURN n", limit=1000)
    assert len(recs) == 1000


def test_is_healthy_failure(monkeypatch):
    sess = MagicMock()
    calls = {"n": 0}

    def run_effect(q, *a, **k):
        if "RETURN 1" in q:
            calls["n"] += 1
            if calls["n"] == 1:  # constructor validation succeeds
                r = MagicMock()
                r.single.return_value = ("ok",)
                return r
            raise RuntimeError("health check failed")  # health check raises
        r = MagicMock()
        r.__iter__.return_value = iter([])
        return r

    sess.run.side_effect = run_effect
    driver = MagicMock()
    driver.session.return_value.__enter__.return_value = sess
    driver.session.return_value.__exit__.return_value = False
    monkeypatch.setattr(neo4j.GraphDatabase, "driver", lambda *a, **k: driver)
    conn = Neo4jConnection()
    assert conn.is_healthy() is False


def test_write_exception_falls_back(monkeypatch):
    sess = MagicMock()

    def run_effect(q, *a, **k):
        if "RETURN 1" in q:
            r = MagicMock()
            r.single.return_value = ("ok",)
            return r
        r = MagicMock()
        r.list.return_value = []
        return r

    sess.run.side_effect = run_effect
    driver = MagicMock()
    driver.session.return_value.__enter__.return_value = sess
    driver.session.return_value.__exit__.return_value = False
    driver.session.return_value.__enter__.return_value.execute_write.side_effect = RuntimeError("write failed")
    monkeypatch.setattr(neo4j.GraphDatabase, "driver", lambda *a, **k: driver)
    conn = Neo4jConnection()
    assert conn.execute_write("CYPHER", lambda tx: None) is None
    assert conn.use_fallback is True
