from unittest.mock import MagicMock, patch
from app.graph.queries import run_cypher, main


def _driver():
    driver = MagicMock()
    sess = MagicMock()
    sess.__enter__.return_value = sess
    sess.__exit__.return_value = False
    driver.session.return_value = sess
    return driver, sess


def test_run_cypher_splits_statements():
    driver, sess = _driver()
    with patch("app.graph.queries.GraphDatabase.driver", return_value=driver):
        run_cypher("MATCH (n) RETURN n; CREATE (m);")
    assert sess.run.call_count == 2
    sess.run.assert_any_call("MATCH (n) RETURN n")
    sess.run.assert_any_call("CREATE (m)")


def test_run_cypher_empty():
    driver, sess = _driver()
    with patch("app.graph.queries.GraphDatabase.driver", return_value=driver):
        run_cypher("   ;  ; ")
    sess.run.assert_not_called()


def test_main_init(monkeypatch):
    monkeypatch.setattr("sys.argv", ["queries", "--init"])
    driver, sess = _driver()
    with patch("app.graph.queries.GraphDatabase.driver", return_value=driver):
        main()
    assert sess.run.called


def test_main_no_init(monkeypatch):
    monkeypatch.setattr("sys.argv", ["queries"])
    driver, sess = _driver()
    with patch("app.graph.queries.GraphDatabase.driver", return_value=driver):
        main()
    sess.run.assert_not_called()
