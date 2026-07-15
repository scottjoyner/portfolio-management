import json
from datetime import datetime
from unittest.mock import MagicMock, patch

import app.data.news_ingest_simple as m
from app.data.news_ingest_simple import SimpleNewsIngestor, Config


def _resp(status, text=""):
    r = MagicMock()
    r.status_code = status
    r.text = text
    return r


def test_load_feeds():
    ing = SimpleNewsIngestor()
    feeds = ing.load_feeds()
    assert len(feeds) == 3
    assert all(isinstance(f, tuple) and len(f) == 2 for f in feeds)


def test_fetch_articles_success_and_failure():
    ing = SimpleNewsIngestor()
    xml = "<rss><channel><item><title>t</title><link>u</link></item></channel></rss>"
    good = _resp(200, xml)
    bad = _resp(500, "")
    with patch.object(m.requests, "get", side_effect=[good, RuntimeError("net"), bad]):
        arts = ing.fetch_articles()
    # only the first feed yields an article
    assert len(arts) == 1
    assert arts[0]["title"] == "t"


def test_parse_rss_valid():
    ing = SimpleNewsIngestor()
    xml = "<rss><channel><item><title>t</title><link>u</link><pubDate>Mon, 01 Jan 2024 00:00:00 GMT</pubDate></item></channel></rss>"
    arts = ing.parse_rss(xml, "CoinDesk")
    assert len(arts) == 1
    assert arts[0]["source"] == "CoinDesk"


def test_parse_rss_missing_fields():
    ing = SimpleNewsIngestor()
    xml = "<rss><channel><item><title>t</title></item></channel></rss>"
    arts = ing.parse_rss(xml, "X")
    assert arts == []


def test_parse_rss_bad_pubdate():
    ing = SimpleNewsIngestor()
    xml = "<rss><channel><item><title>t</title><link>u</link><pubDate>not-a-date</pubDate></item></channel></rss>"
    arts = ing.parse_rss(xml, "X")
    assert len(arts) == 1


def test_parse_rss_malformed():
    ing = SimpleNewsIngestor()
    assert ing.parse_rss("<<not xml", "X") == []


def test_analyze_sentiment_positive():
    s, lab = SimpleNewsIngestor().analyze_sentiment("bitcoin gain surge bullish", "")
    assert s > 0
    assert lab == "positive"


def test_analyze_sentiment_negative():
    s, lab = SimpleNewsIngestor().analyze_sentiment("bitcoin crash drop bearish", "")
    assert s < 0
    assert lab == "negative"


def test_analyze_sentiment_neutral():
    s, lab = SimpleNewsIngestor().analyze_sentiment("something happened", "")
    assert lab == "neutral"


def test_extract_tickers():
    t = SimpleNewsIngestor().extract_tickers("$BTC and ETH-USD and $SOL mention")
    assert "BTC-USD" in t and "ETH-USD" in t and "SOL-USD" in t


def test_process_article_empty_title():
    assert SimpleNewsIngestor().process_article({"title": ""}) is None


def test_process_article_normal():
    art = {
        "title": "bitcoin gain",
        "url": "http://x/1",
        "source": "X",
        "description": "",
        "published_at": datetime.utcnow(),
    }
    out = SimpleNewsIngestor().process_article(art)
    assert out["id"]
    assert out["sentiment_label"] == "positive"


def test_save_graph_new(tmp_path):
    ing = SimpleNewsIngestor()
    p = tmp_path / "kg.json"
    n = ing.save_graph([{"id": "a", "tickers": ["BTC-USD"]}], str(p))
    assert n == 1
    data = json.loads(p.read_text())
    assert data["articles"][0]["id"] == "a"


def test_save_graph_existing_dedup(tmp_path):
    ing = SimpleNewsIngestor()
    p = tmp_path / "kg.json"
    p.write_text(json.dumps({"articles": [{"id": "a", "tickers": []}], "tickers": {}}))
    n = ing.save_graph([{"id": "a", "tickers": []}, {"id": "b", "tickers": ["ETH-USD"]}], str(p))
    assert n == 1
    data = json.loads(p.read_text())
    assert len(data["articles"]) == 2
    assert "ETH-USD" in data["tickers"]


def test_push_empty():
    ing = SimpleNewsIngestor()
    assert ing.push_to_graph_server([]) == {"status": "ok", "count": 0}


def test_push_to_graph_server(tmp_path):
    ing = SimpleNewsIngestor()
    ing.config.graph_server_url = "http://localhost:9"
    arts = [
        {"id": "1", "title": "t", "url": "u", "source": "X",
         "published_at": "2024", "sentiment_score": 0.5,
         "sentiment_label": "neutral", "tickers": ["BTC-USD"]},
    ]
    created = {"status": "ok", "count": 0}

    def fake_post(url, **kw):
        r = MagicMock()
        r.status_code = 201
        return r

    with patch.object(m.requests, "post", side_effect=fake_post):
        res = ing.push_to_graph_server(arts)
    assert res["created_articles"] == 1
    assert res["created_tickers"] == 1


def test_push_to_graph_server_item_errors(tmp_path):
    ing = SimpleNewsIngestor()

    def fake_post(url, **kw):
        raise RuntimeError("down")

    with patch.object(m.requests, "post", side_effect=fake_post):
        res = ing.push_to_graph_server([{"id": "1", "tickers": ["BTC-USD"]}])
    assert res["errors"]


def test_run_one_cycle(monkeypatch, tmp_path):
    ing = SimpleNewsIngestor(Config())
    ing.fetch_articles = lambda: []
    monkeypatch.setattr(m.time, "sleep", lambda s: (_ for _ in ()).throw(KeyboardInterrupt()))
    # avoid writing to cwd
    ing.save_graph = lambda arts, path="knowledge_graph.json": 0
    ing.push_to_graph_server = lambda arts, symbols=None: {"created_articles": 0, "created_tickers": 0}
    ing.run(symbols=["BTC-USD"], interval_seconds=1)


def test_main(monkeypatch):
    monkeypatch.setattr("sys.argv", ["news_ingest_simple", "--symbols", "BTC-USD", "--interval", "5"])
    called = {}
    monkeypatch.setattr(m.SimpleNewsIngestor, "run", lambda *a, **k: called.setdefault("ran", True))
    m.main()
    assert called.get("ran")
