import json
from datetime import datetime, timedelta
from unittest.mock import MagicMock

from app.pipelines.news_ingestion import NewsIngestionPipeline, setup_logging
import app.pipelines.news_ingestion as m


def _article(title, summary="", tickers=None, sentiment=0.5, topic=None, is_break=False, published="2024-01-01T00:00:00Z"):
    return {
        "id": title[:6], "title": title, "url": "http://x/" + title,
        "published_at": published, "source": "test", "summary": summary,
        "tickers": tickers or [], "sentiment_score": sentiment,
        "topic": topic, "freshness": 1.0, "is_breaking": is_break,
    }


def test_setup_logging_default(tmp_path, monkeypatch):
    monkeypatch.setenv("HOME", str(tmp_path))
    setup_logging()
    setup_logging(str(tmp_path / "custom.log"))


def test_track_volume_prune_empty():
    p = NewsIngestionPipeline()
    p._volume_window["x"] = []  # already empty
    p.track_volume(None)  # prune loop deletes empty topic
    assert "x" not in p._volume_window


def test_fetch_rss_feed_success(monkeypatch, tmp_path):
    p = NewsIngestionPipeline()
    p.cache_dir = tmp_path
    xml = (b'<rss><channel>'
           b'<item><title>BTC gains</title><link>http://x/1</link>'
           b'<pubDate>Mon, 01 Jan 2024 00:00:00 GMT</pubDate>'
           b'<description>rally</description></item>'
           b'</channel></rss>')
    resp = MagicMock()
    resp.read.return_value = xml
    resp.__enter__.return_value = resp
    req = MagicMock()
    monkeypatch.setattr("urllib.request.urlopen", lambda *a, **k: resp)
    monkeypatch.setattr("urllib.request.Request", lambda *a, **k: req)
    arts = p.fetch_rss_feed("http://feed", "test")
    assert len(arts) == 1
    assert arts[0]["title"] == "BTC gains"


def test_parse_rss_missing_title_link():
    p = NewsIngestionPipeline()
    xml = (b'<rss><channel>'
           b'<item><description>no title or link</description></item>'
           b'<item><title>OK</title><link>http://x/2</link></item>'
           b'</channel></rss>')
    arts = p._parse_rss(xml, "test", 10)
    assert len(arts) == 1
    assert arts[0]["title"] == "OK"


def test_detect_breaking_ratio_path():
    p = NewsIngestionPipeline()
    now = __import__("time").time()
    p._volume_window["adoption"] = [now - 4000, now - 60, now - 120, now - 180, now - 240]
    # prior hour 1, last hour 4 -> ratio path
    assert p.detect_breaking_news("adoption", threshold=3.0) is True


def test_run_once_with_breaking(tmp_path, monkeypatch):
    p = NewsIngestionPipeline()
    p.cache_dir = tmp_path
    p.kg_file = tmp_path / "kg_break.json"
    now = __import__("time").time()
    p._volume_window["adoption"] = [now - i * 60 for i in range(5)]
    arts = [_article("Bitcoin surges on ETF approval and adoption by bank",
                     tickers=["BTC-USD"], topic="adoption", sentiment=0.9, is_break=True)]
    monkeypatch.setattr(p, "fetch_rss_feed", lambda url, name, max_articles=20: arts)
    result = p.run_once()
    assert result["articles_collected"] == 1
    assert "adoption" in result["breaking_topics"]


def test_main_runs(monkeypatch, tmp_path):
    monkeypatch.setenv("HOME", str(tmp_path))
    captured = {"r": {"status": "success", "articles_collected": 0,
                      "sources_used": [], "topics_found": [],
                      "breaking_topics": [], "fetch_times": {}}}
    monkeypatch.setattr(NewsIngestionPipeline, "run_once", lambda self: captured["r"])
    m.main()


def test_extract_ticker_stats_branches():
    p = NewsIngestionPipeline()
    arts = [
        _article("BTC surges", tickers=["BTC-USD"], topic="adoption", sentiment=0.9, is_break=True),
        _article("BTC surges again", tickers=["BTC-USD"], topic="adoption", sentiment=0.8, is_break=False),
        _article("ETH moves", tickers=["ETH-USD"], sentiment=0.5),  # no topic
    ]
    stats = p._extract_ticker_stats(arts)
    assert stats["BTC-USD"]["count"] == 2
    assert stats["BTC-USD"]["breaking_count"] == 1
    assert "adoption" in stats["BTC-USD"]["topics"]
    assert "ETH-USD" in stats


def test_fetch_rss_feed_stale_cache_then_network(monkeypatch, tmp_path):
    import hashlib
    p = NewsIngestionPipeline()
    p.cache_dir = tmp_path
    url = "http://feed2"
    cache = tmp_path / (hashlib.md5(url.encode()).hexdigest()[:16] + ".json")
    cache.write_text('{"last_fetch": "2000-01-01T00:00:00+00:00", "articles": []}')
    resp = MagicMock()
    resp.read.return_value = b'<rss><channel><item><title>X</title><link>http://x/1</link></item></channel></rss>'
    resp.__enter__.return_value = resp
    monkeypatch.setattr("urllib.request.urlopen", lambda *a, **k: resp)
    monkeypatch.setattr("urllib.request.Request", lambda *a, **k: MagicMock())
    assert len(p.fetch_rss_feed(url, "test")) == 1


def test_fetch_rss_feed_corrupt_cache(monkeypatch, tmp_path):
    import hashlib
    p = NewsIngestionPipeline()
    p.cache_dir = tmp_path
    url = "http://feed3"
    cache = tmp_path / (hashlib.md5(url.encode()).hexdigest()[:16] + ".json")
    cache.write_text("{not valid json")
    resp = MagicMock()
    resp.read.return_value = b'<rss><channel><item><title>Y</title><link>http://x/1</link></item></channel></rss>'
    resp.__enter__.return_value = resp
    monkeypatch.setattr("urllib.request.urlopen", lambda *a, **k: resp)
    monkeypatch.setattr("urllib.request.Request", lambda *a, **k: MagicMock())
    assert len(p.fetch_rss_feed(url, "test")) == 1
