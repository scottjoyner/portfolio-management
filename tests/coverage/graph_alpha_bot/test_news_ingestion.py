import hashlib
import time
import types
from datetime import datetime, timedelta
from app.pipelines.news_ingestion import NewsIngestionPipeline, setup_logging, main


def _article(title, summary="", tickers=None, sentiment=0.5, topic=None, is_break=False, published="2024-01-01T00:00:00Z"):
    return {
        "id": title[:6],
        "title": title,
        "url": "http://x/" + title,
        "published_at": published,
        "source": "test",
        "summary": summary,
        "tickers": tickers or [],
        "sentiment_score": sentiment,
        "topic": topic,
        "freshness": 1.0,
        "is_breaking": is_break,
    }


def test_classify_topic_matches_keywords():
    p = NewsIngestionPipeline()
    assert p.classify_topic("SEC investigation lawsuit", "") == "regulation"
    assert p.classify_topic("hack exploit breach", "") == "hacks_security"
    assert p.classify_topic("partnership adoption bank", "") == "adoption"
    assert p.classify_topic("fork scaling rollup", "") == "technology"
    assert p.classify_topic("inflation fed interest rate", "") == "macro"


def test_classify_topic_no_match_returns_none():
    p = NewsIngestionPipeline()
    assert p.classify_topic("random weather news", "") is None


def test_extract_tickers_known_symbols():
    p = NewsIngestionPipeline()
    found = p.extract_tickers("Bitcoin and ETH and solana mentioned")
    assert "BTC-USD" in found and "ETH-USD" in found and "SOL-USD" in found


def test_extract_tickers_dedup():
    p = NewsIngestionPipeline()
    found = p.extract_tickers("BTC BTC bitcoin")
    assert found.count("BTC-USD") == 1


def test_compute_sentiment_positive():
    p = NewsIngestionPipeline()
    s = p.compute_sentiment("bitcoin surges gain bullish rally", "")
    assert s > 0.5


def test_compute_sentiment_negative():
    p = NewsIngestionPipeline()
    s = p.compute_sentiment("bitcoin crash drop hack bearish", "")
    assert s < 0.5


def test_compute_sentiment_neutral():
    p = NewsIngestionPipeline()
    s = p.compute_sentiment("something happened today", "")
    assert s == 0.5


def test_compute_freshness_weight_bad_date():
    p = NewsIngestionPipeline()
    assert p.compute_freshness_weight("not-a-date") == 0.5


def test_compute_freshness_weight_future():
    p = NewsIngestionPipeline()
    assert p.compute_freshness_weight("2999-01-01T00:00:00Z") == 1.0


def test_compute_freshness_weight_decay():
    p = NewsIngestionPipeline()
    # 10 minutes ago -> still very fresh
    recent = (datetime.utcnow() - timedelta(minutes=10)).isoformat().replace("+00:00", "Z")
    assert p.compute_freshness_weight(recent) > 0.9
    # Far in the past -> effectively zero
    assert p.compute_freshness_weight("2000-01-01T00:00:00Z") < 0.01


def test_track_volume_and_prune():
    p = NewsIngestionPipeline()
    p.track_volume("regulation")
    assert len(p._volume_window["regulation"]) == 1
    # prune non-existent topic list stays stable
    p.track_volume(None)
    assert "regulation" in p._volume_window


def test_detect_breaking_news_no_topic():
    p = NewsIngestionPipeline()
    assert p.detect_breaking_news(None) is False


def test_detect_breaking_news_threshold_without_prior():
    p = NewsIngestionPipeline()
    now = __import__("time").time()
    p._volume_window["regulation"] = [now - i * 60 for i in range(5)]  # 5 in last hour
    assert p.detect_breaking_news("regulation", threshold=3.0) is True


def test_detect_breaking_news_ratio_with_prior():
    p = NewsIngestionPipeline()
    now = __import__("time").time()
    # prior hour 1 event, last hour 4 events => ratio 4 >= 2 and last_hour>=threshold
    p._volume_window["adoption"] = [now - 4000, now - 60, now - 120, now - 180, now - 240]
    assert p.detect_breaking_news("adoption", threshold=3.0) is True


def test_parse_rss_valid():
    p = NewsIngestionPipeline()
    xml = """<rss><channel>
      <item><title>Bitcoin surges</title><link>http://x/1</link><pubDate>Mon, 01 Jan 2024 00:00:00 GMT</pubDate><description>gain</description></item>
      <item><title>ETH hack</title><link>http://x/2</link></item>
    </channel></rss>"""
    arts = p._parse_rss(xml, "test", 10)
    assert len(arts) == 2
    assert arts[0]["title"] == "Bitcoin surges"
    assert arts[0]["sentiment_score"] == 0.5  # default placeholder in parser


def test_parse_rss_malformed():
    p = NewsIngestionPipeline()
    assert p._parse_rss("<<not xml", "test", 10) == []


def test_fetch_rss_feed_cache_hit(monkeypatch, tmp_path):
    p = NewsIngestionPipeline()
    p.cache_dir = tmp_path
    url = "http://feed"
    # Pre-seed cache file
    cache = tmp_path / (f"{__import__('hashlib').md5(url.encode()).hexdigest()[:16]}.json")
    cache.write_text('{"last_fetch": "2999-01-01T00:00:00+00:00", "articles": [{"a": 1}]}')
    arts = p.fetch_rss_feed(url, "test")
    assert arts == [{"a": 1}]


def test_fetch_rss_feed_network_error(monkeypatch, tmp_path):
    p = NewsIngestionPipeline()
    p.cache_dir = tmp_path

    def boom(*a, **k):
        raise RuntimeError("network down")

    monkeypatch.setattr("urllib.request.urlopen", boom)
    arts = p.fetch_rss_feed("http://feed", "test")
    assert arts == []


def test_run_once(monkeypatch, tmp_path):
    p = NewsIngestionPipeline()
    p.cache_dir = tmp_path
    p.kg_file = tmp_path / "knowledge_graph.json"

    arts = [
        _article("Bitcoin surges on ETF approval and adoption by bank", tickers=["BTC-USD"], topic="adoption", sentiment=0.9),
        _article("ETH hack exploit breach security", tickers=["ETH-USD"], topic="hacks_security", sentiment=0.1, is_break=True),
    ]
    monkeypatch.setattr(p, "fetch_rss_feed", lambda url, name, max_articles=20: arts)
    # avoid real breaking detection windows affecting counts
    monkeypatch.setattr(p, "detect_breaking_news", lambda topic, threshold=3.0: False)

    result = p.run_once()
    assert result["status"] == "success"
    assert result["articles_collected"] == 2
    assert "BTC-USD" in result["sources_used"] or result["sources_used"]
    # Knowledge graph file written
    import json
    kg = json.loads(p.kg_file.read_text())
    assert kg["metadata"]["total_articles"] == 2
    assert kg["tickers"]["BTC-USD"]["count"] == 1


def test_run_once_fetch_failure(monkeypatch, tmp_path):
    p = NewsIngestionPipeline()
    p.cache_dir = tmp_path
    p.kg_file = tmp_path / "kg2.json"
    monkeypatch.setattr(p, "fetch_rss_feed", lambda url, name, max_articles=20: [])
    result = p.run_once()
    assert result["articles_collected"] == 0


# ---------------------------------------------------------------------------
# Coverage for previously-untested branches
# ---------------------------------------------------------------------------

def test_setup_logging_default():
    # default path -> builds ~/.hermes/.../news.log and mkdir (lines 12-16)
    setup_logging()


def test_setup_logging_with_path(tmp_path):
    setup_logging(str(tmp_path / "news.log"))


def test_track_volume_prunes_empty_topics():
    p = NewsIngestionPipeline()
    # old topic window -> pruned and deleted (line 206)
    p._volume_window["regulation"] = [time.time() - 10 * 3600]
    # recent topic window -> kept
    p._volume_window["macro"] = [time.time()]
    p.track_volume(None)
    assert "regulation" not in p._volume_window
    assert "macro" in p._volume_window


def test_fetch_rss_feed_stale_cache_fetches_network(monkeypatch, tmp_path):
    # stale cache -> proceeds to network fetch (branch 228->232) and rewrites cache (238-246)
    p = NewsIngestionPipeline()
    p.cache_dir = tmp_path
    url = "http://feed-stale"
    cache = tmp_path / (hashlib.md5(url.encode()).hexdigest()[:16] + ".json")
    cache.write_text('{"last_fetch": "2000-01-01T00:00:00+00:00", "articles": [{"a": 1}]}')
    xml = "<rss><channel><item><title>T</title><link>L</link></item></channel></rss>"

    class Res:
        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

        def read(self):
            return xml.encode()

    monkeypatch.setattr("urllib.request.urlopen", lambda *a, **k: Res())
    arts = p.fetch_rss_feed(url, "test")
    assert len(arts) == 1 and arts[0]["title"] == "T"
    assert cache.exists()


def test_fetch_rss_feed_corrupt_cache(monkeypatch, tmp_path):
    # corrupt cache json -> exception (230-231) -> falls through to network
    p = NewsIngestionPipeline()
    p.cache_dir = tmp_path
    url = "http://feed-corrupt"
    cache = tmp_path / (hashlib.md5(url.encode()).hexdigest()[:16] + ".json")
    cache.write_text("{not json")
    xml = "<rss><channel><item><title>Z</title><link>L</link></item></channel></rss>"

    class Res:
        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

        def read(self):
            return xml.encode()

    monkeypatch.setattr("urllib.request.urlopen", lambda *a, **k: Res())
    arts = p.fetch_rss_feed(url, "test")
    assert arts[0]["title"] == "Z"


def test_parse_rss_missing_title_or_link_skips():
    # item without title/link -> continue (line 261)
    p = NewsIngestionPipeline()
    xml = "<rss><channel><item><description>no title</description></item></channel></rss>"
    assert p._parse_rss(xml, "test", 10) == []


def test_extract_ticker_stats_branches():
    # empty tickers (351->353 outer skip), topic None (354->356),
    # is_breaking True (358), and repeated ticker (351->353 inner False branch)
    p = NewsIngestionPipeline()
    articles = [
        {"sentiment_score": 0.5, "is_breaking": False, "tickers": []},
        {"topic": None, "sentiment_score": 0.5, "is_breaking": False, "tickers": ["BTC-USD"]},
        {"topic": "macro", "sentiment_score": 0.5, "is_breaking": True, "tickers": ["ETH-USD"]},
        {"topic": "macro", "sentiment_score": 0.5, "is_breaking": False, "tickers": ["BTC-USD"]},
    ]
    stats = p._extract_ticker_stats(articles)
    assert stats["BTC-USD"]["count"] == 2
    assert stats["ETH-USD"]["count"] == 1
    assert stats["ETH-USD"]["breaking_count"] == 1


def test_main_prints(monkeypatch, capsys):
    monkeypatch.setattr("app.pipelines.news_ingestion.setup_logging", lambda *a, **k: None)
    monkeypatch.setattr(
        NewsIngestionPipeline, "run_once",
        lambda self: {"articles_collected": 3, "sources_used": ["coindesk"],
                      "topics_found": ["macro"], "breaking_topics": [],
                      "fetch_times": {}, "timestamp": "t"},
    )
    main()
    out = capsys.readouterr().out
    assert "Articles collected: 3" in out
