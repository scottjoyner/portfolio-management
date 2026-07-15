import json
from pathlib import Path

import app.tools.live_paper_trading as m
from app.tools.live_paper_trading import analyze_article_sentiment


def test_analyze_bullish():
    s = analyze_article_sentiment("Bitcoin surges breakout rally record high bullish", "")
    assert s > 0.5


def test_analyze_bearish():
    s = analyze_article_sentiment("Ethereum crash drop bearish hack fraud", "")
    assert s < 0.5


def test_analyze_neutral():
    s = analyze_article_sentiment("something happened today", "")
    assert s == 0.5


def test_main_no_kg(tmp_path, monkeypatch):
    (tmp_path / "app").mkdir(parents=True, exist_ok=True)
    monkeypatch.chdir(tmp_path)
    # No knowledge graph -> early return
    m.main()


def _kg(tmp_path, articles):
    d = tmp_path / "app" / "data"
    d.mkdir(parents=True)
    (d / "knowledge_graph.json").write_text(json.dumps({"articles": articles}))


def test_main_with_signals(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    arts = [
        {"title": "Bitcoin surges breakout rally record high bullish", "summary": ""},
        {"title": "Ethereum crash drop bearish hack fraud", "summary": ""},
    ]
    _kg(tmp_path, arts)
    m.main()


def test_main_neutral_and_missing(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    # bitcoin neutral -> signals continue; ethereum absent -> None continue
    arts = [{"title": "Bitcoin something happened today", "summary": ""}]
    _kg(tmp_path, arts)
    m.main()


def test_main_bitcoin_absent(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    # only ethereum articles -> btc_analysis is None -> skip branch
    arts = [{"title": "Ethereum surges breakout rally record high bullish", "summary": ""}]
    _kg(tmp_path, arts)
    m.main()


def test_main_eth_long(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    arts = [
        {"title": "Bitcoin surges breakout rally record high bullish", "summary": ""},
        {"title": "Ethereum surges breakout rally record high bullish", "summary": ""},
    ]
    _kg(tmp_path, arts)
    m.main()
