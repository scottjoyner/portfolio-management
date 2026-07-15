import json
import os
import sys
import types
from pathlib import Path
from unittest.mock import patch

ROOT = "/home/scott/git/portfolio-management"
# live_trading_demo imports `connectors`, `strategies`, `pipelines` as
# top-level packages, which requires graph-alpha-bot/app on the path.
if os.path.join(ROOT, "graph-alpha-bot", "app") not in sys.path:
    sys.path.insert(0, os.path.join(ROOT, "graph-alpha-bot", "app"))

# Stub heavy strategy imports BEFORE importing the demo module so its
# deferred `from strategies.signal_generator import SignalGenerator` succeeds
# without pulling in the unified signal generator chain.
_fake_uni = types.ModuleType("app.strategies.unified_signal_generator")
_fake_uni.UnifiedSignalConfig = object
_fake_uni.UnifiedSignalGenerator = object
sys.modules.setdefault("app.strategies.unified_signal_generator", _fake_uni)
_fake_sg = types.ModuleType("strategies.signal_generator")
_fake_sg.SignalGenerator = object
sys.modules["strategies.signal_generator"] = _fake_sg

import app.tools.live_trading_demo as m
from app.tools.live_trading_demo import analyze_article_sentiment


def test_analyze_bullish():
    assert analyze_article_sentiment("Bitcoin surges breakout rally record high bullish", "") > 0.5


def test_analyze_bearish():
    assert analyze_article_sentiment("Ethereum crash drop bearish hack fraud", "") < 0.5


def test_analyze_neutral():
    assert analyze_article_sentiment("something happened today", "") == 0.5


def _kg(tmp_path, articles):
    d = tmp_path / "app" / "data"
    d.mkdir(parents=True)
    (d / "knowledge_graph.json").write_text(json.dumps({"articles": articles}))


def test_main_no_kg(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    with patch("pipelines.news_ingestion.NewsIngestionPipeline.run_once",
               return_value={"articles_collected": 0}):
        m.main()


def test_main_with_signals(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    arts = [
        {"title": "Bitcoin surges breakout rally record high bullish", "summary": ""},
        {"title": "Ethereum crash drop bearish hack fraud", "summary": ""},
    ]
    _kg(tmp_path, arts)
    with patch("pipelines.news_ingestion.NewsIngestionPipeline.run_once",
               return_value={"articles_collected": 2}):
        m.main()


def test_main_neutral_and_missing(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    arts = [{"title": "Bitcoin something happened today", "summary": ""}]
    _kg(tmp_path, arts)
    with patch("pipelines.news_ingestion.NewsIngestionPipeline.run_once",
               return_value={"articles_collected": 1}):
        m.main()
