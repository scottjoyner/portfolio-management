import json
from pathlib import Path

import app.tools.pipeline_demo as m


def test_main_no_kg(tmp_path, monkeypatch):
    (tmp_path / "app").mkdir(parents=True, exist_ok=True)
    monkeypatch.chdir(tmp_path)
    m.main()


def test_main_with_kg(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    d = tmp_path / "app" / "data"
    d.mkdir(parents=True)
    arts = [
        {"id": "a1", "title": "Bitcoin ETF approval"},
        {"id": "a2", "title": "Ethereum staking upgrade"},
    ]
    (d / "knowledge_graph.json").write_text(json.dumps({"articles": arts}))
    m.main()
