import json
import subprocess
from pathlib import Path


def test_replay_fixture_outputs_toxic_fade_and_pressure_metrics():
    root = Path(__file__).resolve().parents[2]
    cmd = ["python", "apps/replay_engine/runner.py", "--fixture", "apps/replay_engine/fixtures/maker_toxic_flow.jsonl"]
    out = subprocess.check_output(cmd, cwd=root, text=True)
    payload = json.loads(out)
    assert payload["events"] == 5
    assert payload["fade_events"] >= 2
    assert payload["avg_cancel_replace_pressure"] > 0
