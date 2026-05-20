import json
import subprocess
from pathlib import Path


def test_backtester_writes_deterministic_summary_and_validation(tmp_path: Path):
    output = tmp_path / "summary.json"
    validation = tmp_path / "validation.json"
    cmd = [
        "python",
        "apps/backtester/runner.py",
        "--config",
        "configs/backtest_demo.yaml",
        "--output",
        str(output),
        "--validation-report",
        str(validation),
    ]
    root = Path(__file__).resolve().parents[2]
    subprocess.run(cmd, check=True, cwd=root)
    first = json.loads(output.read_text())
    first_v = json.loads(validation.read_text())

    subprocess.run(cmd, check=True, cwd=root)
    second = json.loads(output.read_text())
    second_v = json.loads(validation.read_text())

    assert first["metrics"] == second["metrics"]
    assert first["queue_model"] == second["queue_model"]
    assert first["live_portability_score"] == second["live_portability_score"]
    assert first_v["strategy_rankings"] == second_v["strategy_rankings"]
    assert len(first_v["strategy_rankings"]) >= 50
