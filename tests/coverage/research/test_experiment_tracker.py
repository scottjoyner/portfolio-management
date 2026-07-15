import unittest
import tempfile
import os
import json

from trading_system.research.experiment_tracking.tracker import (
    ExperimentTracker,
    ExperimentRun,
)


class TestExperimentTracker(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.path = os.path.join(self.tmp, "nested", "experiments.jsonl")

    def test_record_creates_parent_and_writes(self):
        tr = ExperimentTracker(manifest_path=self.path)
        run = ExperimentRun(
            run_id="r1", strategy_id="s1", config_hash="h1", metrics={"acc": 0.9}
        )
        tr.record(run)
        self.assertTrue(os.path.exists(self.path))
        with open(self.path) as f:
            lines = f.read().splitlines()
        self.assertEqual(len(lines), 1)
        payload = json.loads(lines[0])
        self.assertEqual(payload["run_id"], "r1")
        self.assertEqual(payload["metrics"]["acc"], 0.9)
        self.assertIn("recorded_at", payload)


if __name__ == "__main__":
    unittest.main()
