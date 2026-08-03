import importlib.util
from pathlib import Path
import unittest
from unittest import mock

ROOT = Path(__file__).resolve().parents[3]
VERSIONS = ROOT / "trading_system" / "alembic" / "versions"


def load_module(filename, modname):
    path = VERSIONS / filename
    spec = importlib.util.spec_from_file_location(modname, path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


class TestMigration0001(unittest.TestCase):
    def setUp(self):
        self.mod = load_module("0001_initial.py", "alembic_v0001")

    def test_metadata(self):
        self.assertEqual(self.mod.revision, "0001")
        self.assertIsNone(self.mod.down_revision)

    def test_upgrade(self):
        with mock.patch.object(self.mod, "op") as op:
            self.mod.upgrade()
        self.assertTrue(op.create_table.called)
        self.assertGreaterEqual(op.create_table.call_count, 10)

    def test_downgrade(self):
        with mock.patch.object(self.mod, "op") as op:
            self.mod.downgrade()
        self.assertTrue(op.drop_table.called)


class TestMigration0002(unittest.TestCase):
    def setUp(self):
        self.mod = load_module("0002_onchain_runtime.py", "alembic_v0002")

    def test_metadata(self):
        self.assertEqual(self.mod.revision, "0002")
        self.assertEqual(self.mod.down_revision, "0001")

    def test_upgrade(self):
        with mock.patch.object(self.mod, "op") as op:
            self.mod.upgrade()
        self.assertEqual(op.create_table.call_count, 4)

    def test_downgrade(self):
        with mock.patch.object(self.mod, "op") as op:
            self.mod.downgrade()
        self.assertEqual(op.drop_table.call_count, 4)


if __name__ == "__main__":
    unittest.main()
