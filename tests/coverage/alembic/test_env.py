import importlib.util
import os
from pathlib import Path
import sys
import types
import unittest
from unittest import mock

ROOT = Path(__file__).resolve().parents[3]
ENV_PATH = ROOT / "trading_system" / "alembic" / "env.py"


def _fake_storage():
    storage_pkg = types.ModuleType("storage")
    storage_pkg.__path__ = []
    storage_postgres = types.ModuleType("storage.postgres")
    storage_postgres.__path__ = []
    models = types.ModuleType("storage.postgres.models")
    base = mock.MagicMock(name="Base")
    base.metadata = mock.MagicMock(name="metadata")
    models.Base = base
    storage_postgres.models = models
    storage_pkg.postgres = storage_postgres
    return {
        "storage": storage_pkg,
        "storage.postgres": storage_postgres,
        "storage.postgres.models": models,
    }


def load_env(offline, with_file, with_dburl, modname):
    ctx = mock.MagicMock(name="context")
    ctx.is_offline_mode.return_value = offline
    ctx.config.config_file_name = "alembic.ini" if with_file else None
    ctx.config.get_main_option.return_value = "sqlite://"
    ctx.config.get_section.return_value = {"sqlalchemy.url": "sqlite://"}

    fake_alembic = types.ModuleType("alembic")
    fake_alembic.context = ctx

    mods = {"alembic": fake_alembic}
    mods.update(_fake_storage())

    env = dict(os.environ)
    if with_dburl:
        env["DATABASE_URL"] = "postgresql://user:pass@localhost/db"
    else:
        env.pop("DATABASE_URL", None)

    with mock.patch.dict(sys.modules, mods), \
            mock.patch.dict(os.environ, env, clear=True), \
            mock.patch("logging.config.fileConfig") as fc:
        spec = importlib.util.spec_from_file_location(modname, ENV_PATH)
        module = importlib.util.module_from_spec(spec)
        assert spec.loader is not None
        spec.loader.exec_module(module)
    return module, ctx, fc


class TestAlembicEnv(unittest.TestCase):
    def test_offline_with_file_and_dburl(self):
        module, ctx, fc = load_env(offline=True, with_file=True, with_dburl=True,
                                   modname="alembic_env_offline")
        fc.assert_called_once_with("alembic.ini")
        ctx.config.set_main_option.assert_called_once()
        ctx.run_migrations.assert_called_once()
        ctx.configure.assert_called_once()

    def test_online_no_file_no_dburl(self):
        module, ctx, fc = load_env(offline=False, with_file=False, with_dburl=False,
                                   modname="alembic_env_online")
        fc.assert_not_called()
        ctx.config.set_main_option.assert_not_called()
        ctx.run_migrations.assert_called_once()
        ctx.configure.assert_called_once()

    def test_functions_callable_directly(self):
        module, ctx, fc = load_env(offline=True, with_file=False, with_dburl=False,
                                   modname="alembic_env_direct")
        ctx.reset_mock()
        module.run_migrations_online()
        ctx.configure.assert_called()
        ctx.run_migrations.assert_called()


if __name__ == "__main__":
    unittest.main()
