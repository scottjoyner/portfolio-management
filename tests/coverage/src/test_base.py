import unittest
from datetime import datetime

from src.sources.base import DataSource, DataSourceError


class _Concrete(DataSource):
    async def fetch(self, symbol, start_date=None, end_date=None):
        return await super().fetch(symbol, start_date, end_date)

    async def health_check(self):
        return await super().health_check()

    async def get_available_symbols(self, asset_class=None):
        return await super().get_available_symbols(asset_class)


class TestBase(unittest.TestCase):
    def test_data_source_error(self):
        err = DataSourceError("boom")
        self.assertIsInstance(err, Exception)
        self.assertEqual(str(err), "boom")

    def test_cannot_instantiate_abstract(self):
        with self.assertRaises(TypeError):
            DataSource()

    def test_normalize_date(self):
        c = _Concrete()
        dt = datetime(2024, 3, 5)
        self.assertEqual(c._normalize_date(dt), "2024-03-05")

    def test_abstract_method_bodies_executed(self):
        import asyncio
        c = _Concrete()
        # executing the `pass` bodies of abstract methods
        self.assertEqual(asyncio.run(c.fetch("X")), None)
        self.assertEqual(asyncio.run(c.health_check()), None)
        self.assertEqual(asyncio.run(c.get_available_symbols()), None)


if __name__ == "__main__":
    unittest.main()
