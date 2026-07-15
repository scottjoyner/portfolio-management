import sys
import types
from unittest.mock import MagicMock


def _build_fake_storage_postgres_models():
    """portfolio.manager imports CapitalBucket/Portfolio/PortfolioSleeve from
    storage.postgres.models, but that module does not define them (clear bug:
    wrong import target). Inject a stub module so the module is importable for
    testing without modifying source. Reported as a bug."""
    mod = types.ModuleType("storage.postgres.models")

    class CapitalBucket:
        id = MagicMock()
        portfolio_id = MagicMock()

        def __init__(self, **kwargs):
            self.__dict__.update(kwargs)

    class Portfolio:
        id = MagicMock()

        def __init__(self, **kwargs):
            self.__dict__.update(kwargs)

    class PortfolioSleeve:
        portfolio_id = MagicMock()
        name = MagicMock()

        def __init__(self, **kwargs):
            self.__dict__.update(kwargs)

    mod.CapitalBucket = CapitalBucket
    mod.Portfolio = Portfolio
    mod.PortfolioSleeve = PortfolioSleeve
    return mod


_fake = _build_fake_storage_postgres_models()
sys.modules["storage.postgres.models"] = _fake

