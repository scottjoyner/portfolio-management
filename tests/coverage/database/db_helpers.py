"""Fake external modules for database query coverage tests.

The query modules do lazy ``from storage.postgres.models import ...`` and
``from plaid... import ...`` inside methods. We inject fakes into sys.modules
so those imports resolve to controllable stand-ins.
"""
import sys
import types
from unittest import mock


class _Row:
    """Generic ORM-row stand-in storing kwargs as attributes."""

    def __init__(self, **kwargs):
        for k, v in kwargs.items():
            setattr(self, k, v)


def _make_model(name, columns):
    """Create a fake model class with MagicMock column attrs + kwargs init."""
    attrs = {c: mock.MagicMock(name=f"{name}.{c}") for c in columns}

    def __init__(self, **kwargs):
        for k, v in kwargs.items():
            setattr(self, k, v)

    attrs["__init__"] = __init__
    return type(name, (), attrs)


def install_fakes():
    # storage / storage.postgres / storage.postgres.models chain
    storage_pkg = types.ModuleType("storage")
    storage_pkg.__path__ = []  # mark as package
    storage_postgres = types.ModuleType("storage.postgres")
    storage_postgres.__path__ = []
    mod = types.ModuleType("storage.postgres.models")
    mod.Portfolio = _make_model("Portfolio", ["id", "objective", "nav", "name"])
    mod.PortfolioSleeve = _make_model("PortfolioSleeve", ["portfolio_id", "name", "weight"])
    mod.CapitalBucket = _make_model("CapitalBucket", ["id", "portfolio_id"])
    mod.Order = _make_model(
        "Order",
        ["id", "order_id", "portfolio_id", "product_id", "status", "order_type", "created_at"],
    )
    mod.Fill = _make_model("Fill", ["id", "fill_id", "order_id", "product_id", "size", "side"])
    mod.Approval = _make_model("Approval", ["approval_id", "status"])
    plaid_items_table = mock.MagicMock(name="plaid_items_table")
    mod.plaid_items_table = plaid_items_table
    storage_postgres.models = mod
    storage_pkg.postgres = storage_postgres
    sys.modules["storage"] = storage_pkg
    sys.modules["storage.postgres"] = storage_postgres
    sys.modules["storage.postgres.models"] = mod

    # plaid.models
    plaid_models = types.ModuleType("plaid.models")

    class _Enum:
        pass

    class InstitutionStatus:
        ACTIVE = "ACTIVE"

    class ConsentState:
        GRANTED = "GRANTED"
        REVOKED = "REVOKED"

    plaid_models.PlaidItem = _make_model("PlaidItem", ["id", "status", "access_token"])
    plaid_models.InstitutionStatus = InstitutionStatus
    plaid_models.ConsentState = ConsentState

    plaid_pkg = types.ModuleType("plaid")
    plaid_pkg.__path__ = []
    plaid_db = types.ModuleType("plaid.database_models")
    plaid_db.PlaidItem = _make_model("PlaidItem", ["id", "status", "access_token"])
    plaid_pkg.models = plaid_models
    plaid_pkg.database_models = plaid_db

    sys.modules["plaid"] = plaid_pkg
    sys.modules["plaid.models"] = plaid_models
    sys.modules["plaid.database_models"] = plaid_db
    return mod, plaid_models, plaid_db


install_fakes()


class QueryStub:
    """Chainable stand-in for a SQLAlchemy Query."""

    def __init__(self, rows=None, first=None, count=0):
        self._rows = rows if rows is not None else []
        self._first = first
        self._count = count

    def filter(self, *a, **k):
        return self

    def join(self, *a, **k):
        return self

    def order_by(self, *a, **k):
        return self

    def limit(self, *a, **k):
        return self

    def offset(self, *a, **k):
        return self

    def all(self):
        return self._rows

    def __iter__(self):
        return iter(self._rows)

    def first(self):
        return self._first

    def count(self):
        return self._count


def make_db(query_map=None, default=None):
    """Return a MagicMock db whose .query(Model) returns a QueryStub.

    query_map: dict mapping model class -> QueryStub (or callable).
    """
    db = mock.MagicMock()
    query_map = query_map or {}

    def _query(model, *a, **k):
        val = query_map.get(model, default)
        if callable(val):
            return val()
        if val is None:
            return QueryStub()
        return val

    db.query.side_effect = _query
    return db
