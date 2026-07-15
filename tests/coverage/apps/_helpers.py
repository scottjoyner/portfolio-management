"""Helpers for apps coverage tests.

Provides a controllable fake-module injector. Internal dependencies of the
target modules (storage.postgres.*, core.*, strategies.*, risk.*, research.*,
execution.*, analytics.*, market_data.*, event_markets.*, etc.) are replaced
with lightweight fake modules whose attributes are MagicMocks. This makes the
target modules importable without databases / network, and lets tests control
return values via monkeypatch.
"""

import importlib.util
import sys
import types
from unittest.mock import MagicMock


class FakeModule(types.ModuleType):
    """Module whose attribute access returns a cached MagicMock by default."""

    def __init__(self, name):
        super().__init__(name)
        self._fake_attrs = {}

    def __getattr__(self, name):
        if name in ("_fake_attrs", "__name__", "__path__", "__file__", "__package__"):
            raise AttributeError(name)
        if name in self._fake_attrs:
            return self._fake_attrs[name]
        m = MagicMock(name=f"{self.__name__}.{name}")
        self._fake_attrs[name] = m
        return m

    def __setattr__(self, name, value):
        if name == "_fake_attrs":
            super().__setattr__(name, value)
        elif name in self.__dict__:
            self.__dict__[name] = value
        else:
            self._fake_attrs[name] = value


def install_fakes(spec):
    """Install fake modules.

    spec: dict mapping dotted module name -> dict of explicit attributes
    (values assigned as-is) OR None to just create an empty fake module.
    Parent packages are created as fake modules ONLY when they are not
    already importable as real packages (so we never clobber real packages
    such as `trading_system`).
    """
    for name, attrs in spec.items():
        parts = name.split(".")
        for i in range(1, len(parts)):
            parent = ".".join(parts[:i])
            if parent in sys.modules:
                continue
            if importlib.util.find_spec(parent) is not None:
                continue
            if parent not in sys.modules:
                sys.modules[parent] = FakeModule(parent)
        if name not in sys.modules or not isinstance(sys.modules[name], FakeModule):
            mod = FakeModule(name)
            if attrs:
                for k, v in attrs.items():
                    setattr(mod, k, v)
            sys.modules[name] = mod
        else:
            mod = sys.modules[name]
            if attrs:
                for k, v in attrs.items():
                    setattr(mod, k, v)
    return None


def set_fake_attr(module_name, attr, value):
    mod = sys.modules.get(module_name)
    if mod is None:
        mod = FakeModule(module_name)
        sys.modules[module_name] = mod
    setattr(mod, attr, value)
    return mod
