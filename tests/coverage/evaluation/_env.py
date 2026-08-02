"""Local import shim for the shared generated-coverage environment helpers."""

from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path

_SHARED_ENV = Path(__file__).resolve().parents[1] / "_env.py"
_spec = spec_from_file_location("generated_coverage_shared_env", _SHARED_ENV)
if _spec is None or _spec.loader is None:
    raise ImportError(f"Unable to load shared coverage helpers from {_SHARED_ENV}")
_module = module_from_spec(_spec)
_spec.loader.exec_module(_module)

install_stubs = _module.install_stubs

__all__ = ["install_stubs"]
