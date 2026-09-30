#!/usr/bin/env python3
"""Public certified-runtime API with exact native-binary identity binding.

The implementation lives in ``certified_strategy_runtime_impl`` so this module
can bind runtime identity to the actual PyO3 extension loaded by the process,
not merely the Python package that re-exports it.
"""
from __future__ import annotations

import importlib
from pathlib import Path

from scripts import certified_strategy_runtime_impl as _impl


def _native_runtime_binary_path() -> Path:
    try:
        package = importlib.import_module("rust_core")
        if getattr(package, "RUST_CORE_AVAILABLE", False) is not True:
            raise _impl.CertifiedRuntimeError("compiled_rust_core_required")
        native = importlib.import_module("rust_core.rust_core")
    except (ImportError, ModuleNotFoundError) as exc:
        raise _impl.CertifiedRuntimeError("compiled_rust_core_required") from exc
    path = Path(getattr(native, "__file__", ""))
    if not path.is_file() or path.suffix.lower() not in {".so", ".pyd", ".dylib"}:
        raise _impl.CertifiedRuntimeError("compiled_rust_core_path_invalid")
    return path.resolve()


def runtime_binary_sha256() -> str:
    """Hash the exact native extension that owns the configured evaluator."""

    return _impl._sha256_file(_native_runtime_binary_path())


# Functions defined in the implementation resolve globals from that module, so
# patch its binary-identity function before re-exporting the public surface.
_impl.runtime_binary_sha256 = runtime_binary_sha256

for _name in dir(_impl):
    if not _name.startswith("__"):
        globals().setdefault(_name, getattr(_impl, _name))

# Ensure the corrected public function wins over the implementation's original
# package-file hash helper.
globals()["runtime_binary_sha256"] = runtime_binary_sha256
