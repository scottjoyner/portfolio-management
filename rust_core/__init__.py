"""Optional Python bindings for the Rust acceleration layer.

The compiled extension is an optimization, not a package-import prerequisite.
Callers can inspect ``RUST_CORE_AVAILABLE`` or catch the explicit runtime error
from fallback callables when the extension has not been built.
"""

from __future__ import annotations

RUST_CORE_AVAILABLE = False
RUST_CORE_IMPORT_ERROR: Exception | None = None

try:
    from .rust_core import *  # type: ignore  # noqa: F401,F403

    RUST_CORE_AVAILABLE = True
except (ImportError, ModuleNotFoundError) as exc:
    RUST_CORE_IMPORT_ERROR = exc

    def _missing_extension(*args, **kwargs):
        raise RuntimeError(
            "rust_core extension is unavailable; build/install the optional "
            "Rust acceleration module or use the Python fallback path"
        ) from RUST_CORE_IMPORT_ERROR

    # Keep the principal fast-path symbol patchable and introspectable in
    # tests and fallback-aware callers even when the compiled module is absent.
    evaluate_all_opens_py = _missing_extension


__all__ = [
    "RUST_CORE_AVAILABLE",
    "RUST_CORE_IMPORT_ERROR",
    "evaluate_all_opens_py",
]
