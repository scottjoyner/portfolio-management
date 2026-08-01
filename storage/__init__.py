"""Canonical persistence package for repository-level storage adapters.

This file intentionally makes ``storage`` a regular package. Without it,
Python can prefer the unrelated ``trading_system.storage`` package when both
the repository root and ``trading_system`` are present on ``PYTHONPATH``.
"""
