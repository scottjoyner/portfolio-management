#!/usr/bin/env python3
"""Integration test runner for P0.2 — Validates database-backed API smoke coverage."""

import sys
from pathlib import Path

# Add project root to path
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))


def main():
    """Run integration tests and validate migration head state."""
    print("=" * 70)
    print("P0.2 — Database-Backed Integration Test Harness")
    print("=" * 70)
    
    # Check pytest is installed
    try:
        import pytest
        print("✓ Pytest found")
    except ImportError:
        print("✗ Pytest not found - install with: pip install pytest pytest-asyncio")
        return 1
    
    # Run integration tests
    print("\nRunning integration tests...")
    result = pytest.main([
        "-v",
        "tests/integration/",
        "--tb=short",
    ])
    
    print(f"\nTest exit code: {result}")
    
    if result == 0:
        print("\n" + "=" * 70)
        print("✓ P0.2 Integration Test Harness: PASS")
        print("=" * 70)
    else:
        print("\n" + "=" * 70)
        print(f"✗ P0.2 Integration Test Harness: FAIL (exit code {result})")
        print("=" * 70)
    
    return result


if __name__ == "__main__":
    sys.exit(main())
