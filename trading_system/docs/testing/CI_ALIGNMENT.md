# CI Alignment

## Existing CI workflows found
- No `.github/workflows/*.yml` files were found in this repository.

## Issues discovered
- Quality commands were runnable locally but not codified in CI.
- Previously, docs listed local quality commands but Makefile lacked a matching unified target.

## Changes made
- Added `[tool.mypy]` config in `pyproject.toml`:
  - `explicit_package_bases = true`
  - `ignore_missing_imports = true`
- Added Makefile targets:
  - `typecheck` -> `mypy .`
  - `ci` -> `lint typecheck test`
- Updated README/testing docs to align on canonical commands.

## Canonical local commands to mirror in CI
1. `make ci`
2. (Equivalent split) `ruff check . && mypy . && pytest -q`

## Remaining gaps
- No actual CI workflow file yet; next pass should add workflow definitions and cache strategy.
- No container-image build validation job (and no Dockerfile present to build).
