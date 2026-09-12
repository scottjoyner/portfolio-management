#!/usr/bin/env python3
"""Generate the compact runtime bundle consumed by production images.

This command must run in a full research checkout before a production image is
built.  It deliberately exercises the expensive authority path first:

* promotion registry + active config binding
* alpha evidence verification
* canonical replay source reverification
* untouched terminal holdout + lineage verification
* Git ancestry / strategy-source drift verification
* local native Rust evaluator availability

Only after all of those pass does it emit a compact bundle that can be copied
into a production image.  The production runtime then re-hashes the deployed
strategy sources and its *own* native Rust binary; it does not need Git history
or historical feed-cache data inside the container.
"""
from __future__ import annotations

import argparse
import json
import os
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from scripts.alpha_validation import stable_hash
from scripts.certified_strategy_runtime import (
    DEFAULT_DEPLOYMENT_BUNDLE_PATH,
    DEPLOYMENT_BUNDLE_SCHEMA_VERSION,
    DEPLOYMENT_BUNDLE_TYPE,
    load_local_reverified_runtime_context,
    strategy_source_manifest,
)

ROOT = Path(__file__).resolve().parents[1]


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _atomic_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=path.parent)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, indent=2, sort_keys=True, allow_nan=False)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    finally:
        try:
            os.unlink(temporary)
        except FileNotFoundError:
            pass


def build_bundle(*, root: Path = ROOT) -> dict[str, Any]:
    context = load_local_reverified_runtime_context(root=root)
    identity = dict(context["identity"])
    host_binary_sha256 = identity.pop("runtime_binary_sha256", None)
    if not host_binary_sha256:
        raise RuntimeError("full verification did not bind a native runtime binary")

    # Deployment binary bytes may differ from the local verification build, so
    # the runtime binary hash is intentionally inserted later by the container.
    # Every research/config/source identity field remains immutable here.
    identity.pop("deployment_bundle_hash", None)
    identity.pop("strategy_source_manifest_hash", None)

    manifest = strategy_source_manifest(root=root)
    manifest_hash = stable_hash(manifest)
    core = {
        "schema_version": DEPLOYMENT_BUNDLE_SCHEMA_VERSION,
        "bundle_type": DEPLOYMENT_BUNDLE_TYPE,
        "generated_at": _utc_now(),
        "identity": identity,
        "deployment": {
            "mode": context["deployment"],
            "canary_fraction": context["canary_fraction"],
        },
        "strategy_source_manifest": manifest,
        "strategy_source_manifest_hash": manifest_hash,
        "verification": {
            "full_research_reverification": True,
            "canonical_replay_reverified": True,
            "terminal_holdout_reverified": True,
            "lineage_reverified": True,
            "source_ancestry_verified": True,
            "host_native_runtime_sha256": host_binary_sha256,
        },
    }
    return {**core, "bundle_hash": stable_hash(core)}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--output",
        type=Path,
        default=DEFAULT_DEPLOYMENT_BUNDLE_PATH,
        help="Generated deployment bundle path",
    )
    parser.add_argument(
        "--print-only",
        action="store_true",
        help="Print the verified bundle without writing it",
    )
    args = parser.parse_args()

    bundle = build_bundle(root=ROOT)
    if not args.print_only:
        _atomic_json(args.output, bundle)
    summary = {
        "ok": True,
        "output": None if args.print_only else str(args.output),
        "bundle_hash": bundle["bundle_hash"],
        "candidate_id": bundle["identity"]["candidate_id"],
        "candidate_source_sha": bundle["identity"]["candidate_source_sha"],
        "strategy_name": bundle["identity"]["strategy_name"],
        "strategy_config_hash": bundle["identity"]["strategy_config_hash"],
        "replay_execution_config_hash": bundle["identity"]["replay_execution_config_hash"],
        "strategy_source_manifest_hash": bundle["strategy_source_manifest_hash"],
        "canary_fraction": bundle["deployment"]["canary_fraction"],
        "full_research_reverification": True,
    }
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
