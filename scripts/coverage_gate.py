#!/usr/bin/env python3
"""Per-module coverage gate.

Enforces a minimum line + branch coverage threshold (default 90%) for every
module listed in a manifest. Works for Python (coverage.py JSON), Node
(`node --test --experimental-test-coverage` text table) and Rust
(cargo-llvm-cov JSON) via small format adapters.

Usage:
    coverage_gate.py --lang python --manifest manifest.txt --data coverage.json
    coverage_gate.py --lang node   --manifest manifest.txt --data node_cov.txt
    coverage_gate.py --lang rust   --manifest manifest.txt --data rust_cov.json

Exit code is non-zero if ANY targeted module is below threshold.
"""
from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path


def normalize_python(data: dict) -> dict[str, dict[str, float]]:
    out: dict[str, dict[str, float]] = {}
    files = data.get("files", {})
    for path, info in files.items():
        s = info.get("summary", {})
        line_pct = s.get("percent_covered")  # 0-100 float
        branch_pct = _branch_pct(s)
        out[path] = {"lines": line_pct or 0.0, "branches": branch_pct}
    return out


def _branch_pct(s: dict) -> float:
    nb = s.get("num_branches")
    if not nb:
        # No branches -> treat as 100 (nothing to cover)
        return 100.0
    mb = s.get("missing_branches", 0)
    cov = nb - mb
    return 100.0 * cov / nb


def normalize_rust(data: dict) -> dict[str, dict[str, float]]:
    out: dict[str, dict[str, float]] = {}
    # cargo-llvm-cov --json v1: {data:[{files:[{filename, summary:{lines,branches,regions}}]}]}
    recs = data.get("data") or []
    if isinstance(recs, dict):
        recs = [recs]
    files = []
    for rec in recs:
        files.extend(rec.get("files", []))
    for f in files:
        name = f.get("filename") or f.get("file") or ""
        s = f.get("summary", {})
        lines = s.get("lines", {}).get("percent")
        if lines is None:
            lines = _pct(s.get("lines", {}).get("covered"), s.get("lines", {}).get("total"))
        # Branch coverage: prefer `branches`, fall back to `regions` (llvm-cov
        # often reports 0 branches but non-zero region coverage).
        br = s.get("branches", {})
        if br.get("count"):
            branches = br.get("percent")
        else:
            branches = s.get("regions", {}).get("percent")
        if branches is None:
            branches = _pct(s.get("branches", {}).get("covered"), s.get("branches", {}).get("total"))
        out[name] = {
            "lines": float(lines or 0.0),
            "branches": float(branches if branches is not None else 100.0),
        }
    return out


def _pct(covered, total):
    if not total:
        return 100.0
    return 100.0 * (covered or 0) / total


def normalize_node(text: str) -> dict[str, dict[str, float]]:
    """Parse the `node --test --experimental-test-coverage` text table.

    The table is a directory tree: parent rows have blank percentage columns
    and leaf rows carry the numeric line/branch percentages. We reconstruct
    full paths from the indentation stack.
    """
    out: dict[str, dict[str, float]] = {}
    leaf_pat = re.compile(
        r"^#(?P<indent>\s*)(?P<name>\S+)\s*\|\s*(?P<line>[\d.]+)\s*\|"
        r"\s*(?P<branch>[\d.]+)\s*\|\s*(?P<func>[\d.]+)\s*\|"
    )
    # A header row: name followed by blank percentage columns.
    header_pat = re.compile(r"^#(?P<indent>\s*)(?P<name>\S+)\s*\|\s*\|")
    stack: list[tuple[int, str]] = []  # (indent_len, name)
    for line in text.splitlines():
        mh = header_pat.match(line)
        if mh and not leaf_pat.match(line):
            indent = len(mh.group("indent"))
            name = mh.group("name")
            # pop deeper-or-equal entries, then push
            while stack and stack[-1][0] >= indent:
                stack.pop()
            stack.append((indent, name))
            continue
        ml = leaf_pat.match(line)
        if not ml:
            continue
        indent = len(ml.group("indent"))
        name = ml.group("name")
        while stack and stack[-1][0] >= indent:
            stack.pop()
        parts = [s[1] for s in stack] + [name]
        path = "/".join(parts)
        out[path] = {
            "lines": float(ml.group("line")),
            "branches": float(ml.group("branch")),
        }
    return out


def load_manifest(path: str) -> list[str]:
    items = []
    for raw in Path(path).read_text().splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        items.append(line)
    return items


def best_match(norm: dict[str, dict[str, float]], target: str) -> dict | None:
    """Find the coverage entry whose path ends with the target (normalized)."""
    target_n = target.replace("\\", "/")
    # exact suffix match
    for k, v in norm.items():
        if k.replace("\\", "/").endswith(target_n):
            return v
    # prefix match (directory targets)
    matches = [v for k, v in norm.items() if k.replace("\\", "/").startswith(target_n.rstrip("/") + "/")]
    if len(matches) == 1:
        return matches[0]
    if matches:
        # aggregate directory
        ls = [m["lines"] for m in matches]
        bs = [m["branches"] for m in matches]
        return {"lines": sum(ls) / len(ls), "branches": sum(bs) / len(bs)}
    return None


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--lang", choices=["python", "node", "rust"], required=True)
    ap.add_argument("--manifest", required=True)
    ap.add_argument("--data", required=True, help="coverage data file (json or text)")
    ap.add_argument("--threshold", type=float, default=90.0)
    ap.add_argument("--branch-threshold", type=float, default=None)
    args = ap.parse_args()

    branch_thr = args.branch_threshold if args.branch_threshold is not None else args.threshold

    raw = Path(args.data).read_text()
    if args.lang == "python":
        norm = normalize_python(json.loads(raw))
    elif args.lang == "rust":
        norm = normalize_rust(json.loads(raw))
    else:
        norm = normalize_node(raw)

    manifest = load_manifest(args.manifest)
    failures = 0
    print(f"{'MODULE':<70} {'LINES':>7} {'BRANCH':>7}  STATUS")
    print("-" * 96)
    for target in manifest:
        cov = best_match(norm, target)
        if cov is None:
            print(f"{target:<70} {'N/A':>7} {'N/A':>7}  MISSING (no data)")
            failures += 1
            continue
        line_ok = cov["lines"] >= args.threshold
        branch_ok = cov["branches"] >= branch_thr
        ok = line_ok and branch_ok
        status = "PASS" if ok else "FAIL"
        if not ok:
            failures += 1
        print(f"{target:<70} {cov['lines']:>6.1f}% {cov['branches']:>6.1f}%  {status}")

    print("-" * 96)
    print(f"Threshold: lines>={args.threshold}% branches>={branch_thr}%")
    print(f"Modules checked: {len(manifest)}  Failures: {failures}")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
