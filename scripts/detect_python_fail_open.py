#!/usr/bin/env python3
"""Fail-open detector for the Python trading engine.

Auditing found a consistent defect shape across the Python trading engine: a
guard that returns a permissive value when it has no information. Concretely
that shows up as

  * an ``except`` branch that returns True/None instead of refusing
  * a "missing input" branch (``if x is None``) that permits
  * a state machine that enumerates known states then falls through to True
  * a verifier that short-circuits to success before inspecting its input
  * ``mapping.get(key, True)`` on a required check

Fixing these one at a time does not scale, and a fix without a detector does not
hold. So this detects the class.

Because a legacy tree cannot be clean on day one, findings are baselined. CI
fails on any finding *not* in the baseline, so recorded debt can only shrink.

  python3 scripts/detect_python_fail_open.py             # check (exit 1 on new findings)
  python3 scripts/detect_python_fail_open.py --baseline  # re-record the baseline
  python3 scripts/detect_python_fail_open.py --json

This is complementary to behavioural tests, not a replacement. It finds shapes
mechanically; tests catch semantics. Both are needed.

What this does NOT cover, so it is not over-trusted:

  * Missing sign/range validation. A spend limiter whose checks are all ``> cap``
    accepts negative amounts; nothing here notices.
  * Type mismatches between writer and reader. Records stamped as ISO 8601 and
    parsed with float() made an entire approval TTL inert, with no permissive
    return anywhere in sight.
  * Divergent duplicates of the same safety decision. A second, weaker
    kill-switch implementation in the optimizer is invisible to a per-file scan;
    only a test asserting the two agree catches it.
  * Files outside trading_system/ and coinbase/. The root-level modules
    (portfolio_optimizer.py, approval_server.py) are deliberately not scanned.

Every one of those has been a real defect here. Treat a clean scan as "these five
shapes are absent", not as "the tree is safe".
"""
from __future__ import annotations

import argparse
import ast
import json
import re
import sys
import warnings
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
BASELINE_PATH = REPO_ROOT / "config" / "python-fail-open-baseline.json"

EXCLUDE_DIRS = {
    ".git", "node_modules", ".venv", ".venv_test", "venv", "__pycache__",
    ".pytest_cache", ".mypy_cache", ".ruff_cache", "data", "artifacts",
    "deploy", "archive", "dist", "build",
}

SCOPED_ROOTS = ("trading_system", "coinbase")

# Reporting/telemetry legitimately swallow errors; a swallowed exception in a
# health probe is not a fail-open.
NETWORK_TOLERANT = re.compile(
    r"(^|/)(health|metrics|telemetry|logging|notification|notify|reporting)\w*\.py$",
    re.I,
)

# A function whose name promises a decision about safety.
SAFETY_NAME = re.compile(
    r"(can_|is_|should_|must_|verify|validate|check|enforce|guard|assert)"
    r"[\w]*(safe|execute|exec|trade|order|approve|allowed|allow|permit|block|halt|kill|"
    r"within|limit|risk|auth|eligib|eligible|live|valid|signature|balance|"
    r"cash|notional|position|drawdown|circuit|breaker|reject|token|secret|amount|size)",
    re.I,
)

VERIFY_NAME = re.compile(r"^(verify|validate|check|assert|is_valid)", re.I)
DECIDE_NAME = re.compile(r"^(can_|is_|should_|must_)", re.I)

# Predicates where True is the *safe* answer, so a `return True` on an error
# path is correct rather than a fail-open. `is_kill_switch_active` returning True
# means halted; treating that as a permit is backwards.
NEGATIVE_POLARITY = re.compile(
    r"(is_.*(active|halted|blocked|disabled|engaged|tripped|faulted|killed|cancelled|expired))"
    r"|(should_(halt|stop|block|kill|deny|refuse|reject))"
    r"|(^has_(error|failed))",
    re.I,
)

FUNCTION_NODES = (ast.FunctionDef, ast.AsyncFunctionDef)

PERMISSIVE = (True, None)

# Distinguishes "this return is permissive" from "this return is not permissive".
# Conflating the two flagged every ordinary `return False, "reason"` guard.
NOT_PERMISSIVE = object()


def _relative(path: Path) -> str:
    try:
        return str(path.relative_to(REPO_ROOT))
    except ValueError:
        return str(path)


def _in_scope(path: Path) -> bool:
    """Whether a path is in an analysed tree.

    Exclusions are matched against the path *relative to the repo root*, never
    the absolute path. An absolute-path match excludes everything when the
    checkout itself sits under an excluded name -- this repo was at
    /media/scott/data/git/..., so the "data" exclusion silently excluded all
    872 files and every scan came back clean.
    """
    try:
        rel = path.resolve().relative_to(REPO_ROOT)
    except ValueError:
        return False
    if set(rel.parts) & EXCLUDE_DIRS:
        return False
    rel_str = str(rel)
    return any(rel_str.startswith(root + "/") for root in SCOPED_ROOTS)


def _is_safety_function(node) -> bool:
    return not node.name.startswith(("test_", "_")) and bool(SAFETY_NAME.search(node.name))


def _expr(node) -> str:
    try:
        return ast.unparse(node)
    except Exception:
        return "<expr>"


def _permissive_value(return_node: ast.Return):
    """Return the permissive value of a return, or NOT_PERMISSIVE.

    Handles the ``(ok, reason)`` tuple idiom, which is the dominant guard shape
    in this codebase, and the bare ``True``/``None`` forms.
    """
    value = return_node.value
    if value is None:
        return NOT_PERMISSIVE
    if isinstance(value, ast.Tuple) and value.elts:
        return _permissive_const(value.elts[0])
    return _permissive_const(value)


def _is_permissive(return_node: ast.Return) -> bool:
    return _permissive_value(return_node) is not NOT_PERMISSIVE


def _permissive_const(node):
    """Return the permissive value, or NOT_PERMISSIVE.

    Note this must return the sentinel rather than None: a bare ``None`` here
    is indistinguishable from a legitimately permissive ``return None``, which
    silently turned every ``return False`` into a finding.
    """
    if isinstance(node, ast.Constant) and node.value in PERMISSIVE:
        return node.value
    return NOT_PERMISSIVE


def _is_missing_input_test(test: ast.expr) -> bool:
    """True when a branch condition is about absence of information.

    Only ``x is None``, ``not x``, ``x not in d`` and ``len(x) == 0`` count.
    Deliberately narrow: an earlier version treated ``== 0`` as absence and
    flagged every ``if result.returncode == 0: return True`` in the tree.
    """
    for node in ast.walk(test):
        if isinstance(node, ast.Compare):
            for op in node.ops:
                if isinstance(op, ast.NotIn):
                    return True
                if isinstance(op, (ast.Is, ast.IsNot)):
                    if any(isinstance(c, ast.Constant) and c.value is None
                           for c in node.comparators):
                        return True
            for comparator in node.comparators:
                # len(x) == 0 / len(x) < 1
                if isinstance(comparator, ast.Constant) and comparator.value in (0, 1):
                    if isinstance(node.left, ast.Call) and getattr(node.left.func, "attr", None) == "len":
                        return True
                if isinstance(comparator, (ast.Dict, ast.List, ast.Tuple, ast.Set)) and not comparator.elts:
                    return True
        if isinstance(node, ast.UnaryOp) and isinstance(node.op, ast.Not):
            return True
    return False


def _safe_answer_is_true(symbol: str) -> bool:
    return bool(NEGATIVE_POLARITY.search(symbol))


def _iter_returns(nodes):
    for node in nodes if isinstance(nodes, list) else [nodes]:
        if isinstance(node, ast.Return):
            yield node
        else:
            for inner in ast.walk(node):
                if isinstance(inner, ast.Return):
                    yield inner


def _statement_returns(body) -> bool:
    """Whether a branch body unconditionally returns."""
    real = [n for n in body if not isinstance(n, (ast.Expr, ast.Pass))]
    return bool(real) and isinstance(real[-1], ast.Return)


def _find_unguarded_return_true(func, symbol: str) -> list[dict]:
    """A top-level ``return True`` in a verifier that is *not* the success path.

    ``verify_x`` returning True before it has inspected anything means the
    function authenticates nothing. The final ``return True`` of a checker is
    excluded: that is the ordinary success path, not a short-circuit.
    """
    findings = []
    top_level = list(func.body)
    for index, node in enumerate(top_level):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            continue
        if not isinstance(node, ast.Return):
            continue
        if (_is_permissive(node) and _permissive_value(node) is True
                and index < len(top_level) - 1 and not _safe_answer_is_true(symbol)):
            findings.append({
                "file": node.lineno,
                "kind": "verifier_short_circuits_to_permit",
                "detail": f"{symbol}: returns True at line {node.lineno} before any check; nothing is verified",
                "symbol": symbol,
            })
        break
    return findings


def _find_missing_input_permits(func, symbol: str) -> list[dict]:
    """A guard clause for absent input, or an except branch, that permits."""
    findings = []
    for node in ast.walk(func):
        if isinstance(node, ast.Try):
            for handler in node.handlers:
                for ret in _iter_returns(handler.body):
                    if _is_permissive(ret) and not _safe_answer_is_true(symbol):
                        findings.append({
                            "file": ret.lineno,
                            "kind": "except_returns_permissive",
                            "detail": f"{symbol}: except {_handler_label(handler)} returns {_expr(ret.value)}",
                            "symbol": symbol,
                        })
            continue
        if not isinstance(node, ast.If):
            continue
        if not _is_missing_input_test(node.test):
            continue
        if not _statement_returns(node.body):
            continue
        for ret in _iter_returns(node.body):
            if _is_permissive(ret) and not _safe_answer_is_true(symbol):
                findings.append({
                    "file": ret.lineno,
                    "kind": "missing_input_returns_permissive",
                    "detail": f"{symbol}: absent-input branch returns {_expr(ret.value)}",
                    "symbol": symbol,
                })
    return findings


def _handler_label(handler: ast.ExceptHandler) -> str:
    if handler.type is None:
        return "block"
    try:
        return ast.unparse(handler.type)
    except Exception:
        return "Exception"


def _find_state_machine_fallthrough(func, symbol: str) -> list[dict]:
    findings = []
    if not DECIDE_NAME.match(func.name) or _safe_answer_is_true(symbol):
        return findings
    body = [n for n in func.body if not isinstance(n, ast.Expr)]
    if not body:
        return findings
    last = body[-1]
    if not isinstance(last, ast.Return) or _permissive_value(last) is not True:
        return findings
    states = _string_state_comparisons(func)
    if len(states) >= 2:
        findings.append({
            "file": last.lineno,
            "kind": "state_machine_falls_through_to_permit",
            "detail": (
                f"{symbol}: compares against {sorted(states)} then falls through to True, "
                "so an unrecognised state permits"
            ),
            "symbol": symbol,
        })
    return findings


def _string_state_comparisons(func) -> set[str]:
    found = set()
    for node in ast.walk(func):
        if isinstance(node, ast.Compare) and isinstance(node.left, ast.Attribute):
            if node.left.attr in ("_state", "state", "status"):
                for comparator in node.comparators:
                    if isinstance(comparator, ast.Constant) and isinstance(comparator.value, str):
                        found.add(comparator.value)
    return found


def _find_permissive_defaults(func, symbol: str) -> list[dict]:
    findings = []
    for node in ast.walk(func):
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute) and node.func.attr == "get":
            for default in node.args[1:]:
                if _permissive_const(default) is True:
                    findings.append({
                        "file": node.lineno,
                        "kind": "permissive_lookup_default",
                        "detail": f"{symbol}: .get(key, True) makes an absent required check pass",
                        "symbol": symbol,
                    })
    return findings


def analyse_file(path: Path) -> list[dict]:
    try:
        with warnings.catch_warnings():
            # Some modules contain invalid escape sequences. That is theirs to
            # fix; it should not add noise to this gate.
            warnings.simplefilter("ignore")
            tree = ast.parse(path.read_text(encoding="utf-8", errors="replace"))
    except SyntaxError:
        # A file that does not parse is a build problem, not a fail-open, and is
        # already excluded from scripts/coverage/python_manifest.txt. Tracking it
        # here conflated two unrelated gates.
        return []

    findings = []
    for node in ast.walk(tree):
        if not isinstance(node, FUNCTION_NODES):
            continue
        symbol = node.name
        if symbol.startswith("test_"):
            continue
        if VERIFY_NAME.match(symbol):
            findings.extend(_find_unguarded_return_true(node, symbol))
        if _is_safety_function(node):
            findings.extend(_find_missing_input_permits(node, symbol))
            findings.extend(_find_state_machine_fallthrough(node, symbol))
            findings.extend(_find_permissive_defaults(node, symbol))

    for finding in findings:
        finding["path"] = _relative(path)
    return findings


def scan() -> list[dict]:
    findings = []
    for root in SCOPED_ROOTS:
        base = REPO_ROOT / root
        if not base.exists():
            continue
        for path in sorted(base.rglob("*.py")):
            if not _in_scope(path) or NETWORK_TOLERANT.search(_relative(path)):
                continue
            findings.extend(analyse_file(path))
    unique = {}
    for finding in findings:
        unique[fingerprint(finding)] = finding
    return sorted(unique.values(), key=lambda f: (f["path"], f["file"], f["kind"]))


def fingerprint(finding: dict) -> str:
    return "|".join([finding["path"], finding["kind"], finding["symbol"], finding["detail"]])


def load_baseline() -> set[str]:
    if not BASELINE_PATH.exists():
        return set()
    return {e["fingerprint"] for e in json.loads(BASELINE_PATH.read_text()).get("findings", [])}


def main() -> int:
    parser = argparse.ArgumentParser(description="Detect fail-open guards in the Python trading engine.")
    parser.add_argument("--baseline", action="store_true", help="record current findings as the baseline")
    parser.add_argument("--json", action="store_true", help="emit JSON")
    args = parser.parse_args()

    findings = scan()

    if args.baseline:
        BASELINE_PATH.parent.mkdir(parents=True, exist_ok=True)
        BASELINE_PATH.write_text(json.dumps({
            "description": (
                "Known fail-open findings in the Python trading engine, recorded so they are tracked "
                "rather than blocking. CI fails on any finding not listed here, so this file can "
                "only shrink. Regenerate deliberately with: "
                "python3 scripts/detect_python_fail_open.py --baseline"
            ),
            "findings": [{**f, "fingerprint": fingerprint(f)} for f in findings],
        }, indent=2, sort_keys=True) + "\n")
        print(json.dumps({"ok": True, "baselined": len(findings), "path": str(BASELINE_PATH)}, indent=2))
        return 0

    baseline = load_baseline()
    unbaselined = [f for f in findings if fingerprint(f) not in baseline]

    if args.json:
        print(json.dumps({
            "ok": not unbaselined,
            "total": len(findings),
            "baselined": len(findings) - len(unbaselined),
            "unbaselined": [{**f, "fingerprint": fingerprint(f)} for f in unbaselined],
        }, indent=2))
    else:
        print(f"python fail-open scan: {len(findings)} finding(s); "
              f"{len(findings) - len(unbaselined)} baselined; {len(unbaselined)} new")
        for finding in unbaselined:
            print(f"  NEW {finding['path']}:{finding['file']}  {finding['kind']}: {finding['detail']}")
        if not unbaselined:
            print("  no new fail-open patterns")

    if unbaselined:
        print(f"FAIL: {len(unbaselined)} new fail-open finding(s) absent from the baseline", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
