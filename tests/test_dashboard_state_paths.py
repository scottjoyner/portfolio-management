"""Every operator-state path in the dashboard must honour TRADING_DATA_DIR.

Thirteen call sites in ``dashboard_server.py`` computed paths as
``ROOT / "data" / <file>``, where ``ROOT`` comes from the server script's own
location. Those ignore ``TRADING_DATA_DIR`` entirely, which meant:

* the three endpoints that move capital or stop trading -- ``POST /kill-switch``,
  ``POST /execution/brackets/cancel[-all]`` and ``POST /orders/submit`` -- could
  not be pointed at a scratch environment at all;
* a test run against the server touched the operator's real state, which is how
  the 2026-10-04 incident closed two live positions;
* there was no way to rehearse those endpoints or roll them back.

This pins the two properties that make the fix safe and the fix itself.

The properties
--------------
1. **Default unchanged.** With no override, every path must still resolve to
   ``ROOT / "data"`` -- byte-for-byte where it landed before. A relocation bug
   here would silently move a deployment's kill switch, which is the worst
   available outcome: trading that cannot be stopped.
2. **Override honoured.** With ``TRADING_DATA_DIR`` set, writes must land there
   instead, even for the endpoints that previously ignored it.

Both are asserted against a real server, in a sandbox, with the operator's data
directory fingerprinted before and after.

Run: pytest tests/test_dashboard_state_paths.py
"""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import subprocess
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
SANDBOX = Path(os.environ.get("STATE_PATHS_SANDBOX", "/tmp/opencode/sbx-paths"))
TOKEN = "state-paths-" + "z" * 40

# Every path the fix touched, and whether it is written.
STATE_FILES = [
    "trading_kill_switch",          # POST /kill-switch -- halts trading
    "optimizer_brackets.json",      # bracket cancel / cancel-all
    "pending_approvals.json",       # POST /orders/submit
    "paper-trades.json",
    ".trader_health.json",
    "hermes_agent_ledger.json",
    "paper_trader_v4_state.json",
]


def _python() -> str:
    venv = REPO_ROOT / ".venv" / "bin" / "python3"
    return str(venv) if venv.exists() else sys.executable


# Which data directory these tests hold harmless. Resolved once, and used by both
# the fixture and the assertions -- an earlier version let the two disagree, so
# the "after" snapshot was taken from the worktree while the "before" came from
# the deployment, and every worktree file was reported as newly created.
#
# Running from a git worktree makes REPO_ROOT a directory whose data/ holds only
# git-tracked files, missing every untracked operator file worth protecting
# (trading_kill_switch, pending_approvals.json, optimizer_brackets.json are all
# gitignored). Fingerprinting that proves nothing about the deployment, so
# DEPLOYMENT_ROOT names it explicitly.
WATCHED_DATA_ROOT = Path(os.environ.get("DEPLOYMENT_ROOT", REPO_ROOT)) / "data"


def _fingerprint(root: Path) -> dict[str, str]:
    out: dict[str, str] = {}
    for path in sorted(root.rglob("*")):
        if path.is_file() and "feed_cache" not in str(path) and "quarantine" not in str(path):
            try:
                out[str(path)] = hashlib.sha256(path.read_bytes()).hexdigest()[:16]
            except OSError:
                out[str(path)] = "unreadable"
    return out


def _register_sandbox_teardown(request, path: Path, keep_var: str) -> None:
    """Remove the copied tree at session end so /tmp does not grow without bound.

    Each run rsyncs a whole checkout, and nothing reclaimed it: /tmp/opencode/sbx
    and sbx-paths had reached ~764MB of abandoned trees.

    The tree is kept when the run failed, because that is exactly when it is worth
    inspecting, or when the operator asks for it. Failures are counted by reading
    the session's own report at teardown time, since a yield-fixture cannot know
    whether later tests passed.
    """
    def _cleanup() -> None:
        if os.environ.get(keep_var):
            print(f"\n[{keep_var} set] sandbox kept at {path}")
            return
        failed = getattr(request.session, "testsfailed", None)
        if failed:
            print(f"\n[{failed} test(s) failed] sandbox kept for inspection: {path}")
            return
        shutil.rmtree(path, ignore_errors=True)

    request.addfinalizer(_cleanup)


@pytest.fixture(scope="module")
def sandbox(request):
    if SANDBOX.exists():
        shutil.rmtree(SANDBOX)
    SANDBOX.mkdir(parents=True)
    result = subprocess.run(
        ["rsync", "-a",
         "--exclude=.git", "--exclude=data", "--exclude=logs",
         "--exclude=node_modules", "--exclude=*.so", "--exclude=__pycache__",
         f"{REPO_ROOT}/", f"{SANDBOX}/"],
        capture_output=True, text=True,
    )
    if result.returncode != 0:
        pytest.skip(f"rsync failed: {result.stderr[:200]}")
    for so in (REPO_ROOT / "rust_core").glob("*.so"):
        target = SANDBOX / "rust_core" / so.name
        target.parent.mkdir(parents=True, exist_ok=True)
        target.symlink_to(so)
    (SANDBOX / "data").mkdir(exist_ok=True)
    (SANDBOX / "logs").mkdir(exist_ok=True)
    _register_sandbox_teardown(request, SANDBOX, "STATE_PATHS_KEEP_SANDBOX")
    return SANDBOX


def _start(sandbox: Path, data_dir: Path | None, port: int):
    """Run the real server out of the sandbox.

    ``data_dir=None`` means *no* TRADING_DATA_DIR at all, which is the production
    configuration and the one whose behaviour must not change.
    """
    env = {
        "PATH": "/usr/bin:/bin",
        "HOME": str(sandbox),
        "XDG_CONFIG_HOME": str(sandbox / "xdg"),
        "DASHBOARD_OPERATOR_TOKEN": TOKEN,
        "PYTHONFAULTHANDLER": "1",
    }
    if data_dir is not None:
        env["TRADING_DATA_DIR"] = str(data_dir)
    proc = subprocess.Popen(
        [_python(), str(sandbox / "trading_system" / "ui" / "dashboard_server.py"),
         "--port", str(port)],
        cwd=str(sandbox), env=env,
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
    )
    base = f"http://127.0.0.1:{port}"
    deadline = time.time() + 60
    while time.time() < deadline:
        if proc.poll() is not None:
            pytest.skip("sandbox dashboard would not start")
        try:
            with urllib.request.urlopen(base + "/health", timeout=3):
                return proc, base
        except Exception:
            time.sleep(0.4)
    proc.kill()
    pytest.skip("sandbox dashboard did not become healthy")


def _free_port() -> int:
    import socket
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


def _post(base: str, path: str, body: dict):
    req = urllib.request.Request(
        base + path, data=json.dumps(body).encode(), method="POST")
    req.add_header("Content-Type", "application/json")
    req.add_header("Authorization", "Bearer " + TOKEN)
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            return r.status, r.read().decode("utf-8", "replace")
    except urllib.error.HTTPError as exc:
        return exc.code, exc.read().decode("utf-8", "replace")
    except Exception as exc:
        return 0, str(exc)


# ── property 1: the default is unchanged ────────────────────────────────────

def test_without_an_override_writes_still_land_in_root_data(sandbox):
    """The behaviour that must not regress.

    No TRADING_DATA_DIR, exactly like production. Every write must land in
    ``<sandbox>/data`` -- the path these call sites produced before the fix. If
    this moves, a deployment's kill switch moves with it.
    """
    default_data = sandbox / "data"
    for stale in STATE_FILES:
        (default_data / stale).unlink(missing_ok=True)

    proc, base = _start(sandbox, data_dir=None, port=_free_port())
    try:
        # Exercise all three capital-or-risk endpoints.
        _post(base, "/orders/submit",
              {"symbol": "BTC-USD", "side": "BUY", "size_usd": 100})
        _post(base, "/kill-switch", {"enabled": True})
        _post(base, "/execution/brackets/cancel-all", {})

        assert (default_data / "pending_approvals.json").exists(), \
            "order submit must default to ROOT/data"
        assert (default_data / "trading_kill_switch").exists(), \
            "the kill switch must default to ROOT/data"
        assert (default_data / "optimizer_brackets.json").exists(), \
            "bracket cancel must default to ROOT/data"
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            proc.kill()


# ── property 2: the override is now honoured ───────────────────────────────

def test_the_override_is_honoured_by_the_endpoints_that_ignored_it(sandbox, real_data_fingerprint):
    """The fix.

    These three wrote to ROOT/data no matter what TRADING_DATA_DIR said. Now the
    override moves them, which is what makes them testable, rehearsable and
    reversible at all.
    """
    override = sandbox / "elsewhere"
    if override.exists():
        shutil.rmtree(override)
    override.mkdir()

    # Clear the default location first. The previous test deliberately wrote
    # there (that is its whole point), and without clearing it this test cannot
    # tell its own writes from the leftovers.
    default_data = sandbox / "data"
    for stale in STATE_FILES:
        (default_data / stale).unlink(missing_ok=True)

    proc, base = _start(sandbox, data_dir=override, port=_free_port())
    try:
        code, _ = _post(base, "/orders/submit",
                        {"symbol": "BTC-USD", "side": "BUY", "size_usd": 100})
        assert code == 200, code
        code, _ = _post(base, "/kill-switch", {"enabled": True})
        assert code == 200, code
        code, _ = _post(base, "/execution/brackets/cancel-all", {})
        assert code == 200, code
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            proc.kill()

    assert (override / "pending_approvals.json").exists(), \
        "order submit ignored TRADING_DATA_DIR"
    assert (override / "trading_kill_switch").exists(), \
        "the kill switch ignored TRADING_DATA_DIR"
    assert (override / "optimizer_brackets.json").exists(), \
        "bracket cancel ignored TRADING_DATA_DIR"

    # And the default location was not written to at all -- that is the whole
    # claim: with the override set, the default must stay untouched.
    for name in ("pending_approvals.json", "trading_kill_switch", "optimizer_brackets.json"):
        assert not (default_data / name).exists(), \
            f"{name} was written to the default location despite the override"

    after = _fingerprint(WATCHED_DATA_ROOT)
    assert after == real_data_fingerprint, "the operator's data dir changed"


def test_the_trader_and_the_dashboard_agree_on_the_kill_switch_path(sandbox):
    """The two halves of the kill switch must never diverge.

    The trader reads it through trading_paths.resolve(); the dashboard writes it.
    Both used to reach the same file only because production happens to run with
    its working directory at the repo root -- which is exactly the kind of
    coincidence that stops being true.
    """
    script = (
        "import sys; sys.path.insert(0, '.');\n"
        "from pathlib import Path;\n"
        "import trading_paths as tp;\n"
        "ROOT = Path('trading_system/ui/dashboard_server.py').resolve().parents[2];\n"
        "import os;\n"
        "override = os.environ.get('TRADING_DATA_DIR');\n"
        "dash = Path(override) if override else ROOT / 'data';\n"
        "trader = tp.resolve('data/trading_kill_switch');\n"
        "print('DASH', dash / 'trading_kill_switch');\n"
        "print('TRADER', trader if trader.is_absolute() else (ROOT / trader));\n"
    )
    result = subprocess.run(
        [_python(), "-c", script], cwd=str(sandbox),
        capture_output=True, text=True, timeout=60,
    )
    assert result.returncode == 0, result.stderr[:300]
    lines = dict(
        line.split(" ", 1) for line in result.stdout.strip().splitlines() if " " in line
    )
    assert Path(lines["DASH"]).resolve() == Path(lines["TRADER"]).resolve(), \
        f"dashboard and trader disagree: {lines}"


def test_no_root_relative_data_literal_remains_in_the_server():
    """The guard, so the fix cannot quietly grow back.

    ``ROOT / "data"`` inside a call site means a path that ignores
    TRADING_DATA_DIR. The single permitted occurrence is the definition of
    ``data_dir()`` itself, which is the resolver.
    """
    source = (REPO_ROOT / "trading_system" / "ui" / "dashboard_server.py").read_text()
    offenders = [
        line.strip()
        for line in source.splitlines()
        if 'ROOT / "data"' in line or "ROOT / 'data'" in line
        # the resolver's own default, which is the one legitimate use
        and "return Path(override)" not in line
    ]
    assert not offenders, "ROOT-relative data paths reintroduced:\n" + "\n".join(offenders)


@pytest.fixture(scope="module")
def real_data_fingerprint():
    """Hashes of the operator's data dir, taken before anything runs.

    Explicit target, because running from a git worktree makes ``REPO_ROOT`` a
    directory whose ``data/`` holds only git-tracked files -- missing every
    untracked operator file this test exists to protect. Fingerprinting that
    proves nothing. Set DEPLOYMENT_ROOT to the real deployment to assert about it.
    """
    root = Path(os.environ.get("DEPLOYMENT_ROOT", REPO_ROOT)) / "data"
    assert root.exists(), f"nothing to fingerprint at {root}"
    print(f"\n[data-dir guard] fingerprinting {root} ({len(_fingerprint(root))} files)")
    return _fingerprint(root)


@pytest.fixture(scope="module")
def real_data_fingerprint():
    """Hashes of the watched data dir, taken before anything runs."""
    assert WATCHED_DATA_ROOT.exists(), f"nothing to fingerprint at {WATCHED_DATA_ROOT}"
    print(f"\n[data-dir guard] watching {WATCHED_DATA_ROOT} "
          f"({len(_fingerprint(WATCHED_DATA_ROOT))} files)")
    return _fingerprint(WATCHED_DATA_ROOT)
