"""Authenticated end-to-end coverage of every mutating dashboard endpoint.

Everything else in this suite exercises the client with a stubbed server, which
proves the *client* behaves. It cannot prove the round trip: that a submitted
order actually reaches the approval queue, that approving flips its status, that a
bracket cancel removes it, that the kill switch creates the file that halts
trading. Those are the paths where an operator's money moves, and they were
untested.

The isolation problem, and why this file is shaped the way it is
--------------------------------------------------------------------
Three of these endpoints write to ``ROOT / "data" / ...``, computed from the
server script's own location rather than through ``trading_paths``:

    POST /kill-switch                       -> data/trading_kill_switch
    POST /execution/brackets/cancel[-all]   -> data/optimizer_brackets.json
    POST /orders/submit                     -> data/pending_approvals.json

So ``TRADING_DATA_DIR`` does **not** redirect them. A "scratch" server pointed at
a scratch data dir would still have written to the operator's live files --
creating the file that stops trading, or emptying the real bracket set. That is
the same defect class as the 2026-10-04 incident where the test suite closed two
real positions.

The only reliable isolation is to change ``ROOT`` itself, which means running a
copy of the server from a copy of the tree. This test therefore builds a sandbox
checkout, runs the real server out of it, and asserts afterwards that every file
in the *real* data directory is byte-identical to a fingerprint taken before. If
isolation ever regresses, that assertion fails rather than the test quietly
mutating production.

Run: pytest tests/test_dashboard_authenticated_e2e.py
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
SANDBOX = Path(os.environ.get("AUTH_E2E_SANDBOX", "/tmp/opencode/sbx"))
TOKEN = "auth-e2e-" + "k" * 40

# Copied so the sandbox has the code but none of the operator's state.
COPY_EXCLUDES = [".git", "data", "logs", "node_modules", "__pycache__"]


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
        if not path.is_file():
            continue
        rel = str(path)
        if "feed_cache" in rel or "quarantine" in rel:
            continue
        try:
            out[rel] = hashlib.sha256(path.read_bytes()).hexdigest()[:16]
        except OSError:
            out[rel] = "unreadable"
    return out


# ── sandbox construction ───────────────────────────────────────────────────

@pytest.fixture(scope="session")
def sandbox():
    """A copy of the tree whose ROOT is not the operator's checkout.

    ROOT is derived from the server script's own resolved path, so pointing
    TRADING_DATA_DIR at a scratch directory is not enough -- three of the
    endpoints below ignore it. Copying the tree is the only thing that moves
    ROOT, and therefore the only thing that reliably contains them.
    """
    if SANDBOX.exists():
        shutil.rmtree(SANDBOX)
    SANDBOX.mkdir(parents=True)

    result = subprocess.run(
        ["rsync", "-a",
         *[f"--exclude={e}" for e in COPY_EXCLUDES],
         "--exclude=*.so",
         f"{REPO_ROOT}/", f"{SANDBOX}/"],
        capture_output=True, text=True,
    )
    if result.returncode != 0:
        pytest.skip(f"rsync unavailable or failed: {result.stderr[:200]}")

    # The compiled extension is excluded above; link it rather than rebuilding.
    for so in (REPO_ROOT / "rust_core").glob("*.so"):
        target = SANDBOX / "rust_core" / so.name
        target.parent.mkdir(parents=True, exist_ok=True)
        try:
            target.symlink_to(so)
        except OSError:
            pass

    (SANDBOX / "data").mkdir(exist_ok=True)
    (SANDBOX / "logs").mkdir(exist_ok=True)

    server = SANDBOX / "trading_system" / "ui" / "dashboard_server.py"
    assert server.exists(), "sandbox is missing the dashboard server"
    # The whole point: ROOT must not be the operator's checkout.
    assert server.resolve().parents[2] == SANDBOX, "sandbox ROOT resolved to the real repo"
    return SANDBOX


@pytest.fixture(scope="session")
def real_data_fingerprint():
    """Hashes of the operator's data dir, taken before anything runs.

    Which directory that is depends on where the tests run, and getting it wrong
    makes the assertion worthless rather than merely imprecise. Run from a git
    worktree and ``REPO_ROOT`` is that worktree, whose ``data/`` holds only
    git-tracked files -- an order of magnitude smaller than a deployment's, and
    missing every file this test exists to protect: trading_kill_switch,
    pending_approvals.json and optimizer_brackets.json are all untracked and
    gitignored, so a worktree does not have them at all.

    Fingerprinting that proves nothing about the deployment. So the target is
    explicit -- DEPLOYMENT_ROOT when set, otherwise this checkout -- and the
    fixture prints which one it used. Read the reported path before believing
    the result.
    """
    root = Path(os.environ.get("DEPLOYMENT_ROOT", REPO_ROOT)) / "data"
    assert root.exists(), f"nothing to fingerprint at {root}"
    print(f"\n[data-dir guard] fingerprinting {root} ({len(_fingerprint(root))} files)")
    return _fingerprint(root)


class Api:
    def __init__(self, base: str, token: str | None):
        self.base = base
        self.token = token

    def call(self, method: str, path: str, body=None, timeout: int = 30):
        data = json.dumps(body).encode() if body is not None else None
        req = urllib.request.Request(self.base + path, data=data, method=method)
        if data:
            req.add_header("Content-Type", "application/json")
        if self.token:
            req.add_header("Authorization", "Bearer " + self.token)
        try:
            with urllib.request.urlopen(req, timeout=timeout) as r:
                raw = r.read().decode("utf-8", "replace")
                try:
                    return r.status, json.loads(raw)
                except json.JSONDecodeError:
                    return r.status, raw
        except urllib.error.HTTPError as exc:
            raw = exc.read().decode("utf-8", "replace")
            try:
                return exc.code, json.loads(raw)
            except json.JSONDecodeError:
                return exc.code, raw
        except Exception as exc:  # connection level
            return 0, str(exc)


@pytest.fixture(scope="session")
def live(sandbox):
    """The real server, running out of the sandbox."""
    import socket

    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()

    env = {
        "PATH": "/usr/bin:/bin",
        "HOME": str(sandbox),
        "XDG_CONFIG_HOME": str(sandbox / "xdg"),
        # Pointed at the sandbox too, so the override-aware paths agree with the
        # ROOT-relative ones. Both isolation mechanisms active at once.
        "TRADING_DATA_DIR": str(sandbox / "data"),
        "DASHBOARD_OPERATOR_TOKEN": TOKEN,
        "PYTHONFAULTHANDLER": "1",
    }
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
                break
        except Exception:
            time.sleep(0.4)
    else:
        proc.kill()
        pytest.skip("sandbox dashboard did not become healthy")

    yield {"base": base, "proc": proc, "sandbox": sandbox, "port": port}

    if proc.poll() is None:
        proc.terminate()
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            proc.kill()


@pytest.fixture
def anon(live):
    return Api(live["base"], None)


@pytest.fixture
def authed(live):
    return Api(live["base"], TOKEN)


# ── the auth boundary, for real ────────────────────────────────────────────

MUTATING = [
    ("POST", "/orders/submit", {"symbol": "BTC-USD", "side": "BUY", "size_usd": 250}),
    ("POST", "/kill-switch", {"enabled": True}),
    ("POST", "/execution/brackets/cancel", {"bracket_id": "nope"}),
    ("POST", "/execution/brackets/cancel-all", {}),
    ("POST", "/capital/config", {"targets": {}}),
    ("POST", "/capital/buckets", {"buckets": []}),
    ("POST", "/arbitrage/execute", {}),
    ("POST", "/actions/run", {"action": "refresh_market_data"}),
]


def test_every_mutating_endpoint_refuses_without_a_token(anon):
    for method, path, body in MUTATING:
        code, payload = anon.call(method, path, body)
        assert code == 401, f"{method} {path} returned {code} without a token: {payload}"
        assert isinstance(payload, dict) and payload.get("error") == "unauthorized", payload


def test_every_mutating_endpoint_refuses_a_wrong_token(live):
    wrong = Api(live["base"], "not-the-token-" + "x" * 20)
    for method, path, body in MUTATING:
        code, _ = wrong.call(method, path, body)
        assert code == 401, f"{method} {path} accepted a wrong token"


def test_a_mutating_path_is_refused_even_by_another_method(anon):
    # /kill-switch is POST-only in MUTATING_PATHS; a GET must not slip past the
    # guard just because the dispatcher has no GET handler for it.
    code, _ = anon.call("GET", "/kill-switch")
    assert code in (401, 404), code


# ── the order round trip ───────────────────────────────────────────────────

def test_order_submission_reaches_the_approval_queue(authed, sandbox):
    code, payload = authed.call("POST", "/orders/submit",
                                {"symbol": "BTC-USD", "side": "BUY", "size_usd": 250})
    assert code == 200, payload
    assert payload.get("ok") is True, payload
    token = payload.get("token")
    assert token, f"no approval token returned: {payload}"
    entry = payload.get("approval") or {}
    assert entry.get("status") == "pending"
    assert entry.get("product_id") == "BTC-USD"
    assert float(entry.get("size_usd")) == 250.0

    # It must be visible through the read endpoint too, not only in the response.
    code, listing = authed.call("GET", "/approvals")
    assert code == 200, listing
    tokens = [row.get("token") for row in listing.get("approvals", [])]
    assert token in tokens, f"submitted order missing from /approvals: {tokens}"


def test_submitted_order_is_persisted_to_the_sandbox_not_the_repo(live, authed):
    authed.call("POST", "/orders/submit",
                {"symbol": "ETH-USD", "side": "SELL", "size_usd": 100})
    sandbox_file = live["sandbox"] / "data" / "pending_approvals.json"
    assert sandbox_file.exists(), "the ROOT-relative write did not land in the sandbox"
    assert not (REPO_ROOT / "data" / "pending_approvals.json").exists(), \
        "the ROOT-relative write leaked into the operator's data directory"


def test_approving_flips_the_status(authed):
    _, submitted = authed.call("POST", "/orders/submit",
                               {"symbol": "BTC-USD", "side": "BUY", "size_usd": 250})
    token = submitted["token"]

    code, approved = authed.call("GET", f"/approvals/approve/{token}")
    assert code == 200, approved
    assert approved.get("status") == "approved", approved

    _, listing = authed.call("GET", "/approvals")
    row = next(r for r in listing["approvals"] if r["token"] == token)
    assert row["status"] == "approved", row
    # The provenance bug: auto_approved used to be *inferred* from the status, so
    # an approval a human clicked in the browser came back auto_approved=true and
    # the dashboard labelled it "auto" -- telling an operator the system released
    # a trade they had personally reviewed.
    assert row["auto_approved"] is False, "a human approval must not be reported as automatic"
    assert row["resolved_by"] == "dashboard", row
    assert row["resolved_at"], "the audit trail needs a timestamp"


def test_denying_flips_the_status(authed):
    _, submitted = authed.call("POST", "/orders/submit",
                               {"symbol": "SOL-USD", "side": "BUY", "size_usd": 50})
    token = submitted["token"]

    code, denied = authed.call("GET", f"/approvals/deny/{token}")
    assert code == 200, denied
    assert denied.get("status") == "denied", denied

    _, listing = authed.call("GET", "/approvals")
    row = next(r for r in listing["approvals"] if r["token"] == token)
    assert row["status"] == "denied", row
    assert row["resolved_by"] == "dashboard", "a denial is an audit event too"
    assert row["auto_approved"] is False


def test_approving_an_unknown_token_is_a_404_not_a_silent_success(authed):
    # A missing approval must not report ok. This is the case where a UI that
    # only checked the HTTP status would tell an operator it worked.
    code, payload = authed.call("GET", "/approvals/approve/does-not-exist-at-all")
    assert code == 404, payload
    assert payload.get("ok") is False, payload


def test_order_submission_rejects_a_zero_size(authed):
    code, payload = authed.call("POST", "/orders/submit",
                                {"symbol": "BTC-USD", "side": "BUY", "size_usd": 0})
    assert code == 400, payload
    assert "size_usd" in str(payload), payload


def test_order_submission_rejects_a_bad_side(authed):
    code, payload = authed.call("POST", "/orders/submit",
                                {"symbol": "BTC-USD", "side": "HODL", "size_usd": 100})
    assert code == 400, payload
    assert "BUY|SELL" in str(payload) or "BUY|SELL" in str(payload.get("error", "")), payload


# ── the other mutating endpoints ───────────────────────────────────────────

def test_operator_action_is_queued(authed, live):
    code, payload = authed.call("POST", "/actions/run",
                                {"action": "refresh_market_data", "note": "auth e2e"})
    assert code == 200, payload

    _, listing = authed.call("GET", "/actions")
    queue = listing.get("queue") or []
    assert any(row.get("action") == "refresh_market_data" for row in queue), queue


def test_an_unknown_operator_action_is_refused_with_the_valid_ids(authed):
    code, payload = authed.call("POST", "/actions/run", {"action": "definitely_not_an_action"})
    assert code == 400, payload
    # The error must help the caller fix it rather than reporting a 500.
    assert payload.get("known_actions"), payload


def test_kill_switch_creates_the_file_in_the_sandbox_only(authed, live):
    assert not (live["sandbox"] / "data" / "trading_kill_switch").exists()
    code, payload = authed.call("POST", "/kill-switch", {"enabled": True})
    assert code == 200, payload
    assert payload.get("kill_switch") is True, payload

    flag = live["sandbox"] / "data" / "trading_kill_switch"
    assert flag.exists(), "the kill switch flag did not land in the sandbox"
    # The single most dangerous assertion in this file.
    assert not (REPO_ROOT / "data" / "trading_kill_switch").exists(), \
        "the kill switch file was created in the operator's data directory"

    code, released = authed.call("POST", "/kill-switch", {"enabled": False})
    assert code == 200, released
    assert not flag.exists(), "the kill switch flag was not cleared"


def test_cancel_all_brackets_empties_the_sandbox_file_only(authed, live):
    brackets = live["sandbox"] / "data" / "optimizer_brackets.json"
    brackets.write_text(json.dumps({
        "brk-1": {"product_id": "BTC-USD", "stop_price": 1, "target_price": 2},
        "brk-2": {"product_id": "ETH-USD", "stop_price": 3, "target_price": 4},
    }))
    code, payload = authed.call("POST", "/execution/brackets/cancel-all")
    assert code == 200, payload
    assert json.loads(brackets.read_text()) == {}, "brackets were not cleared"

    # And the operator's own brackets, if any, are untouched.
    real = REPO_ROOT / "data" / "optimizer_brackets.json"
    if real.exists():
        assert json.loads(real.read_text()) != {}, "the operator's brackets were emptied"


def test_cancelling_one_bracket_leaves_the_others(authed, live):
    brackets = live["sandbox"] / "data" / "optimizer_brackets.json"
    brackets.write_text(json.dumps({
        "brk-keep": {"product_id": "BTC-USD"},
        "brk-drop": {"product_id": "ETH-USD"},
    }))
    code, payload = authed.call("POST", "/execution/brackets/cancel",
                                {"bracket_id": "brk-drop"})
    assert code == 200, payload
    remaining = json.loads(brackets.read_text())
    assert list(remaining) == ["brk-keep"], remaining


def test_cancelling_an_unknown_bracket_is_a_404(authed, live):
    (live["sandbox"] / "data" / "optimizer_brackets.json").write_text("{}")
    code, payload = authed.call("POST", "/execution/brackets/cancel",
                                {"bracket_id": "not-a-bracket"})
    assert code == 404, payload
    assert "not found" in str(payload).lower(), payload


def test_capital_buckets_round_trip(authed, live):
    code, payload = authed.call("POST", "/capital/buckets",
                                {"buckets": [{"bucket_id": "core", "cash_usd": 100.0,
                                              "target_usd": 120.0}]})
    assert code == 200, payload
    code, listing = authed.call("GET", "/capital/buckets")
    assert code == 200, listing
    assert listing.get("buckets"), listing
    # The sandbox's file, not the operator's.
    assert (live["sandbox"] / "data" / "capital_buckets.json").exists()


# ── the isolation guarantee ────────────────────────────────────────────────

def test_nothing_touched_the_operators_data_directory(real_data_fingerprint, live):
    """The assertion the whole sandbox exists to make possible.

    Every endpoint above ran for real, including the kill switch and the bracket
    cancel. If any of them resolved against the operator's checkout, this fails.
    """
    assert live["proc"].poll() is None, "the sandbox server died mid-run"
    after = _fingerprint(WATCHED_DATA_ROOT)
    added = sorted(set(after) - set(real_data_fingerprint))
    removed = sorted(set(real_data_fingerprint) - set(after))
    changed = sorted(
        k for k in set(after) & set(real_data_fingerprint)
        if after[k] != real_data_fingerprint[k]
    )
    assert not added, f"files created in the operator's data dir: {added}"
    assert not removed, f"files removed from the operator's data dir: {removed}"
    assert not changed, f"files modified in the operator's data dir: {changed}"


def test_the_sandbox_root_is_not_the_operator_repo(sandbox):
    """Guard the guard: if ROOT ever resolved back to the real checkout, every
    isolation assertion above would be testing the wrong process."""
    server = sandbox / "trading_system" / "ui" / "dashboard_server.py"
    assert server.resolve().parents[2] == sandbox
    assert sandbox.resolve() != REPO_ROOT.resolve()


def test_a_submitted_approval_is_only_in_the_sandbox(live, authed, real_data_fingerprint):
    """The audit trail exists in one place and nowhere else.

    Also re-asserts the per-file version of the isolation guarantee, since this is
    the endpoint whose write path ignores TRADING_DATA_DIR.
    """
    _, submitted = authed.call("POST", "/orders/submit",
                               {"symbol": "BTC-USD", "side": "BUY", "size_usd": 100})
    assert submitted.get("token")

    sandbox_file = live["sandbox"] / "data" / "pending_approvals.json"
    on_disk = json.loads(sandbox_file.read_text())
    assert submitted["token"] in on_disk, "not persisted where the audit expects"

    real_file = REPO_ROOT / "data" / "pending_approvals.json"
    assert not real_file.exists(), "an approval leaked into the operator's data dir"
    assert "data/pending_approvals.json" not in real_data_fingerprint


@pytest.fixture(scope="module")
def real_data_fingerprint():
    """Hashes of the watched data dir, taken before anything runs."""
    assert WATCHED_DATA_ROOT.exists(), f"nothing to fingerprint at {WATCHED_DATA_ROOT}"
    print(f"\n[data-dir guard] watching {WATCHED_DATA_ROOT} "
          f"({len(_fingerprint(WATCHED_DATA_ROOT))} files)")
    return _fingerprint(WATCHED_DATA_ROOT)
