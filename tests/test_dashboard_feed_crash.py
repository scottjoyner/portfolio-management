"""The dashboard must survive serving both feed endpoints.

Reproduces a segfault that killed the whole dashboard process, reproducibly, and
silently: `/market/watchlist` followed by `/market/candles`.

Both endpoints write what they fetched to the Arrow-backed parquet feed cache.
Doing that from more than one thread segfaults the interpreter inside pandas'
Arrow string-array construction, and because the dashboard is a
``ThreadingHTTPServer`` each request is served on its own thread. The requests did
not even have to overlap in time -- one after the other was enough, because the
watchlist leaves an internal thread pool running while the next request arrives.

Nothing about it looked like a crash from the outside: ``/health`` kept answering
200 until the process was already gone, and the browser reported a generic
network error rather than anything pointing at the cache.

The fix is that the dashboard opts out of cache writes (``FEED_CACHE_PERSIST=0``,
honoured in ``feed_cache.save_candles``): it is an observer, and the feed daemon
and trader keep the cache current. These tests pin the gate (both halves of it),
plus a smoke test that the server survives the sequence.

Read the smoke tests honestly. The segfault is **not deterministic**: whether it
fires depends on how many symbols the watchlist's live pair discovery returns,
which moves with exchange volumes. Measured on 2026-10-05, hitting
``/market/watchlist?limit=30`` then ``/market/candles`` killed the process 5 times
out of 5 before the gate and 0 times out of 5 after, but the pytest version of the
same sequence passed against the *unfixed* code on some runs. So these tests
regress the gate's contract and catch an obvious removal; they are not by
themselves proof that the segfault cannot come back. The evidence for the fix is
the before/after measurement, not this file.
"""

import importlib.util
import os
import subprocess
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]


# ── the gate itself ────────────────────────────────────────────────────────

@pytest.fixture
def feed_cache(tmp_path, monkeypatch):
    """(module, cache_root) with data.feed_cache imported against a scratch dir.

    Both variables must be set *before* the import. feed_cache binds NAS_FEED_ROOT
    at module import time, and when the NAS root is not writable it silently
    falls back to the repository's own data/feed_cache -- which is exactly the
    fallback the production log warns about. Point it at a writable tmp path and
    the fallback never engages. TRADING_DATA_DIR is set as well so that anything
    else the import reaches for is redirected too.

    Setting either afterwards, in the test body, has no effect and the write lands
    in the repo, where the data guard correctly refuses it.
    """
    root = tmp_path / "feed_cache"
    root.mkdir()
    monkeypatch.setenv("NAS_FEED_ROOT", str(root))
    monkeypatch.setenv("TRADING_DATA_DIR", str(tmp_path / "data"))
    (tmp_path / "data").mkdir()
    monkeypatch.delenv("FEED_CACHE_PERSIST", raising=False)

    spec = importlib.util.spec_from_file_location(
        "feed_cache_under_test", REPO_ROOT / "data" / "feed_cache.py"
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module, root


@pytest.mark.parametrize("raw", ["0", "false", "FALSE", "no", "off", " 0 "])
def test_writes_disabled_by_flag(monkeypatch, feed_cache, raw):
    feed_cache, _root = feed_cache
    monkeypatch.setenv("FEED_CACHE_PERSIST", raw)
    assert feed_cache.writes_enabled() is False


@pytest.mark.parametrize("raw", ["1", "true", "yes", "on", ""])
def test_writes_enabled_by_default(monkeypatch, feed_cache, raw):
    feed_cache, _root = feed_cache
    monkeypatch.setenv("FEED_CACHE_PERSIST", raw)
    assert feed_cache.writes_enabled() is True


def test_writes_enabled_when_unset(monkeypatch, feed_cache):
    # The trader and the feed daemon must keep writing; an unset variable has to
    # mean "enabled" or this silently turns off durability in production.
    feed_cache, _root = feed_cache
    monkeypatch.delenv("FEED_CACHE_PERSIST", raising=False)
    assert feed_cache.writes_enabled() is True


def test_disabled_write_is_a_no_op_not_an_error(monkeypatch, feed_cache):
    feed_cache, root = feed_cache
    monkeypatch.setenv("FEED_CACHE_PERSIST", "0")
    rows = [[1700000000 + i * 3600, 100.0, 101.0, 99.0, 100.5, 10.0] for i in range(5)]
    # Returns the "no bars written" sentinel and must not raise or write anything.
    assert feed_cache.save_candles("coinbase_candles", "BTC-USD", 3600, rows) == 0
    assert not list(root.rglob("*.parquet"))


def test_enabled_write_still_writes(monkeypatch, feed_cache):
    # The other half: opting out must not have broken the write path for the
    # processes that rely on it.
    feed_cache, root = feed_cache
    monkeypatch.delenv("FEED_CACHE_PERSIST", raising=False)
    rows = [[1700000000 + i * 3600, 100.0, 101.0, 99.0, 100.5, 10.0] for i in range(5)]
    written = feed_cache.save_candles("coinbase_candles", "BTC-USD", 3600, rows)
    assert written == 5, "an enabled write must still persist"
    assert list(root.rglob("*.parquet")), "an enabled write must leave a parquet file"


def test_dashboard_opts_out_of_cache_writes():
    """Set unconditionally in dashboard_server, not inherited from the environment.

    Inherited would mean an operator's exported FEED_CACHE_PERSIST=1 silently
    re-enables the crash path.
    """
    source = (REPO_ROOT / "trading_system" / "ui" / "dashboard_server.py").read_text()
    assert 'os.environ["FEED_CACHE_PERSIST"] = "0"' in source


# ── the crash itself ───────────────────────────────────────────────────────

@pytest.fixture
def dashboard(tmp_path):
    """A real dashboard server on a scratch port and data dir."""
    port = _free_port()
    scratch = tmp_path / "data"
    scratch.mkdir()
    env = {
        "PATH": "/usr/bin:/bin",
        "HOME": str(tmp_path),
        "XDG_CONFIG_HOME": str(tmp_path / "xdg"),
        "TRADING_DATA_DIR": str(scratch),
        "DASHBOARD_OPERATOR_TOKEN": "t-" + "x" * 40,
        "PYTHONFAULTHANDLER": "1",
    }
    proc = subprocess.Popen(
        [_python(), str(REPO_ROOT / "trading_system" / "ui" / "dashboard_server.py"),
         "--port", str(port)],
        cwd=str(REPO_ROOT), env=env,
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
    )
    base = f"http://127.0.0.1:{port}"
    deadline = time.time() + 60
    while time.time() < deadline:
        if proc.poll() is not None:
            pytest.skip("dashboard would not start in this environment")
        try:
            with urllib.request.urlopen(base + "/health", timeout=3):
                break
        except Exception:
            time.sleep(0.4)
    else:
        proc.kill()
        pytest.skip("dashboard did not become healthy in time")

    yield base, proc, scratch
    if proc.poll() is None:
        proc.terminate()
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            proc.kill()


def _python() -> str:
    """The project's interpreter if there is one, else the running one.

    A git worktree has no .venv of its own, and CI may not build one, so this
    falls back rather than erroring out on a missing path.
    """
    venv = REPO_ROOT / ".venv" / "bin" / "python3"
    return str(venv) if venv.exists() else sys.executable


def _free_port() -> int:
    import socket
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


def _get(url: str, timeout: int = 90):
    try:
        with urllib.request.urlopen(url, timeout=timeout) as r:
            return r.status
    except urllib.error.HTTPError as e:
        return e.code
    except Exception as e:
        return f"error: {e}"


@pytest.mark.slow
def test_survives_watchlist_then_candles(dashboard):
    """The exact sequence that killed it: watchlist, then candles.

    limit=30 is not arbitrary. The crash needs the watchlist's batch fetch to
    cover enough symbols to leave its thread pool and Arrow state in play; at
    limit=6 it does not reproduce, and a test written at limit=6 passes against
    the unfixed code -- a green test that proved nothing.
    """
    base, proc, scratch = dashboard

    _get(f"{base}/market/watchlist?limit=30")
    assert proc.poll() is None, "the server died while serving /market/watchlist"

    code = _get(f"{base}/market/candles?symbol=BTC-USD&granularity=3600&limit=200")
    assert proc.poll() is None, (
        "the server died serving /market/candles after /market/watchlist -- this "
        "is the Arrow-backed cache write crossing threads"
    )
    assert code == 200, f"candles returned {code}"


@pytest.mark.slow
def test_survives_the_reverse_order(dashboard):
    base, proc, scratch = dashboard
    _get(f"{base}/market/candles?symbol=ETH-USD&granularity=3600&limit=200")
    assert proc.poll() is None, "the server died serving candles first"
    _get(f"{base}/market/watchlist?limit=30")
    assert proc.poll() is None, "the server died on watchlist after candles"


@pytest.mark.slow
def test_stays_alive_under_concurrent_feed_load(dashboard):
    """Both endpoints at once, which is what a dashboard poll actually does."""
    import threading
    base, proc, scratch = dashboard

    codes = []
    lock = threading.Lock()

    def hit(path):
        code = _get(f"{base}{path}")
        with lock:
            codes.append((path, code))

    threads = [
        threading.Thread(target=hit, args=("/market/watchlist?limit=30",)),
        threading.Thread(target=hit, args=("/market/candles?symbol=BTC-USD&granularity=3600&limit=200",)),
        threading.Thread(target=hit, args=("/market/candles?symbol=ETH-USD&granularity=3600&limit=200",)),
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=120)

    assert proc.poll() is None, "the server died under concurrent feed requests"
    assert all(c == 200 for _, c in codes), f"unexpected codes: {codes}"


@pytest.mark.slow
def test_read_only_endpoints_still_answer_after_feed_traffic(dashboard):
    """The opt-out must not degrade what the dashboard is for."""
    base, proc, scratch = dashboard
    _get(f"{base}/market/watchlist?limit=30")
    _get(f"{base}/market/candles?symbol=BTC-USD&granularity=3600&limit=200")
    assert proc.poll() is None
    for path in ("/health", "/ready", "/positions", "/approvals", "/portfolio/summary"):
        code = _get(f"{base}{path}")
        assert code in (200, 503), f"{path} returned {code} after feed traffic"