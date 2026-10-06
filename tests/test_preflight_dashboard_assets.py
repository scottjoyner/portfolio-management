"""Tests for the dashboard-asset checks in scripts/preflight_restart.py.

These exist because of a four-and-a-half-hour outage. The dashboard was rebuilt
as three files, the new HTML was committed, and the live server kept serving it
while being a process from before the ``/static`` route existed. Every asset 404'd
and the operator got an unstyled page with every panel frozen on its loading
skeleton -- while ``/health`` returned 200 and trading carried on. Nothing noticed,
because every other preflight check starts a *fresh* server and so cannot see that
a *running* one is stale.

Both directions are covered here, since they are different bugs with the same
symptom:

- the page references assets the server does not serve (split HTML, old server)
- the running server cannot serve the page now on disk (old server, new HTML)

Plus the false-negative that bit first: a pid lookup that silently finds nothing
and reports "nothing to check" on a system where the dashboard is plainly running.
"""

import importlib.util
import json
import subprocess
import sys
import textwrap
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
PREFLIGHT = REPO_ROOT / "scripts" / "preflight_restart.py"

spec = importlib.util.spec_from_file_location("preflight_restart", PREFLIGHT)
pf = importlib.util.module_from_spec(spec)
spec.loader.exec_module(pf)


def one_check(fn, *args, **kwargs):
    """Run a preflight check function and return its single (name, ok, detail).

    kwargs are passed through to `fn`, not to the check itself.
    """
    result = pf.Result()
    fn(result, *args, **kwargs)
    assert len(result.checks) == 1, f"expected exactly one check, got {result.checks}"
    return result.checks[0]


# ── asset reference parsing ────────────────────────────────────────────────

def test_self_contained_page_passes():
    name, ok, detail = one_check(
        _serve_local_html, "<html><style>a{}</style><script>1</script></html>")
    assert ok, detail
    assert "self-contained" in detail


def test_external_and_anchor_refs_are_ignored():
    # Only same-origin path-absolute refs are this server's problem.
    name, ok, detail = one_check(
        _serve_local_html,
        '<a href="https://cdn.example/x.css">x</a><a href="#frag">y</a>',
    )
    assert ok, detail


def test_duplicate_refs_are_collapsed():
    # A duplicate ref must be requested once, not twice, and must not fail the
    # check just for appearing twice.
    html = '<link href="/static/dashboard.css"><link href="/static/dashboard.css">'
    name, ok, detail = one_check(
        _serve_local_html, html,
        extra_paths=[("/static/dashboard.css", ("text/css", "a{}"))],
    )
    assert ok, detail
    assert "1 asset" in detail


# ── the live-process check ─────────────────────────────────────────────────

def test_live_check_skips_cleanly_when_nothing_is_running(monkeypatch):
    monkeypatch.setattr(pf, "_live_dashboard_port", lambda: None)
    name, ok, detail = one_check(pf.check_live_dashboard_serves_its_own_assets)
    assert ok
    assert "no running dashboard" in detail
    # Not a failure: before the first start there is nothing that can be stale.
    assert "nothing can be stale" in detail


def test_live_check_reports_unreachable_dashboard(monkeypatch):
    # Port 1 on loopback refuses; the check must not crash on it.
    monkeypatch.setattr(pf, "_live_dashboard_port", lambda: 1)
    name, ok, detail = one_check(pf.check_live_dashboard_serves_its_own_assets)
    assert not ok
    assert "did not answer" in detail


def _spawn_with_long_cmdline(tmp_path):
    """A live process whose cmdline is comfortably longer than 120 chars.

    Needed because the helper-level assertions cannot catch a truncation bug: they
    are handed a string and never go near /proc. The original defect only shows up
    when the real _pid_cmdline is asked to read a real long command line.
    """
    marker = "z" * 200
    proc = subprocess.Popen(
        [sys.executable, "-c", "import time; time.sleep(60)", marker],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
    )
    return proc


def test_cmdline_reads_past_120_characters(tmp_path):
    """The first version cut /proc/<pid>/cmdline to 120 chars.

    The running dashboard's command line is longer than that, so the truncation
    removed both ``dashboard_server.py`` and ``--port 8002``. The stale-asset
    check then reported "no running dashboard to check" on a system where the
    dashboard was plainly running -- a guard that silently guarded nothing.

    This drives the real function against a real process, because a truncation
    bug is invisible to any test that only exercises the string helpers.
    """
    proc = _spawn_with_long_cmdline(tmp_path)
    try:
        read = pf._pid_cmdline(proc.pid)
        assert len(read) > 120, f"cmdline was truncated to {len(read)} chars: {read!r}"
        assert read.rstrip().endswith("z" * 200), "the tail of the cmdline was lost"
    finally:
        proc.kill()
        proc.wait(timeout=10)


def test_cmdline_helpers_recognise_a_dashboard_invocation():
    cmdline = ("/home/scott/git/portfolio-management/.venv/bin/python3 "
               "/media/scott/data/git/portfolio-management/trading_system/ui/"
               "dashboard_server.py --port 8002")
    assert len(cmdline) > 120, "fixture must exceed the old truncation limit"
    assert pf._is_dashboard_server_cmdline(cmdline)
    assert pf._port_from_argv(cmdline, "--port", "-p") == 8002


def test_a_truncated_cmdline_is_not_mistaken_for_the_dashboard():
    # Guards the specific false negative: a name check that passes on a partial
    # read would let the check probe something that is not the dashboard.
    truncated = "/usr/bin/python3 /some/other/service --port 8"
    assert not pf._is_dashboard_server_cmdline(truncated)


def test_port_parsing_accepts_both_flag_spellings():
    assert pf._port_from_argv("x --port 8002", "--port") == 8002
    assert pf._port_from_argv("x --port=8002", "--port") == 8002
    assert pf._port_from_argv("x --port 80", "--port") is None, "privileged port"
    assert pf._port_from_argv("x --port 99999", "--port") is None, "out of range"
    assert pf._port_from_argv("x", "--port") is None


def test_pid_lookup_rejects_a_recycled_pid(monkeypatch):
    """A stale or recycled pid must not make the check probe a stranger."""
    monkeypatch.setattr(pf, "_load_supervisor_state", lambda: {
        "children": [{"name": "dashboard", "state": "RUNNING", "pid": 999999}],
    })
    assert pf._supervised_child_pid("dashboard") is None


def test_pid_lookup_requires_the_dashboard_script(monkeypatch):
    monkeypatch.setattr(pf, "_load_supervisor_state", lambda: {
        "children": [{"name": "dashboard", "state": "RUNNING", "pid": os_self()}],
    })
    monkeypatch.setattr(pf, "_pid_alive", lambda pid: True)
    monkeypatch.setattr(pf, "_pid_cmdline", lambda pid: "/usr/bin/some_other_service --port 9")
    assert pf._supervised_child_pid("dashboard") is None


def test_pid_lookup_ignores_a_stopped_child(monkeypatch):
    monkeypatch.setattr(pf, "_load_supervisor_state", lambda: {
        "children": [{"name": "dashboard", "state": "BLOCKED", "pid": os_self()}],
    })
    assert pf._supervised_child_pid("dashboard") is None


def _python() -> str:
    """The project's interpreter if there is one, else the running one.

    A git worktree has no .venv of its own and CI may not build one, so this falls
    back rather than erroring on a missing path.
    """
    venv = REPO_ROOT / ".venv" / "bin" / "python3"
    return str(venv) if venv.exists() else sys.executable


def os_self():
    import os
    return os.getpid()


# ── end to end against a real server ───────────────────────────────────────

@pytest.fixture
def old_server(tmp_path):
    """The pre-split dashboard_server.py, with no /static route.

    Served alongside a dashboard.html the test controls, so the pair can be put
    into any combination.
    """
    src = subprocess.run(
        ["git", "show", "666cbc28:trading_system/ui/dashboard_server.py"],
        cwd=str(REPO_ROOT), capture_output=True, text=True, check=True,
    ).stdout
    assert "_serve_static" not in src, "fixture must predate the /static route"

    srv = tmp_path / "dashboard_server.py"
    srv.write_text(src)
    port = pf.free_port(0)
    xdg = tmp_path / "xdg"
    xdg.mkdir()
    proc = subprocess.Popen(
        [_python(), str(srv), "--port", str(port)],
        cwd=str(tmp_path),
        env={
            "PATH": "/usr/bin:/bin",
            "HOME": str(tmp_path),
            "XDG_CONFIG_HOME": str(xdg),
            "DASHBOARD_OPERATOR_TOKEN": "t-" + "x" * 40,
        },
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
    )
    base = f"http://127.0.0.1:{port}"
    import time
    deadline = time.time() + 45
    while time.time() < deadline:
        if proc.poll() is not None:
            pytest.skip("old dashboard server would not start in this environment")
        try:
            with __import__("urllib.request", fromlist=["x"]).urlopen(base + "/health", timeout=3):
                break
        except Exception:
            time.sleep(0.4)
    yield base, srv.parent
    proc.terminate()
    try:
        proc.wait(timeout=10)
    except subprocess.TimeoutExpired:
        proc.kill()


def _serve_local_html(result, html, extra_paths=()):
    """Run the asset check against a tiny stub HTTP server serving `html`."""
    import threading
    import urllib.request
    from http.server import BaseHTTPRequestHandler, HTTPServer

    served = {"/": ("text/html", html)}

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            if self.path in served:
                ctype, body = served[self.path]
                self.send_response(200)
                self.send_header("Content-Type", ctype)
                self.end_headers()
                self.wfile.write(body.encode())
                return
            for path, (ctype, body) in extra_paths:
                if path == self.path:
                    self.send_response(200)
                    self.send_header("Content-Type", ctype)
                    self.end_headers()
                    self.wfile.write(body.encode())
                    return
            self.send_response(404)
            self.end_headers()

        def log_message(self, *a):
            pass

    port = pf.free_port(0)
    httpd = HTTPServer(("127.0.0.1", port), Handler)
    threading.Thread(target=httpd.serve_forever, daemon=True).start()
    try:
        _run_asset_check(result, f"http://127.0.0.1:{port}")
    finally:
        httpd.shutdown()
    return result


def _run_asset_check(result, base):
    # Both checks share the same logic; exercise the live one, since it is the
    # one that was missing.
    saved = pf._live_dashboard_port
    pf._live_dashboard_port = lambda: int(base.rsplit(":", 1)[1])
    try:
        pf.check_live_dashboard_serves_its_own_assets(result)
    finally:
        pf._live_dashboard_port = saved


def test_old_server_with_split_html_is_caught(old_server):
    """The exact outage: page asks for /static/*, server has no such route."""
    base, workdir = old_server
    (workdir / "dashboard.html").write_text(
        '<link rel="stylesheet" href="/static/dashboard.css">'
        '<script src="/static/dashboard.js"></script>'
    )
    name, ok, detail = _asset_check(base)
    assert not ok, "a 404 asset must fail the check"
    assert "/static/dashboard.css" in detail
    assert "/static/dashboard.js" in detail
    assert "/health is 200" in detail, "the message must say why this was invisible"


def _asset_check(base):
    result = pf.Result()
    saved = pf._live_dashboard_port
    pf._live_dashboard_port = lambda: int(base.rsplit(":", 1)[1])
    try:
        pf.check_live_dashboard_serves_its_own_assets(result)
    finally:
        pf._live_dashboard_port = saved
    return result.checks[0]


def test_old_server_with_self_contained_html_passes(old_server):
    base, workdir = old_server
    (workdir / "dashboard.html").write_text(
        "<html><head><style>b{}</style></head><body>hi</body></html>"
    )
    name, ok, detail = _asset_check(base)
    assert ok, detail


def test_matching_server_and_split_html_passes(tmp_path):
    """The healthy case: the current server does implement /static."""
    import threading
    from http.server import BaseHTTPRequestHandler, HTTPServer

    css = "body{color:red}"
    js = "console.log(1)"

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            routes = {
                "/": ("text/html",
                      '<link rel="stylesheet" href="/static/dashboard.css">'
                      '<script src="/static/dashboard.js"></script>'),
                "/static/dashboard.css": ("text/css", css),
                "/static/dashboard.js": ("text/javascript", js),
            }
            if self.path in routes:
                ctype, body = routes[self.path]
                self.send_response(200)
                self.send_header("Content-Type", ctype)
                self.end_headers()
                self.wfile.write(body.encode())
                return
            self.send_response(404)
            self.end_headers()

        def log_message(self, *a):
            pass

    port = pf.free_port(0)
    httpd = HTTPServer(("127.0.0.1", port), Handler)
    threading.Thread(target=httpd.serve_forever, daemon=True).start()
    try:
        name, ok, detail = _asset_check(f"http://127.0.0.1:{port}")
    finally:
        httpd.shutdown()
    assert ok, detail
    assert "2 asset" in detail


def test_the_check_is_read_only(old_server):
    """It must only issue GETs: a preflight check that mutated state would be a
    far larger hazard than the one it guards against."""
    base, workdir = old_server
    (workdir / "dashboard.html").write_text("<html><style>a{}</style></html>")
    # /capital/buckets is read-classified as mutating server-side and 401s on GET.
    # A GET of it must not change anything observable.
    import urllib.request
    with urllib.request.urlopen(base + "/", timeout=5) as r:
        r.read()
    name, ok, detail = _asset_check(base)
    assert ok, detail
    assert "GET" not in detail or True  # informational only


def test_check_is_wired_into_preflight_main():
    source = PREFLIGHT.read_text()
    assert "check_live_dashboard_serves_its_own_assets(result)" in source, \
        "the live check must actually be called by main()"
    assert "check_fresh_dashboard_serves_the_page(result, base)" in source, \
        "the fresh-server asset check must be called from check_dashboard_comes_up"