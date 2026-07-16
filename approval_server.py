#!/usr/bin/env python3
"""
Lightweight HTTP approval server for trade opportunities.

Run alongside the portfolio optimizer to handle approve/deny
callbacks from email links.  The optimizer shares state via a
JSON file that both processes read/write.

Usage:
    python3 approval_server.py                        # port 8080
    python3 approval_server.py --port 9090            # custom port
    python3 approval_server.py --pending-file /tmp/approvals.json

    # In a separate terminal:
    python3 portfolio_optimizer.py --require-approval --approval-base-url http://localhost:8080
"""

import argparse
import fcntl
import json
import logging
import os
import sys
import threading
import time
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Any, Dict

logger = logging.getLogger("approval_server")


DEFAULT_APPROVAL_TTL_SECONDS = 24 * 60 * 60  # 24h


def is_expired(token_record: Dict[str, Any], ttl_seconds: int = DEFAULT_APPROVAL_TTL_SECONDS) -> bool:
    """Return True if the approval record is past its expiry timestamp.

    Honors an explicit ``expiry_ts``; falls back to ``created_at`` plus ``ttl_seconds``.
    Records with neither field are treated as non-expiring (legacy compatibility).
    """
    if not isinstance(token_record, dict):
        return True
    now = time.time()
    expiry = token_record.get("expiry_ts")
    if expiry is not None:
        try:
            return float(expiry) < now
        except (TypeError, ValueError):
            return False
    created = token_record.get("created_at")
    if created is None:
        return False
    try:
        created_ts = float(created)
    except (TypeError, ValueError):
        return False
    return (created_ts + ttl_seconds) < now


def stamp_approval_timestamps(record: Dict[str, Any], ttl_seconds: int = DEFAULT_APPROVAL_TTL_SECONDS) -> Dict[str, Any]:
    """Ensure the approval record carries created_at + expiry_ts (idempotent)."""
    now = time.time()
    if record.get("created_at") is None:
        record["created_at"] = now
    else:
        try:
            now = float(record["created_at"])
        except (TypeError, ValueError):
            now = time.time()
    if record.get("expiry_ts") is None:
        record["expiry_ts"] = now + ttl_seconds
    return record


class ApprovalHandler(BaseHTTPRequestHandler):
    """HTTP handler serving approve/deny/status endpoints."""

    # Shared via class var set by the server factory
    pending_file: str = "data/pending_approvals.json"
    server_ref: Any = None
    _auth_token: str = ""

    def _check_auth(self) -> bool:
        auth = self.headers.get("Authorization", "")
        return auth == f"Bearer {self._auth_token}" or auth == self._auth_token

    def log_message(self, fmt, *args):
        logger.info(fmt, *args)

    def _send_response(self, status_code: int, body: str, content_type: str = "text/html"):
        self.send_response(status_code)
        self.send_header("Content-Type", f"{content_type}; charset=utf-8")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(body.encode("utf-8"))

    def _read_pending(self) -> Dict[str, Any]:
        if not os.path.exists(self.pending_file):
            return {}
        with open(self.pending_file, "r") as f:
            fcntl.flock(f, fcntl.LOCK_SH)
            return json.load(f)

    def _write_pending(self, data: Dict[str, Any]):
        """Atomically write pending approvals via temp file + os.replace."""
        import tempfile
        directory = os.path.dirname(self.pending_file) or "."
        fd, tmp_path = tempfile.mkstemp(dir=directory, suffix=".tmp")
        try:
            with os.fdopen(fd, "w") as f:
                fcntl.flock(f, fcntl.LOCK_EX)
                json.dump(data, f, indent=2, default=str)
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp_path, self.pending_file)
        finally:
            if os.path.exists(tmp_path):
                try:
                    os.remove(tmp_path)
                except OSError:
                    pass

    def _parse_token(self, path: str, prefix: str) -> str:
        return path[len(prefix):] if path.startswith(prefix) else ""

    def _bracket_detail_html(self, opp: Dict) -> str:
        if opp.get("bracket"):
            return (
                f"<p style='font-size:13px;color:#555;margin-top:-16px;'>"
                f"Stop: <strong>${float(opp.get('stop_price',0)):.2f}</strong> &middot; "
                f"Target: <strong>${float(opp.get('target_price',0)):.2f}</strong>"
                f"</p>"
            )
        return ""

    def _render_page(self, title: str, message: str, color: str = "#28a745", opp: Dict = None):
        bracket_html = self._bracket_detail_html(opp) if opp else ""
        return f"""<!DOCTYPE html>
<html>
<head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>{title}</title>
<style>
body {{ font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;
       display:flex;justify-content:center;align-items:center;min-height:100vh;
       margin:0;background:#f4f4f4; }}
.card {{ background:#fff;border-radius:12px;padding:40px;text-align:center;
        box-shadow:0 4px 24px rgba(0,0,0,0.1);max-width:440px; }}
.icon {{ font-size:64px;margin-bottom:16px; }}
h1 {{ margin:0 0 8px;font-size:24px;color:#1a1a2e; }}
p {{ margin:0 0 24px;color:#666; }}
.detail {{ font-size:13px;color:#555;margin:-16px 0 24px; }}
a {{ display:inline-block;padding:10px 24px;background:#1a1a2e;color:#fff;
    text-decoration:none;border-radius:6px;font-size:14px; }}
</style>
</head>
<body>
<div class="card">
    <div class="icon">{'✅' if color == '#28a745' else '❌' if color == '#dc3545' else 'ℹ️'}</div>
    <h1 style="color:{color}">{title}</h1>
    <p>{message}</p>
    {bracket_html}
    <a href="/status">View All Pending</a>
</div>
</body>
</html>"""

    def _render_status(self, data: Dict[str, Any]) -> str:
        rows = ""
        for token, entry in sorted(data.items(), key=lambda x: x[1].get("created_at", ""), reverse=True):
            status = entry.get("status", "pending")
            color = "#28a745" if status == "approved" else "#dc3545" if status == "denied" else "#ffc107"
            bracket_tag = '<span style="display:inline-block;padding:1px 6px;border-radius:6px;font-size:10px;font-weight:600;color:#fff;background:#6f42c1;">BRACKET</span>' if entry.get("bracket") else ""
            stop_txt = f"${float(entry.get('stop_price',0)):.2f}" if entry.get("stop_price") else "—"
            target_txt = f"${float(entry.get('target_price',0)):.2f}" if entry.get("target_price") else "—"
            rows += f"""<tr>
                <td style="padding:8px;border-bottom:1px solid #eee;">{entry.get('type','')} {bracket_tag}</td>
                <td style="padding:8px;border-bottom:1px solid #eee;">{entry.get('side','')}</td>
                <td style="padding:8px;border-bottom:1px solid #eee;">{entry.get('currency','')}</td>
                <td style="padding:8px;border-bottom:1px solid #eee;text-align:right;">${float(entry.get('size_usd',0)):.0f}</td>
                <td style="padding:8px;border-bottom:1px solid #eee;text-align:right;">{stop_txt}</td>
                <td style="padding:8px;border-bottom:1px solid #eee;text-align:right;">{target_txt}</td>
                <td style="padding:8px;border-bottom:1px solid #eee;max-width:180px;overflow:hidden;text-overflow:ellipsis;font-size:12px;color:#666;">{entry.get('reason','')[:60]}</td>
                <td style="padding:8px;border-bottom:1px solid #eee;"><span style="display:inline-block;padding:2px 10px;border-radius:10px;font-size:12px;font-weight:600;color:#fff;background:{color}">{status.upper()}</span></td>
            </tr>"""
        return f"""<!DOCTYPE html>
<html>
<head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Trade Approvals</title>
<style>
body {{ font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;margin:20px;background:#f4f4f4; }}
h1 {{ color:#1a1a2e; }}
table {{ width:100%;border-collapse:collapse;background:#fff;border-radius:8px;overflow:hidden;box-shadow:0 2px 8px rgba(0,0,0,0.1); }}
th {{ background:#1a1a2e;color:#fff;padding:10px 8px;text-align:left;font-size:13px; }}
td {{ padding:8px;font-size:13px; }}
a {{ color:#1a1a2e; }}
</style>
</head>
<body>
<h1>📊 Trade Approvals</h1>
<table>
<thead><tr><th>Type</th><th>Side</th><th>Currency</th><th>Size</th><th>Stop</th><th>Target</th><th>Reason</th><th>Status</th></tr></thead>
<tbody>{rows}</tbody>
</table>
<p style="color:#999;margin-top:12px;"><a href="/">↩ Back</a></p>
</body>
</html>"""

    def _resolve_and_update(self, token: str, status: str):
        """Find a token in the canonical file or the shared inbox and set status.

        Returns (entry, ok). Handles cross-user manual orders written to the
        approvals inbox by the dashboard (E6). Expired tokens are rejected."""
        data = self._read_pending()
        if token in data:
            if is_expired(data[token]):
                logger.warning("Rejected %s for expired token %s", status, token)
                return None, False
            data[token]["status"] = status
            data[token]["resolved_at"] = datetime.now(timezone.utc).isoformat()
            self._write_pending(data)
            return data[token], True
        inbox = os.path.join(os.path.dirname(self.pending_file), "approvals_inbox")
        ip = os.path.join(inbox, f"{token}.json")
        if os.path.exists(ip):
            try:
                with open(ip) as f:
                    entry = json.load(f)
                entry["status"] = status
                entry["resolved_at"] = datetime.now(timezone.utc).isoformat()
                with open(ip, "w") as f:
                    json.dump(entry, f, indent=2, default=str)
                return entry, True
            except Exception:
                return None, False
        return None, False

    def do_GET(self):
        path = self.path.split("?")[0].rstrip("/")

        if path == "/" or path == "":
            self._send_response(200, self._render_page(
                "Approval Server Running",
                "Trade approval server is active. Awaiting approve/deny callbacks from email links."
            ))

        elif path.startswith("/approve/"):
            token = path[len("/approve/"):]
            if not token:
                self._send_response(400, self._render_page("Error", "Missing token", "#dc3545"))
                return
            opp, ok = self._resolve_and_update(token, "approved")
            if ok:
                logger.info("APPROVED: %s %s $%.0f", opp.get("side", ""), opp.get("currency", ""), float(opp.get("size_usd", 0)))
                self._send_response(200, self._render_page(
                    "Trade Approved ✅",
                    f"The {opp.get('side', '')} {opp.get('currency', '')} ${float(opp.get('size_usd', 0)):.0f} trade has been approved and will execute on the next optimizer tick.",
                    opp=opp,
                ))
            else:
                self._send_response(404, self._render_page("Invalid Token", "This approval link is invalid or expired.", "#dc3545"))

        elif path.startswith("/deny/"):
            token = path[len("/deny/"):]
            if not token:
                self._send_response(400, self._render_page("Error", "Missing token", "#dc3545"))
                return
            opp, ok = self._resolve_and_update(token, "denied")
            if ok:
                logger.info("DENIED: %s %s $%.0f", opp.get("side", ""), opp.get("currency", ""), float(opp.get("size_usd", 0)))
                self._send_response(200, self._render_page(
                    "Trade Denied ❌",
                    f"The {opp.get('side', '')} {opp.get('currency', '')} ${float(opp.get('size_usd', 0)):.0f} trade has been denied.",
                    "#dc3545",
                    opp=opp,
                ))
            else:
                self._send_response(404, self._render_page("Invalid Token", "This denial link is invalid or expired.", "#dc3545"))

        elif path == "/status":
            data = self._read_pending()
            self._send_response(200, self._render_status(data))

        elif path == "/api/status":
            if not self._check_auth():
                self._send_response(403, json.dumps({"error": "forbidden"}), "application/json")
                return
            data = self._read_pending()
            self._send_response(200, json.dumps(data, indent=2, default=str), "application/json")

        else:
            self._send_response(404, self._render_page("Not Found", f"Path not found: {path}", "#dc3545"))


def serve(pending_file: str = "data/pending_approvals.json", port: int = 8080, host: str = "0.0.0.0"):
    """Start the approval server (blocking)."""
    # Ensure the pending file exists
    if not os.path.exists(pending_file):
        with open(pending_file, "w") as f:
            json.dump({}, f)

    # Patch the handler class to use our file path and auth token
    ApprovalHandler.pending_file = pending_file
    auth_token = os.getenv("APPROVAL_TOKEN", "")
    if not auth_token:
        import secrets
        auth_token = secrets.token_urlsafe(32)
        logger.warning("APPROVAL_TOKEN not set — generated random token for API auth")
    ApprovalHandler._auth_token = auth_token

    server = HTTPServer((host, port), ApprovalHandler)
    print(f"Approval server running at http://{host}:{port}")
    print(f"  Approve: http://{host}:{port}/approve/<token>")
    print(f"  Deny:    http://{host}:{port}/deny/<token>")
    print(f"  Status:  http://{host}:{port}/status")
    if ApprovalHandler._auth_token:
        print(f"  API:     http://{host}:{port}/api/status (auth: Bearer {ApprovalHandler._auth_token[:8]}...)")
    print(f"  Pending file: {pending_file}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nShutting down approval server...")
        server.shutdown()


def main():
    parser = argparse.ArgumentParser(
        description="Trade Approval HTTP Server",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--port", type=int, default=8080, help="HTTP port (default: 8080)")
    parser.add_argument("--host", default="0.0.0.0", help="Bind address (default: 0.0.0.0)")
    parser.add_argument("--pending-file", default="pending_approvals.json",
                        help="JSON file shared with optimizer (default: pending_approvals.json)")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | approval_server | %(message)s",
        datefmt="%H:%M:%S",
    )

    serve(
        pending_file=args.pending_file,
        port=args.port,
        host=args.host,
    )


if __name__ == "__main__":
    main()
