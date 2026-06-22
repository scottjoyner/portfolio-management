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
import json
import logging
import os
import sys
import threading
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Any, Dict

logger = logging.getLogger("approval_server")


class ApprovalHandler(BaseHTTPRequestHandler):
    """HTTP handler serving approve/deny/status endpoints."""

    # Shared via class var set by the server factory
    pending_file: str = "pending_approvals.json"
    server_ref: Any = None

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
            return json.load(f)

    def _write_pending(self, data: Dict[str, Any]):
        with open(self.pending_file, "w") as f:
            json.dump(data, f, indent=2, default=str)

    def _parse_token(self, path: str, prefix: str) -> str:
        return path[len(prefix):] if path.startswith(prefix) else ""

    def _render_page(self, title: str, message: str, color: str = "#28a745"):
        return f"""<!DOCTYPE html>
<html>
<head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>{title}</title>
<style>
body {{ font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;
       display:flex;justify-content:center;align-items:center;min-height:100vh;
       margin:0;background:#f4f4f4; }}
.card {{ background:#fff;border-radius:12px;padding:40px;text-align:center;
        box-shadow:0 4px 24px rgba(0,0,0,0.1);max-width:400px; }}
.icon {{ font-size:64px;margin-bottom:16px; }}
h1 {{ margin:0 0 8px;font-size:24px;color:#1a1a2e; }}
p {{ margin:0 0 24px;color:#666; }}
a {{ display:inline-block;padding:10px 24px;background:#1a1a2e;color:#fff;
    text-decoration:none;border-radius:6px;font-size:14px; }}
</style>
</head>
<body>
<div class="card">
    <div class="icon">{'✅' if color == '#28a745' else '❌' if color == '#dc3545' else 'ℹ️'}</div>
    <h1 style="color:{color}">{title}</h1>
    <p>{message}</p>
    <a href="/status">View All Pending</a>
</div>
</body>
</html>"""

    def _render_status(self, data: Dict[str, Any]) -> str:
        rows = ""
        for token, entry in sorted(data.items(), key=lambda x: x[1].get("created_at", ""), reverse=True):
            status = entry.get("status", "pending")
            color = "#28a745" if status == "approved" else "#dc3545" if status == "denied" else "#ffc107"
            rows += f"""<tr>
                <td style="padding:8px;border-bottom:1px solid #eee;">{entry.get('type','')}</td>
                <td style="padding:8px;border-bottom:1px solid #eee;">{entry.get('side','')}</td>
                <td style="padding:8px;border-bottom:1px solid #eee;">{entry.get('currency','')}</td>
                <td style="padding:8px;border-bottom:1px solid #eee;text-align:right;">${float(entry.get('size_usd',0)):.0f}</td>
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
th {{ background:#1a1a2e;color:#fff;padding:10px 8px;text-align:left; }}
td {{ padding:8px; }}
a {{ color:#1a1a2e; }}
</style>
</head>
<body>
<h1>📊 Trade Approvals</h1>
<table>
<thead><tr><th>Type</th><th>Side</th><th>Currency</th><th>Size</th><th>Status</th></tr></thead>
<tbody>{rows}</tbody>
</table>
<p style="color:#999;margin-top:12px;"><a href="/">↩ Back</a></p>
</body>
</html>"""

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
            data = self._read_pending()
            if token in data:
                data[token]["status"] = "approved"
                data[token]["resolved_at"] = datetime.now(timezone.utc).isoformat()
                self._write_pending(data)
                opp = data[token]
                logger.info("APPROVED: %s %s $%.0f", opp.get("side", ""), opp.get("currency", ""), float(opp.get("size_usd", 0)))
                self._send_response(200, self._render_page(
                    "Trade Approved ✅",
                    f"The {opp.get('side', '')} {opp.get('currency', '')} ${float(opp.get('size_usd', 0)):.0f} trade has been approved and will execute on the next optimizer tick."
                ))
            else:
                self._send_response(404, self._render_page("Invalid Token", "This approval link is invalid or expired.", "#dc3545"))

        elif path.startswith("/deny/"):
            token = path[len("/deny/"):]
            if not token:
                self._send_response(400, self._render_page("Error", "Missing token", "#dc3545"))
                return
            data = self._read_pending()
            if token in data:
                data[token]["status"] = "denied"
                data[token]["resolved_at"] = datetime.now(timezone.utc).isoformat()
                self._write_pending(data)
                opp = data[token]
                logger.info("DENIED: %s %s $%.0f", opp.get("side", ""), opp.get("currency", ""), float(opp.get("size_usd", 0)))
                self._send_response(200, self._render_page(
                    "Trade Denied ❌",
                    f"The {opp.get('side', '')} {opp.get('currency', '')} ${float(opp.get('size_usd', 0)):.0f} trade has been denied.",
                    "#dc3545"
                ))
            else:
                self._send_response(404, self._render_page("Invalid Token", "This denial link is invalid or expired.", "#dc3545"))

        elif path == "/status":
            data = self._read_pending()
            self._send_response(200, self._render_status(data))

        elif path == "/api/status":
            data = self._read_pending()
            self._send_response(200, json.dumps(data, indent=2, default=str), "application/json")

        else:
            self._send_response(404, self._render_page("Not Found", f"Path not found: {path}", "#dc3545"))


def serve(pending_file: str = "pending_approvals.json", port: int = 8080, host: str = "0.0.0.0"):
    """Start the approval server (blocking)."""
    # Ensure the pending file exists
    if not os.path.exists(pending_file):
        with open(pending_file, "w") as f:
            json.dump({}, f)

    # Patch the handler class to use our file path
    ApprovalHandler.pending_file = pending_file

    server = HTTPServer((host, port), ApprovalHandler)
    print(f"Approval server running at http://{host}:{port}")
    print(f"  Approve: http://{host}:{port}/approve/<token>")
    print(f"  Deny:    http://{host}:{port}/deny/<token>")
    print(f"  Status:  http://{host}:{port}/status")
    print(f"  API:     http://{host}:{port}/api/status")
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
