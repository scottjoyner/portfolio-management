"""
Lightweight HTTP health server for background daemons.

Serves a JSON status snapshot on a configurable port so external monitors
(the dashboard, llm-watchdog, systemd) can verify the process is alive and
healthy without coupling to a specific trader implementation.
"""

from __future__ import annotations

import json
import logging
import os
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Callable, Dict, Optional

log = logging.getLogger(__name__)


class HealthServer:
    """Serve a JSON health snapshot over HTTP."""

    def __init__(self, port: int, status_fn: Callable[[], Dict], name: str = "service"):
        self.port = port
        self.status_fn = status_fn
        self.name = name
        self._server: Optional[ThreadingHTTPServer] = None
        self._thread: Optional[threading.Thread] = None

    def start(self):
        if self._server is not None:
            return
        status_fn = self.status_fn

        class H(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def do_GET(self):
                try:
                    body = json.dumps(status_fn()).encode()
                except Exception as e:  # never let a bad status crash the server
                    body = json.dumps({"status": "error", "detail": str(e)}).encode()
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.send_header("Access-Control-Allow-Origin", "*")
                self.end_headers()
                self.wfile.write(body)

            def do_OPTIONS(self):
                self.send_response(200)
                self.send_header("Access-Control-Allow-Origin", "*")
                self.end_headers()

            def log_message(self, *args):
                pass  # silence default stderr logging

        try:
            self._server = ThreadingHTTPServer(("0.0.0.0", self.port), H)
            self._thread = threading.Thread(
                target=self._server.serve_forever, daemon=True, name=f"health-{self.name}"
            )
            self._thread.start()
            log.info("Health server on port %d", self.port)
        except OSError as e:
            log.warning("Health server failed to bind port %d: %s", self.port, e)
            self._server = None

    def stop(self):
        if self._server:
            try:
                self._server.shutdown()
                self._server.server_close()
            except Exception:
                pass
            self._server = None


def build_optimizer_status(optimizer) -> Dict:
    """Build a health snapshot dict from a PortfolioOptimizer instance."""
    state = optimizer.state
    return {
        "status": "running" if optimizer.running else "stopped",
        "health_ok": not getattr(optimizer, "_health_alerts", []),
        "alerts": getattr(optimizer, "_health_alerts", []),
        "tick_count": getattr(optimizer, "_tick_count", 0),
        "last_tick_ts": getattr(optimizer, "_last_tick_ts", 0.0),
        "mode": "live" if not optimizer.dry_run else "dry_run",
        "holdings": len(state.holdings) if state else 0,
        "total_value": round(state.total_value, 2) if state else 0.0,
        "last_opportunities": len(getattr(optimizer, "_last_detected_opportunities", []) or []),
        "smart_feed_active": bool(getattr(optimizer, "_feed_mgr", None)
                                  and optimizer._feed_mgr.running),
        "uptime_s": round(time.time() - getattr(optimizer, "_start_ts", time.time()), 1),
        "smart_feed_stats": (
            optimizer._feed_mgr.stats() if getattr(optimizer, "_feed_mgr", None) else None
        ),
    }
