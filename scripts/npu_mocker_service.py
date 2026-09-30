#!/usr/bin/env python3
"""Mock NPU service exposing the harness overlay over an OpenAI-compatible surface."""
import json
import os
import signal
import threading
import time
import urllib.request
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent

MODEL_ID = os.environ.get("NPU_MOCK_MODEL_ID", "ornith-1.5-35b-npu")
BIND_HOST = os.environ.get("NPU_MOCK_HOST", "127.0.0.1")
# 1235 is the llama-server port; the mock takes 1236 to avoid the collision.
BIND_PORT = int(os.environ.get("NPU_MOCK_PORT", "1236"))
OVERLAY = Path(os.environ.get("NPU_OVERLAY", REPO_ROOT / "data" / "gguf_npu_overlay.json"))
LM_STUDIO_URL = os.environ.get("LM_STUDIO_URL", "http://127.0.0.1:1235/v1/models")


def lm_studio_reachable():
    try:
        with urllib.request.urlopen(LM_STUDIO_URL, timeout=2) as response:
            return response.status == 200
    except (OSError, ValueError):
        return False


def load_overlay():
    try:
        return json.loads(OVERLAY.read_text())
    except (OSError, ValueError):
        return {}


class Handler(BaseHTTPRequestHandler):
    def log_message(self, fmt, *args):
        pass

    def send_json(self, code, data):
        body = json.dumps(data).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        if self.path == "/health":
            overlay = load_overlay()
            self.send_json(200, {
                "status": "ok",
                "model": MODEL_ID,
                "hardware": "xwing-npu-harness",
                "lm_connected": lm_studio_reachable(),
                "overlay_active": bool(overlay.get("overlay_active")),
            })
        elif self.path == "/v1/models":
            self.send_json(200, {
                "object": "list",
                "data": [{
                    "id": MODEL_ID,
                    "object": "model",
                    "created": int(time.time()),
                    "owned_by": "local-npu-harness",
                }],
            })
        else:
            self.send_json(404, {"error": "not found"})

    def do_POST(self):
        if self.path != "/v1/chat/completions":
            self.send_json(404, {"error": "not found"})
            return
        try:
            length = int(self.headers.get("Content-Length", 0))
            body = json.loads(self.rfile.read(length))
            messages = body.get("messages", [])
            prompt_tokens = len(json.dumps(messages))
            completion_tokens = 10
            self.send_json(200, {
                "id": "chatcmpl-" + str(int(time.time())),
                "object": "chat.completion",
                "created": int(time.time()),
                "model": MODEL_ID,
                "choices": [{
                    "index": 0,
                    "message": {"role": "assistant", "content": "[NPU harness inference via overlay]"},
                    "finish_reason": "stop",
                }],
                "usage": {
                    "prompt_tokens": prompt_tokens,
                    "completion_tokens": completion_tokens,
                    "total_tokens": prompt_tokens + completion_tokens,
                },
                "xwing": {"runtime": "npu-harness-translation", "service": "mock-npu"},
            })
        except (OSError, ValueError) as error:
            self.send_json(500, {"error": str(error)})


if __name__ == "__main__":
    server = HTTPServer((BIND_HOST, BIND_PORT), Handler)
    print(f"Mock NPU service listening on {BIND_HOST}:{BIND_PORT} (harness + overlay + LM Studio link)", flush=True)

    def shutdown(_signum, _frame):
        # serve_forever() blocks in a poll loop, so shut down from a signal
        # handler rather than relying on KeyboardInterrupt to unwind it.
        threading.Thread(target=server.shutdown, daemon=True).start()

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)
    try:
        server.serve_forever()
    finally:
        server.server_close()
        print("Mock NPU service stopped", flush=True)
