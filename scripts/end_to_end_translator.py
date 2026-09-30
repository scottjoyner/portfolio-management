#!/usr/bin/env python3
"""End-to-end translator: reports LM Studio GGUF inference and NPU harness status."""
import json
import os
import sys
import time
import urllib.request
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent

GGUF_MODEL = Path(os.environ.get(
    "NPU_GGUF_MODEL",
    "/home/scott/.lmstudio/models/mudler/Ornith-1.5-35B-A3B-APEX-MTP-GGUF/Ornith-1.5-35B-A3B-APEX-MTP-Compact.gguf",
))
NPU_ROOT = Path(os.environ.get("NPU_ROOT", "/media/scott/data/amd-npu-models"))
NPU_OVERLAY = Path(os.environ.get("NPU_OVERLAY", REPO_ROOT / "data" / "gguf_npu_overlay.json"))
# Probe the OpenAI-compatible models endpoint rather than a /health path: it is
# the surface the release contract requires, and it is what llama-server
# actually serves. Override with LM_STUDIO_URL when the node listens elsewhere.
LM_STUDIO_URL = os.environ.get("LM_STUDIO_URL", "http://127.0.0.1:1235/v1/models")


def check_lm_studio():
    """Return (reachable, served_model_ids) from the OpenAI-compatible endpoint."""
    try:
        with urllib.request.urlopen(LM_STUDIO_URL, timeout=2) as response:
            if response.status != 200:
                return False, []
            payload = json.loads(response.read())
    except (OSError, ValueError):
        return False, []
    model_ids = [str(row.get("id") or row.get("name") or "") for row in payload.get("data", [])]
    return True, [model_id for model_id in model_ids if model_id]


def check_npu_harness():
    return (NPU_ROOT / "Ornich-NPU-Harness" / "model.onnx").exists()


def run_e2e():
    try:
        overlay = json.loads(NPU_OVERLAY.read_text()) if NPU_OVERLAY.exists() else {}
    except (OSError, ValueError):
        overlay = {}
    lm_studio, served_models = check_lm_studio()
    npu = check_npu_harness()
    status = {
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "lm_studio_gguf_active": lm_studio,
        "lm_studio_url": LM_STUDIO_URL,
        "lm_studio_models": served_models,
        "npu_harness_active": npu,
        "gguf_present": GGUF_MODEL.exists(),
        "gguf_size_bytes": overlay.get("gguf_size_bytes"),
        "npu_mapping": overlay.get("npu_mapping"),
        "shared_weights_path": str(NPU_ROOT / "Ornich-1.5-35B-A3B.gpt-oss-moe40"),
        "end_to_end": lm_studio and npu,
        "note": (
            "Overlay connects LM Studio .gguf inference to the NPU .onnx harness. "
            "Translation uses shared base weights (.raw -> .onnx). Native NPU service "
            "needs an .onnx metadata fix (context_length)."
        ),
    }
    print(json.dumps(status, indent=2))
    return status["end_to_end"]


if __name__ == "__main__":
    sys.exit(0 if run_e2e() else 1)
