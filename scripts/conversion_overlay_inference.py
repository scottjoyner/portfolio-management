#!/usr/bin/env python3
"""Conversion overlay: maps GGUF model metadata onto the NPU harness weight files."""
import json
import os
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent

GGUF_MODEL = Path(os.environ.get(
    "NPU_GGUF_MODEL",
    "/home/scott/.lmstudio/models/mudler/Ornith-1.5-35B-A3B-APEX-MTP-GGUF/Ornith-1.5-35B-A3B-APEX-MTP-Compact.gguf",
))
NPU_ROOT = Path(os.environ.get("NPU_ROOT", "/media/scott/data/amd-npu-models"))
OVERLAY_OUT = Path(os.environ.get("NPU_OVERLAY", REPO_ROOT / "data" / "gguf_npu_overlay.json"))


def overlay_gguf_to_npu(gguf_path: Path, npu_dir: Path, output_path: Path):
    harness = npu_dir / "Ornich-NPU-Harness"
    if not harness.exists():
        print(f"NPU harness not found at {harness}; overlay inactive")
        return False

    if not gguf_path.exists():
        print(f"GGUF model not found at {gguf_path}; overlay inactive")
        return False

    mapping = {
        "model_on": harness / "model.onnx",
        "optimized": harness / "optimized_model.onnx",
        "deployed": harness / "deploy_model.onnx",
    }
    result = {
        "gguf_path": str(gguf_path),
        "gguf_size_bytes": gguf_path.stat().st_size,
        "npu_mapping": {name: str(path) for name, path in mapping.items()},
        "overlay_active": True,
    }
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(result, indent=2))
    print(f"Overlay written: {output_path}")
    return True


if __name__ == "__main__":
    overlay_gguf_to_npu(GGUF_MODEL, NPU_ROOT, OVERLAY_OUT)
