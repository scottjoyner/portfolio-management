from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Mapping


def load_manifest(path: str | Path) -> dict[str, Any]:
    with Path(path).open("r", encoding="utf-8") as f:
        return json.load(f)


def to_coinbase_research_payload(manifest: Mapping[str, Any], *, product_id: str | None = None, strategy_name: str | None = None) -> dict[str, Any]:
    config = dict(manifest.get("config") or {})
    summary = dict(manifest.get("summary") or {})
    resolved_product_id = product_id or config.get("product_id") or config.get("coinbase_product_id") or config.get("ticker") or summary.get("ticker")
    resolved_strategy = strategy_name or config.get("strategy_name") or "sidecar_rsi_cross"
    return {
        "schema_version": 1,
        "source": "local-trader-agent",
        "strategy_name": resolved_strategy,
        "product_id": str(resolved_product_id).upper() if resolved_product_id else "",
        "ticker": str(config.get("ticker") or summary.get("ticker") or "").upper(),
        "config": config,
        "summary": summary,
        "artifacts": {
            "report_html": manifest.get("report_html"),
            "trades_csv": manifest.get("trades_csv"),
            "data_csv": manifest.get("data_csv"),
            "manifest_json": manifest.get("manifest_json"),
        },
    }


def export_manifest_file(path: str | Path, output_path: str | Path | None = None, *, product_id: str | None = None, strategy_name: str | None = None) -> Path:
    path = Path(path)
    payload = to_coinbase_research_payload(load_manifest(path), product_id=product_id, strategy_name=strategy_name)
    target = Path(output_path) if output_path else path.with_suffix(".coinbase-research.json")
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")
    return target
