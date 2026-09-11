"""Runtime binding for research-tournament-certified strategy configurations.

This module deliberately carries no broker/order authority.  It translates the
already-verified challenger + terminal-holdout identity into a small runtime
credential and only binds that credential to a live signal when the exact
supported strategy/config/symbol/granularity is executed.

The current executable pilot is intentionally narrow: configured ``rsi_revert``
through the same compiled Rust configured entry point used by canonical replay.
Unsupported strategies remain research artifacts rather than receiving a false
runtime-certification label.
"""
from __future__ import annotations

import copy
import json
from pathlib import Path
from typing import Any

from scripts.alpha_validation import stable_hash
from scripts.backtest_framework.canonical_replay import normalize_strategy_config

ROOT = Path(__file__).resolve().parent.parent
ACTIVE_CONFIG_PATH = ROOT / "data" / "agent_runtime_config.json"
CERTIFICATION_SCHEMA_VERSION = 1
CERTIFICATION_TYPE = "research_tournament_terminal_promotion_v1"
RUNTIME_BINDING_METHOD = "configured_rust_signal_v1"

_GRANULARITY_SECONDS = {
    "ONE_MINUTE": 60,
    "FIVE_MINUTE": 300,
    "FIFTEEN_MINUTE": 900,
    "THIRTY_MINUTE": 1800,
    "ONE_HOUR": 3600,
    "TWO_HOUR": 7200,
    "SIX_HOUR": 21600,
    "ONE_DAY": 86400,
}

_IDENTITY_FIELDS = (
    "schema_version",
    "certification_type",
    "candidate_id",
    "strategy_name",
    "symbol",
    "granularity",
    "candidate_source_sha",
    "candidate_config_hash",
    "alpha_validation_evidence_hash",
    "terminal_holdout_evidence_hash",
    "experiment_hash",
    "selection_hash",
    "terminal_result_hash",
    "terminal_metrics_hash",
)


def _identity_core(certification: dict[str, Any]) -> dict[str, Any]:
    return {name: certification.get(name) for name in _IDENTITY_FIELDS}


def _runtime_binding_core(binding: dict[str, Any]) -> dict[str, Any]:
    return {
        "method": binding.get("method"),
        "certification_hash": binding.get("certification_hash"),
        "strategy_name": binding.get("strategy_name"),
        "strategy_config_hash": binding.get("strategy_config_hash"),
        "symbol": binding.get("symbol"),
        "granularity": binding.get("granularity"),
    }


def normalize_granularity_seconds(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, str):
        text = value.strip().upper()
        if text in _GRANULARITY_SECONDS:
            return str(_GRANULARITY_SECONDS[text])
        try:
            number = int(text)
        except ValueError:
            return None
        return str(number) if number > 0 else None
    try:
        number = int(value)
    except (TypeError, ValueError):
        return None
    return str(number) if number > 0 else None


def build_runtime_research_certification(
    challenger: Any,
    alpha_evidence: Any,
    terminal_evidence: Any,
) -> dict[str, Any] | None:
    """Build a runtime credential from already-verified promotion evidence.

    Returning ``None`` is intentional for older/incomplete fixtures and for
    strategy families whose exact runtime execution is not yet supported.  It
    prevents a promotion record from acquiring executable certification merely
    because it passed a research gate.
    """

    if not isinstance(challenger, dict) or not isinstance(alpha_evidence, dict) or not isinstance(terminal_evidence, dict):
        return None
    strategy_name = terminal_evidence.get("strategy_name")
    terminal_dataset = terminal_evidence.get("terminal_dataset")
    metrics = terminal_evidence.get("metrics")
    parameters = challenger.get("parameters")
    if strategy_name != "rsi_revert" or not isinstance(terminal_dataset, dict) or not isinstance(metrics, dict):
        return None
    try:
        normalized_config = normalize_strategy_config(strategy_name, parameters)
    except (TypeError, ValueError):
        return None
    if not normalized_config or normalized_config != terminal_evidence.get("strategy_config"):
        return None
    candidate_config_hash = stable_hash(normalized_config)
    if candidate_config_hash != terminal_evidence.get("candidate_config_hash"):
        return None
    if candidate_config_hash != alpha_evidence.get("candidate_config_hash"):
        return None

    symbol = str(terminal_dataset.get("symbol") or "").strip()
    granularity = normalize_granularity_seconds(terminal_dataset.get("granularity"))
    required_strings = {
        "candidate_id": terminal_evidence.get("candidate_id"),
        "candidate_source_sha": terminal_evidence.get("candidate_source_sha"),
        "alpha_validation_evidence_hash": terminal_evidence.get("alpha_validation_evidence_hash"),
        "terminal_holdout_evidence_hash": terminal_evidence.get("evidence_hash"),
        "experiment_hash": terminal_evidence.get("experiment_hash"),
        "selection_hash": terminal_evidence.get("selection_hash"),
        "terminal_result_hash": terminal_evidence.get("terminal_result_hash"),
    }
    if not symbol or not granularity or any(not isinstance(value, str) or not value for value in required_strings.values()):
        return None
    if required_strings["candidate_id"] != challenger.get("id"):
        return None
    if required_strings["alpha_validation_evidence_hash"] != alpha_evidence.get("evidence_hash"):
        return None

    terminal_metrics = copy.deepcopy(metrics)
    core = {
        "schema_version": CERTIFICATION_SCHEMA_VERSION,
        "certification_type": CERTIFICATION_TYPE,
        "candidate_id": required_strings["candidate_id"],
        "strategy_name": strategy_name,
        "symbol": symbol,
        "granularity": granularity,
        "candidate_source_sha": required_strings["candidate_source_sha"],
        "candidate_config_hash": candidate_config_hash,
        "alpha_validation_evidence_hash": required_strings["alpha_validation_evidence_hash"],
        "terminal_holdout_evidence_hash": required_strings["terminal_holdout_evidence_hash"],
        "experiment_hash": required_strings["experiment_hash"],
        "selection_hash": required_strings["selection_hash"],
        "terminal_result_hash": required_strings["terminal_result_hash"],
        "terminal_metrics_hash": stable_hash(terminal_metrics),
    }
    return {
        **core,
        "candidate_config": normalized_config,
        "terminal_metrics": terminal_metrics,
        "certification_hash": stable_hash(core),
    }


def verify_static_certification(certification: Any) -> tuple[bool, list[str]]:
    reasons: list[str] = []
    if not isinstance(certification, dict):
        return False, ["research_certification_missing"]
    if certification.get("schema_version") != CERTIFICATION_SCHEMA_VERSION:
        reasons.append("research_certification_schema_mismatch")
    if certification.get("certification_type") != CERTIFICATION_TYPE:
        reasons.append("research_certification_type_mismatch")
    for name in _IDENTITY_FIELDS:
        value = certification.get(name)
        if name == "schema_version":
            continue
        if not isinstance(value, str) or not value:
            reasons.append(f"research_certification_field_invalid:{name}")
    config = certification.get("candidate_config")
    metrics = certification.get("terminal_metrics")
    try:
        normalized = normalize_strategy_config(certification.get("strategy_name"), config)
        if normalized != config:
            reasons.append("research_certification_config_not_normalized")
        if stable_hash(normalized) != certification.get("candidate_config_hash"):
            reasons.append("research_certification_config_hash_mismatch")
    except (TypeError, ValueError):
        reasons.append("research_certification_config_invalid")
    if not isinstance(metrics, dict):
        reasons.append("research_certification_terminal_metrics_missing")
    else:
        try:
            if stable_hash(metrics) != certification.get("terminal_metrics_hash"):
                reasons.append("research_certification_terminal_metrics_hash_mismatch")
        except (TypeError, ValueError, OverflowError):
            reasons.append("research_certification_terminal_metrics_invalid")
    try:
        if stable_hash(_identity_core(certification)) != certification.get("certification_hash"):
            reasons.append("research_certification_hash_mismatch")
    except (TypeError, ValueError, OverflowError):
        reasons.append("research_certification_not_canonicalizable")
    return not reasons, list(dict.fromkeys(reasons))


def load_active_research_certification(path: Path | str = ACTIVE_CONFIG_PATH) -> dict[str, Any] | None:
    path = Path(path)
    if not path.exists():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    certification = payload.get("research_certification") if isinstance(payload, dict) else None
    valid, _ = verify_static_certification(certification)
    if not valid:
        return None
    if payload.get("active_challenger_id") != certification.get("candidate_id"):
        return None
    if payload.get("parameters") != certification.get("candidate_config"):
        return None
    if payload.get("alpha_validation_evidence_hash") != certification.get("alpha_validation_evidence_hash"):
        return None
    if payload.get("terminal_holdout_evidence_hash") != certification.get("terminal_holdout_evidence_hash"):
        return None
    return copy.deepcopy(certification)


def bind_certification_to_runtime(
    certification: Any,
    *,
    product_id: str,
    granularity: Any,
) -> dict[str, Any] | None:
    valid, _ = verify_static_certification(certification)
    if not valid:
        return None
    granularity_seconds = normalize_granularity_seconds(granularity)
    if certification.get("symbol") != str(product_id):
        return None
    if certification.get("granularity") != granularity_seconds:
        return None
    if certification.get("strategy_name") != "rsi_revert":
        return None
    binding_core = {
        "method": RUNTIME_BINDING_METHOD,
        "certification_hash": certification["certification_hash"],
        "strategy_name": certification["strategy_name"],
        "strategy_config_hash": certification["candidate_config_hash"],
        "symbol": certification["symbol"],
        "granularity": certification["granularity"],
    }
    return {
        **copy.deepcopy(certification),
        "runtime_binding": {
            **binding_core,
            "runtime_binding_hash": stable_hash(binding_core),
        },
    }


def run_certified_current_signal(
    certification: Any,
    *,
    closes: list[float],
    volumes: list[float],
    highs: list[float],
    lows: list[float],
) -> dict[str, Any] | None:
    """Run the exact certified strategy config for the current bar.

    No Python/default fallback is permitted: if the configured Rust binding is
    unavailable, the result is uncertified/unavailable rather than silently
    changing strategy semantics.
    """

    valid, _ = verify_static_certification(certification)
    binding = certification.get("runtime_binding") if isinstance(certification, dict) else None
    if not valid or not isinstance(binding, dict):
        return None
    if binding.get("method") != RUNTIME_BINDING_METHOD:
        return None
    if stable_hash(_runtime_binding_core(binding)) != binding.get("runtime_binding_hash"):
        return None
    try:
        import rust_core
    except ImportError:
        return None
    runner = getattr(rust_core, "run_rsi_revert_opens_configured_py", None)
    if not callable(runner):
        return None
    config = certification["candidate_config"]
    try:
        result = runner(
            closes,
            list(closes),
            volumes,
            highs,
            lows,
            period=config["period"],
            oversold=config["oversold"],
            overbought=config["overbought"],
        )
    except Exception:
        return None
    if result is None:
        return None
    action, confidence, reason = result
    action = str(action).upper()
    if action not in {"BUY", "SELL", "HOLD"}:
        return None
    return {
        "action": action,
        "confidence": float(confidence),
        "reason": str(reason),
        "strategy": certification["strategy_name"],
        "research_certification": copy.deepcopy(certification),
    }
