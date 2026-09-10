from __future__ import annotations

from pathlib import Path


def replace_once(path: str, old: str, new: str, label: str) -> None:
    p = Path(path)
    text = p.read_text(encoding="utf-8")
    count = text.count(old)
    if count != 1:
        raise SystemExit(f"{label}: expected one match, found {count}")
    p.write_text(text.replace(old, new), encoding="utf-8")


def replace_in_function(path: str, function_marker: str, end_marker: str, old: str, new: str, label: str) -> None:
    p = Path(path)
    text = p.read_text(encoding="utf-8")
    start = text.index(function_marker)
    end = text.index(end_marker, start)
    segment = text[start:end]
    count = segment.count(old)
    if count != 1:
        raise SystemExit(f"{label}: expected one match in function, found {count}")
    segment = segment.replace(old, new)
    p.write_text(text[:start] + segment + text[end:], encoding="utf-8")


replace_once(
    "rust_core/src/strategies.rs",
    '''pub fn rsi_mean_reversion(closes: &[f64]) -> Option<Signal> {\n    let rsi_val = indicators::rsi(closes, 14);\n    if rsi_val < 30.0 {\n        let conf = ((30.0 - rsi_val) / 30.0).min(1.0);\n        Some(Signal { action: "BUY".into(), confidence: conf, reason: format!("RSI oversold {:.1}", rsi_val) })\n    } else if rsi_val > 70.0 {\n        let conf = ((rsi_val - 70.0) / (100.0 - 70.0)).min(1.0);\n        Some(Signal { action: "SELL".into(), confidence: conf, reason: format!("RSI overbought {:.1}", rsi_val) })\n    } else {\n        None\n    }\n}\n''',
    '''pub fn rsi_mean_reversion_configured(\n    closes: &[f64],\n    period: usize,\n    oversold: f64,\n    overbought: f64,\n) -> Option<Signal> {\n    if period < 2\n        || !oversold.is_finite()\n        || !overbought.is_finite()\n        || oversold <= 0.0\n        || oversold >= overbought\n        || overbought >= 100.0\n    {\n        return None;\n    }\n    let rsi_val = indicators::rsi(closes, period);\n    if !rsi_val.is_finite() {\n        return None;\n    }\n    if rsi_val < oversold {\n        let conf = ((oversold - rsi_val) / oversold).clamp(0.0, 1.0);\n        Some(Signal { action: "BUY".into(), confidence: conf, reason: format!("RSI oversold {:.1}", rsi_val) })\n    } else if rsi_val > overbought {\n        let conf = ((rsi_val - overbought) / (100.0 - overbought)).clamp(0.0, 1.0);\n        Some(Signal { action: "SELL".into(), confidence: conf, reason: format!("RSI overbought {:.1}", rsi_val) })\n    } else {\n        None\n    }\n}\n\npub fn rsi_mean_reversion(closes: &[f64]) -> Option<Signal> {\n    rsi_mean_reversion_configured(closes, 14, 30.0, 70.0)\n}\n''',
    "typed RSI strategy",
)

replace_once(
    "rust_core/src/lib.rs",
    '''fn run_strategy_opens_py(strategy_name: &str, closes: Vec<f64>, opens: Vec<f64>,\n                          volumes: Vec<f64>, highs: Vec<f64>, lows: Vec<f64>) -> Option<(String, f64, String)> {\n    let sig = strategies::evaluate_opens(strategy_name, &closes, &opens, &volumes, &highs, &lows);\n    sig.map(|s| (s.action, s.confidence, s.reason))\n}\n''',
    '''fn run_strategy_opens_py(strategy_name: &str, closes: Vec<f64>, opens: Vec<f64>,\n                          volumes: Vec<f64>, highs: Vec<f64>, lows: Vec<f64>) -> Option<(String, f64, String)> {\n    let sig = strategies::evaluate_opens(strategy_name, &closes, &opens, &volumes, &highs, &lows);\n    sig.map(|s| (s.action, s.confidence, s.reason))\n}\n\n/// Typed configured RSI replay entry point.\n#[pyfunction]\n#[pyo3(signature = (closes, opens, volumes, highs, lows, period=14, oversold=30.0, overbought=70.0))]\nfn run_rsi_revert_opens_configured_py(\n    closes: Vec<f64>,\n    opens: Vec<f64>,\n    volumes: Vec<f64>,\n    highs: Vec<f64>,\n    lows: Vec<f64>,\n    period: usize,\n    oversold: f64,\n    overbought: f64,\n) -> PyResult<Option<(String, f64, String)>> {\n    let _ = (opens, volumes, highs, lows);\n    if !(2..=200).contains(&period) {\n        return Err(pyo3::exceptions::PyValueError::new_err("RSI period must be in [2, 200]"));\n    }\n    if !oversold.is_finite() || !overbought.is_finite()\n        || oversold <= 0.0 || oversold >= overbought || overbought >= 100.0\n    {\n        return Err(pyo3::exceptions::PyValueError::new_err(\n            "RSI thresholds must satisfy 0 < oversold < overbought < 100",\n        ));\n    }\n    let sig = strategies::rsi_mean_reversion_configured(&closes, period, oversold, overbought);\n    Ok(sig.map(|s| (s.action, s.confidence, s.reason)))\n}\n''',
    "configured RSI PyO3 binding",
)
replace_once(
    "rust_core/src/lib.rs",
    '    m.add_function(wrap_pyfunction!(run_strategy_opens_py, m)?)?;\n',
    '    m.add_function(wrap_pyfunction!(run_strategy_opens_py, m)?)?;\n    m.add_function(wrap_pyfunction!(run_rsi_revert_opens_configured_py, m)?)?;\n',
    "configured RSI module registration",
)

replace_once(
    "scripts/backtest_framework/canonical_replay.py",
    'ATTESTATION_SCHEMA_VERSION = 1\nATTESTATION_TYPE = "canonical_feed_cache_rust_replay_v1"\nDATASET_KIND = "coinbase_candles"\nRUNNER_ID = "scripts.backtest_framework.canonical_replay"\nREQUIRED_RUST_SYMBOLS = ("run_strategy_opens_py", "backtest_strategy_py")\n',
    'ATTESTATION_SCHEMA_VERSION = 2\nATTESTATION_TYPE = "canonical_feed_cache_rust_replay_v2"\nDATASET_KIND = "coinbase_candles"\nRUNNER_ID = "scripts.backtest_framework.canonical_replay"\nREQUIRED_RUST_SYMBOLS = (\n    "run_strategy_opens_py",\n    "run_rsi_revert_opens_configured_py",\n    "backtest_strategy_py",\n)\nRSI_REVERT_CONFIG_KEYS = frozenset({"period", "oversold", "overbought"})\n',
    "replay schema v2 constants",
)
replace_once(
    "scripts/backtest_framework/canonical_replay.py",
    '''    if not math.isfinite(result):\n        raise ValueError(f"{name} must be finite")\n    return result\n\n\ndef normalize_candle_rows''',
    '''    if not math.isfinite(result):\n        raise ValueError(f"{name} must be finite")\n    return result\n\n\ndef normalize_strategy_config(strategy_name: str, config: Any = None) -> dict[str, Any]:\n    """Normalize the typed strategy parameters that canonical replay can execute."""\n    if config is None or config == {}:\n        return {}\n    if not isinstance(config, dict):\n        raise TypeError("strategy_config must be an object")\n    if strategy_name != "rsi_revert":\n        raise ValueError(f"configured canonical replay is not supported for {strategy_name!r}")\n    if set(config) != RSI_REVERT_CONFIG_KEYS:\n        missing = sorted(RSI_REVERT_CONFIG_KEYS - set(config))\n        extra = sorted(set(config) - RSI_REVERT_CONFIG_KEYS)\n        raise ValueError(f"rsi_revert strategy_config keys mismatch: missing={missing}, extra={extra}")\n    period_number = _finite_float(config["period"], name="strategy_config.period")\n    if isinstance(config["period"], bool) or not period_number.is_integer():\n        raise ValueError("strategy_config.period must be an integer")\n    period = int(period_number)\n    oversold = _finite_float(config["oversold"], name="strategy_config.oversold")\n    overbought = _finite_float(config["overbought"], name="strategy_config.overbought")\n    if period < 2 or period > 200:\n        raise ValueError("strategy_config.period must be in [2, 200]")\n    if not (0.0 < oversold < overbought < 100.0):\n        raise ValueError("strategy_config thresholds must satisfy 0 < oversold < overbought < 100")\n    return {"period": period, "oversold": oversold, "overbought": overbought}\n\n\ndef normalize_candle_rows''',
    "typed strategy config normalizer",
)
replace_once(
    "scripts/backtest_framework/canonical_replay.py",
    '''    warmup: int = 30,\n    fee_bps: float = 0.0,\n    max_hold_bars: int = 0,\n) -> list[float]:\n''',
    '''    warmup: int = 30,\n    fee_bps: float = 0.0,\n    max_hold_bars: int = 0,\n    strategy_config: dict[str, Any] | None = None,\n) -> list[float]:\n''',
    "replay strategy config signature",
)
replace_once(
    "scripts/backtest_framework/canonical_replay.py",
    '''    if not strategy_name:\n        raise ValueError("strategy_name is required")\n    normalized = normalize_candle_rows(rows)\n''',
    '''    if not strategy_name:\n        raise ValueError("strategy_name is required")\n    normalized_strategy_config = normalize_strategy_config(strategy_name, strategy_config)\n    normalized = normalize_candle_rows(rows)\n''',
    "replay normalize strategy config",
)
replace_once(
    "scripts/backtest_framework/canonical_replay.py",
    '''        signal = rust_core.run_strategy_opens_py(\n            strategy_name,\n            closes[: index + 1],\n            opens[: index + 1],\n            volumes[: index + 1],\n            highs[: index + 1],\n            lows[: index + 1],\n        )\n''',
    '''        if normalized_strategy_config:\n            signal = rust_core.run_rsi_revert_opens_configured_py(\n                closes[: index + 1],\n                opens[: index + 1],\n                volumes[: index + 1],\n                highs[: index + 1],\n                lows[: index + 1],\n                period=normalized_strategy_config["period"],\n                oversold=normalized_strategy_config["oversold"],\n                overbought=normalized_strategy_config["overbought"],\n            )\n        else:\n            signal = rust_core.run_strategy_opens_py(\n                strategy_name,\n                closes[: index + 1],\n                opens[: index + 1],\n                volumes[: index + 1],\n                highs[: index + 1],\n                lows[: index + 1],\n            )\n''',
    "configured replay dispatch",
)

# Add strategy_config to attest_snapshot_replay signature.
p = Path("scripts/backtest_framework/canonical_replay.py")
text = p.read_text(encoding="utf-8")
start = text.index("def attest_snapshot_replay(\n")
end = text.index(") -> tuple[list[list[float]], dict[str, Any]]:", start)
segment = text[start:end]
needle = "    max_hold_bars: int = 0,\n"
if segment.count(needle) != 1:
    raise SystemExit("attestation signature anchor mismatch")
segment = segment.replace(needle, needle + "    strategy_config: dict[str, Any] | None = None,\n")
text = text[:start] + segment + text[end:]
p.write_text(text, encoding="utf-8")

replace_in_function(
    "scripts/backtest_framework/canonical_replay.py",
    "def attest_snapshot_replay(\n",
    "\n\ndef verify_replay_attestation(",
    '''    rows = normalize_candle_rows(snapshot.get("rows", []))\n    rebuilt = snapshot_from_rows(\n''',
    '''    normalized_strategy_config = normalize_strategy_config(strategy_name, strategy_config)\n    rows = normalize_candle_rows(snapshot.get("rows", []))\n    rebuilt = snapshot_from_rows(\n''',
    "attestation normalize config",
)
replace_in_function(
    "scripts/backtest_framework/canonical_replay.py",
    "def attest_snapshot_replay(\n",
    "\n\ndef verify_replay_attestation(",
    '''            warmup=int(warmup),\n            fee_bps=float(fee_bps),\n            max_hold_bars=int(max_hold_bars),\n        )\n''',
    '''            warmup=int(warmup),\n            fee_bps=float(fee_bps),\n            max_hold_bars=int(max_hold_bars),\n            strategy_config=normalized_strategy_config,\n        )\n''',
    "fold replay configured input",
)
replace_in_function(
    "scripts/backtest_framework/canonical_replay.py",
    "def attest_snapshot_replay(\n",
    "\n\ndef verify_replay_attestation(",
    '''        "dataset": snapshot["manifest"],\n        "strategy_name": strategy_name,\n        "warmup": int(warmup),\n''',
    '''        "dataset": snapshot["manifest"],\n        "strategy_name": strategy_name,\n        "strategy_config": normalized_strategy_config,\n        "strategy_config_hash": stable_hash(normalized_strategy_config),\n        "execution_config_bound": bool(normalized_strategy_config),\n        "warmup": int(warmup),\n''',
    "attestation config fields",
)
replace_once(
    "scripts/backtest_framework/canonical_replay.py",
    '''        "dataset", "strategy_name", "warmup", "fee_bps", "max_hold_bars",\n        "n_folds", "purge_size", "embargo_size", "folds",\n''',
    '''        "dataset", "strategy_name", "strategy_config", "strategy_config_hash",\n        "execution_config_bound", "warmup", "fee_bps", "max_hold_bars",\n        "n_folds", "purge_size", "embargo_size", "folds",\n''',
    "attestation required config fields",
)
replace_once(
    "scripts/backtest_framework/canonical_replay.py",
    '''    if attestation["attestation_type"] != ATTESTATION_TYPE or attestation["runner"] != RUNNER_ID:\n        reasons.append("replay_attestation_runner_mismatch")\n    if attestation["runner_source_sha256"] != _runner_source_sha256():\n''',
    '''    if attestation["attestation_type"] != ATTESTATION_TYPE or attestation["runner"] != RUNNER_ID:\n        reasons.append("replay_attestation_runner_mismatch")\n    try:\n        normalized_config = normalize_strategy_config(\n            attestation["strategy_name"], attestation["strategy_config"]\n        )\n        if normalized_config != attestation["strategy_config"]:\n            reasons.append("replay_strategy_config_not_normalized")\n        if stable_hash(normalized_config) != attestation["strategy_config_hash"]:\n            reasons.append("replay_strategy_config_hash_mismatch")\n        if bool(normalized_config) != (attestation["execution_config_bound"] is True):\n            reasons.append("replay_execution_config_binding_mismatch")\n    except (TypeError, ValueError, KeyError, OverflowError):\n        reasons.append("replay_strategy_config_invalid")\n    if attestation["runner_source_sha256"] != _runner_source_sha256():\n''',
    "attestation config verification",
)
replace_once(
    "scripts/backtest_framework/canonical_replay.py",
    '''                    warmup=int(attestation["warmup"]),\n                    fee_bps=float(attestation["fee_bps"]),\n                    max_hold_bars=int(attestation["max_hold_bars"]),\n                )\n''',
    '''                    warmup=int(attestation["warmup"]),\n                    fee_bps=float(attestation["fee_bps"]),\n                    max_hold_bars=int(attestation["max_hold_bars"]),\n                    strategy_config=attestation["strategy_config"],\n                )\n''',
    "source reverify configured input",
)
replace_once(
    "scripts/backtest_framework/canonical_replay.py",
    '''    if evidence.get("dataset_hash") != attestation.get("dataset", {}).get("dataset_hash"):\n        replay_reasons.append("replay_dataset_hash_mismatch")\n    if replay_reasons:\n''',
    '''    if evidence.get("dataset_hash") != attestation.get("dataset", {}).get("dataset_hash"):\n        replay_reasons.append("replay_dataset_hash_mismatch")\n    if attestation.get("execution_config_bound") is True and evidence.get("candidate_config") != attestation.get("strategy_config"):\n        replay_reasons.append("replay_candidate_execution_config_mismatch")\n    if replay_reasons:\n''',
    "evidence execution-config binding",
)

# Add strategy_config to build_alpha_evidence_from_canonical_replay signature and body.
p = Path("scripts/backtest_framework/canonical_replay.py")
text = p.read_text(encoding="utf-8")
start = text.index("def build_alpha_evidence_from_canonical_replay(\n")
end = text.index(") -> dict[str, Any]:", start)
segment = text[start:end]
needle = "    strategy_name: str,\n"
if segment.count(needle) != 1:
    raise SystemExit("builder signature anchor mismatch")
segment = segment.replace(needle, needle + "    strategy_config: dict[str, Any] | None = None,\n")
text = text[:start] + segment + text[end:]
p.write_text(text, encoding="utf-8")

p = Path("scripts/backtest_framework/canonical_replay.py")
text = p.read_text(encoding="utf-8")
start = text.index("def build_alpha_evidence_from_canonical_replay(\n")
needle = "    snapshot = load_canonical_snapshot(\n"
pos = text.index(needle, start)
insert = '''    normalized_strategy_config = normalize_strategy_config(strategy_name, strategy_config)\n    if normalized_strategy_config and candidate_config != normalized_strategy_config:\n        raise ValueError("candidate_config must equal the executable strategy_config")\n\n'''
text = text[:pos] + insert + text[pos:]
p.write_text(text, encoding="utf-8")

replace_in_function(
    "scripts/backtest_framework/canonical_replay.py",
    "def build_alpha_evidence_from_canonical_replay(\n",
    "\n\ndef verify_evidence_replay_binding(",
    '''        warmup=warmup,\n        fee_bps=fee_bps,\n        max_hold_bars=max_hold_bars,\n    )\n    manifest = snapshot["manifest"]\n''',
    '''        warmup=warmup,\n        fee_bps=fee_bps,\n        max_hold_bars=max_hold_bars,\n        strategy_config=normalized_strategy_config,\n    )\n    manifest = snapshot["manifest"]\n''',
    "builder configured attestation",
)

# Evaluation and promotion now require executable parameter binding.
replace_in_function(
    "scripts/challenger_manager.py",
    "def evaluate_challenger_evidence(\n",
    "\n\nclass ChallengerRegistry:",
    '''        replay_valid, replay_reasons = verify_evidence_replay_binding(\n            evidence, reverify_source=True\n        )\n''',
    '''        attestation = evidence.get("replay_attestation")\n        if not isinstance(attestation, dict) or attestation.get("execution_config_bound") is not True:\n            return {\n                "approved": False,\n                "reasons": ["alpha_validation_execution_config_binding_required"],\n                "pnl_improvement_usd": 0.0,\n                "drawdown_increase_pct_points": 0.0,\n                "thresholds": {**DEFAULT_THRESHOLDS, **(thresholds or {})},\n                "evaluated_at": _utc_now(),\n                "evidence_hash": evidence.get("evidence_hash"),\n                "evidence_schema_version": evidence.get("schema_version"),\n            }\n        if attestation.get("strategy_config") != evidence.get("candidate_config"):\n            return {\n                "approved": False,\n                "reasons": ["alpha_validation_execution_config_mismatch"],\n                "pnl_improvement_usd": 0.0,\n                "drawdown_increase_pct_points": 0.0,\n                "thresholds": {**DEFAULT_THRESHOLDS, **(thresholds or {})},\n                "evaluated_at": _utc_now(),\n                "evidence_hash": evidence.get("evidence_hash"),\n                "evidence_schema_version": evidence.get("schema_version"),\n            }\n        replay_valid, replay_reasons = verify_evidence_replay_binding(\n            evidence, reverify_source=True\n        )\n''',
    "evaluation executable-config gate",
)
replace_once(
    "scripts/challenger_manager.py",
    '''        if evidence.get("replay_provenance_bound") is not True:\n            raise ValueError("challenger alpha-validation replay provenance is required")\n        replay_valid, replay_reasons = verify_evidence_replay_binding(\n''',
    '''        if evidence.get("replay_provenance_bound") is not True:\n            raise ValueError("challenger alpha-validation replay provenance is required")\n        attestation = evidence.get("replay_attestation")\n        if not isinstance(attestation, dict) or attestation.get("execution_config_bound") is not True:\n            raise ValueError("challenger alpha-validation executable config binding is required")\n        if attestation.get("strategy_config") != challenger.get("parameters"):\n            raise ValueError("challenger alpha-validation executable config mismatch")\n        replay_valid, replay_reasons = verify_evidence_replay_binding(\n''',
    "promotion executable-config gate",
)

replace_once(
    "tests/test_alpha_validation.py",
    '''    bound["replay_attestation"] = {"fixture": "registry-mechanics-only"}\n    bound["replay_provenance_bound"] = True\n''',
    '''    bound["replay_attestation"] = {\n        "fixture": "registry-mechanics-only",\n        "execution_config_bound": True,\n        "strategy_config": copy.deepcopy(bound["candidate_config"]),\n    }\n    bound["replay_provenance_bound"] = True\n''',
    "registry fixture executable config binding",
)

p = Path("tests/test_alpha_validation.py")
text = p.read_text(encoding="utf-8")
anchor = "def test_registry_canary_carries_verified_evidence_hash(tmp_path, monkeypatch):\n"
if text.count(anchor) != 1:
    raise SystemExit("alpha test insertion anchor mismatch")
addition = '''def test_registry_rejects_replay_without_executable_config_binding(tmp_path, monkeypatch):\n    registry, challenger = _registry(tmp_path)\n    evidence = _fixture_replay_bound(_evidence(challenger["id"]))\n    evidence["replay_attestation"]["execution_config_bound"] = False\n    evidence.pop("evidence_hash", None)\n    evidence["evidence_hash"] = stable_hash(evidence)\n    monkeypatch.setattr(\n        "scripts.challenger_manager.verify_evidence_replay_binding",\n        lambda evidence, reverify_source=True: (True, []),\n    )\n    result = registry.evaluate(\n        challenger["id"],\n        {"net_pnl_after_cost_usd": 0, "max_drawdown_pct": 0},\n        validation_evidence=evidence,\n    )\n    assert result["approved"] is False\n    assert result["reasons"] == ["alpha_validation_execution_config_binding_required"]\n\n\ndef test_registry_rejects_attested_execution_config_mismatch(tmp_path, monkeypatch):\n    registry, challenger = _registry(tmp_path)\n    evidence = _fixture_replay_bound(_evidence(challenger["id"]))\n    evidence["replay_attestation"]["strategy_config"] = {"lookback": 99, "threshold": 0.7}\n    evidence.pop("evidence_hash", None)\n    evidence["evidence_hash"] = stable_hash(evidence)\n    monkeypatch.setattr(\n        "scripts.challenger_manager.verify_evidence_replay_binding",\n        lambda evidence, reverify_source=True: (True, []),\n    )\n    result = registry.evaluate(\n        challenger["id"],\n        {"net_pnl_after_cost_usd": 0, "max_drawdown_pct": 0},\n        validation_evidence=evidence,\n    )\n    assert result["approved"] is False\n    assert result["reasons"] == ["alpha_validation_execution_config_mismatch"]\n\n\n'''
p.write_text(text.replace(anchor, addition + anchor), encoding="utf-8")

p = Path("tests/test_canonical_replay.py")
text = p.read_text(encoding="utf-8")
if "test_configured_rsi_defaults_match_legacy_replay" in text:
    raise SystemExit("configured RSI replay tests already present")
text = text.rstrip() + '''\n\n\ndef test_configured_rsi_defaults_match_legacy_replay():\n    _require_native_replay()\n    rows = _rows()\n    legacy = C.replay_trade_returns_rust("rsi_revert", rows, warmup=14, fee_bps=10.0)\n    configured = C.replay_trade_returns_rust(\n        "rsi_revert", rows, warmup=14, fee_bps=10.0,\n        strategy_config={"period": 14, "oversold": 30.0, "overbought": 70.0},\n    )\n    assert configured == legacy\n\n\ndef test_configured_rsi_attestation_binds_exact_execution_config():\n    _require_native_replay()\n    snapshot = C.snapshot_from_rows(\n        _rows(), kind=C.DATASET_KIND, symbol="BTC-USD", granularity=3600\n    )\n    config = {"period": 10, "oversold": 25.0, "overbought": 75.0}\n    fold_returns, attestation = C.attest_snapshot_replay(\n        snapshot, strategy_name="rsi_revert", strategy_config=config, n_folds=3, warmup=10\n    )\n    assert len(fold_returns) == 3\n    assert attestation["execution_config_bound"] is True\n    assert attestation["strategy_config"] == config\n    assert attestation["strategy_config_hash"] == C.stable_hash(config)\n\n\ndef test_strategy_config_normalization_fails_closed():\n    assert C.normalize_strategy_config("rsi_revert", None) == {}\n    assert C.normalize_strategy_config(\n        "rsi_revert", {"period": 14, "oversold": 30, "overbought": 70}\n    ) == {"period": 14, "oversold": 30.0, "overbought": 70.0}\n    with pytest.raises(ValueError, match="keys mismatch"):\n        C.normalize_strategy_config("rsi_revert", {"period": 14, "oversold": 30.0})\n    with pytest.raises(ValueError, match="integer"):\n        C.normalize_strategy_config(\n            "rsi_revert", {"period": 14.5, "oversold": 30.0, "overbought": 70.0}\n        )\n    with pytest.raises(ValueError, match="thresholds"):\n        C.normalize_strategy_config(\n            "rsi_revert", {"period": 14, "oversold": 80.0, "overbought": 70.0}\n        )\n    with pytest.raises(ValueError, match="not supported"):\n        C.normalize_strategy_config(\n            "ema_cross", {"period": 14, "oversold": 30.0, "overbought": 70.0}\n        )\n\n\ndef test_binding_rejects_candidate_metadata_different_from_executed_config():\n    _require_native_replay()\n    snapshot = C.snapshot_from_rows(\n        _rows(), kind=C.DATASET_KIND, symbol="BTC-USD", granularity=3600\n    )\n    config = {"period": 14, "oversold": 30.0, "overbought": 70.0}\n    fold_returns, attestation = C.attest_snapshot_replay(\n        snapshot, strategy_name="rsi_revert", strategy_config=config, n_folds=3, warmup=14\n    )\n    evidence = build_alpha_validation_evidence(\n        candidate_id="challenger-config-mismatch",\n        candidate_source_sha="a" * 40,\n        candidate_config={"period": 7, "oversold": 30.0, "overbought": 70.0},\n        dataset_id=snapshot["manifest"]["dataset_id"],\n        dataset_hash=snapshot["manifest"]["dataset_hash"],\n        fold_returns=fold_returns,\n        net_pnl_after_cost_usd=1.0,\n        cost_coverage_ratio=2.0,\n        regimes_tested=["up", "down", "range"],\n        accounting_invariants_ok=True,\n        lineage_verified=True,\n        parameter_stability_score=0.9,\n        bootstrap_samples=20,\n        created_at="2026-09-10T12:00:00+00:00",\n    )\n    with pytest.raises(ValueError, match="replay_candidate_execution_config_mismatch"):\n        C.bind_evidence_to_replay(evidence, attestation)\n''' + "\n"
p.write_text(text, encoding="utf-8")
