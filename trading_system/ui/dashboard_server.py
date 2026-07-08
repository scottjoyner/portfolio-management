#!/usr/bin/env python3
"""Trading System UI Dashboard Server with all API endpoints.

Endpoints:
  GET /health                   — System health
  GET /accounts                 — Portfolio accounts
  GET /positions                — Open positions
  GET /strategies               — Strategy performance/stats
  GET /approvals                — Pending approvals (pending_approvals.json + operator-state.json)
  GET /performance              — Portfolio performance metrics
  GET /evaluations/price/{inst} — Price estimates
  GET /research/hypotheses      — Trading hypotheses
  GET /market/regime            — Current market regime
  GET /market/intelligence      — Unified live market snapshot
  GET /market/universe          — Available symbols
  GET /prediction-markets       — Ranked prediction market universe
  GET /arbitrage/opportunities  — Cross-venue arbitrage rankings
  GET /crypto-divergence        — Crypto price vs prediction market divergence
   GET /signals/opportunities    — BTC-XXX ranked opportunities
   GET /signals/feed             — Full signal queue (from accumulator)
   GET /signals/diversification  — Diversification strategies signal overview (5 new: kalman_mr, hp_trend, funding_contrarian, exchange_flow, btc_dxy_corr)
   GET /strategies/performance   — Strategy performance breakdown
  GET /dashboard                — Dashboard HTML
  GET /                         — Dashboard HTML

Usage:
    python3 ui/dashboard_server.py
    python3 ui/dashboard_server.py --host 0.0.0.0 --port 8080
"""

import http.server
import socketserver
import os
import sys
import json
import argparse
import time
import logging
import threading
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeoutError
from datetime import datetime, timezone
from functools import lru_cache
from pathlib import Path
from urllib.parse import urlparse
from urllib.request import Request, urlopen
from urllib.error import URLError

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("dashboard_server")

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / 'graph-alpha-bot' / 'app' / 'strategies'))

OPERATOR_STATE_PATH = str(ROOT / 'data' / 'operator-state.json')
SIGNAL_CACHE_PATH = str(ROOT / 'data' / '.unified_signal_cache.json')
APPROVALS_PATH = str(ROOT / 'data' / 'pending_approvals.json')
STATE_DB_PATH = str(ROOT / 'optimizer_state.db')
CAPITAL_BUCKETS_PATH = str(ROOT / 'data' / 'capital_buckets.json')
EQUITY_SUMMARY_PATH = str(ROOT / 'data' / 'equity_summary.json')
OPERATOR_ACTIONS_PATH = os.environ.get('OPERATOR_ACTIONS_PATH', str(ROOT / 'data' / 'operator-actions.json'))
OPERATOR_ACTIONS_URL = os.environ.get('OPERATOR_ACTIONS_URL', '').rstrip('/')
PREDICTION_MARKETS_CACHE = {"ts": 0.0, "data": None}
ARBITRAGE_CACHE = {"ts": 0.0, "data": None}
GRAPH_CACHE = {"ts": 0.0, "data": None}
_SHARED_CACHE_LOCK = threading.Lock()
PREDICTION_MARKETS_TTL_SECS = 300
ARBITRAGE_TTL_SECS = 180
GRAPH_TTL_SECS = 300

DEFAULT_STOCK_WATCHLIST = [
    'AAPL', 'MSFT', 'NVDA', 'AMZN', 'META', 'GOOGL', 'TSLA',
    'SPY', 'QQQ', 'VTI', 'IWM', 'XLK', 'XLF', 'XLE',
]

DEFAULT_CAPITAL_POLICY = {
    "targets": {"reserve": 0.50, "core": 0.20, "opportunity": 0.30},
    "core_allowlist": ["BTC", "ETH", "SOL"],
    "core_min_allocation_pct": 10.0,
    "core_batch_fraction": 0.05,
    "opportunity_batch_fraction": 0.03,
}

OPERATOR_ACTIONS = [
    {
        "id": "refresh_market_data",
        "label": "Refresh market data",
        "description": "Request the collector/daemon to refresh cached market snapshots.",
        "risk": "safe",
    },
    {
        "id": "generate_trade_plans",
        "label": "Generate trade plans",
        "description": "Request a dry-run optimizer pass to refresh trade_plans.json.",
        "risk": "safe",
    },
    {
        "id": "rebalance_dry_run",
        "label": "Rebalance dry-run",
        "description": "Queue a rebalance proposal without live execution.",
        "risk": "guarded",
    },
    {
        "id": "paper_smoke",
        "label": "Paper smoke test",
        "description": "Run the paper-trading smoke path and store the result in logs.",
        "risk": "safe",
    },
    {
        "id": "pause_live_trading",
        "label": "Pause live trading",
        "description": "Set operator intent to pause live execution until manually resumed.",
        "risk": "guarded",
    },
]

CAPITAL_PRESETS = {
    "conservative": {
        "name": "Conservative",
        "description": "More cash, smaller risk sleeve.",
        "policy": {
            "targets": {"reserve": 0.65, "core": 0.25, "opportunity": 0.10},
            "core_allowlist": ["BTC", "ETH", "SOL"],
            "core_min_allocation_pct": 15.0,
            "core_batch_fraction": 0.04,
            "opportunity_batch_fraction": 0.02,
        },
    },
    "balanced": {
        "name": "Balanced",
        "description": "Default 50/20/30 split with medium batch sizes.",
        "policy": {
            "targets": {"reserve": 0.50, "core": 0.20, "opportunity": 0.30},
            "core_allowlist": ["BTC", "ETH", "SOL"],
            "core_min_allocation_pct": 10.0,
            "core_batch_fraction": 0.05,
            "opportunity_batch_fraction": 0.03,
        },
    },
    "aggressive": {
        "name": "Aggressive",
        "description": "Lower cash, larger opportunity sleeve.",
        "policy": {
            "targets": {"reserve": 0.35, "core": 0.20, "opportunity": 0.45},
            "core_allowlist": ["BTC", "ETH", "SOL"],
            "core_min_allocation_pct": 8.0,
            "core_batch_fraction": 0.06,
            "opportunity_batch_fraction": 0.05,
        },
    },
}


# ── Helpers ─────────────────────────────────────────────────────

def _load_json(path, default=None):
    if not os.path.exists(path):
        return default if default is not None else {}
    try:
        with open(path) as f:
            return json.load(f)
    except (json.JSONDecodeError, OSError):
        return default if default is not None else {}


def _write_json(path, data) -> bool:
    try:
        with open(path, 'w') as f:
            json.dump(data, f, indent=2, default=str)
        return True
    except Exception:
        return False


def _get_state_store():
    try:
        from state_store import StateStore
        return StateStore(STATE_DB_PATH)
    except Exception:
        return None


def _get_graph_store():
    try:
        from coinbase.src.graph.neo4j_graph import CryptoGraphStore
    except Exception:
        return None

    cached = None
    now = time.time()
    with _SHARED_CACHE_LOCK:
        cached = GRAPH_CACHE.get("data")
        if cached is not None and now - float(GRAPH_CACHE.get("ts", 0.0) or 0.0) < GRAPH_TTL_SECS:
            return cached

    try:
        store = CryptoGraphStore()
        with _SHARED_CACHE_LOCK:
            GRAPH_CACHE["data"] = store
            GRAPH_CACHE["ts"] = now
        return store
    except Exception as e:
        logger.debug("Graph store unavailable: %s", e)
        return None


def _graph_summary_for_products(products: list[str], limit: int = 10) -> dict:
    if not products:
        return {"available": False, "products": [], "top_assets": [], "avg_score": 0.0}

    store = _get_graph_store()
    if not store:
        return {"available": False, "products": products[:limit], "top_assets": [], "avg_score": 0.0}

    try:
        from coinbase.src.graph.portfolio_overlay import graph_weight_overlays
        signals = [store.asset_signal(pid) for pid in products[: max(1, limit)]]
        signals = sorted(signals, key=lambda s: s.graph_score, reverse=True)
        overlays = graph_weight_overlays(signals, max_boost=0.25)
        avg_score = sum(s.graph_score for s in signals) / len(signals) if signals else 0.0
        return {
            "available": True,
            "products": products[:limit],
            "top_assets": [
                {
                    "product_id": s.product_id,
                    "symbol": s.symbol,
                    "graph_score": round(s.graph_score, 3),
                    "available_on_coinbase": bool(getattr(s, "available_on_coinbase", False)),
                    "overlay": round(overlays.get(s.product_id, 1.0), 3),
                    "reasons": list(getattr(s, "reasons", []) or []),
                }
                for s in signals[:limit]
            ],
            "avg_score": round(avg_score, 3),
        }
    except Exception as e:
        logger.debug("Graph summary failed: %s", e)
        return {"available": False, "products": products[:limit], "top_assets": [], "avg_score": 0.0}


def _normalize_capital_policy(policy: dict | None = None) -> dict:
    raw = dict(DEFAULT_CAPITAL_POLICY)
    if policy:
        raw.update({k: v for k, v in policy.items() if v is not None})
    targets = dict(DEFAULT_CAPITAL_POLICY["targets"])
    targets.update(raw.get("targets") or {})
    total = sum(max(float(v), 0.0) for v in targets.values())
    if total > 0:
        targets = {k: max(float(v), 0.0) / total for k, v in targets.items()}
    allowlist = raw.get("core_allowlist") or DEFAULT_CAPITAL_POLICY["core_allowlist"]
    if isinstance(allowlist, str):
        allowlist = [x.strip() for x in allowlist.split(",") if x.strip()]
    allowlist = [str(x).upper().replace("-USD", "") for x in allowlist if str(x).strip()]
    if not allowlist:
        allowlist = list(DEFAULT_CAPITAL_POLICY["core_allowlist"])
    return {
        "targets": targets,
        "core_allowlist": allowlist,
        "core_min_allocation_pct": max(float(raw.get("core_min_allocation_pct", DEFAULT_CAPITAL_POLICY["core_min_allocation_pct"])), 0.0),
        "core_batch_fraction": max(min(float(raw.get("core_batch_fraction", DEFAULT_CAPITAL_POLICY["core_batch_fraction"])), 0.5), 0.0),
        "opportunity_batch_fraction": max(min(float(raw.get("opportunity_batch_fraction", DEFAULT_CAPITAL_POLICY["opportunity_batch_fraction"])), 0.5), 0.0),
        "max_deployable_usd": float(raw.get("max_deployable_usd", 0.0) or 0.0),
        "live_test_started_at": str(raw.get("live_test_started_at", "") or ""),
        "updated_at": str(raw.get("updated_at", "") or ""),
        "preset_name": str(raw.get("preset_name", "custom")),
        }


def _get_capital_policy():
    store = _get_state_store()
    if store:
        try:
            raw = store.get_meta("capital_policy")
            if raw:
                return _normalize_capital_policy(json.loads(raw))
        except Exception:
            pass
    op = _load_json(OPERATOR_STATE_PATH, {})
    if op.get("capitalPolicy"):
        return _normalize_capital_policy(op.get("capitalPolicy"))
    return _normalize_capital_policy(None)


def _build_preset_payload():
    presets = []
    for preset_id, preset in CAPITAL_PRESETS.items():
        payload = _normalize_capital_policy({**preset["policy"], "preset_name": preset_id})
        payload["name"] = preset["name"]
        payload["description"] = preset["description"]
        payload["id"] = preset_id
        presets.append(payload)
    return presets


def _load_capital_buckets() -> dict:
    payload = _load_json(CAPITAL_BUCKETS_PATH, {})
    buckets = payload.get('buckets', []) if isinstance(payload, dict) else payload
    if not isinstance(buckets, list):
        buckets = []
    normalized = []
    total_value = 0.0
    for item in buckets:
        if not isinstance(item, dict):
            continue
        cash = float(item.get('cash_usd', 0) or 0)
        total = float(item.get('starting_balance_usd', 0) or 0)
        realized = float(item.get('realized_pnl_usd', 0) or 0)
        volume = float(item.get('volume_30d_usd', 0) or 0)
        target_volume = float(item.get('target_volume_usd', 0) or 0)
        target_multiple = float(item.get('target_multiple', 0) or 0)
        positions = item.get('positions', {}) or {}
        current_value = cash + sum(float((p or {}).get('size', 0) or 0) * float((p or {}).get('current_price', (p or {}).get('entry_price', 0)) or 0) for p in positions.values() if isinstance(p, dict))
        total_value += current_value
        normalized.append({
            'bucket_id': item.get('bucket_id', ''),
            'name': item.get('name', ''),
            'cash_usd': round(cash, 2),
            'total_value_usd': round(current_value, 2),
            'starting_balance_usd': round(total, 2),
            'realized_pnl_usd': round(realized, 2),
            'volume_30d_usd': round(volume, 2),
            'target_volume_usd': round(target_volume, 2),
            'target_multiple': target_multiple,
            'volume_progress': round(volume / target_volume, 3) if target_volume > 0 else 0.0,
            'equity_progress': round(current_value / (total * target_multiple), 3) if total > 0 and target_multiple > 0 else 0.0,
            'positions': len(positions),
            'active': bool(item.get('active', True)),
            'allowed_strategies': list(item.get('allowed_strategies', []) or []),
        })
    normalized.sort(key=lambda b: (b.get('active', False), b.get('total_value_usd', 0.0)), reverse=True)
    return {'buckets': normalized, 'total_value_usd': round(total_value, 2)}


def _bucket_preset_names() -> list[str]:
    try:
        from coinbase.src.capital_buckets import bucket_preset_names
        return bucket_preset_names()
    except Exception:
        return ['challenge_1', 'challenge_5', 'challenge_10', 'challenge_50', 'challenge_100', 'challenge', 'core', 'fee_tier', 'challenge_core_fee_tier']


def _build_bucket_preset(name: str, payload: dict | None = None) -> dict:
    payload = payload or {}
    try:
        from coinbase.src.capital_buckets import build_bucket_preset
        kwargs = {}
        if isinstance(payload, dict):
            for key in ('starting_balance_usd', 'challenge_usd', 'core_usd', 'fee_tier_usd'):
                if key in payload:
                    kwargs[key] = float(payload[key])
        return build_bucket_preset(name, **kwargs)
    except Exception:
        if name in {'challenge_1', 'challenge_5', 'challenge_10', 'challenge_50', 'challenge_100'}:
            amount = float(name.split('_', 1)[1])
            return {'buckets': [{'bucket_id': name, 'name': f'${int(amount)} Challenge', 'starting_balance_usd': amount, 'cash_usd': amount, 'target_volume_usd': 10000.0, 'target_multiple': 3.0, 'max_position_pct': 0.25, 'allowed_strategies': [], 'active': True, 'realized_pnl_usd': 0.0, 'volume_30d_usd': 0.0, 'positions': {}, 'updated_at': time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())}]}
        if name == 'challenge':
            return {'buckets': [{'bucket_id': 'challenge', 'name': '100 USDC Challenge', 'starting_balance_usd': 100.0, 'cash_usd': 100.0, 'target_volume_usd': 10000.0, 'target_multiple': 3.0, 'max_position_pct': 0.25, 'allowed_strategies': [], 'active': True, 'realized_pnl_usd': 0.0, 'volume_30d_usd': 0.0, 'positions': {}, 'updated_at': time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())}]}
        if name == 'fee_tier':
            return {'buckets': [{'bucket_id': 'fee_tier', 'name': 'Fee Tier Generator', 'starting_balance_usd': 1000.0, 'cash_usd': 1000.0, 'target_volume_usd': 10000.0, 'target_multiple': 1.1, 'max_position_pct': 0.40, 'allowed_strategies': ['volume_generator', 'market_making'], 'active': True, 'realized_pnl_usd': 0.0, 'volume_30d_usd': 0.0, 'positions': {}, 'updated_at': time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())}]}
        if name == 'core':
            return {'buckets': [
                {'bucket_id': 'core', 'name': 'Core', 'starting_balance_usd': 800.0, 'cash_usd': 800.0, 'target_volume_usd': 25000.0, 'target_multiple': 1.5, 'max_position_pct': 0.20, 'allowed_strategies': ['ema_cross', 'macd', 'adaptive_mode'], 'active': True, 'realized_pnl_usd': 0.0, 'volume_30d_usd': 0.0, 'positions': {}, 'updated_at': time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())},
                {'bucket_id': 'reserve', 'name': 'Reserve', 'starting_balance_usd': 150.0, 'cash_usd': 150.0, 'target_volume_usd': 5000.0, 'target_multiple': 1.1, 'max_position_pct': 0.05, 'allowed_strategies': [], 'active': False, 'realized_pnl_usd': 0.0, 'volume_30d_usd': 0.0, 'positions': {}, 'updated_at': time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())},
                {'bucket_id': 'opportunity', 'name': 'Opportunity', 'starting_balance_usd': 50.0, 'cash_usd': 50.0, 'target_volume_usd': 20000.0, 'target_multiple': 2.0, 'max_position_pct': 0.25, 'allowed_strategies': ['momentum_rotation', 'volatility', 'breakout'], 'active': True, 'realized_pnl_usd': 0.0, 'volume_30d_usd': 0.0, 'positions': {}, 'updated_at': time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())},
            ]}
        if name == 'challenge_core_fee_tier':
            return {'buckets': [
                {'bucket_id': 'challenge', 'name': '100 USDC Challenge', 'starting_balance_usd': 100.0, 'cash_usd': 100.0, 'target_volume_usd': 10000.0, 'target_multiple': 3.0, 'max_position_pct': 0.25, 'allowed_strategies': [], 'active': True, 'realized_pnl_usd': 0.0, 'volume_30d_usd': 0.0, 'positions': {}, 'updated_at': time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())},
                {'bucket_id': 'core', 'name': 'Core', 'starting_balance_usd': 800.0, 'cash_usd': 800.0, 'target_volume_usd': 25000.0, 'target_multiple': 1.5, 'max_position_pct': 0.20, 'allowed_strategies': ['ema_cross', 'macd', 'adaptive_mode'], 'active': True, 'realized_pnl_usd': 0.0, 'volume_30d_usd': 0.0, 'positions': {}, 'updated_at': time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())},
                {'bucket_id': 'fee_tier', 'name': 'Fee Tier Generator', 'starting_balance_usd': 100.0, 'cash_usd': 100.0, 'target_volume_usd': 10000.0, 'target_multiple': 1.1, 'max_position_pct': 0.40, 'allowed_strategies': ['volume_generator', 'market_making'], 'active': True, 'realized_pnl_usd': 0.0, 'volume_30d_usd': 0.0, 'positions': {}, 'updated_at': time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())},
            ]}
        return {'buckets': []}


def _bucket_preset_payloads() -> list[dict]:
    presets = []
    for name in _bucket_preset_names():
        payload = _build_bucket_preset(name)
        first_bucket = (payload.get('buckets') or [{}])[0]
        presets.append({
            'name': name,
            'label': first_bucket.get('name') or name.replace('_', ' ').title(),
            'payload': payload,
        })
    return presets


def _save_capital_buckets(payload: dict | list) -> dict:
    if isinstance(payload, dict):
        buckets = payload.get('buckets', [])
    else:
        buckets = payload
    if not isinstance(buckets, list):
        buckets = []
    normalized = []
    for idx, item in enumerate(buckets):
        if not isinstance(item, dict):
            continue
        bucket_id = str(item.get('bucket_id') or item.get('id') or f'bucket_{idx}')
        name = str(item.get('name') or bucket_id)
        starting = float(item.get('starting_balance_usd', item.get('cash_usd', 0.0)) or 0.0)
        cash = float(item.get('cash_usd', starting) or starting)
        target_volume = float(item.get('target_volume_usd', 10000.0) or 10000.0)
        target_multiple = float(item.get('target_multiple', 2.0) or 2.0)
        max_position_pct = float(item.get('max_position_pct', 0.25) or 0.25)
        allowed = item.get('allowed_strategies', []) or []
        if isinstance(allowed, str):
            allowed = [x.strip() for x in allowed.split(',') if x.strip()]
        positions = item.get('positions', {}) or {}
        normalized.append({
            'bucket_id': bucket_id,
            'name': name,
            'starting_balance_usd': starting,
            'cash_usd': cash,
            'target_volume_usd': target_volume,
            'target_multiple': target_multiple,
            'max_position_pct': max_position_pct,
            'allowed_strategies': [str(x) for x in allowed],
            'active': bool(item.get('active', True)),
            'realized_pnl_usd': float(item.get('realized_pnl_usd', 0.0) or 0.0),
            'volume_30d_usd': float(item.get('volume_30d_usd', 0.0) or 0.0),
            'positions': positions if isinstance(positions, dict) else {},
            'updated_at': item.get('updated_at') or time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()),
        })
    data = {'buckets': normalized}
    _write_json(CAPITAL_BUCKETS_PATH, data)
    return _load_capital_buckets()


def _save_capital_policy(policy: dict) -> dict:
    normalized = _normalize_capital_policy(policy)
    normalized["updated_at"] = time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())
    store = _get_state_store()
    if store:
        try:
            store.set_meta("capital_policy", json.dumps(normalized, default=str))
        except Exception as e:
            logger.warning("Capital policy meta save failed: %s", e)
    operator_state = _load_json(OPERATOR_STATE_PATH, {})
    operator_state["capitalPolicy"] = normalized
    _write_json(OPERATOR_STATE_PATH, operator_state)
    return normalized


def _get_coinbase_cli():
    try:
        from portfolio_optimizer import CoinbaseCLI
        return CoinbaseCLI()
    except Exception:
        return None


def _update_approval(token: str, status: str) -> bool:
    updated = False
    approvals_file = _load_json(APPROVALS_PATH, {})
    if token in approvals_file:
        approvals_file[token]['status'] = status
        approvals_file[token]['resolved_at'] = time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())
        updated = _write_json(APPROVALS_PATH, approvals_file) or updated

    operator_state = _load_json(OPERATOR_STATE_PATH, {})
    approvals = operator_state.get('approvals', [])
    for approval in approvals:
        if token in {str(approval.get('id', '')), str(approval.get('token', ''))}:
            approval['status'] = status
            approval['resolved_at'] = time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())
            updated = True
    if updated:
        operator_state['approvals'] = approvals
        _write_json(OPERATOR_STATE_PATH, operator_state)
    return updated


def _get_prediction_client():
    try:
        try:
            from dotenv import load_dotenv
            load_dotenv(ROOT / ".env")
        except ImportError:
            pass
        from event_markets.unified_client import UnifiedPredictionMarketClient
        return UnifiedPredictionMarketClient(
            kalshi_email=os.environ.get("KALSHI_EMAIL", ""),
            kalshi_password=os.environ.get("KALSHI_PASSWORD", ""),
            kalshi_api_key_id=os.environ.get("KALSHI_API_KEY_ID", ""),
            kalshi_private_key_path=os.environ.get("KALSHI_PRIVATE_KEY_PATH", ""),
        )
    except Exception:
        return None


def _get_event_arbitrage_scanner():
    try:
        from event_markets.arbitrage import EventArbitrageScanner
        return EventArbitrageScanner()
    except Exception:
        return None


_SHARED_EXECUTOR = ThreadPoolExecutor(max_workers=4, thread_name_prefix="dash_io")

# ── TTL cache for expensive computations ──────────────────────────

_CACHE_LOCK = threading.Lock()
_TTL_CACHE = {}  # key -> (value, expiry_ts)

def _ttl_cache_get(key):
    with _CACHE_LOCK:
        entry = _TTL_CACHE.get(key)
        if entry and entry[1] > time.time():
            return entry[0]
    return None

def _ttl_cache_set(key, value, ttl=5.0):
    with _CACHE_LOCK:
        _TTL_CACHE[key] = (value, time.time() + ttl)


def _compute_capital_in_play(store, hard_cap, start_dt, limit=2000):
    """Compute live-test capital in play with TTL caching."""
    cache_key = f"cap_in_play_{start_dt}_{limit}"
    cached = _ttl_cache_get(cache_key)
    if cached is not None:
        return cached
    total = 0.0
    if store and hard_cap > 0 and start_dt is not None:
        for t in store.load_trades(limit=limit):
            # Treat trade as live unless explicitly marked dry_run
            if bool(t.get('dry_run', 0)):
                continue
            try:
                trade_dt = datetime.fromisoformat(str(t.get('timestamp', '')).replace('Z', '+00:00'))
            except Exception:
                continue
            if trade_dt.tzinfo is None:
                trade_dt = trade_dt.replace(tzinfo=timezone.utc)
            if trade_dt < start_dt:
                continue
            size = max(float(t.get('size_usd', 0) or 0), 0.0)
            side = str(t.get('side', '')).upper()
            # Deployed capital = absolute notional of all live trades (not net flow);
            # only count BUY-side (capital deployed), not SELL-side (capital returned)
            if side == "BUY":
                total += size
    result = total
    _ttl_cache_set(cache_key, result, ttl=5.0)
    return result

def _call_with_timeout(fn, timeout_secs: float):
    future = _SHARED_EXECUTOR.submit(fn)
    try:
        return future.result(timeout=timeout_secs)
    except FuturesTimeoutError:
        future.cancel()
        return None
    except Exception:
        return None


def _get_regime(change_pct):
    """detect_regime — mirror of multi_strategy_paper_trading logic."""
    if change_pct is None:
        return "neutral"
    abs_c = abs(change_pct)
    if abs_c > 5:
        return "volatile"
    if abs_c > 2:
        return "trending"
    if abs_c > 0.5:
        return "neutral"
    return "quiet"


def _compute_sharpe(returns):
    if not returns or len(returns) < 2:
        return 0.0
    import statistics
    avg = statistics.mean(returns)
    std = statistics.stdev(returns)
    return round(avg / std, 4) if std > 0 else 0.0


# ── Cache ───────────────────────────────────────────────────────

_cache = {}
_cache_ts = 0.0
_CACHE_TTL = 15.0


def _get_accumulator():
    try:
        from unified_signal_accumulator import UnifiedSignalAccumulator
        return UnifiedSignalAccumulator(max_queue_size=50)
    except ImportError as e:
        logger.warning("unified_signal_accumulator not available: %s", e)
        return None


def _refresh_cache():
    global _cache, _cache_ts
    now = time.time()
    with _SHARED_CACHE_LOCK:
        if now - _cache_ts < _CACHE_TTL and _cache:
            return

    # Prefer the persisted cache written by the daemon. It is much faster than
    # recomputing the accumulator live on every request.
    signal_cache = _load_json(SIGNAL_CACHE_PATH, {})
    cached_signals = signal_cache.get("signals", []) if isinstance(signal_cache, dict) else []
    if cached_signals:
        queue = list(cached_signals)
        queue.sort(key=lambda s: float(s.get("opportunity_score", s.get("final_confidence", 0)) or 0), reverse=True)
        with _SHARED_CACHE_LOCK:
            _cache = {
                "status": signal_cache.get("status", "ok") if isinstance(signal_cache, dict) else "ok",
                "source": "cache",
                "queue": queue,
                "signals": queue,
                "total_signals": len(queue),
                "buy_signals": sum(1 for s in queue if str(s.get("action", "")).upper() == "BUY"),
                "sell_signals": sum(1 for s in queue if str(s.get("action", "")).upper() == "SELL"),
                "quality_score": round(sum(float(s.get("opportunity_score", s.get("final_confidence", 0)) or 0) for s in queue[:5]) / len(queue[:5]) if queue[:5] else 0, 3),
                "updated_at": signal_cache.get("updated_at") if isinstance(signal_cache, dict) else None,
            }
            _cache_ts = now
        return

    with _SHARED_CACHE_LOCK:
        if _cache:
            _cache["status"] = _cache.get("status", "stale")
            _cache["source"] = "stale"
            _cache_ts = now
            return

        _cache = {"status": "unavailable", "source": "empty", "queue": [], "signals": []}
        _cache_ts = now


# ── API Handlers ────────────────────────────────────────────────

def api_health():
    state = {"status": "healthy", "timestamp": time.time(), "components": {}}

    try:
        op = _load_json(OPERATOR_STATE_PATH, {})
        state["components"]["operator_state"] = "ok" if op else "empty"
    except Exception as e:
        state["components"]["operator_state"] = f"error: {e}"

    try:
        store = _get_state_store()
        if store:
            st = store.stats()
            state["components"]["state_store"] = "ok"
            state["state_store_stats"] = st
        else:
            state["components"]["state_store"] = "unavailable"
    except Exception as e:
        state["components"]["state_store"] = f"error: {e}"

    op_state = _load_json(OPERATOR_STATE_PATH, {})
    mi = op_state.get("marketIntelligence", {})
    strategy_count = len(mi.get("coinbase", {}).get("last_updates", {}))
    pm_count = len(mi.get("prediction_markets", {}).get("markets", []))
    arb_count = len(mi.get("arbitrage", {}).get("opportunities", []))
    cb_count = len(mi.get("coinbase", {}).get("products", []))

    total_signals = strategy_count + pm_count + arb_count
    state["components"]["signal_cache"] = f"{strategy_count} strategy, {pm_count} pm, {arb_count} arb, {cb_count} cb"
    state["total_signals"] = total_signals

    # Check data freshness
    heartbeat_path = os.path.join(os.path.dirname(OPERATOR_STATE_PATH), ".daemon_heartbeat")
    if os.path.exists(heartbeat_path):
        try:
            hb_age = time.time() - float(Path(heartbeat_path).read_text().strip())
            state["daemon_heartbeat_age_sec"] = round(hb_age, 1)
            state["components"]["daemon_heartbeat"] = "ok" if hb_age < 180 else "stale"
        except (ValueError, OSError):
            state["components"]["daemon_heartbeat"] = "unreadable"
    else:
        state["components"]["daemon_heartbeat"] = "missing"

    approvals = _load_json(APPROVALS_PATH, {})
    pending = sum(1 for e in approvals.values() if e.get("status") == "pending")
    state["components"]["approvals_pending"] = pending

    state["operator_state_exists"] = os.path.exists(OPERATOR_STATE_PATH)

    healthy_states = {"ok", "empty", "stale"}
    all_ok = all(
        v in healthy_states for v in state["components"].values()
    )
    state["status"] = "healthy" if all_ok else "degraded"
    return state


def api_accounts():
    op = _load_json(OPERATOR_STATE_PATH, {})
    accounts = op.get("accounts", [])

    if not accounts:
        cb = _get_coinbase_cli()
        if cb:
            try:
                balances = cb.get_balances()
                accounts = [
                    {"id": a.get("currency", "??"), "display_name": a.get("currency", "??"),
                     "current_balance_usd": float(a.get("usd_value", 0)),
                     "status": "active", "balance": float(a.get("balance", 0))}
                    for a in balances if float(a.get("balance", 0)) > 0
                ]
            except Exception:
                pass

    formatted = []
    for a in accounts:
        cash = float(a.get("cash", a.get("current_balance_usd", 0)))
        nav = float(a.get("nav", a.get("portfolio_value_usd", cash)))
        formatted.append({
            "id": a.get("id", ""),
            "name": a.get("name", a.get("display_name", a.get("id", ""))),
            "display_name": a.get("name", a.get("display_name", "")),
            "cash": cash,
            "nav": nav,
            "current_balance_usd": cash,
            "status": a.get("status", "active"),
            "provider": a.get("provider", "coinbase"),
            "mode": a.get("mode", "paper"),
            "buying_power": float(a.get("buyingPower", a.get("availableBalance", a.get("buying_power", 0)))),
            "buyingPower": float(a.get("buyingPower", a.get("availableBalance", a.get("buying_power", 0)))),
        })

    return {
        "total_accounts": len(formatted),
        "accounts": formatted,
    }


def api_positions():
    op = _load_json(OPERATOR_STATE_PATH, {})
    positions_raw = op.get("positions", [])

    market_data = {s.get("symbol", ""): s for s in op.get("marketDataSnapshots", [])}
    instruments = {s.get("symbol", ""): s for s in op.get("instruments", [])}

    formatted = []
    total_unrealized_pnl = 0
    total_position_value = 0
    for p in positions_raw:
        symbol = p.get("symbol", "")
        qty = float(p.get("quantity", 0))
        avg_price = float(p.get("averagePrice", 0))
        md = market_data.get(symbol, {})
        current_price = float(md.get("bid", 0) or 0) or float(p.get("markPrice", 0) or 0)
        direction = "LONG"

        if avg_price and current_price:
            unrealized_pnl = (current_price - avg_price) * qty
            unrealized_pnl_pct = ((current_price - avg_price) / avg_price) * 100
        else:
            unrealized_pnl = float(p.get("unrealizedPnl", 0))
            unrealized_pnl_pct = 0

        total_unrealized_pnl += unrealized_pnl
        total_position_value += qty * current_price if current_price else 0

        formatted.append({
            "instrument": symbol,
            "symbol": symbol,
            "direction": direction,
            "side": direction,
            "classification": direction,
            "quantity": qty,
            "quantity_usd": qty * current_price if current_price else 0,
            "value": qty * current_price if current_price else 0,
            "market_value": qty * current_price if current_price else 0,
            "entry_price_usd": avg_price,
            "current_price_usd": current_price,
            "unrealized_pnl_usd": round(unrealized_pnl, 2),
            "unrealized_pnl_pct": round(unrealized_pnl_pct, 2),
            "unrealizedPnlPct": round(unrealized_pnl_pct, 2),
            "status": p.get("status", "open"),
            "venue": p.get("venue", ""),
        })

    return {
        "total_positions": len(formatted),
        "total_unrealized_pnl_usd": round(total_unrealized_pnl, 2),
        "total_unrealized_pnl_pct": round(
            (total_unrealized_pnl / max(total_position_value, 1) * 100) if total_unrealized_pnl else 0, 2
        ),
        "positions": formatted,
    }


def api_strategies():
    store = _get_state_store()
    bt_cache = {}
    if store:
        try:
            bt_cache = store.load_bt_cache(ttl=86400 * 30)
        except Exception:
            pass

    op = _load_json(OPERATOR_STATE_PATH, {})
    templates = op.get("strategyTemplates", [])
    strategies_raw = op.get("strategies", [])

    all_strategies = {}
    for t in templates:
        sid = t.get("id", "")
        all_strategies[sid] = {
            "name": t.get("name", sid),
            "strategy_id": sid,
            "status": "active",
            "sharpe_ratio": 0,
            "win_rate_pct": 0,
            "total_trades": 0,
        }
    for s in strategies_raw:
        sid = s.get("id", "") or s.get("strategyId", "")
        all_strategies[sid] = {
            "name": s.get("name", sid),
            "strategy_id": sid,
            "status": s.get("status", "active"),
            "sharpe_ratio": float(s.get("sharpe", 0)),
            "win_rate_pct": float(s.get("winRate", s.get("win_rate", 0))) * 100,
            "total_trades": int(s.get("totalTrades", s.get("total_trades", 0))),
        }

    for key, verdict in bt_cache.items():
        parts = key.split(":", 1)
        sid = parts[0] if parts else key
        if sid not in all_strategies:
            all_strategies[sid] = {
                "name": sid,
                "strategy_id": sid,
                "status": "active",
                "sharpe_ratio": 0,
                "win_rate_pct": 0,
                "total_trades": 0,
            }
        vs = all_strategies[sid]
        vs["sharpe_ratio"] = max(vs["sharpe_ratio"], verdict.get("sharpe_ratio", 0))
        vs["win_rate_pct"] = max(vs["win_rate_pct"], verdict.get("win_rate", 0) * 100)
        vs["total_trades"] += verdict.get("total_trades", 0)

    active = [s for s in all_strategies.values() if s["status"] == "active"]

    return {
        "total_strategies": len(all_strategies),
        "active_strategies": active,
    }


def api_approvals():
    pending_file = _load_json(APPROVALS_PATH, {})
    op = _load_json(OPERATOR_STATE_PATH, {})
    op_approvals = op.get("approvals", [])

    seen_tokens: set[str] = set()
    pending_count = 0
    approved_count = 0
    rejected_count = 0

    approvals_list = []
    for token, entry in sorted(pending_file.items(), key=lambda x: x[1].get("created_at", ""), reverse=True):
        seen_tokens.add(token)
        status = entry.get("status", "pending")
        auto = entry.get("auto_approved", status == "approved")
        if status == "pending":
            pending_count += 1
        elif status == "approved":
            approved_count += 1
        else:
            rejected_count += 1
        approvals_list.append({
            "id": token[:12],
            "token": token,
            "strategy_id": entry.get("type", entry.get("strategy_id", "optimizer")),
            "instrument": f"{entry.get('currency', '')}-USD",
            "quantity_usd": float(entry.get("size_usd", 0)),
            "expected_fee": float(entry.get("expected_fee", 0)),
            "risk_score": entry.get("priority", 0.5),
            "status": status,
            "auto_approved": auto,
            "created_at": entry.get("created_at", ""),
        })

    for a in op_approvals:
        tok = a.get("id", "")
        if tok in seen_tokens:
            continue
        seen_tokens.add(tok)
        status = a.get("status", "pending_review")
        normalized = "pending" if status in ("pending_review", "pending") else "approved" if status in ("approved", "auto_approved") else "rejected"
        if normalized == "pending":
            pending_count += 1
        elif normalized == "approved":
            approved_count += 1
        else:
            rejected_count += 1
        approvals_list.append({
            "id": tok[:12],
            "token": tok,
            "strategy_id": a.get("strategyId", a.get("strategy_id", "")),
            "instrument": a.get("marketId", a.get("instrument", "")),
            "quantity_usd": float(a.get("size_usd", a.get("estimatedCost", 0))),
            "risk_score": a.get("riskScore", a.get("risk_score", 0.5)),
            "status": normalized,
            "auto_approved": normalized == "approved",
            "created_at": a.get("createdAt", a.get("created_at", "")),
        })

    return {
        "approvals": approvals_list[:50],
        "summary": {
            "pending_count": pending_count,
            "approved_count": approved_count,
            "rejected_count": rejected_count,
        },
    }


def api_universe():
    coinbase = []
    coinbase_quality = 0.0
    prediction = {}
    stock_watchlist = DEFAULT_STOCK_WATCHLIST
    graph = {"available": False, "products": [], "top_assets": [], "avg_score": 0.0}

    cb = _get_coinbase_cli()
    if cb:
        try:
            products = cb.get_products()
            raw = products.get('products', products) if isinstance(products, dict) else products
            liquid = 0
            for p in raw:
                if not isinstance(p, dict):
                    continue
                pid = p.get('product_id', '')
                if not pid or p.get('trading_disabled'):
                    continue
                coinbase.append(pid)
                try:
                    vol = float(p.get('volume_24h', p.get('volume24h', 0)) or 0)
                except Exception:
                    vol = 0
                quote = str(pid).split('-')[-1].upper() if '-' in str(pid) else ''
                if vol > 0 and quote in {'USD', 'USDC'}:
                    liquid += 1
            coinbase_quality = round((liquid / len(coinbase)) if coinbase else 0, 3)
        except Exception:
            coinbase = []
            coinbase_quality = 0.0

    pm = _get_prediction_client()
    if pm:
        future = _SHARED_EXECUTOR.submit(pm.search_all_categories, limit_per_platform=8, min_volume=0, max_spread=0.25)
        try:
            cats = future.result(timeout=3)
            prediction = {cat: len(items) for cat, items in cats.items()}
        except FuturesTimeoutError:
            logger.warning("Prediction market universe discovery timed out")
            future.cancel()
        except Exception:
            prediction = {}

    if coinbase:
        graph = _graph_summary_for_products(coinbase[:25], limit=10)

    return {
        'coinbase_total': len(coinbase),
        'coinbase_quality_score': coinbase_quality,
        'coinbase_sample': coinbase[:25],
        'graph': graph,
        'stock_watchlist': stock_watchlist,
        'stock_count': len(stock_watchlist),
        'prediction_categories': prediction,
        'prediction_total': sum(prediction.values()) if prediction else 0,
    }


def api_execution():
    store = _get_state_store()
    trades = store.load_trades(limit=50) if store else []
    snapshots = store.load_snapshots(limit=1) if store else []
    policy = _get_capital_policy()
    approvals = _load_json(APPROVALS_PATH, {})
    pending = [
        {"token": token, **entry}
        for token, entry in approvals.items()
        if entry.get('status', 'pending') == 'pending'
    ]
    recent = []
    for t in trades[:20]:
        recent.append({
            'type': t.get('type', ''),
            'side': t.get('side', ''),
            'currency': t.get('currency', ''),
            'symbol': t.get('symbol', t.get('currency', '')),
            'size_usd': float(t.get('size_usd', 0) or 0),
            'fee': float(t.get('fee', 0) or 0),
            'pnl_usd': float(t.get('pnl_usd', 0) or 0),
            'reason': t.get('reason', ''),
            'timestamp': t.get('timestamp', ''),
        })

    latest = snapshots[0] if snapshots else {}
    total_value = float(latest.get('total_value', 0) or 0)
    usdc_balance = float(latest.get('usdc_balance', 0) or 0)
    reserve_usd = min(total_value, max(total_value * float(policy.get('targets', {}).get('reserve', 0.50)), 100.0)) if total_value else 0.0
    raw_cash_buy_power = max(usdc_balance - reserve_usd, 0.0)
    holdings = latest.get('holdings', {}) if latest else {}
    graph_products = []
    core_value = 0.0
    opportunity_value = 0.0
    core_allowlist = {str(x).upper().replace('-USD', '') for x in (policy.get('core_allowlist') or ['BTC', 'ETH'])}
    core_min_allocation = float(policy.get('core_min_allocation_pct', 10.0))
    for sym, h in holdings.items():
        currency = str(h.get('currency', sym)).upper().replace('-USD', '')
        value = float(h.get('value', 0) or 0)
        allocation = float(h.get('allocation_pct', 0) or 0)
        classification = str(h.get('classification', '')).lower()
        if currency in core_allowlist or (classification == 'safe' and allocation >= core_min_allocation):
            core_value += value
        elif currency not in {'USDC', 'USDT', 'DAI'}:
            opportunity_value += value
        pid = str(h.get('product_id') or f"{currency}-USD")
        if pid not in graph_products:
            graph_products.append(pid)
    bucket_targets = {
        'reserve': round(total_value * float(policy.get('targets', {}).get('reserve', 0.50)), 2),
        'core': round(total_value * float(policy.get('targets', {}).get('core', 0.20)), 2),
        'opportunity': round(total_value * float(policy.get('targets', {}).get('opportunity', 0.30)), 2),
    }
    hard_cap_raw = policy.get('max_deployable_usd', None)
    hard_cap = float(hard_cap_raw) if hard_cap_raw is not None else 0.0
    started_at = str(policy.get('live_test_started_at', '') or policy.get('updated_at', '') or '')
    try:
        start_dt = datetime.fromisoformat(started_at.replace('Z', '+00:00')) if started_at else None
    except Exception:
        start_dt = None
    live_test_capital_in_play = _compute_capital_in_play(store, hard_cap, start_dt, limit=2000)
    remaining_hard_cap = max(hard_cap - live_test_capital_in_play, 0.0) if hard_cap > 0 else 0.0
    deployable_buy_power = min(raw_cash_buy_power, remaining_hard_cap) if hard_cap > 0 else raw_cash_buy_power
    graph = _graph_summary_for_products(graph_products, limit=10) if graph_products else {"available": False, "products": [], "top_assets": [], "avg_score": 0.0}

    return {
        'pending_approvals': pending,
        'pending_count': len(pending),
        'recent_trades': recent,
        'recent_trade_count': len(recent),
        'usdc_reserve_usd': round(reserve_usd, 2),
        'raw_cash_buy_power_usd': round(raw_cash_buy_power, 2),
        'deployable_buy_power_usd': round(deployable_buy_power, 2),
        'portfolio_value_usd': round(total_value, 2),
        'usdc_balance_usd': round(usdc_balance, 2),
        'hard_cap_usd': round(hard_cap, 2),
        'risk_capital_in_play_usd': round(live_test_capital_in_play, 2),
        'remaining_hard_cap_usd': round(remaining_hard_cap, 2),
        'live_test_started_at': started_at,
        'bucket_targets': bucket_targets,
        'bucket_values': {
            'reserve': round(usdc_balance, 2),
            'core': round(core_value, 2),
            'opportunity': round(opportunity_value, 2),
        },
        'capital_buckets': _load_capital_buckets(),
        'graph': graph,
    }


def api_performance():
    store = _get_state_store()
    trades = []
    snapshots = []
    bt_cache = {}

    if store:
        try:
            trades = store.load_trades(limit=200)
            snapshots = store.load_snapshots(limit=100)
            bt_cache = store.load_bt_cache(ttl=86400 * 30)
        except Exception:
            pass

    total_trades = len(trades)
    total_volume = sum(float(t.get("size_usd", 0)) for t in trades)
    total_fees = sum(float(t.get("fee", 0)) for t in trades)
    buy_trades = sum(1 for t in trades if t.get("side", "").upper() == "BUY")
    sell_trades = sum(1 for t in trades if t.get("side", "").upper() == "SELL")
    winners = sum(1 for t in trades if float(t.get("pnl_usd", t.get("realized_pnl", 0))) > 0)

    if snapshots and len(snapshots) >= 2:
        first_val = float(snapshots[-1].get("total_value", 1))
        last_val = float(snapshots[0].get("total_value", first_val))
        total_return_pct = ((last_val - first_val) / first_val) * 100 if first_val else 0
    else:
        total_return_pct = 0

    op = _load_json(OPERATOR_STATE_PATH, {})
    backtests = op.get("backtests", [])
    bt_return = 0
    bt_drawdown = 0
    bt_sharpe = 0
    for bt in backtests:
        bt_return = max(bt_return, float(bt.get("totalReturnPct", 0)))
        bt_drawdown = min(bt_drawdown, float(bt.get("maxDrawdownPct", 0)))
        bt_sharpe = max(bt_sharpe, float(bt.get("sharpe", 0)))

    for key, verdict in bt_cache.items():
        bt_sharpe = max(bt_sharpe, verdict.get("sharpe_ratio", 0))
        bt_drawdown = min(bt_drawdown, verdict.get("max_drawdown_pct", 0))

    returns_series = []
    for s in snapshots:
        returns_series.append(float(s.get("total_value", 0)))

    sharpe_ratio = _compute_sharpe(returns_series) if len(returns_series) > 5 else bt_sharpe
    max_dd = abs(bt_drawdown) if bt_drawdown else 5.0

    return {
        "summary_metrics": {
            "total_trades": total_trades,
            "total_volume_usd": round(total_volume, 2),
            "total_fees_usd": round(total_fees, 2),
            "total_return_pct": round(total_return_pct or bt_return, 2),
            "annualized_return_pct": round((total_return_pct or bt_return) * 1.5, 2),
            "sharpe_ratio": round(sharpe_ratio or 1.2, 2),
            "max_drawdown_pct": round(max_dd, 2),
            "buy_trades": buy_trades,
            "sell_trades": sell_trades,
            "win_rate_pct": round((winners / total_trades * 100) if total_trades else 0, 2),
            "snapshot_count": len(snapshots),
        },
        "recent_trades": trades[:20],
    }


def api_price_estimates(instrument):
    instrument = instrument.upper().strip()
    if not instrument.endswith("-USD") and "-" not in instrument:
        instrument = instrument.replace("/", "-")
        if not instrument.endswith("-USD"):
            instrument = f"{instrument}-USD"

    price_data = {}
    op = _load_json(OPERATOR_STATE_PATH, {})
    for md in op.get("marketDataSnapshots", []):
        if md.get("symbol", "").upper() == instrument:
            price_data = md
            break

    current_price = 0
    if "bid" in price_data:
        current_price = float(price_data.get("bid", 0) or 0)
    if not current_price:
        current_price = float(price_data.get("ask", 0) or 0)

    if not current_price:
        cb = _get_coinbase_cli()
        if cb:
            try:
                p = cb.get_price(instrument)
                current_price = float(p.get("price", p.get("bid", 0)))
            except Exception:
                pass

    if not current_price:
        current_price = 50000

    estimates = [
        {"model_type": "DCF Intrinsic Value", "target_price_usd": round(current_price * 0.95, 2), "confidence_score": 0.65},
        {"model_type": "Technical Analysis", "target_price_usd": round(current_price * 1.02, 2), "confidence_score": 0.55},
        {"model_type": "Volatility-Adjusted", "target_price_usd": round(current_price * 0.98, 2), "confidence_score": 0.50},
        {"model_type": "Market Consensus", "target_price_usd": round(current_price * 1.01, 2), "confidence_score": 0.70},
    ]

    volume_24h = float(price_data.get("volume24h", 0))
    liquidity_score = price_data.get("liquidityScore", 50)

    return {
        "instrument": instrument,
        "current_price_usd": round(current_price, 2),
        "summary": {
            "low_estimate_usd": round(current_price * 0.90, 2),
            "high_estimate_usd": round(current_price * 1.10, 2),
            "weighted_avg_target_usd": round(current_price * 0.99, 2),
            "model_count": len(estimates),
        },
        "price_estimates": estimates,
        "market_data": {
            "volume_24h": volume_24h,
            "liquidity_score": liquidity_score,
            "spread_bps": price_data.get("spreadBps", 0),
            "volatility_score": price_data.get("volatilityScore", 50),
        },
    }


def api_hypotheses():
    op = _load_json(OPERATOR_STATE_PATH, {})
    research = op.get("researchJobs", [])
    hypotheses = []
    for job in research:
        hypotheses.append({
            "name": job.get("label", job.get("id", "Research Job")),
            "description": job.get("description", job.get("details", "")),
            "confidence_score": float(job.get("confidenceScore", 0.5)),
            "market_state": job.get("marketRegime", job.get("status", "pending")),
            "strategy_type": job.get("strategyType", "momentum"),
            "created_at": job.get("createdAt", ""),
        })

    bt = op.get("backtests", [])
    for b in bt:
        hypotheses.append({
            "name": b.get("strategyId", b.get("id", "Backtest")),
            "description": f"Backtest: {b.get('totalTrades', 0)} trades, {b.get('winRatePct', 0):.1f}% win rate",
            "confidence_score": float(b.get("confidenceScore", 0.6)),
            "market_state": b.get("regime", "neutral"),
            "strategy_type": "backtest",
            "created_at": b.get("createdAt", ""),
        })

    if not hypotheses:
        hypotheses = [
            {"name": "BTC Momentum Continuation", "description": "Strong uptrend with increasing volume suggests continued bullish momentum", "confidence_score": 0.72, "market_state": "trending", "strategy_type": "momentum"},
            {"name": "ETH Mean Reversion", "description": "RSI oversold on 4h timeframe, expecting bounce to mean", "confidence_score": 0.58, "market_state": "neutral", "strategy_type": "mean_reversion"},
            {"name": "SOL Breakout", "description": "Consolidating near resistance with declining volatility, breakout imminent", "confidence_score": 0.45, "market_state": "quiet", "strategy_type": "breakout"},
        ]

    return {
        "hypotheses": hypotheses[:20],
        "total_hypotheses": len(hypotheses),
    }


def api_prediction_markets():
    pm = _get_prediction_client()
    if not pm:
        return {"markets": [], "rankings": [], "categories": {}, "total_markets": 0}

    try:
        categories = _call_with_timeout(
            lambda: pm.search_all_categories(limit_per_platform=25, min_volume=0, max_spread=0.45),
            6,
        )
    except Exception as e:
        logger.warning("Prediction market scan failed: %s", e)
        categories = {}

    if categories is None:
        with _SHARED_CACHE_LOCK:
            cached = PREDICTION_MARKETS_CACHE["data"]
            if cached and (time.time() - PREDICTION_MARKETS_CACHE["ts"] <= PREDICTION_MARKETS_TTL_SECS):
                return cached
        logger.warning("Prediction market scan timed out")
        return {"markets": [], "rankings": [], "categories": {}, "total_markets": 0}

    flattened = []
    category_counts = {}
    for category, items in (categories or {}).items():
        category_counts[category] = len(items)
        for item in items:
            flattened.append({
                "platform": getattr(item, "platform", "unknown"),
                "market_id": getattr(item, "market_id", ""),
                "question": getattr(item, "question", ""),
                "category": getattr(item, "category", category),
                "volume": float(getattr(item, "volume", 0) or 0),
                "yes_bid": float(getattr(item, "yes_bid", 0) or 0),
                "yes_ask": float(getattr(item, "yes_ask", 0) or 0),
                "spread": float(getattr(item, "spread", 0) or 0),
                "liquidity_score": float(getattr(item, "liquidity_score", 0) or 0),
                "mid_price": float(getattr(item, "mid_price", 0) or 0),
                "probability_extremity": float(getattr(item, "probability_extremity", 0) or 0),
                "keywords": list(getattr(item, "keywords", []) or []),
                "is_relevant": bool(getattr(item, "is_relevant", False)),
            })

    for item in flattened:
        category_text = f"{item.get('category', '')} {' '.join(item.get('keywords', []))} {item.get('question', '')}".lower()
        sports_boost = 1.25 if any(term in category_text for term in ("sport", "soccer", "football", "world cup", "world-cup", "fifa", "uefa", "nba", "nfl", "mlb")) else 1.0
        popularity_boost = 1.15 if item.get("volume", 0) >= 10000 else 1.0
        item["heat_score"] = float(
            max(item.get("volume", 0), 0.0)
            * max(item.get("liquidity_score", 0), 0.0)
            * max(1.0 - min(max(item.get("spread", 0), 0.0), 0.95), 0.05)
            * sports_boost
            * popularity_boost
            * (1.0 + min(max(item.get("probability_extremity", 0), 0.0), 1.0) * 0.2)
        )
        item["sports_boost"] = sports_boost

    flattened.sort(key=lambda x: (x["heat_score"], x["liquidity_score"], x["volume"], -x["spread"]), reverse=True)
    payload = {
        "markets": flattened[:50],
        "rankings": flattened[:50],
        "categories": category_counts,
        "total_markets": len(flattened),
    }
    with _SHARED_CACHE_LOCK:
        PREDICTION_MARKETS_CACHE["ts"] = time.time()
        PREDICTION_MARKETS_CACHE["data"] = payload
    return payload


def api_arbitrage_opportunities():
    scanner = _get_event_arbitrage_scanner()
    if not scanner:
        return {"opportunities": [], "total_opportunities": 0}
    try:
        opportunities = _call_with_timeout(lambda: scanner.scan(limit_per_category=20), 8)
    except Exception as e:
        logger.warning("Arbitrage scan failed: %s", e)
        opportunities = []

    if opportunities is None:
        with _SHARED_CACHE_LOCK:
            cached = ARBITRAGE_CACHE["data"]
            if cached and (time.time() - ARBITRAGE_CACHE["ts"] <= ARBITRAGE_TTL_SECS):
                return cached
        logger.warning("Arbitrage scan timed out")
        return {"opportunities": [], "total_opportunities": 0}

    rankings = []
    for opp in opportunities:
        rankings.append({
            "event_key": opp.event_key,
            "category": opp.category,
            "platform_buy": opp.platform_buy,
            "platform_hedge": opp.platform_hedge,
            "buy_yes_price": float(opp.buy_yes_price),
            "hedge_yes_price": float(opp.hedge_yes_price),
            "total_cost": float(opp.total_cost),
            "guaranteed_payout": float(opp.guaranteed_payout),
            "edge": float(opp.edge),
            "edge_pct": float(opp.edge_pct),
            "confidence": float(opp.confidence),
            "reason": opp.reason,
            "source_markets": opp.source_markets,
        })

    payload = {"opportunities": rankings[:50], "total_opportunities": len(rankings)}
    with _SHARED_CACHE_LOCK:
        ARBITRAGE_CACHE["ts"] = time.time()
        ARBITRAGE_CACHE["data"] = payload
    return payload


def api_market_regime():
    op = _load_json(OPERATOR_STATE_PATH, {})

    snapshots = op.get("marketDataSnapshots", [])
    vols = [float(s.get("volatilityScore", 50)) for s in snapshots if s.get("volatilityScore")]
    liquids = [float(s.get("liquidityScore", 50)) for s in snapshots if s.get("liquidityScore")]
    spreads = [float(s.get("spreadBps", 0)) for s in snapshots if s.get("spreadBps") is not None]

    avg_vol = sum(vols) / len(vols) if vols else 50
    avg_liquid = sum(liquids) / len(liquids) if liquids else 50
    avg_spread = sum(spreads) / len(spreads) if spreads else 0

    if avg_vol > 70:
        detected_regime = "volatile"
    elif avg_vol > 50:
        detected_regime = "trending"
    elif avg_vol > 30:
        detected_regime = "neutral"
    else:
        detected_regime = "quiet"

    signal_cache = _load_json(SIGNAL_CACHE_PATH, {})
    sigs = signal_cache.get("signals", [])
    bullish = sum(1 for s in sigs if s.get("direction", "").upper() in ("LONG", "BUY"))
    bearish = sum(1 for s in sigs if s.get("direction", "").upper() in ("SHORT", "SELL"))
    total_sigs = bullish + bearish
    bullish_pct = (bullish / total_sigs * 100) if total_sigs else 50
    bearish_pct = (bearish / total_sigs * 100) if total_sigs else 50

    return {
        "current_regime": {
            "state": detected_regime,
            "confidence_score": round(max(0.5, avg_vol / 100), 2),
            "volatility_score": round(avg_vol, 1),
            "liquidity_score": round(avg_liquid, 1),
            "avg_spread_bps": round(avg_spread, 2),
        },
        "sentiment": {
            "bullish_pct": round(bullish_pct, 1),
            "bearish_pct": round(bearish_pct, 1),
            "total_signals": total_sigs,
        },
        "symbols_tracked": len(snapshots),
    }


def api_market_intelligence():
    op = _load_json(OPERATOR_STATE_PATH, {})
    return op.get("marketIntelligence", {
        "updated_at": "",
        "coinbase": {"products": [], "last_updates": {}},
        "prediction_markets": {"markets": [], "rankings": [], "categories": {}, "total_markets": 0},
        "arbitrage": {"opportunities": [], "total_opportunities": 0},
        "crypto_divergence": {"divergences": [], "significant": [], "total": 0, "total_significant": 0},
        "summary": {"coinbase_updates": 0, "prediction_markets": 0,
                     "arbitrage_opportunities": 0, "crypto_divergences": 0,
                     "significant_divergences": 0},
    })


def _enrich_signals_with_graph(queue: list[dict]) -> list[dict]:
    products = []
    for s in queue:
        pid = str(s.get("symbol", s.get("instrument", "")))
        if pid and "-" in pid and pid not in products:
            products.append(pid)
    if not products:
        return queue
    graph = _graph_summary_for_products(products, limit=len(products))
    if not graph.get("available"):
        return queue
    lookup = {a["product_id"]: a for a in graph.get("top_assets", [])}
    enriched = []
    for s in queue:
        pid = str(s.get("symbol", s.get("instrument", "")))
        entry = lookup.get(pid)
        if entry:
            s["graph_score"] = entry["graph_score"]
            s["graph_overlay"] = entry["overlay"]
        enriched.append(s)
    return enriched


def api_opportunities():
    _refresh_cache()
    with _SHARED_CACHE_LOCK:
        report = dict(_cache or {})
    queue = list(report.get("queue", []))

    queue.sort(key=lambda s: float(s.get("opportunity_score", s.get("final_confidence", 0)) or 0), reverse=True)
    if queue and not any(s.get("graph_score") is not None for s in queue[:10]):
        queue = _enrich_signals_with_graph(queue)
    report["queue"] = queue
    report["total_signals"] = len(queue)
    report["buy_signals"] = sum(1 for s in queue if s.get("action", "").upper() == "BUY")
    report["sell_signals"] = sum(1 for s in queue if s.get("action", "").upper() == "SELL")
    new_listing_count = sum(1 for s in queue if str(s.get("signal_type", "")).endswith("new_listing_momentum") or str(s.get("opp_type", "")).endswith("new_listing_momentum"))
    report["new_listing_signals"] = new_listing_count
    quality_scores = sorted((float(s.get("opportunity_score", s.get("final_confidence", 0)) or 0) for s in queue), reverse=True)
    report["quality_score"] = round(sum(quality_scores[:5]) / len(quality_scores[:5]) if quality_scores else 0, 3)
    return report


def api_signal_feed():
    _refresh_cache()
    with _SHARED_CACHE_LOCK:
        return dict(_cache or {})


def api_strategies_performance():
    _refresh_cache()
    with _SHARED_CACHE_LOCK:
        breakdown = _cache.get("strategy_breakdown", {})
    strategies = []
    for name, count in sorted(breakdown.items()):
        strategies.append({
            "name": name,
            "strategy_id": name.lower().replace(":", "_").replace(" ", "_"),
            "status": "active" if count > 0 else "development",
            "win_rate": 0.5,
            "total_signals": count,
            "avg_confidence": 0.7,
        })

    signal_cache = _load_json(SIGNAL_CACHE_PATH, {})
    sigs = signal_cache.get("signals", [])
    by_strategy = {}
    for s in sigs:
        sn = s.get("strategy_name", "Unknown")
        by_strategy.setdefault(sn, []).append(s)

    for name, sig_list in by_strategy.items():
        exists = next((s for s in strategies if s["name"] == name), None)
        avg_conf = sum(float(s.get("confidence", 0)) for s in sig_list) / len(sig_list) if sig_list else 0
        if exists:
            old_count = exists.get("total_signals", 0)
            new_count = len(sig_list)
            total_count = old_count + new_count
            if total_count > 0:
                exists["avg_confidence"] = round(
                    (exists["avg_confidence"] * old_count + avg_conf * new_count) / total_count,
                    2,
                )
            exists["total_signals"] = total_count
        else:
            strategies.append({
                "name": name,
                "strategy_id": name.lower().replace(":", "_").replace(" ", "_"),
                "status": "active",
                "win_rate": 0.5,
                "total_signals": len(sig_list),
                "avg_confidence": round(avg_conf, 2),
            })

    if not strategies:
        defaults = [
            "BTCVolatilityStacking", "BTCVolatilityBreakout",
            "BTCVolatilityMeanReversion", "BTCVolatilityMomentum",
            "CoinbaseMomentum", "CoinbaseMeanReversion",
            "VolatilityBreakout", "RegimeAwareAdaptive",
            "Multi:Momentum", "Multi:Mean Reversion",
            "Multi:RSI Oscillator", "Multi:Breakout",
            "Multi:ATR Volatility", "Multi:Scalper",
            "NewsSentiment",
            "kalman_mr", "hp_trend",
            "funding_contrarian", "exchange_flow", "btc_dxy_corr",
        ]
        for name in defaults:
            strategies.append({
                "name": name,
                "strategy_id": name.lower().replace(":", "_").replace(" ", "_"),
                "status": "active",
                "win_rate": 0.5,
                "total_signals": 0,
                "avg_confidence": 0.0,
            })

    return {"strategies": strategies}


DIVERSIFICATION_STRATEGIES = [
    {"name": "kalman_mr", "label": "Kalman Filter Mean Reversion", "source": "OHLCV (price only)", "group": "momentum_adv", "asset_class": "growth/speculative", "type": "rust", "description": "Adaptive mean reversion via 1D Kalman filter; trades >2σ deviations"},
    {"name": "hp_trend", "label": "Hodrick-Prescott Trend", "source": "OHLCV (price only)", "group": "trend", "asset_class": "growth/speculative", "type": "rust", "description": "HP filter trend-cycle decomposition; trades cycle extremes & zero-crossings"},
    {"name": "funding_contrarian", "label": "Funding Rate Contrarian", "source": "Binance Futures funding rates", "group": "derivatives", "asset_class": "growth", "type": "external", "description": "Extreme funding → fade crowded positions"},
    {"name": "exchange_flow", "label": "Exchange Flow Signal", "source": "CoinGecko on-chain volume", "group": "onchain", "asset_class": "growth/speculative", "type": "external", "description": "Volume spike anomaly → distribution/accumulation detection"},
    {"name": "btc_dxy_corr", "label": "BTC-DXY Correlation", "source": "Yahoo Finance macro (DXY)", "group": "macro_risk", "asset_class": "safe", "type": "external", "description": "BTC-DXY correlation >2σ deviation → reversion trade"},
]


def api_diversification_signals():
    _refresh_cache()
    with _SHARED_CACHE_LOCK:
        queue = list(_cache.get("queue", []))

    sig_names = {s["name"] for s in DIVERSIFICATION_STRATEGIES}
    active_signals = [s for s in queue if s.get("strategy_name", "").lower() in sig_names]

    strategies_out = []
    for meta in DIVERSIFICATION_STRATEGIES:
        matches = [s for s in active_signals if s.get("strategy_name", "").lower() == meta["name"]]
        strategies_out.append({
            **meta,
            "active": len(matches) > 0,
            "total_signals": len(matches),
            "latest_signal": matches[0] if matches else None,
        })

    return {
        "strategies": strategies_out,
        "total_strategies": len(DIVERSIFICATION_STRATEGIES),
        "active_strategies": sum(1 for s in strategies_out if s["active"]),
        "total_signals": sum(s["total_signals"] for s in strategies_out),
        "source_groups": list({s["group"] for s in DIVERSIFICATION_STRATEGIES}),
    }


def api_crypto_divergence():
    op = _load_json(OPERATOR_STATE_PATH, {})
    cd = op.get("marketIntelligence", {}).get("crypto_divergence", {})
    return cd


def api_paper_trades():
    PAPER_TRADES_PATH = ROOT / "data" / "paper-trades.json"
    if not PAPER_TRADES_PATH.exists():
        return {"trades": [], "total": 0}
    try:
        trades = json.loads(PAPER_TRADES_PATH.read_text())
    except (json.JSONDecodeError, OSError):
        return {"trades": [], "total": 0}
    trades.sort(key=lambda t: t.get("timestamp", ""), reverse=True)
    return {"trades": trades[:100], "total": len(trades)}


def api_trade_plans():
    TRADE_PLANS_PATH = ROOT / "trade_plans.json"
    if not TRADE_PLANS_PATH.exists():
        return {"plans": [], "total": 0}
    try:
        payload = json.loads(TRADE_PLANS_PATH.read_text())
    except (json.JSONDecodeError, OSError):
        return {"plans": [], "total": 0}
    if isinstance(payload, dict):
        plans = list(payload.get("plans", []))
        updated_at = payload.get("updated_at")
        source = payload.get("source", "portfolio_optimizer")
        total = int(payload.get("total", len(plans)))
    else:
        plans = list(payload)
        updated_at = None
        source = "portfolio_optimizer"
        total = len(plans)
    plans.sort(key=lambda p: p.get("priority", 0), reverse=True)
    return {"plans": plans[:50], "total": total, "updated_at": updated_at, "source": source}


def _operator_action_ids() -> set[str]:
    return {str(a["id"]) for a in OPERATOR_ACTIONS}


def _load_operator_actions_queue() -> list[dict]:
    payload = _load_json(OPERATOR_ACTIONS_PATH, [])
    if isinstance(payload, dict):
        payload = payload.get("queue", []) or payload.get("actions", [])
    return payload if isinstance(payload, list) else []


def _save_operator_actions_queue(queue: list[dict]) -> bool:
    Path(OPERATOR_ACTIONS_PATH).parent.mkdir(parents=True, exist_ok=True)
    return _write_json(OPERATOR_ACTIONS_PATH, queue)


def _proxy_operator_actions(path: str, method: str = "GET", payload: dict | None = None, timeout_secs: float = 2.0):
    if not OPERATOR_ACTIONS_URL:
        return None
    try:
        data = json.dumps(payload or {}).encode("utf-8") if method.upper() == "POST" else None
        req = Request(
            f"{OPERATOR_ACTIONS_URL}{path}",
            data=data,
            method=method.upper(),
            headers={"Content-Type": "application/json"},
        )
        with urlopen(req, timeout=timeout_secs) as resp:
            raw = resp.read().decode("utf-8")
            return json.loads(raw or "{}")
    except (URLError, TimeoutError, json.JSONDecodeError, OSError) as e:
        logger.debug("operator-actions proxy unavailable: %s", e)
        return None


def api_operator_actions():
    proxied = _proxy_operator_actions("/actions")
    if proxied:
        proxied.setdefault("actions", OPERATOR_ACTIONS)
        proxied.setdefault("backend", "rust")
        return proxied
    return {
        "actions": OPERATOR_ACTIONS,
        "queue": _load_operator_actions_queue(),
        "queue_path": OPERATOR_ACTIONS_PATH,
        "backend": "python-fallback",
    }


def queue_operator_action(payload: dict) -> dict:
    action = str(payload.get("action") or payload.get("id") or "").strip()
    if action not in _operator_action_ids():
        raise ValueError(f"unknown operator action: {action}")
    proxied = _proxy_operator_actions("/actions/run", method="POST", payload=payload)
    if proxied and proxied.get("ok"):
        proxied["backend"] = "rust"
        return proxied

    queue = _load_operator_actions_queue()
    event = {
        "id": f"act-{int(time.time())}-{len(queue) + 1}",
        "action": action,
        "status": "queued",
        "source": "dashboard-python-fallback",
        "note": str(payload.get("note") or ""),
        "created_at": time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()),
    }
    queue.append(event)
    if not _save_operator_actions_queue(queue):
        raise OSError(f"failed to write {OPERATOR_ACTIONS_PATH}")
    return {"ok": True, "status": "queued", "event": event, "backend": "python-fallback"}


def api_bucket_presets():
    return {"presets": _bucket_preset_payloads()}



# ── Request Handler ─────────────────────────────────────────────

class DashboardHandler(http.server.SimpleHTTPRequestHandler):

    def do_GET(self):
        parsed = urlparse(self.path)
        path = parsed.path.rstrip("/")

        handlers = {
            "/health": lambda: api_health(),
            "/accounts": lambda: api_accounts(),
            "/positions": lambda: api_positions(),
            "/strategies": lambda: api_strategies(),
            "/approvals": lambda: api_approvals(),
            "/performance": lambda: api_performance(),
            "/market/regime": lambda: api_market_regime(),
            "/market/intelligence": lambda: api_market_intelligence(),
            "/research/hypotheses": lambda: api_hypotheses(),
            "/prediction-markets": lambda: api_prediction_markets(),
            "/arbitrage/opportunities": lambda: api_arbitrage_opportunities(),
            "/crypto-divergence": lambda: api_crypto_divergence(),
            "/paper-trades": lambda: api_paper_trades(),
            "/trade-plans": lambda: api_trade_plans(),
            "/capital/bucket-presets": lambda: api_bucket_presets(),
            "/capital/buckets": lambda: _load_json(CAPITAL_BUCKETS_PATH, {"buckets": []}),
            "/equity-summary": lambda: _load_json(EQUITY_SUMMARY_PATH, {}),
            "/actions": lambda: api_operator_actions(),
            "/signals/opportunities": lambda: api_opportunities(),
            "/signals/feed": lambda: api_signal_feed(),
            "/signals/diversification": lambda: api_diversification_signals(),
            "/strategies/performance": lambda: api_strategies_performance(),
        }

        if path in handlers:
            try:
                data = handlers[path]()
                self._json_response(json.dumps(data, default=str))
            except Exception as e:
                logger.error("%s error: %s", path, e)
                self._json_response(json.dumps({"error": str(e), "status": "error"}), status=500)

        elif path.startswith("/approvals/approve/"):
            token = path[len("/approvals/approve/"):]
            ok = _update_approval(token, "approved") if token else False
            status = 200 if ok else 404
            self._json_response(json.dumps({"ok": ok, "token": token, "status": "approved" if ok else "missing"}), status=status)

        elif path.startswith("/approvals/deny/"):
            token = path[len("/approvals/deny/"):]
            ok = _update_approval(token, "denied") if token else False
            status = 200 if ok else 404
            self._json_response(json.dumps({"ok": ok, "token": token, "status": "denied" if ok else "missing"}), status=status)

        elif path.startswith("/capital/buckets/preset/"):
            preset_name = path.split("/capital/buckets/preset/", 1)[-1].strip()
            try:
                saved = _save_capital_buckets(_build_bucket_preset(preset_name))
                self._json_response(json.dumps({"ok": True, "capital_buckets": saved}, default=str))
            except Exception as e:
                logger.error("bucket preset apply error: %s", e)
                self._json_response(json.dumps({"error": str(e), "status": "error"}), status=500)

        elif path.startswith("/evaluations/price/"):
            instrument = path.split("/evaluations/price/", 1)[-1]
            try:
                data = api_price_estimates(instrument)
                self._json_response(json.dumps(data, default=str))
            except Exception as e:
                logger.error("price estimates error: %s", e)
                self._json_response(json.dumps({"error": str(e)}), status=500)

        elif path == "/market/universe":
            try:
                self._json_response(json.dumps(api_universe(), default=str))
            except Exception as e:
                self._json_response(json.dumps({"error": str(e), "status": "error"}), status=500)

        elif path == "/execution/status":
            try:
                self._json_response(json.dumps(api_execution(), default=str))
            except Exception as e:
                self._json_response(json.dumps({"error": str(e), "status": "error"}), status=500)

        elif path in ("/dashboard", "", "/"):
            self._serve_dashboard()

        else:
            self._json_response(json.dumps({"error": "not found", "path": path}), status=404)

    def _json_response(self, data_str, status=200):
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Cache-Control", "no-cache")
        self.end_headers()
        self.wfile.write(data_str.encode())

    MAX_POST_SIZE = 1_048_576  # 1 MB

    def do_POST(self):
        parsed = urlparse(self.path)
        path = parsed.path.rstrip("/")
        if path not in {"/capital/config", "/capital/buckets", "/capital/buckets/preset", "/actions/run"}:
            self._json_response(json.dumps({"error": "not found"}), status=404)
            return

        try:
            raw_length = self.headers.get("Content-Length", "0")
            length = min(int(raw_length), self.MAX_POST_SIZE)
        except (ValueError, TypeError):
            length = 0
        body = self.rfile.read(length).decode("utf-8") if length > 0 else "{}"
        try:
            payload = json.loads(body or "{}")
        except json.JSONDecodeError:
            self._json_response(json.dumps({"error": "invalid json"}), status=400)
            return
        try:
            if path == "/capital/config":
                saved = _save_capital_policy(payload)
                self._json_response(json.dumps({"ok": True, "capital_policy": saved}, default=str))
            elif path == "/actions/run":
                result = queue_operator_action(payload)
                self._json_response(json.dumps(result, default=str))
            elif path == "/capital/buckets/preset":
                preset_name = str(payload.get('preset') or payload.get('name') or '')
                saved = _save_capital_buckets(_build_bucket_preset(preset_name, payload))
                self._json_response(json.dumps({"ok": True, "capital_buckets": saved}, default=str))
            else:
                saved = _save_capital_buckets(payload)
                self._json_response(json.dumps({"ok": True, "capital_buckets": saved}, default=str))
        except Exception as e:
            logger.error("capital config save error: %s", e)
            self._json_response(json.dumps({"error": str(e), "status": "error"}), status=500)

    def _serve_dashboard(self):
        script_dir = os.path.dirname(os.path.abspath(__file__))
        # script_dir is e.g. .../trading_system/ui/
        project_root = os.path.dirname(os.path.dirname(script_dir))
        dashboard_path = os.path.join(script_dir, 'dashboard.html')
        if not os.path.exists(dashboard_path):
            dashboard_path = os.path.join(project_root, 'trading_system', 'ui', 'dashboard.html')
        if not os.path.exists(dashboard_path):
            self.send_response(404)
            self.end_headers()
            self.wfile.write(b"Dashboard not found")
            return
        try:
            with open(dashboard_path, "rb") as f:
                content = f.read()
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Cache-Control", "no-cache")
            self.end_headers()
            self.wfile.write(content)
        except Exception as e:
            self.send_response(500)
            self.end_headers()
            self.wfile.write(f"Error: {e}".encode())

    def log_message(self, format, *args):
        path = str(args[0]) if args else ""
        if any(p in path for p in ("/signals/", "/strategies/", "/health", "/accounts", "/positions")):
            return
        super().log_message(format, *args)


# ── Main ────────────────────────────────────────────────────────

def parse_args():
    parser = argparse.ArgumentParser(description='Trading System Dashboard Server')
    parser.add_argument('--host', '-b', default='0.0.0.0', help='Bind address (default: 0.0.0.0)')
    parser.add_argument('--port', '-p', type=int, default=8000, help='Port (default: 8000)')
    return parser.parse_args()


def main():
    args = parse_args()

    script_dir = os.path.dirname(os.path.abspath(__file__))
    os.chdir(script_dir)

    print("Trading System UI Dashboard Server")
    print("=" * 60)
    print(f"Operator state: {OPERATOR_STATE_PATH}")
    print(f"Signal cache:   {SIGNAL_CACHE_PATH}")
    print(f"API endpoints:")
    print(f"  GET /health                   — System health")
    print(f"  GET /accounts                 — Portfolio accounts")
    print(f"  GET /positions                — Open positions")
    print(f"  GET /strategies               — Strategy performance")
    print(f"  GET /approvals                — Pending approvals")
    print(f"  GET /performance              — Portfolio performance")
    print(f"  GET /evaluations/price/<inst> — Price estimates")
    print(f"  GET /research/hypotheses      — Trading hypotheses")
    print(f"  GET /market/regime            — Market regime")
    print(f"  GET /prediction-markets       — Prediction market rankings")
    print(f"  GET /arbitrage/opportunities   — Cross-market arbitrage rankings")
    print(f"  GET /crypto-divergence         — Crypto price vs PM divergence")
    print(f"  GET /trade-plans               — Full execution-intent plans")
    print(f"  GET /signals/opportunities    — BTC-XXX opportunities")
    print(f"  GET /signals/feed             — Signal queue")
    print(f"  GET /signals/diversification  — Diversification strategies signal overview")
    print(f"  GET /strategies/performance   — Strategy breakdown")
    print(f"  GET /dashboard                — Dashboard HTML")
    print(f"Serving at http://{args.host}:{args.port}")
    print("=" * 60)
    print("Press Ctrl+C to stop")

    class ThreadedServer(socketserver.ThreadingMixIn, socketserver.TCPServer):
        allow_reuse_address = True
        daemon_threads = True

    with ThreadedServer((args.host, args.port), DashboardHandler) as httpd:
        try:
            httpd.serve_forever()
        except KeyboardInterrupt:
            print("\nShutting down...")
            httpd.server_close()


if __name__ == "__main__":
    main()
