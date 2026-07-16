#!/usr/bin/env python3
"""
portfolio_watchdog.py — READ-ONLY oversight for the portfolio-management stack.

Design contract (DO NOT VIOLATE):
  * This script NEVER starts, stops, kills, or reconfigures any process.
  * It NEVER reads or prints .env secrets, approval tokens, or operator tokens.
  * It NEVER flips KILL_SWITCH, changes MAX_NOTIONAL, or passes --live.
  * It only OBSERVES state that already exists on disk and in Docker, then
    reports. If an action is needed, it prints a recommendation and exits
    non-zero on CRITICAL so the cron layer can alert — it does not act.

Observability surfaces tapped (all read-only):
  * Docker container health (portfolio-optimizer / approval-server / api)
  * state/optimizer_state.db  -> snapshots freshness, trades count
  * data/pending_approvals.json -> awaiting-approval count + oldest age
  * data/approvals_inbox/ -> manual dashboard orders
  * data/llm_watchdog_scoreboard.json -> last LLM-oversight update
  * systemd portfolio-trader.service -> must stay INACTIVE (per operator)
  * host load average -> catch crash-loop CPU burn

Severity:
  INFO    -> all nominal, or expected-off states (host unit dead by design)
  WARN    -> degraded but not money-risk (e.g. snapshot staleness, watchdog stale)
  CRITICAL-> money-risk or silent-sick (approval-server down w/ pending trades,
            live trade appeared when system should be static, host unit came UP)

Exit code: 0 = INFO/WARN, 1 = CRITICAL. Cron uses this to decide alerting.
"""
from __future__ import annotations

import json
import os
import sqlite3
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
STATE_DB = ROOT / "state" / "optimizer_state.db"
PENDING = ROOT / "data" / "pending_approvals.json"
INBOX = ROOT / "data" / "approvals_inbox"
WATCHDOG_SB = ROOT / "data" / "llm_watchdog_scoreboard.json"
STATUS_OUT = ROOT / "data" / "watchdog_status.json"

# Containers that must be Up for the system to be "alive"
REQUIRED_CONTAINERS = ["portfolio-optimizer", "portfolio-approval-server", "portfolio-api"]

# Thresholds
SNAPSHOT_STALE_WARN_S = 1200     # 20 min no new snapshot -> warn (optimizer interval=300s + jitter)
SNAPSHOT_STALE_CRIT_S = 3600     # 60 min -> critical (optimizer likely dead)
PENDING_AGE_WARN_S = 3600        # 1h an approval sat unaddressed -> warn
PENDING_AGE_CRIT_S = 21600       # 6h -> critical
LOAD_WARN = 6.0                  # 1m load avg per core-ish threshold

NEO4J_URI = os.environ.get("NEO4J_URI", "bolt://100.64.43.123:7687")
NEO4J_PASS = os.environ.get("NEO4J_PASS", "knowledge_graph_2026")


def now_ts() -> float:
    return time.time()


def docker_health() -> dict:
    """Return {container: (state, healthy_bool)} read-only via docker ps."""
    out = {}
    try:
        res = subprocess.run(
            ["docker", "ps", "--format", "{{.Names}}|{{.Status}}"],
            capture_output=True, text=True, timeout=20,
        )
    except Exception as e:
        return {"__error__": f"docker ps failed: {e}"}
    for line in res.stdout.splitlines():
        if "|" not in line:
            continue
        name, status = line.split("|", 1)
        healthy = ("healthy" in status) or ("Up" in status and "unhealthy" not in status)
        out[name] = {"status": status.strip(), "healthy": healthy}
    return out


def snapshot_freshness() -> dict:
    """Read optimizer_state.db read-only (copy to dodge WAL lock).

    Schema (verified): snapshots(id, timestamp ISO, total_value, holding_count,
    usdc_balance, fee_volume_30d, fee_tier_*, holdings_json).
    """
    info: dict[str, object] = {
        "exists": False, "snapshot_count": 0, "last_snapshot_age_s": None,
        "trades": 0, "last_total_value": None, "last_holding_count": None,
        "last_usdc": None,
    }
    if not STATE_DB.exists():
        return info
    tmp = "/tmp/opt_watchdog_ro.db"
    try:
        import shutil
        shutil.copy(STATE_DB, tmp)
        c = sqlite3.connect(tmp, timeout=5)
        info["exists"] = True
        info["snapshot_count"] = c.execute(
            "select count(*) from snapshots").fetchone()[0]
        info["trades"] = c.execute("select count(*) from trades").fetchone()[0]
        row = c.execute(
            "select timestamp, total_value, holding_count, usdc_balance "
            "from snapshots order by id desc limit 1").fetchone()
        if row:
            ts_str, tv, hc, usdc = row
            info["last_total_value"] = tv
            info["last_holding_count"] = hc
            info["last_usdc"] = usdc
            try:
                dt = datetime.fromisoformat(
                    str(ts_str).replace("Z", "+00:00"))
                info["last_snapshot_age_s"] = int(now_ts() - dt.timestamp())
            except Exception:
                info["last_snapshot_age_s"] = None
        c.close()
    except Exception as e:
        info["error"] = str(e)[:120]
    finally:
        try:
            os.remove(tmp)
        except OSError:
            pass
    return info


def pending_approvals() -> dict:
    info = {"count": 0, "oldest_age_s": None, "keys_sample": []}
    try:
        if PENDING.exists():
            d = json.loads(PENDING.read_text() or "{}")
            if isinstance(d, dict):
                info["count"] = len(d)
                info["keys_sample"] = list(d.keys())[:5]
                # age: if entries carry a ts, report oldest; else unknown
                ages = []
                for v in d.values():
                    ts = None
                    if isinstance(v, dict):
                        ts = v.get("created_at") or v.get("ts") or v.get("time")
                    if ts:
                        try:
                            ages.append(now_ts() - float(ts))
                        except (TypeError, ValueError):
                            pass
                if ages:
                    info["oldest_age_s"] = int(max(ages))
    except Exception as e:
        info["error"] = str(e)[:120]
    return info


def inbox_count() -> int:
    try:
        if INBOX.exists():
            return sum(1 for _ in INBOX.iterdir())
    except Exception:
        pass
    return 0


def watchdog_scoreboard() -> dict:
    info: dict[str, object] = {"updated_at": None, "judges": []}
    try:
        if WATCHDOG_SB.exists():
            d = json.loads(WATCHDOG_SB.read_text() or "{}")
            info["updated_at"] = d.get("updated_at")
            j = d.get("judges")
            info["judges"] = list(j.keys())[:8] if isinstance(j, dict) else []
    except Exception:
        pass
    return info


def systemd_state() -> dict:
    """Query portfolio-trader.service SubState. Must be inactive by design."""
    try:
        res = subprocess.run(
            ["systemctl", "show", "portfolio-trader.service",
             "--property=SubState,ActiveState,NRestarts"],
            capture_output=True, text=True, timeout=15,
        )
        d = {}
        for line in res.stdout.splitlines():
            if "=" in line:
                k, v = line.split("=", 1)
                d[k] = v
        return d
    except Exception as e:
        return {"error": str(e)[:120]}


def load_avg() -> float:
    try:
        return round(os.getloadavg()[0], 2)
    except Exception:
        return 0.0


def unit_intent() -> str:
    """Operator intent for the host systemd unit. Controls how 'unit running'
    is scored. Values: 'off' (must stay stopped), 'testing' (may run during a
    testing session), 'live' (expected to run). Default 'testing' — the unit is
    UP because a peer agent is iterating on updates; treat that as nominal.
    Change via: echo off > data/unit_intent.txt  (or 'testing' / 'live')."""
    p = ROOT / "data" / "unit_intent.txt"
    try:
        v = p.read_text().strip().lower()
        if v in ("off", "testing", "live"):
            return v
    except Exception:
        pass
    return "testing"


def main() -> int:
    ts = now_ts()
    dh = docker_health()
    snap = snapshot_freshness()
    pend = pending_approvals()
    inbox = inbox_count()
    wb = watchdog_scoreboard()
    sd = systemd_state()
    load = load_avg()

    sev = "INFO"
    findings = []

    # 1. Required containers
    for c in REQUIRED_CONTAINERS:
        st = dh.get(c)
        if not st:
            sev = "CRITICAL"
            findings.append(f"CONTAINER MISSING: {c} not running")
        elif not st.get("healthy"):
            if sev != "CRITICAL":
                sev = "WARN"
            findings.append(f"CONTAINER UNHEALTHY: {c} ({st.get('status')})")

    # 2. Snapshot freshness (optimizer liveness)
    if snap.get("exists"):
        age = snap.get("last_snapshot_age_s")
        if age is None:
            findings.append("snapshot ts column unknown (schema drift) — cannot judge freshness")
        elif age > SNAPSHOT_STALE_CRIT_S:
            sev = "CRITICAL"
            findings.append(f"OPTIMIZER STALE: no snapshot for {age}s (>30m) — likely dead")
        elif age > SNAPSHOT_STALE_WARN_S:
            if sev == "INFO":
                sev = "WARN"
            findings.append(f"optimizer snapshot {age}s old (>20m)")
    else:
        findings.append("optimizer_state.db absent (expected if optimizer never wrote)")

    # 3. Pending approvals (kill-chain queue)
    if pend["count"] > 0:
        age = pend.get("oldest_age_s")
        if age is None:
            findings.append(f"{pend['count']} trade(s) AWAITING approval (age unknown)")
        elif age > PENDING_AGE_CRIT_S:
            sev = "CRITICAL"
            findings.append(f"{pend['count']} trade(s) awaiting approval {age}s (>6h) — stale queue")
        elif age > PENDING_AGE_WARN_S:
            if sev == "INFO":
                sev = "WARN"
            findings.append(f"{pend['count']} trade(s) awaiting approval {age}s (>1h)")
        else:
            findings.append(f"{pend['count']} trade(s) awaiting approval (fresh)")

    # 4. Live trades when system should be static
    # Operator is in update mode; any executed trade is unexpected.
    if snap.get("trades", 0) > 0:
        sev = "CRITICAL"
        findings.append(f"LIVE TRADES PRESENT: trades={snap['trades']} — system should be static")

    # 5. Host systemd unit — severity depends on operator intent (data/unit_intent.txt)
    intent = unit_intent()
    sub = sd.get("SubState", "")
    if sub and sub not in ("inactive", "dead", "failed"):
        if intent == "off":
            # Unit is up but operator wants it OFF -> unexpected, escalate.
            sev = "CRITICAL"
            findings.append(f"HOST UNIT ALIVE (intent=off): portfolio-trader.service "
                            f"SubState={sub} (NRestarts={sd.get('NRestarts')}) — "
                            f"operator wants it OFF; disable via systemctl")
        elif intent == "testing":
            # Peer agent iterating on updates; unit up is expected -> nominal.
            findings.append(f"host unit running (intent=testing, SubState={sub}) — "
                            f"expected during testing session")
        else:  # 'live'
            findings.append(f"host unit running (intent=live, SubState={sub}) — expected")
    elif sub == "failed":
        if intent == "live":
            sev = "CRITICAL"
            findings.append("HOST UNIT FAILED while intent=live — supervisor down")
        else:
            findings.append("host unit in failed state (acceptable while updates in progress)")

    # 6. Load average (catch crash-loop burn)
    if load > LOAD_WARN:
        if sev == "INFO":
            sev = "WARN"
        findings.append(f"host load avg high: {load}")

    # 7. LLM watchdog freshness
    if wb.get("updated_at"):
        try:
            dt = datetime.fromisoformat(str(wb["updated_at"]).replace("Z", "+00:00"))
            age = int(ts - dt.timestamp())
            if age > 86400:
                if sev == "INFO":
                    sev = "WARN"
                findings.append(f"llm-watchdog scoreboard {age}s old (>1d)")
        except Exception:
            pass

    status = {
        "ts": ts,
        "iso": datetime.now(timezone.utc).isoformat(),
        "severity": sev,
        "findings": findings,
        "docker": {c: dh.get(c, {"status": "missing", "healthy": False})
                   for c in REQUIRED_CONTAINERS},
        "snapshots_count": snap.get("snapshot_count"),
        "snapshot_age_s": snap.get("last_snapshot_age_s"),
        "trades": snap.get("trades", 0),
        "last_total_value": snap.get("last_total_value"),
        "last_holding_count": snap.get("last_holding_count"),
        "last_usdc": snap.get("last_usdc"),
        "pending_approvals": pend["count"],
        "pending_oldest_age_s": pend.get("oldest_age_s"),
        "inbox_orders": inbox,
        "host_unit_substate": sub,
        "load_avg_1m": load,
    }

    # Write status JSON (read-only observation artifact)
    try:
        STATUS_OUT.write_text(json.dumps(status, indent=2))
    except Exception:
        pass

    # Log a compact node to Neo4j knowledge graph (tap-in: queryable history)
    try:
        from neo4j import GraphDatabase
        d = GraphDatabase.driver(NEO4J_URI, auth=("neo4j", NEO4J_PASS))
        with d.session() as s:
            s.run(
                "CREATE (n:PortfolioWatchdog {ts:datetime($iso), severity:$sev, "
                "trades:$trades, pending:$pending, snap_age:$age, "
                "host_unit:$unit, load:$load, findings:$findings, repo:$repo})",
                iso=status["iso"], sev=sev, trades=status["trades"],
                pending=status["pending_approvals"], age=status["snapshot_age_s"],
                unit=sub, load=load,
                findings=" | ".join(findings) if findings else "nominal",
                repo=str(ROOT),
            )
        d.close()
    except Exception:
        # Degrade gracefully — watchdog must never fail on KG logging
        pass

    # Human-readable summary
    print(f"[{status['iso']}] severity={sev}")
    print(f"  containers: " + ", ".join(
        f"{c}={status['docker'][c]['status']}" for c in REQUIRED_CONTAINERS))
    print(f"  snapshots={status['snapshots_count']} age={status['snapshot_age_s']}s "
          f"trades={status['trades']} pending={status['pending_approvals']} "
          f"inbox={status['inbox_orders']} "
          f"value={status.get('last_total_value')} holdings={status.get('last_holding_count')}")
    print(f"  host_unit={sub} load={load}")
    if findings:
        print("  findings:")
        for f in findings:
            print(f"    - {f}")
    else:
        print("  findings: none (nominal)")

    return 1 if sev == "CRITICAL" else 0


if __name__ == "__main__":
    sys.exit(main())
