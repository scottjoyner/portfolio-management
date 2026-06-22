#!/usr/bin/env bash
# Health monitor — run from cron every 5 minutes
# Checks daemon heartbeat and dashboard health endpoint
# Logs warnings and sends a desktop notification on failure

ROOT="/home/scott/git/portfolio-management"
HEARTBEAT="$ROOT/data/.daemon_heartbeat"
LOG="$ROOT/logs/health_monitor.log"
DASHBOARD_URL="http://localhost:8002/health"

mkdir -p "$(dirname "$LOG")"

# ── Check daemon heartbeat ──────────────────────────────────────
if [ -f "$HEARTBEAT" ]; then
    HB=$(cat "$HEARTBEAT")
    NOW=$(python3 -c "import time; print(time.time())")
    AGE=$(python3 -c "print($NOW - $HB)")
    AGE_INT=${AGE%.*}
    
    if [ "$AGE_INT" -gt 300 ]; then
        echo "[$(date)] WARNING: Daemon heartbeat stale (${AGE_INT}s)" >> "$LOG"
        # Try to restart via supervisor
        if [ -f "$ROOT/logs/supervisor.pid" ]; then
            SUP=$(cat "$ROOT/logs/supervisor.pid")
            if kill -0 "$SUP" 2>/dev/null; then
                echo "  Supervisor alive, daemon may be restarting" >> "$LOG"
            fi
        fi
    fi
else
    echo "[$(date)] WARNING: No daemon heartbeat file" >> "$LOG"
fi

# ── Check dashboard health ──────────────────────────────────────
HEALTH=$(curl -sf --max-time 10 "$DASHBOARD_URL" 2>/dev/null)
if [ -z "$HEALTH" ]; then
    echo "[$(date)] WARNING: Dashboard health endpoint unreachable" >> "$LOG"
elif echo "$HEALTH" | python3 -c "import sys,json; d=json.load(sys.stdin); sys.exit(0 if d.get('status')=='healthy' else 1)" 2>/dev/null; then
    # Healthy — extract key stats for log
    echo "$HEALTH" | python3 -c "
import sys, json
d = json.load(sys.stdin)
ts = d.get('total_signals', 0)
hb = d.get('daemon_heartbeat_age_sec', '?')
print(f'[$(date)] OK signals={ts} heartbeat={hb}s')
" >> "$LOG"
else
    echo "[$(date)] WARNING: Dashboard reports degraded health" >> "$LOG"
    echo "$HEALTH" | python3 -c "import sys,json; d=json.load(sys.stdin); print(f'  Components: {d.get(\"components\",{})}')" >> "$LOG"
fi

# Keep only last 200 lines
tail -n 200 "$LOG" > "${LOG}.tmp" && mv "${LOG}.tmp" "$LOG"
