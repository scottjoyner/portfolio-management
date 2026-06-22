#!/usr/bin/env bash
# Production launcher — uses a Python supervisor to keep processes alive
set -euo pipefail

ROOT="$(cd "$(dirname "$0")" && pwd)"
PYTHON="$ROOT/.venv/bin/python3"

mkdir -p "$ROOT/logs" "$ROOT/data"

usage() {
    echo "Usage: $0 {start|stop|restart|status|health|logs}"
    exit 1
}

# ── Python supervisor (reliable process management) ──────────────
# Uses subprocess.Popen which properly handles background processes
_start_supervised() {
    "$PYTHON" -c "
import subprocess, sys, os, time, signal
root = '$ROOT'
python = '$PYTHON'
logdir = os.path.join(root, 'logs')

processes = {}

DAEMON_ARGS = [
    '--products', 'BTC-USD,ETH-USD,SOL-USD',
    '--scan-interval', '90',
    '--pm-timeout', '35',
    '--arb-timeout', '25',
]

def start(name, script, args, pidfile):
    logpath = os.path.join(logdir, f'{name}.log')
    fh = open(logpath, 'a')
    fh.write(f'\n--- Started at {time.strftime(\"%Y-%m-%dT%H:%M:%S\")} ---\n')
    fh.flush()
    p = subprocess.Popen(
        [python, os.path.join(root, script)] + args,
        stdout=fh, stderr=subprocess.STDOUT,
        cwd=root,
    )
    with open(pidfile, 'w') as pf:
        pf.write(str(p.pid))
    processes[name] = p
    print(f'{name}: PID {p.pid}')
    return p

# Start daemon
start('daemon', 'trading_system/apps/worker/unified_market_daemon.py',
      DAEMON_ARGS,
      os.path.join(logdir, 'daemon.pid'))

# Start dashboard
start('dashboard', 'trading_system/ui/dashboard_server.py',
      ['--port', '8002'],
      os.path.join(logdir, 'dashboard.pid'))

# Monitor both processes, restart on unexpected exit
print('Monitoring processes (Ctrl+C to stop)...')
try:
    while True:
        for name, p in list(processes.items()):
            ret = p.poll()
            if ret is not None:
                print(f'{name} exited with code {ret}, restarting...')
                pidfile = os.path.join(logdir, f'{name}.pid')
                start(name,
                      'trading_system/apps/worker/unified_market_daemon.py' if name == 'daemon' else 'trading_system/ui/dashboard_server.py',
                      DAEMON_ARGS if name == 'daemon' else ['--port', '8002'],
                      pidfile)
        time.sleep(5)
except KeyboardInterrupt:
    print('Stopping all processes...')
    for name, p in processes.items():
        p.terminate()
    for name, p in processes.items():
        try:
            p.wait(timeout=10)
        except subprocess.TimeoutExpired:
            p.kill()
    print('All processes stopped.')
" 2>&1 | tee -a "$ROOT/logs/supervisor.log"
}

# ── Commands ─────────────────────────────────────────────────────

case "${1:-}" in
    start)
        echo "=== Starting supervised production system ==="
        _start_supervised &
        SUP_PID=$!
        echo "$SUP_PID" > "$ROOT/logs/supervisor.pid"
        disown
        echo "Supervisor started (PID $SUP_PID)"
        sleep 8
        # Check health
        "$ROOT/run_production.sh" health 2>/dev/null || echo "Waiting for services to start..."
        ;;
    stop)
        echo "=== Stopping ==="
        SUP_PID=$(cat "$ROOT/logs/supervisor.pid" 2>/dev/null || echo "")
        if [ -n "$SUP_PID" ] && kill -0 "$SUP_PID" 2>/dev/null; then
            kill "$SUP_PID" 2>/dev/null
            sleep 3
        fi
        # Kill any orphaned python processes from this system
        for pidfile in "$ROOT/logs/daemon.pid" "$ROOT/logs/dashboard.pid"; do
            if [ -f "$pidfile" ]; then
                kill "$(cat "$pidfile")" 2>/dev/null || true
                rm -f "$pidfile"
            fi
        done
        rm -f "$ROOT/logs/supervisor.pid"
        echo "Stopped."
        ;;
    restart)
        "$0" stop
        sleep 2
        "$0" start
        ;;
    status)
        echo "=== Status ==="
        SUP_PID=$(cat "$ROOT/logs/supervisor.pid" 2>/dev/null || echo "")
        if [ -n "$SUP_PID" ] && kill -0 "$SUP_PID" 2>/dev/null; then
            echo "Supervisor: RUNNING (PID $SUP_PID)"
        else
            echo "Supervisor: STOPPED"
        fi
        for name in daemon dashboard; do
            pidfile="$ROOT/logs/$name.pid"
            if [ -f "$pidfile" ]; then
                pid=$(cat "$pidfile")
                if kill -0 "$pid" 2>/dev/null; then
                    uptime=$(ps -o etime= -p "$pid" 2>/dev/null | tr -d ' ')
                    echo "$name: RUNNING (PID $pid, uptime $uptime)"
                else
                    echo "$name: PID FILE STALE ($pid not found)"
                fi
            else
                echo "$name: STOPPED"
            fi
        done
        # Show heartbeat
        HB="$ROOT/data/.daemon_heartbeat"
        if [ -f "$HB" ]; then
            age=$(python3 -c "import time; print(f'{time.time() - float(open(\"$HB\").read()):.0f}s')")
            echo "Heartbeat age: $age"
        fi
        ;;
    health)
        URL="${2:-http://localhost:8002/health}"
        if curl -sf --max-time 5 "$URL" 2>/dev/null | python3 -c "
import sys, json
d = json.load(sys.stdin)
print(f'Status: {d.get(\"status\", \"unknown\")}')
print(f'Signals: {d[\"components\"].get(\"signal_cache\", \"?\")}')
print(f'Heartbeat age: {d.get(\"daemon_heartbeat_age_sec\", \"?\")}s')
print(f'Operator state: {d[\"components\"].get(\"operator_state\", \"?\")}')
print(f'Total live signals: {d.get(\"total_signals\", 0)}')
" 2>/dev/null; then
            return 0
        else
            echo "Health endpoint unreachable at $URL"
            return 1
        fi
        ;;
    logs)
        echo "=== Tailing logs (Ctrl+C to stop) ==="
        tail -f "$ROOT/logs/daemon.log" "$ROOT/logs/dashboard.log" "$ROOT/logs/supervisor.log"
        ;;
    *)
        usage
        ;;
esac
