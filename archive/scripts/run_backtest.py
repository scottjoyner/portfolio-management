#!/usr/bin/env python3
"""
Backtest Runner - Execute backtests on all Coinbase strategies
"""

import os
import subprocess
import sys
from pathlib import Path

def run_backtest():
    """Run the backtester with all available strategies."""
    
    # Set up environment
    env = {**dict(subprocess.os.environ)}
    env['PYTHONPATH'] = '/home/scott/git/portfolio-management'
    env['TRADING_MODE'] = 'paper'
    env['LIVE_TRADING_ENABLED'] = 'true'
    
    cmd = [
        sys.executable,
        '-m', 'trading_system.backtester',
        '--interval=1h',
        '--output=/results',
    ]
    
    print(f"Running: {' '.join(cmd)}")
    result = subprocess.run(
        cmd,
        env=env,
        cwd='/home/scott/git/portfolio-management',
        capture_output=True,
        text=True
    )
    
    if result.returncode == 0:
        print("✅ Backtest completed successfully")
        print(result.stdout)
    else:
        print("❌ Backtest failed:")
        print(result.stderr)
        sys.exit(1)

if __name__ == '__main__':
    run_backtest()
