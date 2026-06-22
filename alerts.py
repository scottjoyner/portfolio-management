#!/usr/bin/env python3
"""
Alerts Service - Performance monitoring and notification system
Monitors portfolio metrics and sends alerts on threshold breaches.
"""

import os
import time
from datetime import datetime
from typing import Dict, Optional
from pathlib import Path

ROOT = Path(__file__).resolve().parent
HISTORICAL_DIR = ROOT / "data" / "historical"

class AlertsService:
    """Performance monitoring and alerting service."""
    
    def __init__(self):
        self.thresholds = {
            "drawdown_max": -0.25,  # Alert if drawdown > -25%
            "pnl_target": 0.15,     # Alert when PnL > +15%
            "cash_ratio": 0.3,      # Alert if cash > 30% of portfolio
        }
        self.alert_history = []
    
    def check_portfolio_health(self) -> list:
        """Check portfolio against threshold alerts."""
        
        from portfolio_manager import Backtester
        
        backtester = Backtester()
        backtester.load_historical_data(str(HISTORICAL_DIR))
        results = backtester.run_backtest(strategy="hold_all")
        
        alerts = []
        
        # Check drawdown
        current_value = results.get("total_portfolio_value", 0)
        initial_investment = 100000.0
        
        if current_value > 0:
            drawdown = (current_value - initial_investment) / current_value
            
            if drawdown < self.thresholds["drawdown_max"]:
                alerts.append({
                    "type": "drawdown_warning",
                    "message": f"Portfolio drawdown at {drawdown*100:.1f}% - approaching {self.thresholds['drawdown_max']*100:-.0f}%",
                    "severity": "warning" if drawdown < self.thresholds["drawdown_max"] * 0.9 else "critical"
                })
        
        # Check PnL target
        pnl_return = (current_value - initial_investment) / initial_investment
        
        if pnl_return > self.thresholds["pnl_target"]:
            alerts.append({
                "type": "profit_target",
                "message": f"Portfolio reached +{pnl_return*100:.1f}% target!",
                "severity": "info"
            })
        
        # Check cash ratio (placeholder)
        alerts.append({
            "type": "portfolio_summary",
            "message": f"Current portfolio value: ${current_value:,.2f}",
            "severity": "info"
        })
        
        self.alert_history.extend(alerts)
        return alerts
    
    def send_alert(self, message: str, severity: str = "info"):
        """Send alert notification."""
        
        print(f"\n🔔 [{severity.upper()}] {message}")
    
    def generate_dashboard(self) -> dict:
        """Generate performance dashboard data."""
        
        from portfolio_manager import Backtester
        
        backtester = Backtester()
        backtester.load_historical_data(str(HISTORICAL_DIR))
        results = backtester.run_backtest(strategy="hold_all")
        
        return {
            "timestamp": datetime.now().isoformat(),
            "portfolio_value": results.get("total_portfolio_value", 0),
            "return_pct": (results.get("total_portfolio_value", 0) / 100000 - 1) * 100,
            "alerts_count": len(self.alert_history)
        }

def main():
    """Main alert monitoring loop."""
    
    print("\n" + "="*80)
    print("🔔 PERFORMANCE ALERTING SERVICE")
    print("="*80)
    
    alerts = AlertsService()
    
    # Initial check
    print("\n📊 Initial Portfolio Health Check:")
    health = alerts.check_portfolio_health()
    
    for alert in health:
        alerts.send_alert(alert["message"], alert["severity"])
    
    print("\n✅ Alerting service ready")

if __name__ == "__main__":
    main()
