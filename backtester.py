#!/usr/bin/env python3
"""
Backtester Service - Scheduled backtesting with performance analytics
Runs on a schedule to generate reports and send alerts.
"""

import os
import time
from datetime import datetime, timedelta
from typing import Dict, Optional

class ScheduledBacktester:
    """Scheduled backtesting service."""
    
    def __init__(self):
        self.interval_hours = 6  # Run every 6 hours by default
    
    def load_data(self, data_dir: str = "./data/historical") -> bool:
        """Load historical data from CSV files."""
        
        import os
        
        if not os.path.exists(data_dir):
            return False
        
        csv_files = [f for f in os.listdir(data_dir) if f.endswith("_daily.csv")]
        
        if not csv_files:
            print(f"    ⚠️  No CSV files found in {data_dir}")
            return False
        
        print(f"📥 Loaded {len(csv_files)} CSV files for backtesting")
        return True
    
    def run_strategy_hold_all(self) -> dict:
        """Run hold-all strategy and calculate performance metrics."""
        
        from portfolio_manager import Backtester, Portfolio
        
        backtester = Backtester()
        backtester.load_historical_data("/home/falcon/git/portfolio-management/data/historical")
        results = backtester.run_backtest(strategy="hold_all")
        
        return results
    
    def send_alert(self, message: str):
        """Send alert notification (placeholder for actual webhook/email)."""
        print(f"\n🔔 ALERT: {message}")
    
    def generate_report(self) -> dict:
        """Generate performance report."""
        
        results = self.run_strategy_hold_all()
        
        report = {
            "timestamp": datetime.now().isoformat(),
            "initial_investment": 100000.0,
            "final_value": results.get("total_portfolio_value", 0),
            "return": (results.get("total_portfolio_value", 0) / 100000 - 1) * 100,
        }
        
        return report
    
    def run(self):
        """Main backtesting loop."""
        
        print("\n" + "="*80)
        print("🚀 SCHEDULED BACKTESTING SERVICE")
        print("="*80)
        
        # Initial run
        self.run_strategy_hold_all()
        
        print("\n⏰ Backtesting complete. Will run every 6 hours.\n")

def main():
    """Main scheduler entry point."""
    
    backtester = ScheduledBacktester()
    
    try:
        while True:
            backtester.run()
            
            # Sleep for interval hours (6 hours in seconds)
            time.sleep(6 * 3600)
            
    except KeyboardInterrupt:
        print("\n⏹️  Scheduler stopped by user")

if __name__ == "__main__":
    main()
