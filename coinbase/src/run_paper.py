from __future__ import annotations
import argparse, logging
from .cb_client import CBClient
from .sim.paper_sim import SimConfig, simulate
from .config import SETTINGS

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

def main():
    ap = argparse.ArgumentParser(description="Paper Trading Simulator")
    ap.add_argument("--products", type=str, default=None)
    ap.add_argument("--granularity", type=str, default=None)
    ap.add_argument("--lookback-days", type=int, default=None)
    ap.add_argument("--initial-cash", type=float, default=10000.0)
    ap.add_argument("--risk-per-trade", type=float, default=None)
    ap.add_argument("--min-rr", type=float, default=2.0)
    ap.add_argument("--stop-k", type=float, default=2.0)
    ap.add_argument("--target-k", type=float, default=3.0)
    args = ap.parse_args()

    products = (args.products or ",".join(SETTINGS.products)).split(",")
    gran = args.granularity or SETTINGS.bar_granularity
    days = args.lookback_days or SETTINGS.lookback_days
    rpt = args.risk_per_trade or SETTINGS.risk_per_trade

    cfg = SimConfig(initial_cash=args.initial_cash, risk_per_trade=rpt, min_rr=args.min_rr, stop_k=args.stop_k, target_k=args.target_k)
    cb = CBClient()
    eq = simulate(cb, products, start_days=days, granularity=gran, cfg=cfg)
    print(eq.tail())
    out = "state/paper_equity.csv"
    from pathlib import Path
    Path("state").mkdir(exist_ok=True, parents=True)
    eq.to_csv(out)
    print(f"Saved equity curve → {out}")

if __name__ == "__main__":
    main()
