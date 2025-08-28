# src/run_backtest.py
from __future__ import annotations
import os, argparse, json
from dotenv import load_dotenv

from src.cb_client import CBClient
from src.backtest.engine import BTConfig, ExecModel, RiskModel, BacktestEngine, \
    TripleMAAdapter, DonchianAdapter, AggressiveMomoAdapter, FillWhen

load_dotenv(override=False)

ADAPTERS = {
    "ma": lambda portal, cfg: TripleMAAdapter(portal, cfg),
    "donch": lambda portal, cfg: DonchianAdapter(portal, cfg, lookback=20),
    "momo": lambda portal, cfg: AggressiveMomoAdapter(portal, cfg, topk=4),
}

def parse_args():
    ap = argparse.ArgumentParser(description="Crypto backtest runner")
    ap.add_argument("--products", type=str, default=os.getenv("PRODUCTS","BTC-USD,ETH-USD,SOL-USD"))
    ap.add_argument("--granularity", type=str, default=os.getenv("BAR_GRANULARITY","ONE_HOUR"))
    ap.add_argument("--lookback-days", type=int, default=int(os.getenv("LOOKBACK_DAYS","365")))
    ap.add_argument("--adapters", type=str, default="ma,donch,momo", help="comma list: ma,donch,momo")
    ap.add_argument("--initial-cash", type=float, default=float(os.getenv("INITIAL_CASH","15000")))
    ap.add_argument("--risk-per-trade", type=float, default=float(os.getenv("RISK_PER_TRADE","0.01")))
    ap.add_argument("--fee-bps", type=float, default=float(os.getenv("TAKER_FEE_BPS","8.0")))
    ap.add_argument("--slip-bps", type=float, default=float(os.getenv("SLIPPAGE_BPS","1.5")))
    ap.add_argument("--fill-when", type=str, default=os.getenv("FILL_WHEN","next"), choices=["next","close"])
    ap.add_argument("--min-notional", type=float, default=float(os.getenv("MIN_NOTIONAL","25")))
    return ap.parse_args()

def main():
    args = parse_args()
    cb = CBClient()
    products = [p.strip() for p in args.products.split(",") if p.strip()]
    em = ExecModel(fee_bps=args.fee_bps, slippage_bps=args.slip_bps,
                   fill_when=FillWhen.NEXT_OPEN if args.fill_when=="next" else FillWhen.CLOSE)
    rm = RiskModel(initial_cash=args.initial_cash, risk_per_trade=args.risk_per_trade, min_notional=args.min_notional)
    cfg = BTConfig(products=products, granularity=args.granularity, lookback_days=args.lookback_days,
                   exec_model=em, risk_model=rm)

    # We pass the portal lazily inside engine; adapters need portal, so we create with stubs below
    engine = BacktestEngine(cb, cfg, adapters=[])
    # re-bind adapters with the engine's portal
    adapters = []
    for key in [a.strip() for a in args.adapters.split(",") if a.strip()]:
        factory = ADAPTERS.get(key)
        if factory is None:
            raise SystemExit(f"Unknown adapter '{key}'. Options: {list(ADAPTERS.keys())}")
        adapters.append(factory(engine.portal, cfg))
    engine.adapters = adapters

    result = engine.run()
    print(json.dumps(result, indent=2))

if __name__ == "__main__":
    main()

#     # Example: 1-hour bars, 365d, triple-MA + Donchian + momentum, risk 1% / trade
# python -m src.run_backtest \
#   --products BTC-USD,ETH-USD,SOL-USD \
#   --granularity ONE_HOUR \
#   --lookback-days 365 \
#   --adapters ma,donch,momo \
#   --initial-cash 15000 \
#   --risk-per-trade 0.01 \
#   --fee-bps 8 --slip-bps 1.5 --fill-when next
