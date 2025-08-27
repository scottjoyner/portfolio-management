from __future__ import annotations
import os, time, uuid, logging
import pandas as pd
from .config import SETTINGS
from .cb_client import CBClient
from .data import fetch_candles_df
from .strategy import trend_signal, target_weight
from .portfolio import rebalance_plan
from .risk import apply_risk_checks, RiskState

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

BARS_PER_YEAR = {"ONE_MINUTE": 60*24*365, "FIVE_MINUTE": 12*24*365, "FIFTEEN_MINUTE": 4*24*365,
                 "THIRTY_MINUTE": 2*24*365, "ONE_HOUR": 24*365, "TWO_HOUR": 12*365,
                 "FOUR_HOUR": 6*365, "SIX_HOUR": 4*365, "ONE_DAY": 365}

def get_portfolio_value_and_holdings(cb: CBClient, products: list[str], cash_ccy: str) -> tuple[float, dict]:
    acct = cb.list_accounts()
    accounts = acct.get("accounts", acct)
    cash_value = 0.0
    base_holdings = {p: 0.0 for p in products}
    for a in accounts:
        ccy = a.get("currency")
        avail = float(a.get("available_balance", {}).get("value", a.get("available_balance", 0)) or 0)
        if ccy == cash_ccy:
            cash_value += avail
        else:
            # Map base asset to product if we can (e.g., BTC -> BTC-USD)
            for p in products:
                base = p.split("-")[0]
                if base == ccy:
                    base_holdings[p] = avail
    # Get prices to compute marked-to-market
    prices = {}
    best = cb.best_bid_ask(products)
    for bidask in best.get("pricebooks", best if isinstance(best, list) else []):
        pid = bidask.get("product_id")
        ask = float(bidask.get("asks", [{}])[0].get("price", 0)) if bidask.get("asks") else 0.0
        bid = float(bidask.get("bids", [{}])[0].get("price", 0)) if bidask.get("bids") else 0.0
        prices[pid] = (ask + bid)/2 if ask and bid else max(ask, bid, 0.0)
    equity = cash_value + sum(base_holdings[p]*prices.get(p, 0.0) for p in products)
    return equity, base_holdings, prices

def compute_target_weights(cb: CBClient, products: list[str], granularity: str, lookback_days: int, target_ann_vol: float) -> dict[str, float]:
    weights = {}
    bpy = BARS_PER_YEAR[granularity]
    active = 0
    vols = {}
    for p in products:
        df = fetch_candles_df(cb, p, lookback_days=lookback_days, granularity=granularity)
        if len(df) < 210:  # 200 slow SMA + buffer
            weights[p] = 0.0
            continue
        sig = trend_signal(df["close"], 50, 200)
        w = target_weight(df["close"], target_ann_vol, bpy) if sig > 0 else 0.0
        vols[p] = w
        weights[p] = w
        if w > 0: active += 1
    # Normalize across active assets
    if active > 0:
        total = sum(weights.values())
        if total > 0:
            for k in weights:
                weights[k] = weights[k] / total
    return weights

def place_or_preview(cb: CBClient, intents: list[dict], dry_run: bool):
    results = []
    for it in intents:
        pid = it["product_id"]
        side = it["side"]
        # Coinbase uses quote_size for buys (IOC market) and base_size for sells
        if dry_run:
            prev = cb.preview_order(side=side, product_id=pid,
                                    base_size=str(it["base_size"]) if side=="sell" else None,
                                    quote_size=str(it["quote_size"]) if side=="buy" else None)
            results.append({"intent": it, "preview": prev})
        else:
            cid = str(uuid.uuid4())
            if side == "buy":
                res = cb.market_order("buy", product_id=pid, quote_size=str(it["quote_size"]), client_order_id=cid)
            else:
                res = cb.market_order("sell", product_id=pid, base_size=str(it["base_size"]), client_order_id=cid)
            results.append({"intent": it, "order": res})
    return results

def rebalance():
    cb = CBClient()
    settings = SETTINGS
    equity, holdings, prices = get_portfolio_value_and_holdings(cb, settings.products, settings.cash_ccy)
    logging.info(f"Equity ~ ${equity:,.2f}; holdings: {holdings}")
    weights = compute_target_weights(cb, settings.products, settings.bar_granularity, settings.lookback_days, settings.target_vol)
    logging.info(f"Target weights: {weights}")
    intents = rebalance_plan(prices, equity, weights, holdings, settings.min_notional)
    logging.info(f"Intents: {intents}")
    intents = apply_risk_checks(intents, equity, settings.risk_per_trade, settings.max_drawdown, RiskState())
    if not intents:
        logging.info("No trade after risk checks.")
        return
    res = place_or_preview(cb, intents, settings.dry_run)
    for r in res:
        logging.info(str(r))

if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--rebalance", action="store_true", help="Run a one-shot rebalance now")
    args = ap.parse_args()
    if args.rebalance:
        rebalance()
    else:
        print("Nothing to do. Use --rebalance")
