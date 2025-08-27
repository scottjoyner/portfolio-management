from .bandit import ucb1_scores, thompson_scores
from .analytics import rolling_stats, kelly_from_history
from __future__ import annotations
import os, time, uuid, logging
import pandas as pd
from .config import SETTINGS
from .cb_client import CBClient
from .data import fetch_candles_df
from .strategy import trend_signal, target_weight
from .portfolio import rebalance_plan
from .risk import apply_risk_checks, RiskState
from .alpha.alpha import donchian_breakout_setup, trend_rsi_pullback_setup, donchian_breakdown_setup, trend_rsi_rip_setup
from .data import compute_atr
from .execution import place_bracket_long, place_bracket_short, manage_brackets
from .config import BRACKETS, KELLY, BANDIT

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
    ap.add_argument("--rr-trades", action="store_true", help="Scan & place Risk-Reward bracket trades")
    ap.add_argument("--manage-brackets", action="store_true", help="Run bracket manager loop (synthetic OCO)")
    args = ap.parse_args()
    if args.rebalance:
        rebalance()
    elif args.rr_trades:
        place_rr_trades()
    elif args.manage_brackets:
        run_manager_loop()
    else:
        print("Nothing to do. Use --rebalance | --rr-trades | --manage-brackets")


def signal_brackets(cb: CBClient, products: list[str], granularity: str, lookback_days: int, min_rr: float, stop_k: float, target_k: float):
    ideas = {}
    for p in products:
        df = fetch_candles_df(cb, p, lookback_days=lookback_days, granularity=granularity)
        if df.empty:
            continue
        # Try multiple setups; pick the best RR
        candidates = []
        s1 = donchian_breakout_setup(df, stop_atr_mult=stop_k, target_atr_mult=target_k)
        if s1: candidates.append(s1)
        s2 = trend_rsi_pullback_setup(df, stop_atr_mult=stop_k)
        if s2: candidates.append(s2)
        if not candidates:
            continue
        best = max(candidates, key=lambda x: x["rr"])
        if best["rr"] >= min_rr:
            ideas[p] = best
    return ideas

def place_rr_trades():
    cb = CBClient()
    settings = SETTINGS
    # Equity & prices
    equity, holdings, prices = get_portfolio_value_and_holdings(cb, settings.products, settings.cash_ccy)
    logging.info(f"[RR] Equity ~ ${equity:,.2f}")
    # Find bracket ideas
    ideas = signal_brackets(cb, settings.products, settings.bar_granularity, settings.lookback_days,
                            BRACKETS.min_rr, BRACKETS.stop_atr_mult, BRACKETS.target_atr_mult)
    if not ideas:
        logging.info("[RR] No qualifying setups (min RR filter).")
        return
    # Risk budget per trade (R-units): risk_per_trade * equity = $risk; size = $risk / $risk_per_unit
    results = []
    for pid, idea in ideas.items():
        mid = prices.get(pid)
        if not mid or mid <= 0:
            continue
        risk_per_unit = idea["entry"] - idea["stop"]
        if risk_per_unit <= 0:
            continue
        risk_budget_usd = SETTINGS.risk_per_trade * equity
        base_size = max(0.0, risk_budget_usd / risk_per_unit)
        notional = base_size * mid
        if notional < SETTINGS.min_notional:
            logging.info(f"[RR] Skip {pid}, notional too small: ${notional:.2f}")
            continue
        logging.info(f"[RR] {pid} {idea['name']} RR={idea['rr']:.2f} entry={idea['entry']:.2f} stop={idea['stop']:.2f} target={idea['target']:.2f} size={base_size:.6f}")
        res = place_bracket_long(cb, pid, base_size, idea["entry"], idea["stop"], idea["target"], settings.dry_run)
        res["idea"] = idea
        results.append(res)
    for r in results:
        logging.info(str(r))

def run_manager_loop():
    cb = CBClient()
    manage_brackets(cb, poll_secs=BRACKETS.manager_poll_secs, trail_atr_mult=BRACKETS.trail_atr_mult,
                    break_even_after_r=BRACKETS.break_even_after_r, dry_run=SETTINGS.dry_run)



def can_short_spot(holdings: dict, product_id: str, base_size: float) -> bool:
    \"\"\"On spot-only accounts, we can't go net short. Allow 'short' only if we already hold enough base to sell.\"\"\"
    base = product_id.split("-")[0]
    have = 0.0
    for p, amt in holdings.items():
        if p.split("-")[0] == base:
            have = amt
            break
    return have >= base_size - 1e-12
