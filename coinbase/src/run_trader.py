from __future__ import annotations
import os, time, uuid, logging
from .config import SETTINGS, BRACKETS, KELLY, KELLY_CAPS, BANDIT, TCOST
from .cb_client import CBClient
from .data import fetch_candles_df
from .strategy import trend_signal, target_weight
from .portfolio import rebalance_plan
from .risk import apply_risk_checks, RiskState
from .alpha.alpha import donchian_breakout_setup, trend_rsi_pullback_setup, donchian_breakdown_setup, trend_rsi_rip_setup
from .execution import place_bracket_long, place_bracket_short, manage_brackets
from .bandit import ucb1_scores, thompson_scores
from .analytics import rolling_stats, kelly_from_history
from .tcost import effective_fill_price

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
BARS_PER_YEAR = {"ONE_MINUTE": 60*24*365, "FIVE_MINUTE": 12*24*365, "FIFTEEN_MINUTE": 4*24*365,
                 "THIRTY_MINUTE": 2*24*365, "ONE_HOUR": 24*365, "TWO_HOUR": 12*365,
                 "FOUR_HOUR": 6*365, "SIX_HOUR": 4*365, "ONE_DAY": 365}

def get_portfolio_value_and_holdings(cb: CBClient, products: list[str], cash_ccy: str):
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
            for p in products:
                if p.split("-")[0] == ccy:
                    base_holdings[p] = avail
    prices, bids, asks = {}, {}, {}
    best = cb.best_bid_ask(products)
    for bidask in best.get("pricebooks", best if isinstance(best, list) else []):
        pid = bidask.get("product_id")
        ask = float(bidask.get("asks", [{}])[0].get("price", 0)) if bidask.get("asks") else 0.0
        bid = float(bidask.get("bids", [{}])[0].get("price", 0)) if bidask.get("bids") else 0.0
        bids[pid], asks[pid] = bid, ask
        prices[pid] = (ask + bid)/2 if ask and bid else max(ask, bid, 0.0)
    equity = cash_value + sum(base_holdings[p]*prices.get(p, 0.0) for p in products)
    return equity, base_holdings, prices, bids, asks

def compute_target_weights(cb: CBClient, products: list[str], granularity: str, lookback_days: int, target_ann_vol: float) -> dict[str, float]:
    weights = {}
    bpy = BARS_PER_YEAR[granularity]
    active = 0
    for p in products:
        df = fetch_candles_df(cb, p, lookback_days=lookback_days, granularity=granularity)
        if len(df) < 210:
            weights[p] = 0.0
            continue
        sig = trend_signal(df["close"], 50, 200)
        w = target_weight(df["close"], target_ann_vol, bpy) if sig > 0 else 0.0
        weights[p] = w
        if w > 0: active += 1
    if active > 0:
        total = sum(weights.values())
        if total > 0:
            for k in weights: weights[k] = weights[k] / total
    return weights

def place_or_preview(cb: CBClient, intents: list[dict], dry_run: bool):
    results = []
    for it in intents:
        pid = it["product_id"]; side = it["side"]
        if dry_run:
            prev = cb.preview_order(side=side, product_id=pid, base_size=str(it["base_size"]) if side=="sell" else None, quote_size=str(it["quote_size"]) if side=="buy" else None)
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
    cb = CBClient(); s = SETTINGS
    equity, holdings, prices, _, _ = get_portfolio_value_and_holdings(cb, s.products, s.cash_ccy)
    logging.info(f"Equity ~ ${equity:,.2f}; holdings: {holdings}")
    weights = compute_target_weights(cb, s.products, s.bar_granularity, s.lookback_days, s.target_vol)
    logging.info(f"Target weights: {weights}")
    intents = rebalance_plan(prices, equity, weights, holdings, s.min_notional)
    logging.info(f"Intents: {intents}")
    intents = apply_risk_checks(intents, equity, s.risk_per_trade, s.max_drawdown, RiskState())
    if not intents:
        logging.info("No trade after risk checks."); return
    res = place_or_preview(cb, intents, s.dry_run)
    for r in res: logging.info(str(r))

def signal_brackets(cb: CBClient, products: list[str], granularity: str, lookback_days: int, min_rr: float, stop_k: float, target_k: float):
    ideas = {}
    for p in products:
        df = fetch_candles_df(cb, p, lookback_days=lookback_days, granularity=granularity)
        if df.empty: continue
        cands = []
        s1 = donchian_breakout_setup(df, stop_atr_mult=stop_k, target_atr_mult=target_k);  cands += [s1] if s1 else []
        s2 = trend_rsi_pullback_setup(df, stop_atr_mult=stop_k);                           cands += [s2] if s2 else []
        s3 = donchian_breakdown_setup(df, stop_atr_mult=stop_k, target_atr_mult=target_k); cands += [s3] if s3 else []
        s4 = trend_rsi_rip_setup(df, stop_atr_mult=stop_k);                                cands += [s4] if s4 else []
        if not cands: continue
        best = max(cands, key=lambda x: x["rr"])
        if best["rr"] >= min_rr: ideas[p] = best
    return ideas

def can_short_spot(holdings: dict, product_id: str, base_size: float) -> bool:
    base = product_id.split("-")[0]
    have = 0.0
    for p, amt in holdings.items():
        if p.split("-")[0] == base: have = amt; break
    return have >= base_size - 1e-12

def place_rr_trades():
    cb = CBClient(); s = SETTINGS
    equity, holdings, prices, bids, asks = get_portfolio_value_and_holdings(cb, s.products, s.cash_ccy)
    logging.info(f"[RR] Equity ~ ${equity:,.2f}")
    ideas = signal_brackets(cb, s.products, s.bar_granularity, s.lookback_days, BRACKETS.min_rr, BRACKETS.stop_atr_mult, BRACKETS.target_atr_mult)
    if not ideas:
        logging.info("[RR] No qualifying setups (min RR filter)."); return
    arms = list({v['name'] for v in ideas.values()})
    if BANDIT.mode == 'ucb1': scores = ucb1_scores(int(time.time()), arms, c=BANDIT.ucb_c)
    elif BANDIT.mode == 'thompson': scores = thompson_scores(arms)
    else: scores = {a:0.0 for a in arms}
    stats = rolling_stats()
    kelly_f = {a: (kelly_from_history(stats.get(a, {}), KELLY.default_rr) if KELLY.enable else 1.0) for a in arms}
    results = []
    for pid, idea in sorted(ideas.items(), key=lambda kv: (scores.get(kv[1]['name'],0.0), kv[1]['rr']), reverse=True):
        mid = prices.get(pid); 
        if not mid or mid<=0: continue
        if idea['side']=='buy': risk_per_unit = idea['entry'] - idea['stop']
        else: risk_per_unit = idea['stop'] - idea['entry']
        if risk_per_unit <= 0: continue
        base_kelly = kelly_f.get(idea['name'], 1.0)
        kcap_global = KELLY.cap if KELLY.enable else 1.0
        kfloor = KELLY.floor if KELLY.enable else 1.0
        prod_cap = float(KELLY_CAPS.product_caps.get(pid, 1.0))
        setup_cap = float(KELLY_CAPS.setup_caps.get(idea['name'], 1.0))
        kcap = min(kcap_global, prod_cap, setup_cap)
        k_used = min(kcap, max(kfloor, base_kelly)) if KELLY.enable else 1.0
        risk_budget_usd = s.risk_per_trade * equity * k_used
        base_size = max(0.0, risk_budget_usd / risk_per_unit)
        notional = base_size * mid
        bid, ask = bids.get(pid, 0.0), asks.get(pid, 0.0)
        e_entry = effective_fill_price('buy' if idea['side']=='buy' else 'sell', mid, bid, ask, notional,
                                       taker_fee_bps=TCOST.taker_fee_bps, slippage_bps=TCOST.slippage_bps, impact_coeff=TCOST.impact_coeff)
        logging.info(f"[RR] {pid} {idea['name']} {idea['side']} RR={idea['rr']:.2f} entry={idea['entry']:.2f} entry≈{e_entry:.2f} stop={idea['stop']:.2f} target={idea['target']:.2f} size={base_size:.6f} kelly={k_used:.2f} score={scores.get(idea['name'],0):.2f}")
        if notional < s.min_notional: 
            logging.info(f"[RR] Skip {pid}, notional too small: ${notional:.2f}"); 
            continue
        if idea['side'] == 'buy':
            res = place_bracket_long(cb, pid, base_size, idea['entry'], idea['stop'], idea['target'], s.dry_run)
        else:
            if os.getenv('ENABLE_SHORTS','false').lower()!='true' and not can_short_spot(holdings, pid, base_size):
                logging.info(f"[RR] Skip short on {pid}: shorts disabled or insufficient holdings on spot."); 
                continue
            res = place_bracket_short(cb, pid, base_size, idea['entry'], idea['stop'], idea['target'], s.dry_run)
        res["idea"] = idea; results.append(res)
    for r in results: logging.info(str(r))

def run_manager_loop():
    cb = CBClient()
    manage_brackets(cb, poll_secs=BRACKETS.manager_poll_secs, trail_atr_mult=BRACKETS.trail_atr_mult, break_even_after_r=BRACKETS.break_even_after_r, dry_run=SETTINGS.dry_run)

if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--rebalance", action="store_true", help="Run a one-shot rebalance now")
    ap.add_argument("--rr-trades", action="store_true", help="Scan & place Risk-Reward bracket trades")
    ap.add_argument("--manage-brackets", action="store_true", help="Run bracket manager loop (synthetic OCO)")
    args = ap.parse_args()
    if args.rebalance: rebalance()
    elif args.rr_trades: place_rr_trades()
    elif args.manage_brackets: run_manager_loop()
    else: print("Nothing to do. Use --rebalance | --rr-trades | --manage-brackets")
