"""Replay/audit module: score the paper trader's REAL trade events against the
backtesting framework's verdicts.

The paper trader (coinbase/src/run_trader_v4.py) records every trade as a
labeled event via ``_record_trade_event``, appending JSON lines to
``trade_events/<PRODUCT>.jsonl`` (rooted at ``NAS_FEED_ROOT`` or
``data/feed_cache``). Each record is one of:

  entry: {"ts", "kind": "entry", "product_id", "price" (entry px),
          "side": "LONG"/"SHORT", "strategy", "qty", "notional", ...}
  exit:  {"ts", "kind": "exit", "product_id", "price" (exit px),
          "side": "BUY"/"SELL", "strategy", "pnl" (usd), "reason",
          "entry_price", ...}

We pair each entry with the next exit for the same product (later ts) and
score realized PnL vs the strategy's backtested win_rate.
"""
import argparse
import json
import os
import sys


def _default_root():
    """Resolve the trade_events root: NAS_FEED_ROOT if set+exists, else data/feed_cache."""
    nas = os.environ.get("NAS_FEED_ROOT")
    if nas:
        p = os.path.join(nas.rstrip("/"), "trade_events")
        if os.path.isdir(p):
            return p
    here = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    return os.path.join(here, "data", "feed_cache", "trade_events")


def load_trade_events(root_dir=None, product=None):
    """Read trade_events JSONL files and pair entries with their exits.

    Returns a list of dicts:
        {product, entry_ts, entry_price, exit_ts, exit_price, pnl_usd,
         strategy, size_usd}

    Open trades (an entry with no later exit) have exit_ts/exit_price/pnl_usd=None.
    Guards against missing dirs/files and malformed lines.
    """
    root = root_dir or _default_root()
    if not os.path.isdir(root):
        return []

    if product:
        files = [os.path.join(root, f"{product}.jsonl")]
    else:
        files = []
        for fn in sorted(os.listdir(root)):
            if fn.endswith(".jsonl"):
                files.append(os.path.join(root, fn))

    # Collect raw records per product.
    recs_by_product = {}
    for fp in files:
        if not os.path.isfile(fp):
            continue
        pid = os.path.basename(fp)[: -len(".jsonl")]
        with open(fp, "r", encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                try:
                    rec = json.loads(line)
                except (ValueError, json.JSONDecodeError):
                    continue
                if not isinstance(rec, dict) or "kind" not in rec:
                    continue
                recs_by_product.setdefault(pid, []).append(rec)

    trades = []
    for pid, recs in recs_by_product.items():
        recs.sort(key=lambda r: float(r.get("ts", 0) or 0))
        open_entries = []  # entries not yet matched
        for rec in recs:
            kind = rec.get("kind")
            if kind == "entry":
                open_entries.append(rec)
            elif kind == "exit":
                ent = None
                for cand in open_entries:
                    if float(cand.get("ts", 0) or 0) <= float(rec.get("ts", 0) or 0):
                        ent = cand
                        break
                if ent is not None:
                    open_entries.remove(ent)
                    trades.append(_build_trade(pid, ent, rec))
                else:
                    # Exit without a preceding entry: record standalone open->exit
                    trades.append(_build_trade(pid, None, rec))
        # Remaining unmatched entries are open trades.
        for ent in open_entries:
            trades.append(_build_trade(pid, ent, None))

    trades.sort(key=lambda t: (t["product"], t["entry_ts"] or 0))
    return trades


def _build_trade(pid, entry, exit_):
    def f(r, k, d=0.0):
        v = r.get(k) if r else None
        return d if v is None else float(v)

    return {
        "product": pid,
        "entry_ts": f(entry, "ts") if entry else None,
        "entry_price": f(entry, "price") if entry else None,
        "exit_ts": f(exit_, "ts") if exit_ else None,
        "exit_price": f(exit_, "price") if exit_ else None,
        "pnl_usd": f(exit_, "pnl") if exit_ else None,
        "strategy": (exit_ or entry or {}).get("strategy", "unknown"),
        "size_usd": f(entry, "notional") if entry else None,
    }


def score_replay(trades):
    """Aggregate replay statistics over a list of trades from load_trade_events."""
    n = len(trades)
    with_exit = [t for t in trades if t["pnl_usd"] is not None]
    n_with_exit = len(with_exit)
    wins = [t for t in with_exit if t["pnl_usd"] > 0]
    total_pnl = sum(t["pnl_usd"] for t in with_exit)
    mean_pnl = (total_pnl / n_with_exit) if n_with_exit else 0.0
    win_rate = (len(wins) / n_with_exit) if n_with_exit else 0.0

    by_strategy = {}
    for t in with_exit:
        s = t["strategy"]
        d = by_strategy.setdefault(s, {"n": 0, "wins": 0, "pnl": 0.0})
        d["n"] += 1
        d["pnl"] += t["pnl_usd"]
        if t["pnl_usd"] > 0:
            d["wins"] += 1
    for s, d in by_strategy.items():
        d["win_rate"] = (d["wins"] / d["n"]) if d["n"] else 0.0
        d["mean_pnl"] = (d["pnl"] / d["n"]) if d["n"] else 0.0

    return {
        "n_trades": n,
        "n_with_exit": n_with_exit,
        "n_open": n - n_with_exit,
        "win_rate": win_rate,
        "total_pnl_usd": total_pnl,
        "mean_pnl_usd": mean_pnl,
        "by_strategy": by_strategy,
    }


def compare_to_backtest(trades, scorecard_results, tol_pp=20.0):
    """Compare live realized win_rate against backtested win_rate per strategy.

    ``scorecard_results`` is a dict keyed by "strategy/currency" whose values
    hold at least {strategy, passed, win_rate}. Returns a list of:
        {strategy, live_win_rate, bt_win_rate, bt_passed, consistent}.

    A strategy is consistent when the live win_rate is within ``tol_pp``
    percentage points of the backtested win_rate.
    """
    # Build strategy -> bt summary (aggregate across currencies).
    bt_by_strategy = {}
    for key, res in (scorecard_results or {}).items():
        s = res.get("strategy")
        if not s:
            continue
        d = bt_by_strategy.setdefault(s, {"passed_any": False, "win_rates": []})
        if res.get("passed"):
            d["passed_any"] = True
        wr = res.get("win_rate")
        if wr is not None:
            d["win_rates"].append(float(wr))

    # Live win_rate by strategy.
    live_by_strategy = {}
    for t in trades:
        if t["pnl_usd"] is None:
            continue
        s = t["strategy"]
        d = live_by_strategy.setdefault(s, {"n": 0, "wins": 0})
        d["n"] += 1
        if t["pnl_usd"] > 0:
            d["wins"] += 1

    out = []
    for s, live_d in live_by_strategy.items():
        if s not in bt_by_strategy:
            continue
        live_wr = live_d["wins"] / live_d["n"] if live_d["n"] else 0.0
        bt_wr = sum(bt_by_strategy[s]["win_rates"]) / len(bt_by_strategy[s]["win_rates"])
        diff_pp = abs(live_wr - bt_wr) * 100.0
        # If backtest never passed, treat live consistency cautiously: still
        # report the divergence but flag consistent only if rates match.
        consistent = diff_pp <= tol_pp
        out.append({
            "strategy": s,
            "live_win_rate": live_wr,
            "bt_win_rate": bt_wr,
            "bt_passed": bt_by_strategy[s]["passed_any"],
            "consistent": consistent,
            "divergence_pp": round(diff_pp, 2),
        })
    out.sort(key=lambda r: r["strategy"])
    return out


def _print_report(trades, score, comparison):
    print("=== REPLAY SCORING OF LIVE/PAPER TRADES ===")
    print(f"trades={score['n_trades']} with_exit={score['n_with_exit']} "
          f"open={score['n_open']}")
    print(f"win_rate={score['win_rate']:.3f} total_pnl=${score['total_pnl_usd']:.2f} "
          f"mean_pnl=${score['mean_pnl_usd']:.2f}")
    print("by_strategy:")
    for s, d in sorted(score["by_strategy"].items()):
        print(f"  {s}: n={d['n']} win_rate={d['win_rate']:.3f} "
              f"mean_pnl=${d['mean_pnl']:.2f}")
    if comparison:
        print("=== LIVE vs BACKTEST ===")
        for r in comparison:
            print(f"  {r['strategy']}: live={r['live_win_rate']:.3f} "
                  f"bt={r['bt_win_rate']:.3f} passed={r['bt_passed']} "
                  f"consistent={r['consistent']} (divergence {r['divergence_pp']}pp)")


def cli_main(events_dir=None, scorecard_path=None, product=None):
    trades = load_trade_events(events_dir, product=product)
    if not trades:
        print("No trade events found"
              + (f" at {events_dir}" if events_dir else " (auto root)."))
        return 0
    score = score_replay(trades)
    comparison = None
    if scorecard_path and os.path.isfile(scorecard_path):
        try:
            with open(scorecard_path, "r", encoding="utf-8") as fh:
                sc = json.load(fh)
            comparison = compare_to_backtest(trades, sc.get("results", {}))
        except (ValueError, json.JSONDecodeError):
            print(f"Could not parse scorecard {scorecard_path}")
    _print_report(trades, score, comparison)
    return 0


def cli():
    p = argparse.ArgumentParser(description="Replay paper trader trade events "
                                             "against backtest verdicts.")
    p.add_argument("--events-dir", default=None,
                   help="trade_events root (default: NAS_FEED_ROOT or "
                        "data/feed_cache/trade_events)")
    p.add_argument("--scorecard", default=None,
                   help="path to a backtest scorecard.json")
    p.add_argument("--product", default=None,
                   help="filter to a single product (e.g. BTC-USD)")
    args = p.parse_args()
    sys.exit(cli_main(events_dir=args.events_dir,
                      scorecard_path=args.scorecard,
                      product=args.product))


if __name__ == "__main__":
    cli()
