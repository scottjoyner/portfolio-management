#!/usr/bin/env python3
"""
hermes_agent_loop.py — one iteration of the Hermes agent's PAPER trading competition.

REGIME-GATED MOMENTUM (Phase 1 edge over the bot):
  The bot trades each pair in isolation. This loop uses BTC-USD as a cross-asset
  oracle: it classifies the BTC regime (TREND_UP / TREND_DOWN / RANGE / CRISIS)
  and GATES alt signals on regime compatibility:
    - TREND_UP   -> only LONG/BUY momentum on alts
    - TREND_DOWN  -> only SHORT/SELL momentum on alts
    - RANGE       -> either side, but require a stronger extreme to "fade"
    - CRISIS      -> STAND DOWN (no trades; event/vol risk)
  BTC itself only trades its own momentum in TREND regimes (avoids range whipsaw).

All trading is paper/simulation. A live path does not exist here; hermes_agent_trader
refuses any live attempt unless HERMES_AGENT_LIVE is set by the operator.

ENV (read-only, inherited from .env): KILL_SWITCH, REQUIRE_MANUAL_APPROVAL,
MAX_NOTIONAL_PER_TRADE_USD
"""
from __future__ import annotations

import datetime as dt
import json
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

KILL_SWITCH = os.getenv("KILL_SWITCH", "").lower() in ("1", "true", "yes")
try:
    MAX_NOTIONAL = float(os.getenv("MAX_NOTIONAL_PER_TRADE_USD", "10"))
except ValueError:
    MAX_NOTIONAL = 10.0

# alt universe: liquid majors + the bot's PROVEN-LOSING alts (where my regime
# method may have alpha) + the bot's PROVEN-WINNING alts (strategy mimicry,
# Phase 8: when my indicator confirms the SAME setup type the bot wins on, boost).
ALTS = ["ETH-USD", "SOL-USD", "AVAX-USD", "LINK-USD", "DOGE-USD", "ADA-USD",
        "DOT-USD", "NCT-USD", "PERP-USD", "STORJ-USD", "ZEC-USD", "GNO-USD",
        "IOTX-USD", "HFT-USD", "MATH-USD"]  # bot proven winners (cvd_flow/vwap_revert/obv_div)
BTC = "BTC-USD"
HOURS = 5
# momentum thresholds: smaller in RANGE (fade extremes), larger in TREND (confirm)
MOM_TREND = 0.004   # 0.4%
MOM_RANGE = 0.007   # 0.7% (stronger extreme to fade in range)
# Trade at the full cap (fees are ~0.24% round-trip; larger notional dilutes
# fee drag — profit mandate). MAX_NOTIONAL is the inherited safety cap.
NOTIONAL_PER_SIGNAL = round(MAX_NOTIONAL, 2)


def _candles(client, product_id: str, hours: int, granularity: str = "1h") -> list[dict]:
    """Live candles via the bot's OWN REST feed (fetch_candles_batch_sync) —
    the SAME real Coinbase Exchange public API the strategy engine uses, so the
    agent competes on real prices (not the frozen dry-run CLI mock). Converts the
    (ts,open,high,low,close,vol) tuples to the {close,volume,start} dicts the rest
    of the loop expects. Phase 9g: this is what makes PnL real/observable.
    NOTE: this is read-only market data — no orders, no auth."""
    try:
        from coinbase.src.rest_feed import fetch_candles_batch_sync
        gran_s = {"1h": 3600, "4h": 14400, "15m": 900}.get(granularity, 3600)
        lim = min(max(int(hours * 3600 / gran_s), 5), 300)
        out = fetch_candles_batch_sync([product_id], granularity=gran_s, limit=lim)
        rows = out.get(product_id, [])
        if rows:
            return [{"close": float(c[4]), "volume": float(c[5]),
                     "start": int(c[0])} for c in rows]
    except Exception as exc:
        # Fall back to the frozen dry-run CLI mock only if the live feed fails.
        try:
            end = dt.datetime.now(dt.timezone.utc)
            start = end - dt.timedelta(hours=hours)
            r = client._cli_json(
                "products", "candles", product_id, f"granularity={granularity}",
                f"start={start.isoformat()}", f"end={end.isoformat()}",
            )
            if isinstance(r, dict) and "candles" in r:
                return r["candles"]
        except Exception:
            pass
    return []


def momentum_signal(candles: list[dict]) -> tuple[str | None, float, float]:
    if len(candles) < 2:
        return None, 0.0, 0.0
    closes = [float(c["close"]) for c in candles if c.get("close")]
    if len(closes) < 2:
        return None, 0.0, closes[-1] if closes else 0.0
    first, last = closes[0], closes[-1]
    if first <= 0:
        return None, 0.0, last
    mom = (last - first) / first
    thr = MOM_TREND  # default; loop may pass a stricter threshold
    if mom > thr:
        return "BUY", mom, last
    if mom < -thr:
        return "SELL", mom, last
    return None, mom, last


# Phase 15: per-position exit logic (TP / SL / timeout / regime-flip).
# Keeps the book round-tripping so the expectancy table fills and we can
# OBSERVE the agent's edge vs the bot. Risk:reward = 1.5:2.0 (SL tighter than TP).
TP_LONG = 0.040    # +4.0% take-profit on longs (let winners run, bot-style)
SL_LONG = -0.010   # -1.0% stop-loss on longs (cut losers small)
TP_SHORT = -0.040  # -4.0% take-profit on shorts
SL_SHORT = 0.010   # +1.0% stop-loss on shorts
TIMEOUT_RANGE = 8  # cycles (~2h) before giving up in range
TIMEOUT_TREND = 12  # cycles (~3h) in trend
# Breakeven lock: once price moves >=1.2% in our favor, tighten SL to entry.
BE_LOCK = 0.012


def _exit_check(pid: str, pos: dict, cur: float | None, regime: str, stale: bool, local: str = ""):
    try:
        cur_f = float(cur) if cur is not None else 0.0
    except (TypeError, ValueError):
        cur_f = 0.0
    if cur_f <= 0:
        return False, ""
    is_short = pid.startswith("SHORT:")
    # longs store cost_basis (notional); shorts store entry_price (mark)
    entry = (pos.get("entry_price") if is_short else pos.get("cost_basis")) or 0.0
    if entry <= 0:
        return False, ""
    # P&L fraction (long: (cur-entry)/entry; short: (entry-cur)/entry) — both in
    # PROFIT terms (positive = we made money). The TP_/SL_ constants are expressed
    # as PRICE-MOVE direction (TP_SHORT=-0.020 = "price drops 2%"), so for shorts
    # we must NEGATE them to get pnl-space thresholds. OLD code used tp=TP_SHORT
    # (-0.020) directly in `pnl >= tp`, which fired "tp" on ANY small loss
    # (e.g. -0.007 >= -0.020) -> instant fake close + fee churn. Fix: convert.
    pnl = (cur_f - entry) / entry if not is_short else (entry - cur_f) / entry
    # Convert price-move constants (TP_SHORT=-0.020 = "price drops 2%") into
    # pnl-space thresholds (positive = profit). For shorts, a price DROP is a
    # PROFIT, so negate.
    tp_pnl = TP_LONG if not is_short else -TP_SHORT
    sl_pnl = SL_LONG if not is_short else -SL_SHORT
    # breakeven lock: if in profit >= BE_LOCK, never let it hit SL again
    eff_sl = 0.0 if pnl >= BE_LOCK else sl_pnl
    if pnl >= tp_pnl:
        return True, "tp"
    if pnl <= eff_sl:
        return True, "sl"
    # regime flip: use the POSITION'S LOCAL regime (Phase 9d) — not BTC's global
    # regime — so a long on an uptrending alt isn't force-closed just because BTC
    # says TREND_DOWN. Only the global CRISIS stand-down is shared.
    flip = local or regime
    if not is_short and flip in ("TREND_DOWN", "CRISIS"):
        return True, "regimeflip"
    if is_short and flip in ("TREND_UP", "CRISIS"):
        return True, "regimeflip"
    # timeout by regime
    if stale:
        return True, "timeout"
    return False, ""


def run_once(verbose: bool = True) -> dict:
    if KILL_SWITCH:
        msg = "KILL_SWITCH active — agent loop skipped"
        if verbose:
            print(msg)
        return {"skipped": True, "reason": "kill_switch"}

    from coinbase.src.cb_client import CBClient
    from scripts.hermes_agent_trader import (record_signal, close_position,
                                             open_short, close_short,
                                             drawdown_circuit, size_for, load_ledger)
    from scripts.hermes_regime import classify_candles, compatible_side
    from scripts.hermes_meta import load_bot_edge, asset_edge, best_bot_setups, bot_confirms
    from scripts.hermes_mtf import multi_timeframe_regime, vol_regime, conviction
    from scripts.hermes_overlay import overlay_state
    from scripts.hermes_signals import indicator_signal, local_regime
    from scripts.hermes_expectancy import universe_tilt, expectancy_table
    from scripts.hermes_portfolio import correlation_to_btc, exposure_ok

    client = CBClient(dry_run_cli=True)

    # --- Phase 6+7: MULTI-TIMEFRAME regime + VOL + SENTIMENT/EVENT overlay ---
    mtf = multi_timeframe_regime("BTC-USD", client)
    regime = mtf["regime"]  # MIXED if <2/3 timeframes agree (fakeout guard)
    # vol regime on the 4h BTC window (stand down new entries if EXTREME)
    btc_4h = _candles(client, "BTC-USD", 120, granularity="4h")
    vol = vol_regime(btc_4h)
    overlay = overlay_state()  # sentiment (F&G) + macro-event risk
    from scripts.hermes_news import news_sentiment
    news = news_sentiment()  # Phase 14: news-flow sentiment (soft overlay)
    if verbose:
        print(f"[mtf] {regime} votes={mtf['votes']} vol={vol['bucket']} "
              f"fg={overlay['fear_greed'].get('bucket')} ev={overlay['event']['reason']} "
              f"news={news['bucket']}({news['score']})")

    # Composite NEW-ENTRY gate (any of these close the gate; open positions still managed):
    #  - MIXED regime (timeframes disagree)
    #  - EXTREME vol
    #  - drawdown circuit tripped (Phase 4)
    #  - EXTREME_GREED + macro event (sell-the-news, Phase 7)
    circuit = drawdown_circuit()
    circuit_open = circuit["open"]
    entry_gate_open = (regime != "MIXED" and not vol["stand_down"]
                      and circuit_open and not overlay["stand_down_new"]
                      and not news["stand_down_new"])  # Phase 14: news panic soft-gate
    if verbose and not entry_gate_open:
        why = ("MIXED" if regime == "MIXED" else
               "EXTREME_VOL" if vol["stand_down"] else
               f"circuit:{circuit['reason']}" if not circuit_open else
               "sentiment_standdown" if overlay["stand_down_new"] else
               "news_panic")
        print(f"[gate] CLOSED new entries: {why}")

    # quick stand-down on CRISIS / MIXED / EXTREME_VOL
    if regime in ("CRISIS", "MIXED") or vol["stand_down"]:
        # still run close-pass below, but skip new entries
        pass
    # Meta-layer: load the bot's own per-asset edge once per run.
    bot_edge = load_bot_edge()
    from scripts.hermes_agent_trader import load_ledger, close_position, close_short
    led = load_ledger()
    # Phase 10: own-paper universe tilt (auto-drop assets the AGENT keeps losing on)
    tilt = universe_tilt(led)
    # Phase 11: BTC-correlation cache (gates concentration risk). Computed once
    # per run; ~15 assets × 120 candles. Skip in CRISIS/MIXED (no new entries).
    corr_cache: dict = {}
    if entry_gate_open:
        for a in ALTS:
            try:
                corr_cache[a] = round(correlation_to_btc(a, client), 2)
            except Exception:
                corr_cache[a] = 0.0
    HOLD_ITERS = 6
    now = dt.datetime.now(dt.timezone.utc)
    # Phase 15: per-position TP/SL/timeout exit — without this, paper trades
    # never round-trip (only closed on CRISIS/stale), so the expectancy table
    # stays empty and we can't OBSERVE whether the agent beats the bot.
    for pid, pos in list(led.get("positions", {}).items()):
        if pos.get("base", 0.0) <= 1e-12:
            continue
        entry = pos.get("entry_ts")
        stale = False
        if entry:
            try:
                age = (now - dt.datetime.fromisoformat(entry)).total_seconds() / 60.0
                stale = age > HOLD_ITERS * 15  # cron cadence = 15m
            except Exception:
                stale = False
        # current price for this position's asset — use the SAME 120h window last
        # close as entry (Phase 9f: consistent mark source, no window mismatch that
        # distorts PnL between entry and exit). Also reuse it for local-regime.
        true_pid = pid.replace("SHORT:", "")
        try:
            cd = _candles(client, true_pid, 120)
            cur = cd[-1]["close"] if cd else None
        except Exception:
            cur = None
        # Phase 9d: compute the position's LOCAL regime for flip-exit logic.
        local = ""
        try:
            local = local_regime(cd) if cd else ""
        except Exception:
            local = ""
        close_now, reason = _exit_check(pid, pos, cur, regime, stale, local)
        if close_now:
            px = float(cur) if cur is not None else None
            if pid.startswith("SHORT:"):
                res = close_short(true_pid, note=f"exit-{regime}-{reason}", price=px)
                if verbose and res.get("action") == "short_closed":
                    print(f"[close-short] {true_pid} {reason} -> pnl={res.get('realized_pnl')}")
            else:
                res = close_position(pid, note=f"exit-{regime}-{reason}", price=px)
                if verbose and res.get("action") == "closed":
                    print(f"[close] {pid} {reason} -> pnl={res.get('realized_pnl')}")

    if regime == "CRISIS":
        return {"skipped": True, "reason": "crisis_standdown", "regime": regime}

    results = []

    # --- BTC itself: trade its OWN local regime direction (Phase 9d), not the
    # global MTF vote. Long only if BTC's 120h candles are TREND_UP, short if
    # TREND_DOWN. Skip in RANGE/CRISIS. This stops whipsaw longs in a downtrend. ---
    btc_candles = _candles(client, BTC, 120)
    btc_local = local_regime(btc_candles)
    if btc_local == "TREND_UP":
        side, mom, last = momentum_signal(btc_candles)
        if side == "BUY":
            rec = record_signal(BTC, "BUY", quote_size=NOTIONAL_PER_SIGNAL,
                                note=f"btc-localUP-mom{mom:.4f}",
                                price=last if last and last > 0 else None)
            results.append({"pair": BTC, "signal": "BUY", "mom": round(mom, 5),
                           "result": rec.get("action", "?")})
    elif btc_local == "TREND_DOWN":
        side, mom, last = momentum_signal(btc_candles)
        if side == "SELL":
            rec = open_short(BTC, NOTIONAL_PER_SIGNAL,
                             note=f"btc-localDOWN-mom{mom:.4f}",
                             price=last if last and last > 0 else None)
            results.append({"pair": BTC, "signal": "SHORT", "mom": round(mom, 5),
                           "result": rec.get("action", "?")})
    else:
        results.append({"pair": BTC, "signal": "HOLD", "reason": f"range:{btc_local}"})

    # --- Alts: regime-gated, META-filtered, BOTH SIDES, INDICATOR-driven (P1-9) ---
    # TREND_UP   -> LONG on trend-confirmed setups (z>0, price>EMA, rsi>50)
    # TREND_DOWN  -> SHORT on confirmed downtrend setups
    # RANGE       -> mean-reversion fades (Bollinger %B extremes + z-score)
    # Meta-filter: bot_bleeds_here demands 1.5x stronger own-signal.
    # Phase 8 boost: if the BOT provably wins on this asset with the SAME
    #   setup type the agent's indicator just confirmed -> boost conviction.
    for pair in ALTS:
        try:
            ed = asset_edge(pair, bot_edge)
            # Wider window (120h @1h = 120 bars) so indicators have enough
            # samples; the dry-run CLI caps the default HOURS window at ~5 bars.
            candles = _candles(client, pair, 120)
            # Phase 9d: trade each asset on its OWN local regime, not BTC's shadow.
            # The global gate below still enforces risk-off (vol/circuit/news/MIXED).
            local = local_regime(candles)
            # Phase 9: indicator overlay (bot's own TechnicalIndicatorSet math)
            side, strength, det = indicator_signal(candles, local)
            last = float(candles[-1]["close"]) if candles and candles[-1].get("close") else 0.0
            if side == "HOLD":
                results.append({"pair": pair, "signal": "HOLD", "mom": None,
                               "regime": regime, "detail": det.get("reason")})
                continue
            mom = det.get("z", 0.0)  # use z-score as the "momentum" proxy for sizing
            setup = det.get("setup", "other")
            # Phase 8: strategy mimicry — does the bot's backtest endorse THIS
            # asset+setup type? Computed early so the meta-filter (below) can use it.
            setup_type = "mean_revert" if "revert" in setup or "fade" in setup else \
                          "trend" if "trend" in setup else "other"
            conf = bot_confirms(pair, setup_type, best_bot_setups())

            # Decide desired action from regime + indicator side
            want_long = side == "BUY"
            want_short = side == "SELL"
            if not (want_long or want_short):
                results.append({"pair": pair, "signal": "HOLD", "mom": round(mom, 4),
                               "regime": regime})
                continue

            # Composite gate (Phase 4+6+7)
            if not entry_gate_open:
                why = ("MIXED" if regime == "MIXED" else
                       "EXTREME_VOL" if vol["stand_down"] else
                       f"circuit:{circuit['reason']}" if not circuit_open else
                       "sentiment_standdown" if overlay["stand_down_new"] else
                       "news_panic")
                results.append({"pair": pair, "signal": "HOLD", "mom": round(mom, 4),
                               "reason": f"gate:{why}"})
                continue

            # Meta-filter (Phase 9e / profit mandate): the bot's OWN backtest is
            # the ground truth. Trade ONLY assets where the bot shows positive edge.
            #  - bot_bleeds_here -> SKIP entirely (bot loses here; don't trade blind)
            #  - bot_wins_here   -> MIMIC (trade same direction; confirmed alpha)
            #  - unknown/neutral -> SKIP (no edge evidence; don't trade blind)
            verdict = ed["verdict"]
            if verdict != "bot_wins_here":
                results.append({"pair": pair, "signal": "HOLD", "mom": round(mom, 4),
                               "reason": f"meta:{verdict}", "bot_edge": ed["edge"]})
                continue

            # Phase 8: strategy mimicry boost — conf already computed above (asset+setup
            # endorsement). If confirmed, this is validated alpha (gated).
            conf_mult = 1.25 if conf["confirmed"] else 1.0

            # Phase 10: own-paper tilt — drop assets the AGENT itself keeps losing
            at = tilt.get(pair)
            if at and at["tilt"] == "drop":
                results.append({"pair": pair, "signal": "HOLD", "mom": round(mom, 4),
                               "reason": "agent_drop",
                               "agent_pnl": at["agent_pnl"]})
                continue

            # Phase 4+6+7+8 sizing: strength × vol-conviction × sentiment × mimicry
            size_mult = conviction(mom, vol["bucket"]) * overlay["size_mult"] * conf_mult * news["size_mult"]
            notional = round(min(size_for(mom), size_for(0.02)) * strength * size_mult, 2)
            notional = min(notional, MAX_NOTIONAL)

            # Phase 11: concentration gate — don't let BTC-correlated exposure
            # exceed the cap (a single BTC shock must not wreck the whole book).
            ok, why = exposure_ok(pair, "BUY" if want_long else "SHORT",
                                  notional, corr_cache, led)
            if not ok:
                results.append({"pair": pair, "signal": "HOLD", "mom": round(mom, 4),
                               "reason": f"gate:{why}", "corr": corr_cache.get(pair)})
                continue

            # One position per asset at a time — don't pyramid on every tick.
            long_open = led["positions"].get(pair, {}).get("base", 0.0) > 1e-9
            short_open = led["positions"].get(f"SHORT:{pair}", {}).get("base", 0.0) > 1e-9
            if long_open or short_open:
                results.append({"pair": pair, "signal": "HOLD", "mom": round(mom, 4),
                               "reason": "already_open"})
                continue

            if want_long:
                rec = record_signal(pair, "BUY", quote_size=notional,
                                    note=f"regime-{regime}-LONG-{setup}-mom{mom:.3f}"
                                         f"-bot:{ed['verdict']}-conf:{conf['confirmed']}-sz{notional}",
                                    regime=regime, setup=setup,
                                    price=last if last and last > 0 else None)
                results.append({"pair": pair, "signal": "BUY", "mom": round(mom, 4),
                               "size": notional, "bot_edge": ed["edge"],
                               "confirmed": conf["confirmed"],
                               "result": rec.get("action", "?")})
            else:  # short
                rec = open_short(pair, notional,
                               note=f"regime-{regime}-SHORT-{setup}-mom{mom:.3f}"
                                    f"-bot:{ed['verdict']}-conf:{conf['confirmed']}-sz{notional}",
                               regime=regime, setup=setup,
                               price=last if last and last > 0 else None)
                results.append({"pair": pair, "signal": "SHORT", "mom": round(mom, 4),
                               "size": notional, "bot_edge": ed["edge"],
                               "confirmed": conf["confirmed"],
                               "result": rec.get("action", "?")})
        except Exception as exc:
            results.append({"pair": pair, "signal": "ERROR", "error": str(exc)[:120]})

    if verbose:
        ts = dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds")
        print(f"[{ts}] agent loop: {json.dumps(results)}")
    return {"skipped": False, "regime": regime, "iterations": results}


def main() -> int:
    ap = __import__("argparse").ArgumentParser()
    ap.add_argument("--quiet", action="store_true")
    args = ap.parse_args()
    run_once(verbose=not args.quiet)
    return 0


if __name__ == "__main__":
    sys.exit(main())
