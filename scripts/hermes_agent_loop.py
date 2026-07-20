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
    MAX_NOTIONAL = float(os.getenv("MAX_NOTIONAL_PER_TRADE_USD", "250"))
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
# BTC entry bar (2026-07-17): BTC must show >= this |momentum| over the 120h
# window before we commit capital. Higher than the generic MOM_TREND floor to
# stop marginal-drift longs that immediately stop out and grind P&L.
BTC_MIN_MOM = 0.015   # 1.5%
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
            return [{"close": float(c[4]), "open": float(c[1]), "high": float(c[2]),
                     "low": float(c[3]), "volume": float(c[5]),
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
# CATASTROPHIC hard stop (2026-07-17): fires REGARDLESS of breakeven/trail state.
# Because exits are only evaluated every ~15 min, price can gap between checks;
# this is the backstop that guarantees a bounded loss per position even if the
# breakeven lock or trailing logic would otherwise hold. Expressed in pnl-space
# (negative = loss). Aggressive book => a hard -2.5% floor on any single trade.
HARD_STOP = -0.025
TIMEOUT_RANGE = 6  # cycles (~1.5h) before giving up in range (was 8 — cut dead trades faster)
TIMEOUT_TREND = 10  # cycles (~2.5h) in trend (was 12)
# Breakeven lock: once price moves >=0.8% in our favor, tighten SL to entry.
# Tightened 1.2% -> 0.8% (2026-07-17): with only 15-min checks we can't react
# intraday, so bank the "can't lose" state earlier.
BE_LOCK = 0.008
# Phase 18 (edge): trailing stop — once a winner is in profit, lock in gains by
# closing if it retraces TRAIL_PCT from its peak profit. Only active after BE_LOCK.
TRAIL_PCT = 0.012   # 1.2% retrace from peak profit triggers exit (was 1.8%)
MAX_ADDS = 2       # pyramid at most twice into a single winner
# Vol-scaled TP multiplier: in low-vol/trending regimes let winners run further;
# in choppy/extreme-vol regimes take profits sooner.
VOL_TP_MULT = {"LOW": 1.25, "NORMAL": 1.0, "HIGH": 0.85, "EXTREME": 0.7}
ADD_PROFIT_THRESHOLD = 0.020  # add-to-winner only once in profit >= 2%
# Phase 18b: deployment headroom — aim for a DIVERSIFIED book, not 1 position.
# Cap correlated exposure at 40% of live equity (scales with the snowball) and
# allow up to MAX_OPEN_POSITIONS concurrent trades across uncorrelated alts.
MAX_OPEN_POSITIONS = 8        # target diversified book size
CORR_CAP_FRAC = 0.40          # max BTC-correlated notional = equity * this

# ════════════════════════════════════════════════════════════════════════════
# PHASE 19 — EDGE PACK (10 same-team / regime / risk refinements, all paper-only)
# ════════════════════════════════════════════════════════════════════════════
# 1) Session-aware aggression: deploy bigger during high-liquidity windows.
SESSION_BOOST = {"us": 1.30, "asia": 1.10, "eu": 1.05, "dead": 0.80}
def session_bucket(now=None):
    """UTC hour -> liquidity window. US cash + EU overlap = deepest liquidity."""
    h = (now or dt.datetime.now(dt.timezone.utc)).hour
    if 13 <= h < 21:        # 09:00-17:00 ET
        return "us"
    if 0 <= h < 9:          # Asia / early EU
        return "asia"
    if 9 <= h < 13:         # EU morning
        return "eu"
    return "dead"           # 21:00-23:59 UTC = thin

# 2) Regime-adaptive leverage: hotter only when edge aligns, never blind.
def adaptive_leverage(local: str, vol_bucket: str) -> float:
    if local in ("TREND_UP", "TREND_DOWN") and vol_bucket in ("LOW", "NORMAL"):
        return 5.0          # confirmed trend + calm vol -> press
    if local in ("TREND_UP", "TREND_DOWN") and vol_bucket == "HIGH":
        return 3.0
    if vol_bucket == "EXTREME":
        return 2.0          # stand-down vol -> defensive
    return 3.0              # default aggressive baseline

# 5) ATR-based stop distance (replaces flat % SL). Per-asset realized vol.
def atr_from_candles(candles: list, n: int = 20) -> float:
    if not candles or len(candles) < 3:
        return 0.0
    trs = []
    for i in range(1, min(n, len(candles))):
        c0, c1 = candles[-i - 1], candles[-i]
        hi = float(c1.get("high", c1["close"]))
        lo = float(c1.get("low", c1["close"]))
        pc = float(c0["close"])
        tr = max(hi - lo, abs(hi - pc), abs(lo - pc))
        trs.append(tr / pc if pc > 0 else 0.0)
    return (sum(trs) / len(trs)) if trs else 0.0

# 7) Bot's HOT strategies right now (same-team meta). Returns {strategy: score}.
def bot_hot_strategies(min_trades: int = 3) -> dict:
    out = {}
    try:
        import json as _j
        from pathlib import Path as _p
        p = _p(__file__).resolve().parent.parent / "data" / "paper_trader_v4_state.json"
        d = _j.loads(p.read_text())
        ss = d.get("strategy_stats", {})
        for s, v in ss.items():
            n = v.get("trades", 0)
            if n < min_trades:
                continue
            wr = (v.get("wins", 0) / n * 100.0) if n else 0.0
            pnl = v.get("pnl", 0.0)
            if wr >= 55 and pnl > 0:
                out[s] = round(pnl, 2)
    except Exception:
        return out
    return out

# 3) Bot cooldown avoidance: skip entry on assets the bot JUST exited (5m cooldown).
def bot_recent_closes(minutes: int = 5) -> dict:
    out = {}
    try:
        import json as _j
        from pathlib import Path as _p
        p = _p(__file__).resolve().parent.parent / "data" / "paper_trader_v4_state.json"
        d = _j.loads(p.read_text())
        cutoff = dt.datetime.now(dt.timezone.utc).timestamp() - minutes * 60.0
        for t in d.get("paper_trades", []):
            ts = t.get("ts", 0.0)
            if ts < cutoff:
                continue
            r = str(t.get("reason", "")).lower()
            if "exit" in r or "tp" in r or "sl" in r or t.get("side") == "SELL":
                out[t.get("product_id", "")] = ts
    except Exception:
        return out
    return out

# 8) Book-level take-profit: once the session book is up >= BOOK_TP, tighten all
#    exits to breakeven (bank the snowball before mean-reversion erases it).
BOOK_TP = 0.05  # +5% session -> flip to capital-preservation mode
def book_tp_active(equity: float, start: float = 10000.0) -> bool:
    return (equity - start) / start >= BOOK_TP

# 9) Net BTC-correlated exposure (for cross-hedge decision).
def net_corr_exposure(led: dict, corr_cache: dict, threshold: float = 0.5) -> float:
    net = 0.0
    for pid, pos in led.get("positions", {}).items():
        if pos.get("base", 0.0) <= 1e-12:
            continue
        asset = pid.replace("SHORT:", "")
        r = abs(corr_cache.get(asset, 0.0))
        if r < threshold:
            continue
        is_short = pid.startswith("SHORT:")
        notional = (abs(pos.get("entry_price", 0.0) * pos["base"]) if is_short
                    else abs(pos.get("cost_basis", 0.0)))
        # long adds to net long exposure, short subtracts
        net += notional if not is_short else -notional
    return net

HEDGE_CORR_NET = 1500.0   # if net long-correlated > $1500, open a BTC short hedge


def _exit_check(pid: str, pos: dict, cur: float | None, regime: str, stale: bool,
                local: str = "", vol_bucket: str = "NORMAL", bot_coholds: bool = False,
                atr: float = 0.0, book_tp: bool = False):
    try:
        cur_f = float(cur) if cur is not None else 0.0
    except (TypeError, ValueError):
        cur_f = 0.0
    if cur_f <= 0:
        return False, ""
    is_short = pid.startswith("SHORT:")
    # Both longs and shorts store the fill PRICE in entry_price. (Bug fix
    # 2026-07-17: longs previously read cost_basis here — the total NOTIONAL in
    # dollars, not a price — so pnl=(cur-250)/250 computed a ~255x fake profit
    # and instantly tripped TP on every long, every tick. Now both sides use the
    # real entry price. Fall back to cost_basis/base as a last resort.)
    entry = pos.get("entry_price") or 0.0
    if entry <= 0:
        base = pos.get("base", 0.0)
        cb = pos.get("cost_basis", 0.0)
        entry = (cb / base) if base > 1e-12 and cb > 0 else 0.0
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
    tp_mult = VOL_TP_MULT.get(vol_bucket, 1.0)
    # Same-team confirmation: if the bot co-holds the same side, trust the winner
    # a bit more — widen TP slightly and engage the trailing stop earlier.
    if bot_coholds:
        tp_mult *= 1.10
    tp_pnl = (TP_LONG if not is_short else -TP_SHORT) * tp_mult
    # Phase 19.5: ATR-based stop — replace the flat % SL with the asset's own
    # realized vol. Tight for low-vol assets (stop churn), wide for high-vol
    # (don't get chopped). Fallback to the flat constants if ATR is unknown.
    if atr and atr > 0:
        sl_pnl = atr * (1.2 if is_short else 1.2)  # 1.2x ATR stop distance
        if is_short:
            sl_pnl = -sl_pnl
    else:
        sl_pnl = SL_LONG if not is_short else -SL_SHORT
    # Phase 19.8: book take-profit — once the session book is up >= BOOK_TP,
    # lock every winner to breakeven (capital preservation over more upside).
    be_lock = BE_LOCK if not book_tp else 0.0
    # breakeven lock: if in profit >= BE_LOCK, never let it hit SL again
    eff_sl = 0.0 if pnl >= be_lock else sl_pnl
    # Phase 18: trailing stop — track peak profit, exit on retrace from peak.
    peak = pos.get("trail_peak", pnl)
    if pnl > peak:
        peak = pnl
        pos["trail_peak"] = round(peak, 6)
    # CATASTROPHIC hard stop — checked FIRST and unconditionally. Because exits
    # are only evaluated every ~15 min, a position can gap far past its normal SL
    # (or past a breakeven lock) between checks. This floor guarantees a bounded
    # loss regardless of breakeven/trail state. Never suppressed.
    if pnl <= HARD_STOP:
        return True, "hardstop"
    trail_active = peak >= be_lock
    if trail_active and pnl <= peak - TRAIL_PCT:
        return True, "trail"
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
                                             drawdown_circuit, size_for, load_ledger,
                                             update_equity, add_to_position,
                                             bot_recent_fills)
    from scripts.hermes_regime import classify_candles, compatible_side
    from scripts.hermes_meta import load_bot_edge, asset_edge, best_bot_setups, bot_confirms
    from scripts.hermes_mtf import multi_timeframe_regime, vol_regime, conviction
    from scripts.hermes_overlay import overlay_state
    from scripts.hermes_signals import indicator_signal, local_regime, flow_signal
    from scripts.hermes_expectancy import universe_tilt, expectancy_table
    from scripts.hermes_portfolio import correlation_to_btc, exposure_ok, CORRELATED_CAP_USD

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
    # Phase 18 (same-team): pull the bot's live fills once per run so we can
    # (a) widen TP / trail earlier when the bot co-holds our side, and
    # (b) pyramid into winners the bot also endorses.
    bot_fills = bot_recent_fills(minutes=20)
    # Phase 19.7: bot's HOT strategies right now (same-team meta-endorsement).
    bot_hot = bot_hot_strategies(min_trades=3)
    # Anti-churn (2026-07-17): assets whose position we CLOSE this tick must not
    # be re-opened by the entry scan in the SAME tick — otherwise a still-valid
    # momentum signal round-trips the position (close+reopen) and pays double fees
    # for no net change. One-tick cooldown; the asset is eligible again next run.
    closed_this_tick: set[str] = set()
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
        # Phase 19.8: book take-profit flag (tighten all exits to BE once up >=5%)
        book_tp = book_tp_active(led.get("equity", 10000.0),
                                  led.get("starting_capital", 10000.0))
        # current price for this position's asset — use the SAME 120h window last
        # close as entry (Phase 9f: consistent mark source, no window mismatch that
        # distorts PnL between entry and exit). Also reuse it for local-regime + ATR.
        true_pid = pid.replace("SHORT:", "")
        is_short_pos = pid.startswith("SHORT:")
        cd = None
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
        # Phase 19.5: ATR for this position's asset (ATR-based stop distance).
        atr = atr_from_candles(cd) if cd else 0.0
        # Phase 18: same-team co-hold check (bot trading same side, last 20m)
        bot_side = bot_fills.get(true_pid, {}).get("side")
        want_side = "SELL" if is_short_pos else "BUY"
        bot_coholds = bot_side is not None and bot_side == want_side
        close_now, reason = _exit_check(pid, pos, cur, regime, stale, local,
                                        vol_bucket=vol["bucket"], bot_coholds=bot_coholds,
                                        atr=atr, book_tp=book_tp)
        if close_now:
            px = float(cur) if cur is not None else None
            if is_short_pos:
                res = close_short(true_pid, note=f"exit-{regime}-{reason}", price=px,
                                  regime=regime)
                if verbose and res.get("action") == "short_closed":
                    print(f"[close-short] {true_pid} {reason} -> pnl={res.get('realized_pnl')}")
            else:
                res = close_position(pid, note=f"exit-{regime}-{reason}", price=px,
                                     regime=regime)
                if verbose and res.get("action") == "closed":
                    print(f"[close] {pid} {reason} -> pnl={res.get('realized_pnl')}")
            closed_this_tick.add(true_pid)  # anti-churn: block same-tick re-entry
            continue
        # Phase 18: ADD-TO-WINNER — pyramid into a confirmed winner the bot also
        # endorses. Only when: in profit >= 2%, local regime still favors us,
        # under MAX_ADDS, and we have margin headroom. Scales the snowball legally.
        # Phase 19.6: also ADD on a FLOW REVERSAL in our favor (CVD/OBV flipped
        # our way) even if not yet at +2% — catches momentum turning early.
        pos["trail_peak"] = pos.get("trail_peak", 0.0)
        # entry PRICE (both sides store it in entry_price; cost_basis is dollars).
        entry_px = pos.get("entry_price") or 0.0
        if entry_px > 0 and cur:
            pnl_now = ((cur - entry_px) / entry_px if not is_short_pos
                       else (entry_px - cur) / entry_px)
        else:
            pnl_now = 0.0
        adds = pos.get("adds", 0)
        same_dir = ((not is_short_pos and local in ("TREND_UP", "RANGE"))
                    or (is_short_pos and local in ("TREND_DOWN", "RANGE")))
        # Phase 19.6: detect a flow reversal favoring our side
        flow_reversal = False
        try:
            f_side, _, _ = flow_signal(cd) if cd else ("HOLD", 0, {})
            want_f = "BUY" if not is_short_pos else "SELL"
            flow_reversal = (f_side == want_f)
        except Exception:
            flow_reversal = False
        add_trigger = ((pnl_now >= ADD_PROFIT_THRESHOLD or flow_reversal)
                       and adds < MAX_ADDS and same_dir and bot_coholds
                       and entry_gate_open)
        if add_trigger:
            add_margin = min(MAX_NOTIONAL * 0.40, MAX_NOTIONAL - (pos.get("cost_basis", 0.0)))
            if add_margin >= 20.0:
                ar = add_to_position(true_pid, add_margin, price=cur,
                                     note=f"add-{('winner' if pnl_now>=ADD_PROFIT_THRESHOLD else 'flowrev')}-pnl{pnl_now:.3f}-botcohold")
                if verbose and ar.get("action") == "position_added":
                    print(f"[add] {true_pid} +${add_margin} (pyramid #{adds+1}, pnl={pnl_now:.3f}, flowrev={flow_reversal})")

    # Phase 19.9: CROSS-POSITION HEDGE — if the book is net LONG a pile of
    # BTC-correlated alts (> HEDGE_CORR_NET), open a small BTC short to cap the
    # single-shock risk we just allowed by widening the correlation cap. Only when
    # no BTC short is already on and the gate is open (don't hedge into a crash).
    if entry_gate_open:
        try:
            held = [p.replace("SHORT:", "") for p, v in led.get("positions", {}).items()
                    if v.get("base", 0.0) > 1e-12]
            hcache = {}
            for a in set(held):
                try:
                    hcache[a] = round(correlation_to_btc(a, client), 2)
                except Exception:
                    hcache[a] = 0.0
            net_long_corr = net_corr_exposure(led, hcache, threshold=0.5)
            btc_short_open = led["positions"].get(f"SHORT:BTC-USD", {}).get("base", 0.0) > 1e-12
            if net_long_corr > HEDGE_CORR_NET and not btc_short_open:
                hedge_margin = min(MAX_NOTIONAL * 0.50, net_long_corr * 0.30)
                if hedge_margin >= 20.0:
                    hr = open_short("BTC-USD", hedge_margin,
                                    note=f"hedge-netlongcorr{net_long_corr:.0f}",
                                    leverage=adaptive_leverage("TREND_UP", vol["bucket"]))
                    if verbose and hr.get("action") == "short_opened":
                        print(f"[hedge] BTC short ${hedge_margin} (net long-corr=${net_long_corr:.0f})")
        except Exception as exc:
            if verbose:
                print(f"[hedge] skipped: {exc}")

    if regime == "CRISIS":
        return {"skipped": True, "reason": "crisis_standdown", "regime": regime}

    results = []

    # --- BTC itself: trade its OWN local regime direction (Phase 9d), not the
    # global MTF vote. Long only if BTC's 120h candles are TREND_UP, short if
    # TREND_DOWN. Skip in RANGE/CRISIS. This stops whipsaw longs in a downtrend. ---
    btc_candles = _candles(client, BTC, 120)
    btc_local = local_regime(btc_candles)
    if BTC in closed_this_tick:
        # Anti-churn: BTC was closed this tick — don't round-trip it back on.
        results.append({"pair": BTC, "signal": "HOLD", "reason": "closed_this_tick"})
    elif btc_local == "TREND_UP":
        side, mom, last = momentum_signal(btc_candles)
        # BTC entry bar (2026-07-17): require momentum >= BTC_MIN_MOM, not just the
        # generic MOM_TREND floor. A marginal +0.4% drift kept opening a BTC long
        # that immediately stopped out at -1% every few ticks, grinding P&L. Demand
        # a real move before committing capital to BTC.
        if side == "BUY" and abs(mom) >= BTC_MIN_MOM:
            # Phase 10 guard: honor the agent's own universe tilt. If BTC is a
            # proven loser (universe_tilt drop), do NOT open here — this local-
            # momentum path otherwise bypasses the tilt check below and keeps
            # grinding P&L on BTC (the agent's single biggest bleed, -$117).
            _bt_tilt = tilt.get(BTC, {}).get("tilt")
            if _bt_tilt == "drop":
                results.append({"pair": BTC, "signal": "HOLD", "mom": round(mom, 5),
                                "reason": "agent_drop(btc)"})
            else:
                rec = record_signal(BTC, "BUY", quote_size=NOTIONAL_PER_SIGNAL,
                                    note=f"btc-localUP-mom{mom:.4f}",
                                    price=last if last and last > 0 else None)
                results.append({"pair": BTC, "signal": "BUY", "mom": round(mom, 5),
                               "result": rec.get("action", "?")})
        else:
            results.append({"pair": BTC, "signal": "HOLD", "mom": round(mom, 5),
                           "reason": f"mom<{BTC_MIN_MOM}"})
    elif btc_local == "TREND_DOWN":
        side, mom, last = momentum_signal(btc_candles)
        if side == "SELL" and abs(mom) >= BTC_MIN_MOM:
            # Same Phase 10 guard: if BTC is a proven loser, don't short it either.
            _bt_tilt = tilt.get(BTC, {}).get("tilt")
            if _bt_tilt == "drop":
                results.append({"pair": BTC, "signal": "HOLD", "mom": round(mom, 5),
                                "reason": "agent_drop(btc)"})
            else:
                rec = open_short(BTC, NOTIONAL_PER_SIGNAL,
                                 note=f"btc-localDOWN-mom{mom:.4f}",
                                 price=last if last and last > 0 else None)
                results.append({"pair": BTC, "signal": "SHORT", "mom": round(mom, 5),
                               "result": rec.get("action", "?")})
        else:
            results.append({"pair": BTC, "signal": "HOLD", "mom": round(mom, 5),
                           "reason": f"mom<{BTC_MIN_MOM}"})
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
        # Anti-churn: skip any asset we just closed this tick (see closed_this_tick).
        if pair in closed_this_tick:
            continue
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
            # Phase 9h: ORDER-FLOW overlay — if the trend/mean-revert indicator
            # holds, try the bot's FLOW edge (cvd_flow / obv_div). This lets the
            # agent mirror the bot's proven flow winners (IOTX/MATH/HFT).
            if side == "HOLD":
                f_side, f_strength, f_det = flow_signal(candles)
                if f_side != "HOLD":
                    side, strength, det = f_side, f_strength, f_det
            last = float(candles[-1]["close"]) if candles and candles[-1].get("close") else 0.0
            if side == "HOLD":
                results.append({"pair": pair, "signal": "HOLD", "mom": None,
                               "regime": regime, "detail": det.get("reason")})
                continue
            mom = det.get("z", strength)  # z-score for trend; flow strength otherwise
            setup = det.get("setup", "other")
            # Phase 8: strategy mimicry — does the bot's backtest endorse THIS
            # asset+setup type? Computed early so the meta-filter (below) can use it.
            setup_type = ("flow" if ("flow" in setup or "cvd" in setup or "obv" in setup or "div" in setup)
                          else "mean_revert" if ("revert" in setup or "fade" in setup)
                          else "trend" if "trend" in setup else "other")
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
            #  - bot_wins_here   -> MIMIC (trade same direction; confirmed alpha) FULL size
            #  - unknown/neutral -> MOONSHOT: trade only on HIGH-conviction flow/trend
            #                      signals (strength>=0.5), smaller size. The agent is
            #                      the aggressive book — it takes shots the conservative
            #                      bot won't. Still skips bot_bleeds_here (proven losers).
            #  - bot_bleeds_here -> SKIP (don't fight a proven loser; that's not aggression,
            #                      it's just burning margin)
            verdict = ed["verdict"]
            is_moonshot = verdict in ("unknown", "neutral") and strength >= 0.5
            if verdict == "bot_bleeds_here":
                results.append({"pair": pair, "signal": "HOLD", "mom": round(mom, 4),
                               "reason": f"meta:{verdict}", "bot_edge": ed["edge"]})
                continue
            if verdict != "bot_wins_here" and not is_moonshot:
                results.append({"pair": pair, "signal": "HOLD", "mom": round(mom, 4),
                               "reason": f"meta:{verdict}", "bot_edge": ed["edge"]})
                continue

            # Phase 10: own-paper tilt — drop assets the AGENT itself keeps losing
            at = tilt.get(pair)
            if at and at["tilt"] == "drop":
                results.append({"pair": pair, "signal": "HOLD", "mom": round(mom, 4),
                               "reason": "agent_drop",
                               "agent_pnl": at["agent_pnl"]})
                continue
            # Phase 19.4: OWN WINNER TILT — the agent's own positive paper track
            # record on this asset is the best signal we have. Boost size when the
            # agent has banked P&L here before (it already knows this asset).
            agent_boost = 1.0
            if at and at["tilt"] == "boost":
                agent_boost = 1.20
            # Phase 19.3: BOT COOLDOWN AVOIDANCE — if the bot just EXITED this
            # asset (it's in its 5m paper cooldown), don't fight the exit with a
            # fresh entry. Skip cleanly.
            if pair in bot_recent_closes(5):
                results.append({"pair": pair, "signal": "HOLD", "mom": round(mom, 4),
                               "reason": "bot_cooldown"})
                continue
            # Phase 19.10: SHARPER SENTIMENT — extreme greed = sell-the-news risk.
            # If F&G is EXTREME_GREED, shrink new LONG size hard (don't buy the top);
            # shorts are fine (fade the euphoria).
            fg_bucket = overlay["fear_greed"].get("bucket", "")
            greed_penalty = 0.5 if (fg_bucket == "EXTREME_GREED" and want_long) else 1.0

            # Phase 8: strategy mimicry boost — conf already computed above (asset+setup
            # endorsement). If confirmed, this is validated alpha (gated).
            conf_mult = 1.25 if conf["confirmed"] else 1.0
            # Phase 19.7: BOT-HOT-STRATEGY MIRROR — if the bot's backtest is
            # currently HOT on the strategy type the agent just confirmed, that's
            # a same-team meta-endorsement. Boost size.
            if setup_type in bot_hot and bot_hot.get(setup_type, 0) > 0:
                conf_mult *= 1.12
            # Phase 18 (same-team): if the bot just opened the SAME side on this
            # asset in the last 20m, that's a free live confirmation — boost size.
            # The bot is the conservative book; its live fill endorses our alpha.
            bot_fill = bot_fills.get(pair, {})
            if bot_fill and bot_fill.get("side") == ("BUY" if want_long else "SELL"):
                conf_mult *= 1.15

            # Phase 19.1: SESSION-AWARE AGGRESSION — deploy bigger in liquid windows
            # (US/EU), stand down size in dead zones. Our fill quality is better when
            # real liquidity exists.
            sess = session_bucket(now)
            sess_mult = SESSION_BOOST.get(sess, 1.0)

            # Phase 4+6+7+8 sizing: strength x vol-conviction x sentiment x mimicry.
            # AGGRESSIVE: full $250 cap for confirmed alpha; moonshots get 25% (small
            # but real skin in the game); scale to full cap on strong conviction.
            # Phase 18: EQUITY-COMPOUNDING — scale size by growing book equity so the
            # agent snowballs like the bot did (size off equity/start, capped 1.5x).
            # Phase 19: stack session + agent-winner + greed-penalty + hot-strategy.
            eq_factor = min(1.5, max(0.5, (led.get("equity", 10000.0)
                                           / led.get("starting_capital", 10000.0))))
            size_mult = (conviction(mom, vol["bucket"]) * overlay["size_mult"]
                         * conf_mult * news["size_mult"] * agent_boost
                         * sess_mult * greed_penalty)
            raw = min(size_for(mom), size_for(0.02)) * strength * size_mult * eq_factor
            if verdict == "bot_wins_here":
                floor = MAX_NOTIONAL * 0.60
            elif is_moonshot:
                floor = MAX_NOTIONAL * 0.25
            else:
                floor = MAX_NOTIONAL * 0.20
            notional = round(max(raw, floor), 2)
            notional = min(notional, MAX_NOTIONAL)

            # Phase 19.2: REGIME-ADAPTIVE LEVERAGE — press to 5x only when the asset's
            # own local regime confirms a trend AND vol is calm; defensive 2x in extreme
            # vol. Never blind — leverage tracks the agent's edge, not a flat 3x.
            lev = adaptive_leverage(local, vol["bucket"])

            # Phase 11: concentration gate — don't let BTC-correlated exposure
            # exceed the cap (a single BTC shock must not wreck the whole book).
            # Cap scales with live equity (40% of book) so the snowball can deploy
            # more as it grows, while still containing a BTC shock to ~40% of equity.
            corr_cap = max(CORRELATED_CAP_USD, led.get("equity", 10000.0) * CORR_CAP_FRAC)
            ok, why = exposure_ok(pair, "BUY" if want_long else "SHORT",
                                  notional, corr_cache, led, cap=corr_cap)
            if not ok:
                results.append({"pair": pair, "signal": "HOLD", "mom": round(mom, 4),
                               "reason": f"gate:{why}", "corr": corr_cache.get(pair)})
                continue

            # Phase 18b: diversified-book gate — don't open beyond MAX_OPEN_POSITIONS
            # concurrent trades (count longs + shorts). Lets the book spread across
            # uncorrelated alts instead of camping at one position.
            open_count = sum(1 for p, v in led.get("positions", {}).items()
                             if v.get("base", 0.0) > 1e-12)
            if open_count >= MAX_OPEN_POSITIONS:
                results.append({"pair": pair, "signal": "HOLD", "mom": round(mom, 4),
                               "reason": f"max_positions:{open_count}"})
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
                                         f"-bot:{ed['verdict']}-conf:{conf['confirmed']}-sz{notional}-lev{lev}",
                                    regime=regime, setup=setup,
                                    price=last if last and last > 0 else None,
                                    leverage=lev)
                results.append({"pair": pair, "signal": "BUY", "mom": round(mom, 4),
                               "size": notional, "lev": lev, "bot_edge": ed["edge"],
                               "confirmed": conf["confirmed"],
                               "result": rec.get("action", "?")})
            else:  # short
                rec = open_short(pair, notional,
                               note=f"regime-{regime}-SHORT-{setup}-mom{mom:.3f}"
                                    f"-bot:{ed['verdict']}-conf:{conf['confirmed']}-sz{notional}-lev{lev}",
                               regime=regime, setup=setup,
                               price=last if last and last > 0 else None,
                               leverage=lev)
                results.append({"pair": pair, "signal": "SHORT", "mom": round(mom, 4),
                               "size": notional, "lev": lev, "bot_edge": ed["edge"],
                               "confirmed": conf["confirmed"],
                               "result": rec.get("action", "?")})
        except Exception as exc:
            results.append({"pair": pair, "signal": "ERROR", "error": str(exc)[:120]})

    ts = dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds")
    if verbose:
        print(f"[{ts}] agent loop: {json.dumps(results)}")
    # Phase 16: recompute equity (cash + open MTM) so the head-to-head digest
    # compares agent vs bot from a flat $10k start, same as the bot's paper book.
    try:
        eq = update_equity()
        if verbose:
            print(f"[{ts}] equity: cash={eq['cash']} unreal={eq['unrealized']} "
                  f"equity=${eq['equity']} ({eq['return_pct']:+}%) peak=${eq['peak_equity']}")
    except Exception as exc:
        if verbose:
            print(f"[equity] update failed: {exc}")
    return {"skipped": False, "regime": regime, "iterations": results}


def main() -> int:
    ap = __import__("argparse").ArgumentParser()
    ap.add_argument("--quiet", action="store_true")
    args = ap.parse_args()
    run_once(verbose=not args.quiet)
    return 0


if __name__ == "__main__":
    sys.exit(main())
