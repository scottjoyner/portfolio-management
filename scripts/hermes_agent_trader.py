#!/usr/bin/env python3
"""Hermes paper-trading facade backed exclusively by accounting v2.

The strategy loop keeps its established public API, but every ledger mutation is
now delegated to :mod:`scripts.hermes_agent_accounting`. Legacy ledgers are never
healed or reinterpreted; they must be archived and reset with
``scripts/reset_agent_competition.py`` before the agent can trade again.
"""
from __future__ import annotations

import argparse
import contextlib
import datetime as dt
import json
import os
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Iterator

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.hermes_agent_accounting import (
    ACCOUNTING_VERSION,
    apply_marks as accounting_apply_marks,
    assert_invariants,
    close_position as accounting_close_position,
    mark_to_market as accounting_mark_to_market,
    new_ledger,
    open_position as accounting_open_position,
    position_key,
    require_v2,
)

try:
    import fcntl
except ImportError:  # pragma: no cover - Linux is the production target
    fcntl = None

LEDGER = ROOT / "data" / "hermes_agent_ledger.json"
LOCK_FILE = ROOT / "data" / "hermes_agent_ledger.lock"
KILL_SWITCH = os.getenv("KILL_SWITCH", "").lower() in ("1", "true", "yes")
REQUIRE_MANUAL_APPROVAL = os.getenv("REQUIRE_MANUAL_APPROVAL", "").lower() in ("1", "true", "yes")
HERMES_AGENT_LIVE = os.getenv("HERMES_AGENT_LIVE", "").lower() in ("1", "true", "yes")
try:
    MAX_NOTIONAL = float(os.getenv("MAX_NOTIONAL_PER_TRADE_USD", "250"))
except ValueError:
    MAX_NOTIONAL = 10.0
try:
    AGENT_LEVERAGE = float(os.getenv("AGENT_LEVERAGE", "3.0"))
except ValueError:
    AGENT_LEVERAGE = 3.0
try:
    FEE_RATE = float(os.getenv("AGENT_FEE_RATE", "0.0012"))
except ValueError:
    FEE_RATE = 0.0012


class LegacyLedgerError(RuntimeError):
    """Raised when a pre-v2 ledger would otherwise be mutated or ranked."""


def _refuse(reason: str, **extra) -> dict:
    return {"action": "refused", "reason": reason, "live": False, **extra}


@contextlib.contextmanager
def _ledger_lock(exclusive: bool) -> Iterator[None]:
    LOCK_FILE.parent.mkdir(parents=True, exist_ok=True)
    with LOCK_FILE.open("a+") as handle:
        if fcntl is not None:
            mode = fcntl.LOCK_EX if exclusive else fcntl.LOCK_SH
            fcntl.flock(handle.fileno(), mode)
        try:
            yield
        finally:
            if fcntl is not None:
                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


def _read_raw() -> dict:
    if not LEDGER.exists():
        return new_ledger()
    try:
        payload = json.loads(LEDGER.read_text(encoding="utf-8") or "{}")
    except (OSError, json.JSONDecodeError) as exc:
        raise LegacyLedgerError(f"agent_ledger_unreadable:{type(exc).__name__}") from exc
    try:
        require_v2(payload)
        assert_invariants(payload)
    except (ValueError, AssertionError) as exc:
        raise LegacyLedgerError(
            "legacy_or_invalid_agent_ledger; run "
            "python scripts/reset_agent_competition.py --yes after stopping the old agent"
        ) from exc
    return payload


def _atomic_write(payload: dict) -> None:
    require_v2(payload)
    assert_invariants(payload)
    LEDGER.parent.mkdir(parents=True, exist_ok=True)
    fd, temp_name = tempfile.mkstemp(prefix=f".{LEDGER.name}.", suffix=".tmp", dir=LEDGER.parent)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, indent=2, sort_keys=True)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temp_name, LEDGER)
    finally:
        try:
            os.unlink(temp_name)
        except FileNotFoundError:
            pass


def load_ledger() -> dict:
    with _ledger_lock(exclusive=False):
        return _read_raw()


def save_ledger(ledger: dict) -> None:
    """Compatibility export used by maintenance scripts; only accepts v2."""
    with _ledger_lock(exclusive=True):
        _atomic_write(ledger)


def _mutate(operation: Callable[[dict], dict]) -> dict:
    with _ledger_lock(exclusive=True):
        current = _read_raw()
        updated = operation(current)
        _atomic_write(updated)
        return updated


def get_client():
    from coinbase.src.cb_client import CBClient
    return CBClient(dry_run_cli=True)


def _current_price(product_id: str) -> float:
    """Read-only mark from the latest 1-hour Coinbase candle."""
    from coinbase.src.cb_client import CBClient

    client = CBClient(dry_run_cli=True)
    end = dt.datetime.now(dt.timezone.utc)
    start = end - dt.timedelta(hours=1)
    response = client._cli_json(
        "products",
        "candles",
        product_id,
        "granularity=1h",
        f"start={start.isoformat()}",
        f"end={end.isoformat()}",
    )
    if isinstance(response, dict) and isinstance(response.get("candles"), list):
        candles = [row for row in response["candles"] if row.get("close")]
        if candles:
            return float(candles[-1]["close"])
    return 0.0


def quote(product_id: str, side: str, quote_size: float | None = None,
          base_size: float | None = None) -> dict:
    if KILL_SWITCH:
        return _refuse("KILL_SWITCH is active")
    if quote_size is not None and quote_size > MAX_NOTIONAL:
        return _refuse(f"margin {quote_size} exceeds MAX_NOTIONAL {MAX_NOTIONAL}")
    client = get_client()
    response = client.preview_order(
        side,
        product_id,
        quote_size=str(quote_size) if quote_size else None,
        base_size=str(base_size) if base_size else None,
    )
    if isinstance(response, dict) and response.get("status") == "preview_error":
        return {"action": "quote_error", "error": response.get("error"), "raw": response}
    return {
        "action": "quote",
        "live": False,
        "product_id": product_id,
        "side": side.upper(),
        "quote": response,
    }


def _resolve_price(product_id: str, side: str, margin: float | None,
                   base_size: float | None, price: float | None) -> tuple[float, dict | None]:
    if price is not None and price > 0:
        return float(price), None
    preview = quote(product_id, side, quote_size=margin, base_size=base_size)
    if preview.get("action") != "quote":
        return 0.0, preview
    raw = preview.get("quote") or {}
    resolved = float(raw.get("est_average_filled_price", raw.get("price", 0)) or 0)
    return resolved, preview


def _latest_result(updated: dict, action: str, key: str | None = None) -> dict:
    trade = updated.get("trades", [])[-1] if updated.get("trades") else None
    return {
        "action": action,
        "live": False,
        "trade": trade,
        "position": updated.get("positions", {}).get(key) if key else None,
        "realized_pnl": round(float(updated.get("realized_pnl", 0.0)), 6),
        "accounting_version": ACCOUNTING_VERSION,
    }


def record_signal(product_id: str, side: str, quote_size: float | None = None,
                  base_size: float | None = None, note: str = "",
                  regime: str = "", setup: str = "", price: float | None = None,
                  leverage: float | None = None) -> dict:
    """Open/add a long or reduce an existing long using accounting v2."""
    if KILL_SWITCH:
        return _refuse("KILL_SWITCH is active")
    if quote_size is None and base_size is None:
        return _refuse("need quote_size or base_size")
    if quote_size is not None and (quote_size <= 0 or quote_size > MAX_NOTIONAL):
        return _refuse(f"margin {quote_size} outside (0, {MAX_NOTIONAL}]")
    direction = side.upper()
    resolved, error = _resolve_price(product_id, direction, quote_size, base_size, price)
    if error:
        return error
    if resolved <= 0:
        return {"action": "quote_error", "error": "no valid fill price", "product_id": product_id}

    try:
        if direction == "BUY":
            lev = float(leverage or AGENT_LEVERAGE)
            margin = float(quote_size) if quote_size is not None else float(base_size) * resolved / lev
            key = position_key(product_id, "LONG")

            def operation(ledger: dict) -> dict:
                existing = ledger.get("positions", {}).get(key)
                use_leverage = float(existing.get("leverage")) if existing else lev
                return accounting_open_position(
                    ledger,
                    product_id=product_id,
                    side="LONG",
                    margin_usd=margin,
                    entry_price=resolved,
                    leverage=use_leverage,
                    fee_rate=FEE_RATE,
                    note=note,
                    regime=regime,
                    setup=setup,
                )

            updated = _mutate(operation)
            return _latest_result(updated, "signal_recorded", key)

        if direction == "SELL":
            key = position_key(product_id, "LONG")

            def operation(ledger: dict) -> dict:
                pos = ledger.get("positions", {}).get(key)
                if not pos:
                    raise ValueError("position_not_found; use open_short for a short entry")
                if base_size is not None:
                    fraction = min(1.0, float(base_size) / float(pos["quantity"]))
                elif quote_size is not None:
                    fraction = min(1.0, float(quote_size) / float(pos["margin_usd"]))
                else:
                    fraction = 1.0
                return accounting_close_position(
                    ledger,
                    product_id=product_id,
                    side="LONG",
                    exit_price=resolved,
                    fraction=fraction,
                    fee_rate=FEE_RATE,
                    note=note or "signal-sell",
                    regime=regime,
                )

            updated = _mutate(operation)
            return _latest_result(updated, "signal_recorded", key)
        return _refuse("side must be BUY or SELL")
    except (ValueError, AssertionError, LegacyLedgerError) as exc:
        return _refuse(str(exc), product_id=product_id)


def close_position(product_id: str, note: str = "close", price: float | None = None,
                   regime: str | None = None) -> dict:
    if KILL_SWITCH:
        return _refuse("KILL_SWITCH is active")
    exit_price = float(price or 0) or _current_price(product_id)
    if exit_price <= 0:
        return {"action": "quote_error", "error": "no mark", "product_id": product_id}
    try:
        updated = _mutate(lambda ledger: accounting_close_position(
            ledger,
            product_id=product_id,
            side="LONG",
            exit_price=exit_price,
            fee_rate=FEE_RATE,
            note=note,
            regime=regime or "",
        ))
        return _latest_result(updated, "closed")
    except ValueError as exc:
        if str(exc) == "position_not_found":
            return {"action": "no_position", "product_id": product_id}
        return _refuse(str(exc), product_id=product_id)
    except (AssertionError, LegacyLedgerError) as exc:
        return _refuse(str(exc), product_id=product_id)


def open_short(product_id: str, quote_size: float, note: str = "short",
               regime: str = "", setup: str = "", price: float | None = None,
               leverage: float | None = None) -> dict:
    if KILL_SWITCH:
        return _refuse("KILL_SWITCH is active")
    if quote_size is None or quote_size <= 0 or quote_size > MAX_NOTIONAL:
        return _refuse(f"margin must be inside (0, {MAX_NOTIONAL}]")
    entry_price = float(price or 0) or _current_price(product_id)
    if entry_price <= 0:
        return {"action": "quote_error", "error": "no mark", "product_id": product_id}
    key = position_key(product_id, "SHORT")
    try:
        def operation(ledger: dict) -> dict:
            existing = ledger.get("positions", {}).get(key)
            use_leverage = float(existing.get("leverage")) if existing else float(leverage or AGENT_LEVERAGE)
            return accounting_open_position(
                ledger,
                product_id=product_id,
                side="SHORT",
                margin_usd=float(quote_size),
                entry_price=entry_price,
                leverage=use_leverage,
                fee_rate=FEE_RATE,
                note=note,
                regime=regime,
                setup=setup,
            )

        updated = _mutate(operation)
        return _latest_result(updated, "short_opened", key)
    except (ValueError, AssertionError, LegacyLedgerError) as exc:
        return _refuse(str(exc), product_id=product_id)


def close_short(product_id: str, note: str = "close-short", price: float | None = None,
                regime: str | None = None) -> dict:
    if KILL_SWITCH:
        return _refuse("KILL_SWITCH is active")
    exit_price = float(price or 0) or _current_price(product_id)
    if exit_price <= 0:
        return {"action": "quote_error", "error": "no mark", "product_id": product_id}
    try:
        updated = _mutate(lambda ledger: accounting_close_position(
            ledger,
            product_id=product_id,
            side="SHORT",
            exit_price=exit_price,
            fee_rate=FEE_RATE,
            note=note,
            regime=regime or "",
        ))
        return _latest_result(updated, "short_closed")
    except ValueError as exc:
        if str(exc) == "position_not_found":
            return {"action": "no_short", "product_id": product_id}
        return _refuse(str(exc), product_id=product_id)
    except (AssertionError, LegacyLedgerError) as exc:
        return _refuse(str(exc), product_id=product_id)


def add_to_position(product_id: str, add_margin: float, price: float | None = None,
                    note: str = "add-to-winner") -> dict:
    if KILL_SWITCH:
        return _refuse("KILL_SWITCH is active")
    if add_margin is None or add_margin <= 0 or add_margin > MAX_NOTIONAL:
        return _refuse(f"add_margin must be inside (0, {MAX_NOTIONAL}]")
    mark = float(price or 0) or _current_price(product_id.replace("SHORT:", ""))
    if mark <= 0:
        return {"action": "quote_error", "error": "no mark", "product_id": product_id}
    clean_product = product_id.replace("SHORT:", "")
    try:
        def operation(ledger: dict) -> dict:
            long_key = position_key(clean_product, "LONG")
            short_key = position_key(clean_product, "SHORT")
            long_pos = ledger.get("positions", {}).get(long_key)
            short_pos = ledger.get("positions", {}).get(short_key)
            if bool(long_pos) == bool(short_pos):
                raise ValueError("position_not_found_or_ambiguous")
            pos = short_pos or long_pos
            side = "SHORT" if short_pos else "LONG"
            return accounting_open_position(
                ledger,
                product_id=clean_product,
                side=side,
                margin_usd=float(add_margin),
                entry_price=mark,
                leverage=float(pos["leverage"]),
                fee_rate=FEE_RATE,
                note=note,
                regime=str(pos.get("regime", "")),
                setup=str(pos.get("setup", "")),
            )

        updated = _mutate(operation)
        key = position_key(clean_product, "SHORT" if f"SHORT:{clean_product}" in updated["positions"] else "LONG")
        return _latest_result(updated, "position_added", key)
    except (ValueError, AssertionError, LegacyLedgerError) as exc:
        return _refuse(str(exc), product_id=clean_product)


def led_short_key(product_id: str) -> bool:
    return position_key(product_id, "SHORT") in load_ledger().get("positions", {})


def _timestamp(value) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        try:
            return datetime.fromisoformat(str(value).replace("Z", "+00:00")).timestamp()
        except (TypeError, ValueError):
            return 0.0


def bot_recent_fills(minutes: int = 15) -> dict:
    out: dict = {}
    path = ROOT / "data" / "paper_trader_v4_state.json"
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
        cutoff = datetime.now(timezone.utc).timestamp() - minutes * 60.0
        for trade in payload.get("paper_trades", []):
            ts = _timestamp(trade.get("ts"))
            if ts < cutoff:
                continue
            product_id = trade.get("product_id", "")
            side = "BUY" if str(trade.get("side", "")).upper() == "BUY" else "SELL"
            out[product_id] = {"side": side, "ts": ts, "strategy": trade.get("strategy", "")}
    except (OSError, json.JSONDecodeError, TypeError):
        return out
    return out


def recent_stats(ledger: dict | None = None, n: int = 10) -> dict:
    ledger = ledger or load_ledger()
    closed = [trade for trade in ledger.get("trades", []) if "realized_pnl" in trade]
    window = closed[-n:] if n else closed
    pnls = [float(trade.get("realized_pnl", 0.0)) for trade in window]
    wins = sum(1 for pnl in pnls if pnl >= 0)
    running = peak = 0.0
    drawdown = 0.0
    for pnl in pnls:
        running += pnl
        peak = max(peak, running)
        drawdown = min(drawdown, running - peak)
    return {
        "n": len(pnls),
        "wins": wins,
        "losses": len(pnls) - wins,
        "win_rate": round(wins / len(pnls) * 100.0, 2) if pnls else 0.0,
        "pnl": round(sum(pnls), 4),
        "max_drawdown": round(drawdown, 4),
    }


def size_for(mom: float, cap: float | None = None) -> float:
    cap = MAX_NOTIONAL if cap is None else cap
    fraction = max(0.3, min(1.0, abs(mom) / 0.02))
    return round(cap * fraction * 0.99, 2)


def equity_drawdown_pct(ledger: dict | None = None) -> float:
    ledger = ledger or load_ledger()
    equity = float(ledger.get("equity", 0.0) or 0.0)
    peak = float(ledger.get("peak_equity", 0.0) or 0.0)
    return round((equity - peak) / peak * 100.0, 4) if peak > 0 else 0.0


def drawdown_circuit(ledger: dict | None = None, n: int = 10,
                     min_win_rate: float = 33.0, max_dd_pct: float = -9.0) -> dict:
    try:
        ledger = ledger or load_ledger()
    except LegacyLedgerError as exc:
        return {"open": False, "reason": str(exc), "stats": {"n": 0, "equity_dd_pct": 0.0}}
    stats = {**recent_stats(ledger, n), "equity_dd_pct": equity_drawdown_pct(ledger)}
    if stats["equity_dd_pct"] < max_dd_pct:
        return {"open": False, "reason": f"equity_dd {stats['equity_dd_pct']}% < {max_dd_pct}%", "stats": stats}
    if stats["n"] >= 4 and stats["win_rate"] < min_win_rate and stats["equity_dd_pct"] < max_dd_pct / 2.0:
        return {"open": False, "reason": f"win_rate {stats['win_rate']}% < {min_win_rate}% while dd {stats['equity_dd_pct']}%", "stats": stats}
    return {"open": True, "reason": "warming_up" if stats["n"] < 4 else "ok", "stats": stats}


def _marks_for(ledger: dict) -> tuple[dict[str, float], list[str]]:
    marks: dict[str, float] = {}
    missing: list[str] = []
    for position in ledger.get("positions", {}).values():
        product_id = str(position.get("product_id", ""))
        if not product_id or product_id in marks:
            continue
        mark = _current_price(product_id)
        if mark > 0:
            marks[product_id] = mark
        else:
            missing.append(product_id)
    return marks, sorted(set(missing))


def mark_to_market() -> dict:
    if KILL_SWITCH:
        return {"error": "KILL_SWITCH active"}
    try:
        ledger = load_ledger()
        marks, missing = _marks_for(ledger)
        snapshot = accounting_mark_to_market(ledger, marks)
        snapshot["missing_marks"] = sorted(set(snapshot.get("missing_marks", []) + missing))
        return snapshot
    except (LegacyLedgerError, ValueError, AssertionError) as exc:
        return {"error": str(exc), "positions": {}, "total_unrealized_pnl": 0.0, "equity": None}


def update_equity() -> dict:
    try:
        ledger = load_ledger()
        marks, missing = _marks_for(ledger)
        if missing:
            return {"action": "equity_not_updated", "reason": "missing_position_marks", "missing_marks": missing}
        updated = _mutate(lambda current: accounting_apply_marks(current, marks))
        unrealized = accounting_mark_to_market(updated, marks)["total_unrealized_pnl"]
        return {
            "cash": round(float(updated["cash"]), 2),
            "unrealized": round(float(unrealized), 2),
            "equity": round(float(updated["equity"]), 2),
            "peak_equity": round(float(updated["peak_equity"]), 2),
            "return_pct": round(float(updated["return_pct"]), 4),
            "accounting_version": ACCOUNTING_VERSION,
        }
    except (LegacyLedgerError, ValueError, AssertionError) as exc:
        return {"action": "equity_not_updated", "reason": str(exc)}


def close_all(note: str = "close-all") -> dict:
    try:
        ledger = load_ledger()
    except LegacyLedgerError as exc:
        return {"longs": [], "shorts": [], "error": str(exc)}
    longs = [key for key in ledger.get("positions", {}) if not key.startswith("SHORT:")]
    shorts = [key.removeprefix("SHORT:") for key in ledger.get("positions", {}) if key.startswith("SHORT:")]
    return {
        "longs": [{product: close_position(product, note=note).get("action")} for product in longs],
        "shorts": [{product: close_short(product, note=note).get("action")} for product in shorts],
    }


def propose_live(product_id: str, side: str, quote_size: float, note: str = "") -> dict:
    if not HERMES_AGENT_LIVE:
        return _refuse("HERMES_AGENT_LIVE not set — agent may not trade live")
    if KILL_SWITCH:
        return _refuse("KILL_SWITCH is active")
    if REQUIRE_MANUAL_APPROVAL:
        return _refuse("REQUIRE_MANUAL_APPROVAL active — needs human gate")
    return {
        "action": "live_proposal_blocked",
        "reason": "this competition facade is paper-only; live execution requires a separate operator-approved executor",
        "would_preview": quote(product_id, side, quote_size=quote_size),
        "note": note,
    }


def ledger_summary() -> dict:
    try:
        ledger = load_ledger()
        return {
            "accounting_version": ledger["accounting_version"],
            "ranking_eligible": ledger["ranking_eligible"],
            "trades": len(ledger.get("trades", [])),
            "positions": ledger.get("positions", {}),
            "cash": round(float(ledger.get("cash", 0.0)), 6),
            "equity": round(float(ledger.get("equity", 0.0)), 6),
            "realized_pnl": round(float(ledger.get("realized_pnl", 0.0)), 6),
            "fees_paid": round(float(ledger.get("fees_paid", 0.0)), 6),
        }
    except LegacyLedgerError as exc:
        return {"accounting_version": 0, "ranking_eligible": False, "error": str(exc)}


def status() -> dict:
    return {
        "kill_switch": KILL_SWITCH,
        "require_manual_approval": REQUIRE_MANUAL_APPROVAL,
        "max_margin_per_trade_usd": MAX_NOTIONAL,
        "default_leverage": AGENT_LEVERAGE,
        "fee_rate": FEE_RATE,
        "hermes_agent_live": HERMES_AGENT_LIVE,
        "mode": "PAPER/SIM (accounting v2; no money at risk)",
        "ledger": ledger_summary(),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="cmd", required=True)
    quote_cmd = sub.add_parser("quote")
    quote_cmd.add_argument("product")
    quote_cmd.add_argument("side")
    quote_cmd.add_argument("--quote-size", type=float)
    quote_cmd.add_argument("--base-size", type=float)
    signal_cmd = sub.add_parser("signal")
    signal_cmd.add_argument("product")
    signal_cmd.add_argument("side")
    signal_cmd.add_argument("--quote-size", type=float)
    signal_cmd.add_argument("--base-size", type=float)
    signal_cmd.add_argument("--note", default="")
    sub.add_parser("ledger")
    close_cmd = sub.add_parser("close")
    close_cmd.add_argument("product")
    short_cmd = sub.add_parser("short")
    short_cmd.add_argument("product")
    short_cmd.add_argument("--quote-size", type=float, required=True)
    short_cmd.add_argument("--note", default="short")
    close_short_cmd = sub.add_parser("closeshort")
    close_short_cmd.add_argument("product")
    sub.add_parser("closeall")
    sub.add_parser("mtm")
    proposal_cmd = sub.add_parser("propose-live")
    proposal_cmd.add_argument("product")
    proposal_cmd.add_argument("side")
    proposal_cmd.add_argument("--quote-size", type=float, required=True)
    proposal_cmd.add_argument("--note", default="")
    sub.add_parser("status")
    args = parser.parse_args()

    if args.cmd == "quote":
        result = quote(args.product, args.side, args.quote_size, args.base_size)
    elif args.cmd == "signal":
        result = record_signal(args.product, args.side, args.quote_size, args.base_size, args.note)
    elif args.cmd == "ledger":
        result = ledger_summary()
    elif args.cmd == "close":
        result = close_position(args.product)
    elif args.cmd == "short":
        result = open_short(args.product, args.quote_size, args.note)
    elif args.cmd == "closeshort":
        result = close_short(args.product)
    elif args.cmd == "closeall":
        result = close_all()
    elif args.cmd == "mtm":
        result = mark_to_market()
    elif args.cmd == "propose-live":
        result = propose_live(args.product, args.side, args.quote_size, args.note)
    else:
        result = status()
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
