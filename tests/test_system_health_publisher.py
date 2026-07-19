import importlib.util
import json
import os
import time
from pathlib import Path


MODULE_PATH = Path(__file__).resolve().parents[1] / "scripts/trading/system_health_publisher.py"
spec = importlib.util.spec_from_file_location("system_health_publisher", MODULE_PATH)
publisher = importlib.util.module_from_spec(spec)
spec.loader.exec_module(publisher)


def write_state(path, payload, age=5):
    path.write_text(json.dumps(payload))
    timestamp = time.time() - age
    os.utime(path, (timestamp, timestamp))


def test_fresh_paper_state_publishes_authoritative_book_projection(tmp_path):
    state = tmp_path / "paper_trader_v4_state.json"
    write_state(state, {
        "mode": "paper",
        "state_schema_version": 2,
        "paper_cash": 9750.5,
        "paper_realized_pnl": 12.25,
        "paper_fees_paid": 3.5,
        "paper_positions": {
            "BTC-USD": {"qty": 0.1, "entry_price": 1000, "entry_notional": 100},
            "ETH-USD": {"qty": -2, "entry_price": 50},
        },
    })

    book = publisher.paper_book(state, now=time.time())

    assert book == {
        "status": "ok",
        "source": "paper_trader_v4_state.json",
        "state_age_sec": 5.0,
        "mode": "paper",
        "schema_version": 2,
        "cash_usd": 9750.5,
        "realized_pnl_usd": 12.25,
        "fees_paid_usd": 3.5,
        "open_positions": 2,
        "gross_exposure_usd": 200.0,
        "capital_in_play_usd": 200.0,
        "positions": [
            {"product_id": "BTC-USD", "qty": 0.1, "entry_price": 1000.0, "entry_notional_usd": 100.0},
            {"product_id": "ETH-USD", "qty": -2.0, "entry_price": 50.0, "entry_notional_usd": 100.0},
        ],
    }


def test_list_position_schema_is_authoritative_and_empty_book_is_zero(tmp_path):
    state = tmp_path / "paper_trader_v4_state.json"
    write_state(state, {
        "mode": "paper", "state_schema_version": 2, "paper_cash": 10000,
        "paper_realized_pnl": 0, "paper_fees_paid": 0, "paper_positions": [],
    })

    book = publisher.paper_book(state, now=time.time())

    assert book["status"] == "ok"
    assert book["open_positions"] == 0
    assert book["gross_exposure_usd"] == 0.0
    assert book["positions"] == []


def test_invalid_stale_or_live_paper_state_fails_closed(tmp_path):
    state = tmp_path / "paper_trader_v4_state.json"
    assert publisher.paper_book(state, now=time.time())["status"] == "unknown"

    write_state(state, {"mode": "live", "paper_cash": 10000, "paper_positions": {}}, age=5)
    assert publisher.paper_book(state, now=time.time())["status"] == "unknown"

    write_state(state, {"mode": "paper", "paper_cash": "bad", "paper_positions": {}}, age=5)
    assert publisher.paper_book(state, now=time.time())["status"] == "unknown"

    write_state(state, {"mode": "paper", "paper_cash": 10000, "paper_positions": {}}, age=publisher.STALE_SECONDS + 1)
    assert publisher.paper_book(state, now=time.time())["status"] == "unknown"
