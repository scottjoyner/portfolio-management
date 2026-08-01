import json
from pathlib import Path

import pytest

from data import feed_cache
from tests.coverage.coinbase.test_run_trader_v4_extra7 import _mktrader, _opp, _pos


def test_durable_record_append_fsyncs_and_is_readable(tmp_path, monkeypatch):
    monkeypatch.setattr(feed_cache, "_path", lambda kind, name: str(tmp_path / kind / name))
    fsynced = []
    monkeypatch.setattr(feed_cache.os, "fsync", lambda fd: fsynced.append(fd))

    count = feed_cache.save_records_durable(
        "trade_events", "BTC-USD", [{"kind": "scale_in", "qty": 0.5}]
    )

    assert count == 1
    rows = [json.loads(line) for line in (tmp_path / "trade_events" / "BTC-USD.jsonl").read_text().splitlines()]
    assert rows == [{"kind": "scale_in", "qty": 0.5}]
    assert len(fsynced) >= 2  # appended file and containing directory


def test_durable_record_append_propagates_failure(tmp_path, monkeypatch):
    monkeypatch.setattr(feed_cache, "_path", lambda kind, name: str(tmp_path / kind / name))
    monkeypatch.setattr(feed_cache.os, "fsync", lambda fd: (_ for _ in ()).throw(OSError("disk")))

    with pytest.raises(OSError, match="disk"):
        feed_cache.save_records_durable("trade_events", "BTC-USD", [{"kind": "scale_in"}])


def test_scale_in_emits_durable_event_with_ledger_components():
    trader = _mktrader()
    configured_fee_bps = 25.0
    trader._effective_fee_bps = lambda: configured_fee_bps
    position = _pos(
        high=110.0,
        low=100.0,
        stop=10,
        atr=5,
        regime="strong_uptrend",
        entry=100.0,
        qty=1.0,
    )
    trader.paper_positions["BTC-USD"] = position
    trader._last_price = {"BTC-USD": 110.0}
    events = []
    trader._record_trade_event = lambda kind, product, price, **fields: events.append(
        (kind, product, price, fields)
    )

    trader._paper_execute_impl("BTC-USD", 110.0, [_opp("BUY")])

    assert len(events) == 1
    kind, product, price, fields = events[0]
    assert (kind, product, price) == ("scale_in", "BTC-USD", 110.0)
    assert fields["durable"] is True
    assert fields["qty"] > 0
    assert fields["notional"] > 0
    assert fields["margin"] > 0
    assert fields["fee"] == pytest.approx(
        fields["notional"] * configured_fee_bps / 10_000.0
    )
    assert position.fees_paid == pytest.approx(fields["fee"])
    assert trader.paper_fees_paid >= fields["fee"]
    assert fields["position_trades"] == 2
    assert fields["cash_after"] == trader.paper_cash
