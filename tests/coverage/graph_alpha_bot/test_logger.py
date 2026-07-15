import logging
import uuid
from unittest.mock import patch

from app.utils.logger import (
    TraceContext,
    TradeEventLogger,
    PipelineMetricsLogger,
    ErrorLogger,
    get_logger,
)


def test_trace_context_get_set():
    tid = TraceContext.get_trace_id()
    assert isinstance(tid, str) and len(tid) == 16
    TraceContext.set_trace_id("abc123")
    assert TraceContext.get_trace_id() == "abc123"


def test_trace_context_get_trace_id_exception():
    # Force init() to raise so the except branch is taken
    with patch.object(TraceContext, "init", side_effect=RuntimeError("boom")):
        tid = TraceContext.get_trace_id()
    assert isinstance(tid, str)


def test_trace_context_set_trace_id_exception():
    with patch.object(TraceContext, "init", side_effect=RuntimeError("boom")):
        # should not raise
        TraceContext.set_trace_id("x")


def test_trade_event_logger(tmp_path):
    log = TradeEventLogger(str(tmp_path / "pipeline.log"))
    log.log_trade_event("ORDER_PLACED", symbol="BTC-USD")
    log.log_signal({"symbol": "BTC-USD", "direction": "LONG", "confidence": 0.8,
                    "sentiment_score": 0.5, "signal_reason": "x"})
    log.log_order({"order_id": "1", "symbol": "BTC-USD", "side": "BUY",
                   "quantity": 1.0, "price": 100.0, "value_usd": 100.0})
    log.log_position({"symbol": "BTC-USD", "side": "BUY", "quantity": 1.0,
                      "entry_price": 100.0})
    log.log_pnl(12.5)
    assert (tmp_path / "pipeline.log").exists()


def test_pipeline_metrics_logger(tmp_path):
    log = PipelineMetricsLogger(str(tmp_path / "metrics.log"))
    log.log_cycle(1, 5, 12.3)
    log.log_opportunity({"symbol": "BTC-USD", "type": "x", "confidence": 0.5,
                         "reason": "r"})
    log.log_circuit_breaker("open")
    log.log_circuit_breaker("closed")
    assert (tmp_path / "metrics.log").exists()


def test_error_logger(tmp_path):
    log = ErrorLogger(str(tmp_path / "errors.log"))
    try:
        raise ValueError("boom")
    except ValueError as e:
        log.log_error(e, context="ctx", component="comp")
    assert (tmp_path / "errors.log").exists()


def test_get_logger():
    assert get_logger("myname") is logging.getLogger("myname")
