#!/usr/bin/env python3
"""Structured logging infrastructure for trade monitoring and opportunity pipeline."""

import json
import os
import sys
import time
import uuid
import logging
import threading
from datetime import datetime, timezone
from typing import Any, Dict, Optional
from dataclasses import asdict
from pathlib import Path


class TraceContext:
    """Thread-local trace context for correlating log entries across pipeline stages."""

    _local = None

    @classmethod
    def init(cls):
        if cls._local is None:
            cls._local = threading.local()
        return cls._local

    @staticmethod
    def get_trace_id() -> str:
        ctx = TraceContext.init()
        try:
            trace_id = getattr(ctx, "trace_id", None) or uuid.uuid4().hex[:16]
            setattr(ctx, "trace_id", trace_id)
            return trace_id
        except Exception:
            tid = uuid.uuid4().hex[:16]
            return tid

    @staticmethod
    def set_trace_id(trace_id: str):
        ctx = TraceContext.init()
        try:
            setattr(ctx, "trace_id", trace_id)
        except Exception:
            pass


class TradeEventLogger:
    """Structured logger for trade events with full audit trail."""

    EVENT_TYPES = {
        "ORDER_PLACED": "order_placed",
        "ORDER_FILLED": "order_filled",
        "POSITION_OPENED": "position_opened",
        "POSITION_CLOSED": "position_closed",
        "P&L_REALIZED": "pnl_realized",
        "SIGNAL_GENERATED": "signal_generated",
        "OPPORTUNITY_DETECTED": "opportunity_detected",
        "PIPELINE_CYCLE": "pipeline_cycle",
        "CIRCUIT_BREAKER_OPENED": "circuit_breaker_opened",
        "CIRCUIT_BREAKER_CLOSED": "circuit_breaker_closed",
    }

    def __init__(self, log_file: Optional[str] = None):
        self.log_file = log_file or os.path.expanduser(
            "~/.hermes/log/graphalphabot/pipeline.log"
        )
        Path(os.path.dirname(self.log_file)).mkdir(parents=True, exist_ok=True)

        self._logger = logging.getLogger("trade_monitor")
        self._logger.setLevel(logging.DEBUG)
        self._logger.handlers.clear()

        file_handler = logging.FileHandler(self.log_file)
        stream_handler = logging.StreamHandler(sys.stdout)

        formatter = logging.Formatter(
            "%(asctime)s [%(levelname)s] %(name)s - %(message)s"
        )
        file_handler.setFormatter(formatter)
        stream_handler.setFormatter(formatter)

        self._logger.addHandler(file_handler)
        self._logger.addHandler(stream_handler)

    def log_trade_event(self, event_type: str, **kwargs):
        """Log a structured trade event with trace context."""
        trace_id = TraceContext.get_trace_id()

        payload = {
            "event": event_type,
            "trace_id": trace_id,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            **kwargs,
        }

        self._logger.info(json.dumps(payload))

    def log_signal(self, signal_data: dict):
        """Log a trading signal generation."""
        self.log_trade_event(
            "SIGNAL_GENERATED",
            symbol=signal_data.get("symbol"),
            direction=signal_data.get("direction"),
            confidence=signal_data.get("confidence"),
            sentiment_score=signal_data.get("sentiment_score"),
            reason=signal_data.get("signal_reason"),
        )

    def log_order(self, order: dict):
        """Log an executed order."""
        self.log_trade_event(
            "ORDER_FILLED",
            order_id=order.get("order_id"),
            symbol=order.get("symbol"),
            side=order.get("side"),
            quantity=order.get("quantity"),
            price=order.get("price"),
            value_usd=order.get("value_usd"),
        )

    def log_position(self, position: dict):
        """Log a position update."""
        self.log_trade_event(
            "POSITION_OPENED",
            symbol=position.get("symbol"),
            side=position.get("side"),
            quantity=position.get("quantity"),
            entry_price=position.get("entry_price"),
        )

    def log_pnl(self, realized_pnl: float):
        """Log P&L realization."""
        self.log_trade_event("P&L_REALIZED", pnl_usd=realized_pnl)


class PipelineMetricsLogger:
    """Logs pipeline performance metrics and opportunity detection."""

    def __init__(self, log_file: Optional[str] = None):
        self.log_file = log_file or os.path.expanduser(
            "~/.hermes/log/graphalphabot/metrics.log"
        )
        Path(os.path.dirname(self.log_file)).mkdir(parents=True, exist_ok=True)

        self._logger = logging.getLogger("pipeline_metrics")
        self._logger.setLevel(logging.DEBUG)
        self._logger.handlers.clear()

        file_handler = logging.FileHandler(self.log_file)
        stream_handler = logging.StreamHandler(sys.stdout)

        formatter = logging.Formatter(
            "%(asctime)s [%(levelname)s] %(name)s - %(message)s"
        )
        file_handler.setFormatter(formatter)
        stream_handler.setFormatter(formatter)

        self._logger.addHandler(file_handler)
        self._logger.addHandler(stream_handler)

    def log_cycle(self, cycle_num: int, signals_generated: int, duration_ms: float):
        """Log a pipeline cycle completion."""
        payload = {
            "event": "pipeline_cycle",
            "cycle_number": cycle_num,
            "signals_generated": signals_generated,
            "duration_ms": round(duration_ms, 2),
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
        self._logger.info(json.dumps(payload))

    def log_opportunity(self, opportunity: dict):
        """Log an detected trading opportunity."""
        payload = {
            "event": "opportunity_detected",
            "symbol": opportunity.get("symbol"),
            "type": opportunity.get("type"),
            "confidence": opportunity.get("confidence"),
            "reason": opportunity.get("reason"),
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
        self._logger.info(json.dumps(payload))

    def log_circuit_breaker(self, state: str):
        """Log circuit breaker state changes."""
        payload = {
            "event": f"circuit_breaker_{state}",
            "state": state,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
        self._logger.info(json.dumps(payload))


class ErrorLogger:
    """Structured error logger with stack traces."""

    def __init__(self, log_file: Optional[str] = None):
        self.log_file = log_file or os.path.expanduser(
            "~/.hermes/log/graphalphabot/errors.log"
        )
        Path(os.path.dirname(self.log_file)).mkdir(parents=True, exist_ok=True)

        self._logger = logging.getLogger("errors")
        self._logger.setLevel(logging.ERROR)
        self._logger.handlers.clear()

        file_handler = logging.FileHandler(self.log_file)
        stream_handler = logging.StreamHandler(sys.stderr)

        formatter = logging.Formatter(
            "%(asctime)s [%(levelname)s] %(name)s - %(message)s"
        )
        file_handler.setFormatter(formatter)
        stream_handler.setFormatter(formatter)

        self._logger.addHandler(file_handler)
        self._logger.addHandler(stream_handler)

    def log_error(
        self, error: Exception, context: str = "", component: str = "unknown"
    ):
        """Log an error with stack trace and context."""
        payload = {
            "event": "error",
            "component": component,
            "message": str(error),
            "stack_trace": str(error.__traceback__)
            if hasattr(error, "__traceback__")
            else None,
            "context": context,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
        self._logger.error(json.dumps(payload))


# Module-level convenience functions for backward compatibility
def get_logger(name: str) -> logging.Logger:
    """Get a logger instance (backward compatible with old API)."""
    return logging.getLogger(name)
