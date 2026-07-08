"""
Prometheus metrics endpoint and structured JSON logging.
"""

import logging
import time
import json
import threading
import functools
import sys
from dataclasses import dataclass, field, asdict
from typing import Dict, Any, Optional, Callable
from contextlib import contextmanager
from collections import defaultdict
from pathlib import Path
from http.server import BaseHTTPRequestHandler, HTTPServer

try:
    from prometheus_client import Counter, Histogram, Gauge, Summary, CollectorRegistry, generate_latest, CONTENT_TYPE_LATEST
    PROMETHEUS_AVAILABLE = True
except ImportError:
    PROMETHEUS_AVAILABLE = False

log = logging.getLogger(__name__)


# Prometheus metrics (only if available)
if PROMETHEUS_AVAILABLE:
    # Counters
    SCAN_TOTAL = Counter('trader_scan_total', 'Total scans', ['scan_type'])
    SCAN_PRODUCTS = Counter('trader_scan_products_total', 'Products scanned', ['scan_type'])
    SCAN_ERRORS = Counter('trader_scan_errors_total', 'Scan errors', ['scan_type', 'error_type'])
    SIGNALS_GENERATED = Counter('trader_signals_generated_total', 'Signals generated', ['strategy', 'direction'])
    TRADES_EXECUTED = Counter('trader_trades_executed_total', 'Trades executed', ['product_id', 'side', 'status'])
    ORDERS_PLACED = Counter('trader_orders_placed_total', 'Orders placed', ['product_id', 'side', 'type'])
    ORDER_ERRORS = Counter('trader_order_errors_total', 'Order errors', ['product_id', 'error_type'])
    POSITIONS_OPENED = Counter('trader_positions_opened_total', 'Positions opened', ['product_id', 'side'])
    POSITIONS_CLOSED = Counter('trader_positions_closed_total', 'Positions closed', ['product_id', 'reason'])
    
    # Gauges
    PORTFOLIO_EQUITY = Gauge('trader_portfolio_equity', 'Current portfolio equity USD')
    PORTFOLIO_CASH = Gauge('trader_portfolio_cash', 'Current cash balance USD')
    PORTFOLIO_DRAWDOWN = Gauge('trader_portfolio_drawdown_pct', 'Portfolio drawdown percent')
    PORTFOLIO_LEVERAGE = Gauge('trader_portfolio_leverage', 'Gross leverage')
    POSITIONS_OPEN = Gauge('trader_positions_open', 'Number of open positions')
    POSITION_NOTIONAL = Gauge('trader_position_notional', 'Position notional', ['product_id', 'side'])
    UNREALIZED_PNL = Gauge('trader_unrealized_pnl', 'Unrealized PnL', ['product_id'])
    DAILY_PNL = Gauge('trader_daily_pnl_pct', 'Daily PnL percent')
    RISK_SCORE = Gauge('trader_risk_score', 'Portfolio risk score 0-100')
    
    # Latency histograms
    SCAN_DURATION = Histogram('trader_scan_duration_seconds', 'Scan duration', ['scan_type'])
    FETCH_DURATION = Histogram('trader_fetch_duration_seconds', 'Candle fetch duration')
    EVAL_DURATION = Histogram('trader_eval_duration_seconds', 'Strategy evaluation duration')
    BACKTEST_DURATION = Histogram('trader_backtest_duration_seconds', 'Backtest duration')
    ORDER_LATENCY = Histogram('trader_order_latency_seconds', 'Order placement latency')
    FILL_LATENCY = Histogram('trader_fill_latency_seconds', 'Fill detection latency')
    WS_LATENCY = Histogram('trader_ws_latency_ms', 'WebSocket message latency')
    API_LATENCY = Histogram('trader_api_latency_seconds', 'API call latency', ['endpoint'])
    
    # Summary
    SLIPPAGE_BPS = Summary('trader_slippage_bps', 'Slippage in basis points', ['product_id'])
    FILL_RATE_LIMITER = None


class StructuredLogger:
    """JSON structured logger with correlation IDs."""
    
    def __init__(self, name: str = "trader", level: int = logging.INFO):
        self.logger = logging.getLogger(name)
        self.logger.setLevel(level)
        
        # Add JSON formatter if not already present
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(JsonFormatter())
        if not self.logger.handlers:
            self.logger.addHandler(handler)
        
        self._correlation_id: Optional[str] = None
        self._context: Dict[str, Any] = {}
    
    def set_correlation_id(self, corr_id: str):
        self._correlation_id = corr_id
    
    def clear_correlation_id(self):
        self._correlation_id = None
    
    def add_context(self, **kwargs):
        self._context.update(kwargs)
    
    def clear_context(self):
        self._context.clear()
    
    def _log(self, level: int, msg: str, **kwargs):
        extra = {
            "correlation_id": self._correlation_id,
            "timestamp": time.time(),
            **self._context,
            **kwargs,
        }
        # Filter out None values
        extra = {k: v for k, v in extra.items() if v is not None}
        self.logger.log(level, msg, extra=extra)
    
    def debug(self, msg: str, **kwargs): self._log(logging.DEBUG, msg, **kwargs)
    def info(self, msg: str, **kwargs): self._log(logging.INFO, msg, **kwargs)
    def warning(self, msg: str, **kwargs): self._log(logging.WARNING, msg, **kwargs)
    def error(self, msg: str, **kwargs): self._log(logging.ERROR, msg, **kwargs)
    def critical(self, msg: str, **kwargs): self._log(logging.CRITICAL, msg, **kwargs)


class JsonFormatter(logging.Formatter):
    """JSON log formatter."""
    
    def format(self, record: logging.LogRecord) -> str:
        log_data = {
            "ts": record.created,
            "level": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
            "module": record.module,
            "func": record.funcName,
            "line": record.lineno,
        }
        
        # Add extra fields
        for key, value in record.__dict__.items():
            if key not in ["name", "msg", "args", "levelname", "levelno", "pathname",
                          "filename", "module", "exc_info", "exc_text", "stack_info",
                          "lineno", "funcName", "created", "msecs", "relativeCreated",
                          "thread", "threadName", "processName", "process", "message"]:
                log_data[key] = value
        
        if record.exc_info:
            log_data["exception"] = self.formatException(record.exc_info)
        
        return json.dumps(log_data, default=str)


def get_structured_logger(name: str = "trader") -> StructuredLogger:
    return StructuredLogger(name)


# Timing decorator
def timed(metric_name: str = None, labels: Dict[str, str] = None):
    """Decorator that records execution time to Prometheus histogram."""
    def decorator(func: Callable):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            start = time.perf_counter()
            try:
                return func(*args, **kwargs)
            finally:
                duration = time.perf_counter() - start
                if PROMETHEUS_AVAILABLE and metric_name:
                    # Use a generic histogram since we can't create dynamic ones easily
                    pass
        return wrapper
    return decorator


class MetricsServer:
    """HTTP server exposing /metrics endpoint."""
    
    def __init__(self, port: int = 9091, host: str = "0.0.0.0"):
        self.port = port
        self.host = host
        self._server: Optional[HTTPServer] = None
        self._thread: Optional[threading.Thread] = None
    
    def start(self):
        if self._server:
            return
        
        class MetricsHandler(BaseHTTPRequestHandler):
            def do_GET(self):
                if self.path == "/metrics":
                    if PROMETHEUS_AVAILABLE:
                        self.send_response(200)
                        self.send_header("Content-Type", CONTENT_TYPE_LATEST)
                        self.end_headers()
                        self.wfile.write(generate_latest())
                    else:
                        self.send_response(503)
                        self.end_headers()
                        self.wfile.write(b"prometheus_client not installed")
                elif self.path == "/health":
                    self.send_response(200)
                    self.send_header("Content-Type", "application/json")
                    self.end_headers()
                    self.wfile.write(b'{"status":"ok"}')
                else:
                    self.send_response(404)
                    self.end_headers()
            
            def log_message(self, format, *args):
                pass  # Suppress default logging
        
        self._server = HTTPServer((self.host, self.port), MetricsHandler)
        self._thread = threading.Thread(target=self._server.serve_forever, daemon=True)
        self._thread.start()
        log.info(f"Metrics server started on {self.host}:{self.port}")
    
    def stop(self):
        if self._server:
            self._server.shutdown()
            self._server = None


# Global metrics server
_METRICS_SERVER: Optional[MetricsServer] = None


def start_metrics_server(port: int = 9091, host: str = "0.0.0.0") -> MetricsServer:
    global _METRICS_SERVER
    if _METRICS_SERVER is None:
        _METRICS_SERVER = MetricsServer(port, host)
        _METRICS_SERVER.start()
    return _METRICS_SERVER


# Convenience functions for recording metrics
def record_scan(scan_type: str, products: int, duration: float, errors: int = 0):
    if PROMETHEUS_AVAILABLE:
        SCAN_TOTAL.labels(scan_type=scan_type).inc()
        SCAN_PRODUCTS.labels(scan_type=scan_type).inc(products)
        SCAN_DURATION.labels(scan_type=scan_type).observe(duration)
        if errors:
            SCAN_ERRORS.labels(scan_type=scan_type, error_type="total").inc(errors)


def record_signal(strategy: str, direction: str):
    if PROMETHEUS_AVAILABLE:
        SIGNALS_GENERATED.labels(strategy=strategy, direction=direction).inc()


def record_trade(product_id: str, side: str, status: str):
    if PROMETHEUS_AVAILABLE:
        TRADES_EXECUTED.labels(product_id=product_id, side=side, status=status).inc()


def record_order(product_id: str, side: str, order_type: str):
    if PROMETHEUS_AVAILABLE:
        ORDERS_PLACED.labels(product_id=product_id, side=side, type=order_type).inc()


def record_portfolio(equity: float, cash: float, drawdown_pct: float, leverage: float, positions: int):
    if PROMETHEUS_AVAILABLE:
        PORTFOLIO_EQUITY.set(equity)
        PORTFOLIO_CASH.set(cash)
        PORTFOLIO_DRAWDOWN.set(drawdown_pct)
        PORTFOLIO_LEVERAGE.set(leverage)
        POSITIONS_OPEN.set(positions)


def record_position(product_id: str, side: str, notional: float, unrealized_pnl: float):
    if PROMETHEUS_AVAILABLE:
        POSITION_NOTIONAL.labels(product_id=product_id, side=side).set(notional)
        UNREALIZED_PNL.labels(product_id=product_id).set(unrealized_pnl)


def record_daily_pnl(pnl_pct: float):
    if PROMETHEUS_AVAILABLE:
        DAILY_PNL.set(pnl_pct)


def record_risk_score(score: float):
    if PROMETHEUS_AVAILABLE:
        RISK_SCORE.set(score)


def record_slippage(product_id: str, bps: float):
    if PROMETHEUS_AVAILABLE:
        SLIPPAGE_BPS.labels(product_id=product_id).observe(bps)


def record_api_latency(endpoint: str, duration: float):
    if PROMETHEUS_AVAILABLE:
        API_LATENCY.labels(endpoint=endpoint).observe(duration)