#!/usr/bin/env python3
"""Coinbase integration module for paper trading."""

import sys, os, json, time
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, field
from enum import Enum


class CircuitBreakerState(Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


@dataclass
class CircuitBreakerConfig:
    failure_threshold: int = 5
    success_threshold: int = 3
    timeout_seconds: int = 600
    max_position_size_pct: float = 0.10


@dataclass
class CircuitBreaker:
    config: CircuitBreakerConfig
    state: CircuitBreakerState = CircuitBreakerState.CLOSED
    failure_count: int = 0
    success_count: int = 0
    opened_at: Optional[datetime] = None
    
    def record_success(self):
        self.success_count += 1
        if self.state == CircuitBreakerState.HALF_OPEN:
            if self.success_count >= self.config.success_threshold:
                self._close()
    
    def record_failure(self, error: Exception = None):
        self.failure_count += 1
        if self.state == CircuitBreakerState.HALF_OPEN:
            self._close()
        elif self.state == CircuitBreakerState.CLOSED:
            if self.failure_count >= self.config.failure_threshold:
                self._open()
    
    def _open(self):
        print(f"⚡ Circuit breaker OPENED after {self.failure_count} failures")
        self.state = CircuitBreakerState.OPEN
        self.opened_at = datetime.utcnow()
        self.success_count = 0
    
    def _close(self):
        print(f"⚡ Circuit breaker CLOSED")
        self.state = CircuitBreakerState.CLOSED
        self.failure_count = 0
        self.success_count = 0
    
    def is_closed(self) -> bool:
        if self.state == CircuitBreakerState.OPEN:
            if datetime.utcnow() - self.opened_at > timedelta(seconds=self.config.timeout_seconds):
                print(f"⚡ Circuit transitioning to HALF_OPEN")
                self.state = CircuitBreakerState.HALF_OPEN
        
        return self.state != CircuitBreakerState.OPEN
    
    def reset(self):
        self.state = CircuitBreakerState.CLOSED
        self.failure_count = 0


class PriceFetcher:
    """Simple price fetcher with local fallback."""
    
    FALLBACK_PRICES = {
        "BTC-USD": 68500.0,
        "ETH-USD": 3450.0,
        "SOL-USD": 175.0,
    }
    
    def __init__(self):
        self._prices = self.FALLBACK_PRICES.copy()
    
    def get_price(self, symbol: str) -> Optional[float]:
        if symbol in self._prices:
            return round(self._prices[symbol], 2)
        return None


class CoinbaseConnector:
    """
    Coinbase paper trading connector with circuit breaker protection.
    
    Runs entirely in simulation mode when no credentials are provided.
    """
    
    def __init__(self, sandbox: bool = True):
        self.sandbox = sandbox
        self.circuit_breaker = CircuitBreaker(CircuitBreakerConfig())
        self.price_fetcher = PriceFetcher()
        
        # Simulated portfolio
        self.portfolio = {
            "USD": 100000.0,
            "BTC": 0.5,
            "ETH": 2.0,
        }
        
        self.positions: Dict[str, dict] = {}
        
        print(f"🔌 Coinbase connector initialized (sandbox={self.sandbox})")
    
    def get_portfolio_value(self) -> Dict[str, Any]:
        """Get current portfolio valuation."""
        if not self.circuit_breaker.is_closed():
            return {"error": "Circuit breaker open"}
        
        total_usd = self.portfolio["USD"]
        
        for asset in ["BTC-USD", "ETH-USD", "SOL-USD"]:
            price = self.price_fetcher.get_price(asset) or 0.0
            ticker = asset.replace("-USD", "")
            qty = self.portfolio.get(ticker, 0)
            
            if qty > 0:
                total_usd += round(qty * price, 2)
        
        return {
            "timestamp": datetime.utcnow().isoformat(),
            "total_usd": total_usd,
            "circuit_breaker_state": self.circuit_breaker.state.value
        }
    
    def execute_order(self, symbol: str, side: str, quantity: float) -> Dict[str, Any]:
        """Execute a trading order."""
        
        if not self.circuit_breaker.is_closed():
            return {"status": "blocked", "error": "Circuit breaker open"}
        
        try:
            price = self.price_fetcher.get_price(symbol)
            if not price:
                raise ValueError(f"No price found for {symbol}")
            
            order_value = price * quantity
            
            # Update simulated balance
            if side == "BUY":
                ticker = symbol.replace("-USD", "")
                self.portfolio["USD"] -= order_value
                self.portfolio[ticker] = self.portfolio.get(ticker, 0) + quantity
                
                # Track position
                if not self.positions.get(symbol):
                    self.positions[symbol] = {
                        "side": "BUY",
                        "quantity": quantity,
                        "entry_price": price,
                        "timestamp": datetime.utcnow().isoformat()
                    }
            
            return {
                "status": "filled",
                "order_id": f"{side.lower()}_{symbol}_{int(time.time())}",
                "symbol": symbol,
                "side": side,
                "quantity": quantity,
                "price": round(price, 2),
                "value_usd": round(order_value, 2),
                "timestamp": datetime.utcnow().isoformat()
            }
        
        except Exception as e:
            self.circuit_breaker.record_failure(e)
            return {"status": "failed", "error": str(e), "timestamp": datetime.utcnow().isoformat()}
