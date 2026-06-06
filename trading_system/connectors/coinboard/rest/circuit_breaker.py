#!/usr/bin/env python3
"""Coinboard Circuit Breaker Module - Reusable Pattern.

This module provides circuit breaker pattern implementation for Coinboard API calls.
"""

import sys
sys.path.insert(0, '/home/falcon/git/portfolio-management')
from dataclasses import dataclass
from typing import Optional, Any, Tuple
from datetime import datetime


@dataclass
class CircuitBreakerState:
    """Track circuit breaker state."""
    failure_count: int = 0
    last_failure_time: Optional[datetime] = None
    cooldown_minutes: float = 10.0
    
    def is_open(self) -> bool:
        """Check if circuit breaker is open."""
        if self.failure_count < 5:
            return False
        
        now = datetime.now()
        minutes_since_failure = (now - self.last_failure_time).total_seconds() / 60 if self.last_failure_time else 100.0
        
        return minutes_since_failure < self.cooldown_minutes


class CircuitBreakerError(Exception):
    """Raised when circuit breaker is open."""
    pass


class CircuitBreaker:
    """Circuit breaker pattern for Coinboard API calls.
    
    Usage:
        breaker = CircuitBreaker(failure_threshold=5, cooldown_minutes=10)
        
        async def call_with_protection(coro):
            return await breaker.call_if_closed(coro)
    """
    
    def __init__(
        self, 
        failure_threshold: int = 5,
        cooldown_minutes: float = 10.0
    ):
        """Initialize circuit breaker.
        
        Args:
            failure_threshold: Number of consecutive failures before opening
            cooldown_minutes: Minutes to wait before retrying
        """
        self.state = CircuitBreakerState(
            failure_count=failure_threshold,
            cooldown_minutes=cooldown_minutes
        )
    
    async def call_if_closed(self, coro) -> Tuple[Any, bool]:
        """Execute coroutine if circuit is closed.
        
        Args:
            coro: Async coroutine to execute
            
        Returns:
            Tuple of (result, error_occurred)
            
        Raises:
            CircuitBreakerError if circuit is open
        """
        if not self.state.is_open():
            result = await coro
            self.state.failure_count = 0
            return result, False
        else:
            raise CircuitBreakerError(
                f"Circuit breaker open. {self.state.failure_count} failures in last "
                f"{int(self.state.cooldown_minutes)} minutes."
            )
    
    async def record_success(self):
        """Record successful call (reset failure count)."""
        self.state.failure_count = 0
    
    async def record_failure(self):
        """Record failed call."""
        self.state.failure_count += 1
        self.state.last_failure_time = datetime.now()


# Example usage:
async def example_usage():
    """Example circuit breaker usage."""
    
    # Initialize circuit breaker
    breaker = CircuitBreaker(failure_threshold=5, cooldown_minutes=10)
    
    # Simulate API call with occasional failures
    async def flaky_api_call():
        import random
        if random.random() < 0.3:
            raise Exception("API Error")
        return {"status": "success"}
    
    try:
        result, error = await breaker.call_if_closed(flaky_api_call())
        print(f"Success: {result}")
    except CircuitBreakerError as e:
        print(f"Circuit open: {e}")


if __name__ == '__main__':
    import asyncio
    # Don't actually run (would require async main)
    pass
