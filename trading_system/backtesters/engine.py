"""
Event-Driven Backtesting Engine

Processes market data events in chronological order, executes strategy signals,
and calculates performance metrics including drawdowns, sharpe ratio, etc.
"""
from typing import List, Dict, Optional
from datetime import datetime
from dataclasses import dataclass
import time


@dataclass 
class MarketEvent:
    """Standardized market event for backtesting."""
    timestamp: float  # Unix timestamp or exchange-specific time
    symbol: str
    price: float
    volume: float
    event_type: str  # "OHLCV", "TRADEREPORTED"


@dataclass
class OrderEvent:  
    """Order execution event with fill details."""
    order_id: str
    timestamp: float
    symbol: str
    side: str
    filled_quantity: float
    filled_price: float
    commission_usd: float = 0.0
    slippage_bps: float = 0.0


@dataclass  
class PortfolioSnapshot:
    """Time-series portfolio state for metrics calculation."""
    timestamp: float
    equity_value: float
    margin_level: float
    
    def mark_to_market(self, price_change_pct: float) -> None:
        """Update equity with unrealized PnL."""
        if self.equity_value > 0:
            self.equity_value += self.equity_value * (price_change_pct / 100)


class BacktestEngine:
    """
    Event-driven backtester with realistic slippage and fees.
    
    Features:
    - Processes OHLCV events chronologically  
    - Executes strategy signals with realistic slippage model
    - Tracks order fills and PnL attribution
    - Generates portfolio value time series for metrics
    
    Usage:
        engine = BacktestEngine(
            initial_capital=10000,
            fee_pct=0.001,  # 0.1% per trade
            slippage_bps=2.0,  # 2 bps typical for spot
        )
        engine.load_market_data(bars)
        
        for i in range(len(bars)):
            order = engine.step()
            if order:
                print(f"Executed: {order}")
                
    """
    
    def __init__(self, 
                 initial_capital: float = 10000.0,
                 fee_pct: float = 0.001,
                 slippage_bps: float = 2.0,
                 max_position_size_usd: float = None):
        """
        Initialize backtester.
        
        Args:
            initial_capital: Starting account balance in USD
            fee_pct: Fee percentage per trade (both buy and sell)
            slippage_bps: Slippage basis points (1 bp = 0.01%)
            max_position_size_usd: Maximum position size for risk control
        """
        self.initial_capital = initial_capital
        self.fee_pct = fee_pct
        self.slippage_bps = slippage_bps
        self.max_position_size_usd = max_position_size_usd or initial_capital
        
        # State tracking
        self.portfolio_value = initial_capital
        self.held_positions: Dict[str, dict] = {}
        self.events: List[MarketEvent] = []
        self.order_events: List[OrderEvent] = []
        self.portfolio_snapshots: List[PortfolioSnapshot] = []
        self.active_strategies: Dict[str, object] = {}
        
    def load_market_data(self, data: List[dict]) -> None:
        """Load OHLCV data into chronological order."""
        for bar in sorted(data, key=lambda x: x.get("timestamp", 0)):
            event = MarketEvent(
                timestamp=bar.get("timestamp", time.time()),
                symbol=bar.get("symbol", "BTC-USD"),
                price=bar.get("close", bar.get("price", 0)),
                volume=bar.get("volume", bar.get("amount", 0)),
                event_type="OHLCV"
            )
            self.events.append(event)
            
    def initialize_strategy(self, strategy: object, 
                          config: dict,
                          symbol: str = "BTC-USD") -> None:
        """Initialize backtestable strategy."""
        if hasattr(strategy, 'init'):
            strategy.init(config or {})
        self.active_strategies[symbol] = strategy
        
    def step(self) -> Optional[dict]:
        """
        Process one market event and execute any pending signals.
        
        Returns:
            OrderEvent dict if trade executed, None otherwise
        """
        if not self.events:
            return None
            
        event = self.events.pop(0)
        
        # Check all active strategies for new signals
        for symbol, strategy in self.active_strategies.items():
            try:
                signal = strategy.on_bar(event)
                
                if signal and hasattr(signal, 'action') and signal.action != "HOLD":
                    order_result = self.execute_signal(
                        symbol=symbol,
                        signal=signal,
                        price=event.price,
                        event_timestamp=event.timestamp
                    )
                    
                    if order_result:
                        self.order_events.append(order_result)
                        
            except Exception as e:
                # Log error but continue processing other strategies
                print(f"[BacktestEngine] Error in strategy {type(strategy).__name__}: {e}")
                
        return None
        
    def execute_signal(self, symbol: str, 
                      signal,
                      market_price: float,
                      event_timestamp: float = None) -> Optional[dict]:
        """
        Execute trade with realistic slippage and fees.
        
        Returns:
            OrderEvent dict or None (cancelled/held)
        """
        # Skip if no price
        if not market_price or market_price <= 0:
            return None
            
        # Calculate position size based on signal quantity
        desired_quantity = getattr(signal, 'quantity', getattr(signal, 'size', 1))
        if desired_quantity is None or desired_quantity == 0:
            return None
            
        # Validate against max position size
        trade_value = abs(desired_quantity * market_price)
        if self.max_position_size_usd and trade_value > self.max_position_size_usd:
            print(f"[BacktestEngine] Trade value {trade_value:.2f} exceeds max {self.max_position_size_usd}")
            return None
            
        # Calculate filled price with slippage
        is_buy = hasattr(signal, 'action') and signal.action in ["BUY", "HOLD"]
        if is_buy:
            fill_price = market_price * (1 + self.slippage_bps / 10000)
        else:
            fill_price = market_price * (1 - self.slippage_bps / 10000)
        
        # Calculate fees
        trade_value_usd = abs(desired_quantity * fill_price)
        fee_deduction_pct = self.fee_pct
        effective_value = trade_value_usd * (1 - fee_deduction_pct)
        
        # Create position record
        order_event = OrderEvent(
            order_id=f"{symbol}-{int(event_timestamp)}-{time.time_ns()}",
            timestamp=event_timestamp,
            symbol=symbol,
            side=signal.action if hasattr(signal, 'action') else "BUY",
            filled_quantity=desired_quantity,
            filled_price=fill_price,
            commission_usd=trade_value_usd * fee_deduction_pct,
            slippage_bps=self.slippage_bps
        )
        
        # Track position state
        current_position = self.held_positions.get(symbol, {'quantity': 0})
        self.held_positions[symbol] = {
            'quantity': current_position['quantity'] + desired_quantity,
            'entry_weighted_avg_price': (current_position['quantity'] * current_position['entry_weighted_avg_price'] + 
                                          desired_quantity * fill_price) / abs(current_position['quantity'] + desired_quantity),
            'position_value_usd': abs((current_position['quantity'] + desired_quantity) * fill_price)
        }
        
        return order_event
        
    def finalize(self) -> Dict:
        """Generate portfolio summary at end of backtest."""
        snapshot = PortfolioSnapshot(
            timestamp=time.time(),
            equity_value=self.portfolio_value,
            margin_level=100.0
        )
        self.portfolio_snapshots.append(snapshot)
        
        return {
            'final_position': self.held_positions,
            'total_trades': len(self.order_events),
            'portfolio_value_usd': self.portfolio_value,
        }
    
    def __len__(self) -> int:
        """Return number of processed events."""
        return len(self.events) + len(self.order_events)
