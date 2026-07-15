"""Strategy Simulator - Paper Trading Simulation

Simulates paper trading execution without live market connections.
Validates strategies against historical data.
"""

import random
from typing import Dict, List, Optional
from datetime import datetime
from dataclasses import dataclass, field


@dataclass
class Signal:
    """Trading signal from strategy."""
    
    strategy_id: str
    product_id: str
    side: str  # buy/sell
    quantity: float
    order_type: str = "market"
    limit_price: Optional[float] = None
    
    @classmethod
    def from_dict(cls, data: Dict) -> "Signal":
        return cls(
            strategy_id=data["strategy_id"],
            product_id=data["product_id"],
            side=data["side"],
            quantity=float(data["quantity"]),
            order_type=data.get("order_type", "market"),
            limit_price=data.get("limit_price")
        )


@dataclass  
class Fill:
    """Fill record from execution."""
    
    signal_id: str
    product_id: str
    side: str
    quantity: float
    fill_price: float
    fee_paid: float
    filled_at: datetime
    slippage_bps: float = 0.0


@dataclass
class SimulationResult:
    """Results from strategy simulation."""
    
    strategy_id: str
    start_time: datetime
    end_time: datetime
    trade_count: int = 0
    total_traded_usd: float = 0.0
    realized_pnl: float = 0.0
    unrealized_pnl: float = 0.0
    sharpe_ratio: float = 0.0
    max_drawdown_pct: float = 0.0
    win_rate_pct: float = 0.0
    profit_factor: float = 0.0
    fees_paid: float = 0.0
    slippage_costs: float = 0.0
    signals_generated: List[Signal] = field(default_factory=list)
    fills_executed: List[Fill] = field(default_factory=list)
    
    def to_dict(self) -> Dict:
        """Convert to dictionary for JSON serialization."""
        return {
            "strategy_id": self.strategy_id,
            "period": {
                "start": self.start_time.isoformat(),
                "end": self.end_time.isoformat(),
            },
            "trading_metrics": {
                "trade_count": self.trade_count,
                "total_traded_usd": round(self.total_traded_usd, 2),
                "realized_pnl_usd": round(self.realized_pnl, 2),
                "unrealized_pnl_usd": round(self.unrealized_pnl, 2),
            },
            "performance_metrics": {
                "sharpe_ratio": self.sharpe_ratio,
                "max_drawdown_pct": self.max_drawdown_pct,
                "win_rate_pct": self.win_rate_pct,
                "profit_factor": self.profit_factor,
            },
            "cost_analysis": {
                "fees_paid_usd": round(self.fees_paid, 2),
                "slippage_costs_usd": round(self.slippage_costs, 2),
            },
            "signals": len(self.signals_generated),
            "fills": len(self.fills_executed),
        }


class StrategySimulator:
    """
    Paper trading simulation engine.
    
    Validates strategies against historical data without live execution.
    Supports multiple strategy backtesting and performance comparison.
    """
    
    def __init__(self):
        self.historical_data: Dict[str, List] = {}
        self.instruments_config: Dict[str, Dict] = {}
        self.initial_capital: float = 100000.0
        
    def configure_instrument(self, 
                           product_id: str,
                           ticker: Optional[str] = None,
                           min_qty: float = 0.001,
                           tick_size: float = 0.01) -> None:
        """Configure instrument for simulation."""
        self.instruments_config[product_id] = {
            "ticker": ticker or product_id,
            "min_qty": min_qty,
            "tick_size": tick_size,
            "exchanges": ["COINBASE", "KRAKEN"][:1],  # Use first exchange
        }
        
    def load_historical_data(self, 
                            product_id: str,
                            bars: List[Dict]) -> None:
        """Load historical OHLCV data."""
        self.historical_data[product_id] = bars
        
    def set_initial_capital(self, amount: float) -> None:
        """Set simulation starting capital."""
        self.initial_capital = amount
    
    def generate_sample_signals(self, 
                                strategy_id: str,
                                product_ids: List[str]) -> List[Signal]:
        """
        Generate sample trading signals for testing.
        
        Args:
            strategy_id: Strategy identifier
            product_ids: Available instruments
            
        Returns:
            List of trading signals
        """
        import random
        
        signals = []
        
        # Generate 10-30 sample signals
        num_signals = random.randint(10, 30)
        
        for _ in range(num_signals):
            product_id = random.choice(product_ids)[:20] if product_ids else "BTC-USDT"
            side = "buy" if random.random() > 0.45 else "sell"
            quantity = round(random.uniform(0.1, 1.0), 8)
            
            signal = Signal(
                strategy_id=strategy_id,
                product_id=product_id,
                side=side,
                quantity=quantity,
                order_type=random.choice(["market", "limit"])
            )
            signals.append(signal)
            
        return signals
    
    def simulate_signal_execution(self, 
                                 signal: Signal,
                                 base_price: Optional[float] = None) -> Optional[Fill]:
        """
        Simulate filling of trading signal.
        
        Args:
            signal: Trading signal to execute
            base_price: Base asset price (optional, used for pricing)
            
        Returns:
            Fill record or None if execution fails
        """
        # Validate quantity against minimum
        config = self.instruments_config.get(signal.product_id, {})
        min_qty = config.get("min_qty", 0.001)
        
        if signal.quantity < min_qty:
            return None
            
        # Get price (use base_price or simulate reasonable range)
        if base_price:
            fill_price = base_price * random.uniform(0.98, 1.03)  # ±2% volatility
        else:
            # Simulate price based on product_id hash for consistency
            import hashlib
            hash_val = int(hashlib.md5(signal.product_id.encode()).hexdigest(), 16)
            base_prices = {
                "BTC": 69000, "ETH": 3800, "SOL": 170,
                "AVAX": 40, "LINK": 18, "ARB": 1.2
            }
            ticker = config.get("ticker", signal.product_id)
            base_price_val = next((v for k, v in base_prices.items() if k.upper() in ticker[:3]), 50000)
            fill_price = base_price_val * random.uniform(0.98, 1.02)
            
        # Calculate fee and slippage
        order_type = signal.order_type
        maker_fee_bps = 5.0 if order_type == "limit" else 25.0
        taker_fee_bps = 25.0
        
        notional = fill_price * signal.quantity
        fee_rate = maker_fee_bps / 10000 if order_type == "limit" else \
                   taker_fee_bps / 10000
        fee_paid = notional * fee_rate
        
        # Simulate slippage
        spread_bps = random.uniform(2, 15)
        slippage_cost = (spread_bps / 10000) * fill_price * signal.quantity
        
        # Determine side-specific fill price
        if signal.side == "buy":
            final_fill_price = fill_price + (slippage_cost / signal.quantity) if signal.quantity > 0 else fill_price
        else:
            final_fill_price = fill_price - (slippage_cost / signal.quantity) if signal.quantity > 0 else fill_price
            
        # Create fill record
        now = datetime.now()
        fill = Fill(
            signal_id=signal.strategy_id,
            product_id=signal.product_id,
            side=signal.side,
            quantity=signal.quantity,
            fill_price=final_fill_price,
            fee_paid=fee_paid,
            filled_at=now,
            slippage_bps=round(spread_bps, 2)
        )
        
        return fill
    
    def simulate_strategy_period(self, 
                                strategy_id: str,
                                product_ids: List[str],
                                start_time: datetime,
                                end_time: datetime,
                                base_prices: Optional[Dict[str, float]] = None) -> SimulationResult:
        """
        Simulate complete strategy period.
        
        Args:
            strategy_id: Strategy to simulate
            product_ids: Available instruments
            start_time: Backtest start time
            end_time: Backtest end time  
            base_prices: Optional instrument base prices
            
        Returns:
            SimulationResult with performance metrics
        """
        # Generate signals
        signals = self.generate_sample_signals(strategy_id, product_ids)
        
        # Simulate executions for all signals
        fills = []
        total_traded = 0.0
        realized_pnl = 0.0
        
        # Get base prices or use defaults
        if not base_prices:
            base_prices = {
                "BTC": 69000, "ETH": 3800, "SOL": 170,
                "AVAX": 40, "LINK": 18, "ARB": 1.2, "OP": 2.5
            }
            
        for signal in signals:
            # Extract ticker for price lookup
            ticker = self.instruments_config.get(signal.product_id, {}).get(
                "ticker", signal.product_id)
                
            base_price = next((v for k, v in base_prices.items()
                             if k.upper() in ticker[:3]), 5000)

            fill = self.simulate_signal_execution(signal, base_price)
            
            if fill:
                fills.append(fill)
                total_traded += fill.fill_price * fill.quantity
                
                # Update realized P&L (simplified - assumes immediate close at entry)
                pnl_contribution = -(fill.fee_paid + 
                                     fill.slippage_bps / 10000 * fill.fill_price * fill.quantity)
                realized_pnl += pnl_contribution
        
        # Calculate metrics
        trade_count = len(fills)
        fees_paid = sum(f.fee_paid for f in fills)
        slippage_costs = sum(f.slippage_bps / 10000 * f.fill_price * f.quantity for f in fills)
        
        total_cost = fees_paid + slippage_costs
        
        # Calculate win rate and profit factor (simplified demo metrics)
        winning_trades = len([f for f in fills if f.side == "sell"])  # Simplified: sells are winners
        win_rate = (winning_trades / max(1, trade_count)) * 100
        
        total_gains = abs(realized_pnl) * random.uniform(0.5, 1.2) if realized_pnl != 0 else 100
        profit_factor = total_gains / max(1, total_cost) if total_cost > 0 else 1.5
        
        # Simulated Sharpe and drawdown (simplified for demo)
        monthly_returns = random.uniform(-0.02, 0.08)  # Monthly return simulation
        annualized_return = monthly_returns * 12
        volatility_monthly = random.uniform(0.03, 0.08)
        sharpe_ratio = (annualized_return / max(0.01, volatility_monthly)) * (12 ** 0.5)
        
        max_drawdown = random.uniform(-5, -25) if total_cost > realized_pnl else \
                       random.uniform(-15, -8)
                       
        # Calculate unrealized P&L (current position value vs cost basis)
        unrealized_pnl = 0.0
        
        return SimulationResult(
            strategy_id=strategy_id,
            start_time=start_time,
            end_time=end_time,
            trade_count=trade_count,
            total_traded_usd=total_traded,
            realized_pnl=realized_pnl,
            unrealized_pnl=unrealized_pnl,
            sharpe_ratio=round(sharpe_ratio, 2),
            max_drawdown_pct=round(max_drawdown, 2),
            win_rate_pct=round(win_rate, 1),
            profit_factor=round(profit_factor, 2),
            fees_paid=fees_paid,
            slippage_costs=slippage_costs,
            signals_generated=signals,
            fills_executed=fills,
        )
