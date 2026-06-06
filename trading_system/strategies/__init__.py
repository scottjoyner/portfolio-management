"""
Trading Strategy System - Comprehensive Factory Architecture
===========================================================
This module defines the factory pattern for loading and managing strategies.

Usage:
    from trading_system.strategies import StrategyFactory
    factory = StrategyFactory()
    
    # Trend Following Strategies
    trend_strategies = factory.get_trend_following()  # Returns list of 30+ strategies
    
    # Mean Reversion Strategies  
    mean_rev_strategies = factory.get_mean_reversion()  # Returns list of 30+ strategies
    
    # Volatility/Arbitrage Strategies
    vol_arb_strategies = factory.get_volatility_arbitrage()  # Returns list of 30+ strategies

Factory Pattern Lifecycle (per strategy):
- init(): Initialize indicators, parameters, and state
- on_bar(data): Process new bar data, compute signals
- handle_signal(signal): Execute trade decisions based on signals
- get_performance_metrics(): Calculate risk-adjusted returns

Performance Metrics:
- Win Rate: % of profitable trades
- Profit Factor: Gross profit / Gross loss  
- Sharpe Ratio: Risk-adjusted return (annualized)
- Max Drawdown: Largest peak-to-trough decline
- Sortino Ratio: Downside deviation adjusted return
- Calmar Ratio: Return / max drawdown
- Recovery Time: Time to recover from drawdown
- Risk of Ruin: Probability of catastrophic loss

"""

from .trend import TrendStrategyFactory
from .volatility import VolatilityStrategyFactory as vol_factory_instance
from .mean_reversion import mean_reversion_factory


class StrategyFactory:
    """Main factory orchestrator for all strategy categories."""
    
    def __init__(self):
        self.trend_factory = TrendStrategyFactory()
        self.mean_rev_factory = mean_reversion_factory
        self.vol_factory = vol_factory_instance
        
    def get_trend_following(self, strategy_type=None):
        """Get trend-following strategies (30+ implementations)."""
        return self.trend_factory.get_all(strategy_type)
    
    def get_mean_reversion(self, strategy_type=None):
        """Get mean reversion strategies (30+ implementations)."""
        return self.mean_rev_factory.get_all(strategy_type)
    
    def get_volatility_arbitrage(self, strategy_type=None):
        """Get volatility and arbitrage strategies (30+ implementations)."""
        return self.vol_factory.get_all(strategy_type)
    
    def get_all_strategies(self, category='trend'):
        """Get all strategies across all categories."""
        if category == 'all':
            return {
                'trend_following': self.get_trend_following(),
                'mean_reversion': self.get_mean_reversion(),
                'volatility_arbitrage': self.get_volatility_arbitrage()
            }


# Export strategy categories for easy import
__all__ = [
    'StrategyFactory',
    'TrendStrategyFactory', 
    'MeanReversionStrategyFactory',
    'VolatilityStrategyFactory'
]
