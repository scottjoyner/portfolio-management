"""Consensus Estimates Engine - Analyst Ratings & Forecasts

The consensus estimates module integrates analyst forecasts for:
- EPS (Earnings Per Share) estimates
- Revenue growth projections
- Price targets and recommendations  
- Risk ratings from major agencies

Usage:
    from trading_system.valuation.consensus import ConsensusEstimates
    
    consensus = ConsensusEstimates()
    
    # Fetch latest analyst estimates
    eps_estimate, price_target = await consensus.fetch_estimates('AAPL')

Features:
- Aggregated analyst forecasts
- Buy/Hold/Sell distribution  
- Revision tracking (upgrades/downgrades)
- Risk rating integration
"""

from typing import Optional, Dict, List, Tuple


class ConsensusEstimates:
    """Consensus analyst estimates and ratings."""
    
    def __init__(self):
        """Initialize consensus engine."""
        self.cache: Dict[str, Dict] = {}
    
    async def fetch_estimates(
        self, 
        symbol: str,
        date: Optional[str] = None
    ) -> Dict[str, float]:
        """Fetch latest consensus estimates for a security.
        
        Args:
            symbol: Ticker or ISIN identifier  
            date: Specific date (optional, defaults to latest available)
            
        Returns:
            Dictionary containing consolidated analyst estimates
            
        Example:
            >>> await consensus.fetch_estimates("AAPL", "2025-06-01")
        
        """
        # In production, this would call an API or database
        if symbol not in self.cache:
            self.cache[symbol] = {
                "mean_eps_estimate": 6.45,
                "high_eps_estimate": 7.20,
                "low_eps_estimate": 5.90,
                "mean_price_target": 185.00,
                "high_price_target": 210.00,
                "low_price_target": 160.00,
                "analyst_count": 35,
                "buy_ratings": 22,
                "hold_ratings": 10,
                "sell_ratings": 3,
                "average_rating_score": 4.2
            }
        
        return self.cache[symbol]
    
    def get_recommendation_strength(
        self, 
        buy_count: int,
        hold_count: int,
        sell_count: int
    ) -> Tuple[str, float]:
        """Calculate overall recommendation strength.
        
        Args:
            buy_count: Number of Buy ratings  
            hold_count: Number of Hold ratings
            sell_count: Number of Sell ratings
            
        Returns:
            Tuple of (recommendation string, score out of 5)
        
        Example:
            >>> strength, score = consensus.get_recommendation_strength(
            ...     buy_count=20, hold_count=10, sell_count=5
            ... )
        
        """
        total = buy_count + hold_count + sell_count
        
        if total == 0:
            return "unknown", 0.0
        
        buy_ratio = (buy_count / total) * 5.0
        
        if buy_ratio >= 4.0:
            recommendation = "strong_buy"
        elif buy_ratio >= 3.0:
            recommendation = "buy"
        elif buy_ratio < 2.0:
            recommendation = "strong_sell"
        else:
            recommendation = "hold"
        
        return recommendation, round(buy_ratio, 1)
    
    def calculate_revision_impact(
        self, 
        symbol: str,
        new_estimate: float,
        old_estimate: float
    ) -> Tuple[str, float]:
        """Calculate impact of estimate revision.
        
        Args:
            symbol: Ticker symbol  
            new_estimate: New analyst estimate
            old_estimate: Previous analyst estimate
            
        Returns:
            Tuple of (revision direction, percentage change)
        
        Example:
            >>> direction, pct_change = consensus.calculate_revision_impact(
            ...     symbol="AAPL",
            ...     new_estimate=6.80,
            ...     old_estimate=6.45
            ... )
        
        """
        if old_estimate == 0:
            return "unchanged", 0.0
        
        pct_change = ((new_estimate - old_estimate) / abs(old_estimate)) * 100
        
        if pct_change > 2.0:
            direction = "significant_upgrade"
        elif pct_change > 0.5:
            direction = "upgrade"
        elif pct_change < -2.0:
            direction = "significant_downgrade"
        elif pct_change < -0.5:
            direction = "downgrade"
        else:
            direction = "minimal_revision"
        
        return direction, round(pct_change, 1)
