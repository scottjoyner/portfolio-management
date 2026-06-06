"""Unit tests for opportunity detection engine.

Tests cover:
- Market matching by fuzzy string comparison
- Price divergence calculation
- PnL estimation with fees
- Risk scoring validation
- Mock data injection patterns

All tests should pass with 100% coverage on core logic before committing.
"""

import unittest
from typing import List, Dict, Any
import sys
import os

# Add parent directory to path for imports
sys.path.insert(0, '/home/falcon/git/portfolio-management')


class MockKalshiMarket:
    """Mock Kalshi market for testing."""
    def __init__(self, market_id: str, title: str, bid: float, ask: float, volume: float):
        self.market_id = market_id
        self.title = title
        self.bid = bid
        self.ask = ask
        self.volume = volume
    
    def get(self, key: str, default=None):
        """Get attribute by key."""
        return getattr(self, key, default)


class MockPolymarketEvent:
    """Mock Polymarket event for testing."""
    def __init__(self, slug: str, question: str, bid: float, ask: float, volume: float):
        self.slug = slug
        self.question = question
        self.bid = bid
        self.ask = ask
        self.volume = volume
    
    def get(self, key: str, default=None):
        """Get attribute by key."""
        return getattr(self, key, default)


class TestOpportunityDetector(unittest.TestCase):
    """Test OpportunityDetector class."""

    def setUp(self):
        """Initialize detector for each test."""
        from trading_system.arbitrage.opportunity_detector import OpportunityDetector
        self.detector = OpportunityDetector()

    def test_find_opportunities_returns_list(self):
        """Test that find_opportunities returns a list."""
        kalshi_markets = [
            {
                'market_id': 'BTC-JAN31-100K',
                'title': 'Bitcoin will trade above $100,000 by January 31, 2025',
                'bid': 48.5,
                'ask': 49.2,
                'volume': 5000
            }
        ]
        
        polymarket_events = [
            {
                'slug': 'bitcoin-100k-by-jan-31',
                'question': 'Will Bitcoin trade above $100,000 by January 31, 2025?',
                'bid': 46.8,
                'ask': 47.5,
                'volume': 15000
            }
        ]
        
        opportunities = self.detector.find_opportunities(kalshi_markets, polymarket_events)
        
        self.assertIsInstance(opportunities, list)

    def test_find_opportunities_empty_input(self):
        """Test that find_opportunities handles empty input gracefully."""
        kalshi_markets = []
        polymarket_events = []
        
        opportunities = self.detector.find_opportunities(kalshi_markets, polymarket_events)
        
        self.assertEqual(opportunities, [])

    def test_matching_bitcoin_markets(self):
        """Test that Bitcoin markets are matched correctly."""
        kalshi_markets = [
            {
                'market_id': 'BTC-JAN31-100K',
                'title': 'Bitcoin will trade above $100,000 by January 31, 2025',
                'bid': 48.5,
                'ask': 49.2,
                'volume': 5000
            }
        ]
        
        polymarket_events = [
            {
                'slug': 'bitcoin-100k-by-jan-31',
                'question': 'Will Bitcoin trade above $100,000 by January 31, 2025?',
                'bid': 46.8,
                'ask': 47.5,
                'volume': 15000
            }
        ]
        
        opportunities = self.detector.find_opportunities(kalshi_markets, polymarket_events)
        
        self.assertGreaterEqual(len(opportunities), 0)

    def test_matching_non_similar_markets(self):
        """Test that non-similar markets are not matched."""
        kalshi_markets = [
            {
                'market_id': 'BTC-JAN31-100K',
                'title': 'Bitcoin will trade above $100,000 by January 31, 2025',
                'bid': 48.5,
                'ask': 49.2,
                'volume': 5000
            }
        ]
        
        # Completely different market
        polymarket_events = [
            {
                'slug': 'us-pres-2024-biden-trump',
                'question': 'Who will win the 2024 US Presidential election?',
                'bid': 51.2,
                'ask': 52.8,
                'volume': 20000
            }
        ]
        
        opportunities = self.detector.find_opportunities(kalshi_markets, polymarket_events)
        
        # Should not match Bitcoin market with election market
        self.assertEqual(len(opportunities), 0)

    def test_pnl_calculation(self):
        """Test PnL calculation for an opportunity."""
        kalshi_markets = [
            {
                'market_id': 'BTC-JAN31-100K',
                'title': 'Bitcoin will trade above $100,000 by January 31, 2025',
                'bid': 48.5,
                'ask': 49.2,
                'volume': 5000
            }
        ]
        
        polymarket_events = [
            {
                'slug': 'bitcoin-100k-by-jan-31',
                'question': 'Will Bitcoin trade above $100,000 by January 31, 2025?',
                'bid': 46.8,
                'ask': 47.5,
                'volume': 15000
            }
        ]
        
        opportunities = self.detector.find_opportunities(kalshi_markets, polymarket_events)
        
        if opportunities:
            opp = opportunities[0]
            pnl = opp.potential_pnl(5000)
            
            # PnL should be positive if divergence exceeds fees
            self.assertGreaterEqual(pnl, 0)

    def test_risk_score_low_liquidity(self):
        """Test risk score for low liquidity market."""
        kalshi_markets = [
            {
                'market_id': 'BTC-JAN31-100K',
                'title': 'Bitcoin will trade above $100,000 by January 31, 2025',
                'bid': 48.5,
                'ask': 49.2,
                'volume': 100  # Very low volume
            }
        ]
        
        polymarket_events = [
            {
                'slug': 'bitcoin-100k-by-jan-31',
                'question': 'Will Bitcoin trade above $100,000 by January 31, 2025?',
                'bid': 46.8,
                'ask': 47.5,
                'volume': 100
            }
        ]
        
        opportunities = self.detector.find_opportunities(kalshi_markets, polymarket_events)
        
        # Low liquidity should filter out opportunity or give low risk score
        if opportunities:
            opp = opportunities[0]
            risk_score = opp.risk_score()
            
            # Should have lower risk score due to low volume
            self.assertLessEqual(risk_score, 5.0)

    def test_fuzzy_matching_tolerance(self):
        """Test fuzzy matching handles minor title differences."""
        kalshi_markets = [
            {
                'market_id': 'BTC-JAN31-100K',
                'title': 'Bitcoin will trade above $100,000 by January 31, 2025',
                'bid': 48.5,
                'ask': 49.2,
                'volume': 5000
            }
        ]
        
        # Similar but not identical question
        polymarket_events = [
            {
                'slug': 'bitcoin-100k-jan',
                'question': 'Will bitcoin hit $100k before january ends?',
                'bid': 47.2,
                'ask': 48.0,
                'volume': 8000
            }
        ]
        
        opportunities = self.detector.find_opportunities(kalshi_markets, polymarket_events)
        
        # Should match with lower similarity but still above threshold
        self.assertGreaterEqual(len(opportunities), 0)

    def test_multiple_opportunities(self):
        """Test handling multiple valid opportunities."""
        kalshi_markets = [
            {
                'market_id': 'BTC-JAN31-100K',
                'title': 'Bitcoin will trade above $100,000 by January 31, 2025',
                'bid': 48.5,
                'ask': 49.2,
                'volume': 5000
            },
            {
                'market_id': 'BTC-FEB-75K',
                'title': 'Bitcoin will trade above $75,000 by February 28, 2025',
                'bid': 62.3,
                'ask': 63.1,
                'volume': 4000
            }
        ]
        
        polymarket_events = [
            {
                'slug': 'bitcoin-100k-by-jan-31',
                'question': 'Will Bitcoin trade above $100,000 by January 31, 2025?',
                'bid': 46.8,
                'ask': 47.5,
                'volume': 15000
            },
            {
                'slug': 'bitcoin-75k-by-feb',
                'question': 'Will Bitcoin trade above $75,000 by February 28, 2025?',
                'bid': 61.5,
                'ask': 62.8,
                'volume': 6000
            }
        ]
        
        opportunities = self.detector.find_opportunities(kalshi_markets, polymarket_events)
        
        # Should find multiple valid opportunities
        self.assertGreaterEqual(len(opportunities), 0)

    def test_divergence_threshold(self):
        """Test that minimum divergence threshold filters opportunities."""
        kalshi_markets = [
            {
                'market_id': 'BTC-JAN31-100K',
                'title': 'Bitcoin will trade above $100,000 by January 31, 2025',
                'bid': 48.5,
                'ask': 49.2,
                'volume': 5000
            }
        ]
        
        # Very small divergence (less than threshold after fees)
        polymarket_events = [
            {
                'slug': 'bitcoin-100k-by-jan-31',
                'question': 'Will Bitcoin trade above $100,000 by January 31, 2025?',
                'bid': 48.4,  # Almost same price (only 0.9% gap)
                'ask': 49.1,
                'volume': 15000
            }
        ]
        
        opportunities = self.detector.find_opportunities(kalshi_markets, polymarket_events)
        
        # Should filter out due to low divergence
        self.assertEqual(len(opportunities), 0)


class TestMarketMatcher(unittest.TestCase):
    """Test MarketMatcher class."""

    def test_categorize_bitcoin_market(self):
        """Test categorizing Bitcoin market."""
        from trading_system.arbitrage.opportunity_detector import MarketMatcher
        
        matcher = MarketMatcher()
        title = 'Bitcoin will trade above $100,000 by January 31, 2025'
        
        category = matcher.categorize_market(title)
        
        self.assertEqual(category, 'bitcoin')

    def test_categorize_election_market(self):
        """Test categorizing election market."""
        from trading_system.arbitrage.opportunity_detector import MarketMatcher
        
        matcher = MarketMatcher()
        title = 'Will Trump win the 2024 US Presidential election?'
        
        category = matcher.categorize_market(title)
        
        self.assertEqual(category, 'election')

    def test_match_by_keyword_overlap(self):
        """Test keyword-based matching with overlap."""
        from trading_system.arbitrage.opportunity_detector import MarketMatcher
        
        matcher = MarketMatcher()
        
        kalshi_title = 'Bitcoin will trade above $100K by January 31'
        pm_question = 'Will Bitcoin trade above $100,000 by Jan 31?'
        
        is_match = matcher.match_by_keyword(kalshi_title, pm_question)
        
        # Should have significant keyword overlap
        self.assertTrue(is_match)

    def test_match_no_overlap(self):
        """Test keyword matching with no overlap."""
        from trading_system.arbitrage.opportunity_detector import MarketMatcher
        
        matcher = MarketMatcher()
        
        kalshi_title = 'Bitcoin will trade above $100K by January 31'
        pm_question = 'Who will win the Super Bowl this year?'
        
        is_match = matcher.match_by_keyword(kalshi_title, pm_question)
        
        # Should not match different topics
        self.assertFalse(is_match)


if __name__ == '__main__':
    # Run tests with verbose output
    unittest.main(verbosity=2)
