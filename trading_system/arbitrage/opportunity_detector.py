#!/usr/bin/env python3
"""Detect arbitrage opportunities between Kalshi and Polymarket."""

import sys
sys.path.insert(0, '/home/falcon/git/portfolio-management')
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Optional
from difflib import SequenceMatcher


@dataclass(frozen=True)
class Opportunity:
    """Represents an arbitrage opportunity between two markets."""
    kalshi_market_id: str
    polymarket_slug: str
    
    kalshi_price: float  # in decimal (0-1), e.g., 0.4726 for 47.26%
    polymarket_price: float  # in decimal (0-1), e.g., 0.4583 for 45.83%
    
    title_kalshi: str
    title_polymarket: str
    
    divergence: float  # absolute price difference, e.g., 0.0143 for 1.43%
    arbitrage_potential_pct: float  # percentage to earn via arb
    
    timestamp: datetime = None
    
    def __post_init__(self):
        if self.timestamp is None:
            object.__setattr__(self, 'timestamp', datetime.now())


class OpportunityDetector:
    """Detects arbitrage opportunities between Kalshi and Polymarket markets."""

    SIMILARITY_THRESHOLD = 0.75
    
    def __init__(
        self,
        kalshi_markets=None,
        polymarket_events=None,
        kalshi_price_map=None,
        polymarket_price_map=None,
    ):
        """Initialize detector with market data."""
        self.kalshi_markets = kalshi_markets or []
        self.polymarket_events = polymarket_events or []
        self.kalshi_price_map = kalshi_price_map or {}
        self.polymarket_price_map = polymarket_price_map or {}
        self.opportunities: list[Opportunity] = []

    def from_dict(self, data: dict[str, Any]) -> None:
        """Load market data from dictionary."""
        kalshi_markets = data.get('markets', [])
        polymarket_events = data.get('events', [])

        # Build price maps for quick lookup
        self.kalshi_price_map = {m['market_id']: m['bid'] for m in kalshi_markets}
        self.polymarket_price_map = {e['slug']: e['bid'] for e in polymarket_events}

        self.kalshi_markets = kalshi_markets
        self.polymarket_events = polymarket_events
        self.opportunities: list[Opportunity] = []

    def detect_opportunities(self) -> list[Opportunity]:
        """Detect all arbitrage opportunities."""
        self.opportunities = []
        
        for kalshi_market in self.kalshi_markets:
            pm_matches = self._find_pm_matches(kalshi_market)
            for pm_event, pm_match_score in pm_matches:
                divergence_pct = abs(
                    float(kalshi_market['bid']) - float(pm_event['bid'])
                ) / 100
                
                if divergence_pct > 0.01:  # minimum 1% divergence
                    kalshi_price = float(kalshi_market['bid']) / 100
                    polymarket_price = float(pm_event['bid']) / 100
                    
                    arb_potential_pct = abs(
                        polymarket_price - kalshi_price
                    ) * (1.5 + 0.1) if kalshi_price < polymarket_price else \
                    kalshi_price * (1.5 + 0.1) if polymarket_price < kalshi_price else \
                    (polymarket_price - kalshi_price) / min(kalshi_price, polymarket_price)

                    self.opportunities.append(
                        Opportunity(
                            kalshi_market_id=kalshi_market['market_id'],
                            polymarket_slug=pm_event['slug'],
                            kalshi_price=kalshi_price,
                            polymarket_price=polymarket_price,
                            title_kalshi=kalshi_market['title'],
                            title_polymarket=pm_event['question'],
                            divergence=divergence_pct,
                            arbitrage_potential_pct=arb_potential_pct,
                        )
                    )
        
        # Sort by arbitrage potential
        self.opportunities.sort(key=lambda o: o.arbitrage_potential_pct, reverse=True)
        
        return self.opportunities

    def _find_pm_matches(self, kalshi_market: dict) -> list[tuple]:
        """Find polymarket events that match a Kalshi market."""
        title1 = kalshi_market['title'].lower()
        matches = []
        
        for pm_event in self.polymarket_events:
            title2 = pm_event['question'].lower()
            
            # Normalize
            norm1 = ' '.join(title1.split())
            norm2 = ' '.join(title2.split())
            
            sim = SequenceMatcher(None, norm1, norm2).ratio()
            
            if sim >= self.SIMILARITY_THRESHOLD:
                matches.append((pm_event, sim))
        
        return matches

    def to_dict(self) -> dict[str, Any]:
        """Convert opportunities to dictionary."""
        return {
            'opportunities': [self.to_dict(opportunity) for opportunity in self.opportunities],
            'kalshi_markets': self.to_dict_kalshi(),
            'polymarket_events': self.to_dict_polymarket()
        }

    def to_dict_kalshi(self) -> list[dict]:
        """Convert Kalshi markets to dictionary."""
        result = []
        for market in self.kalshi_markets:
            m = {k: v for k, v in market.items() if k != 'id'}
            m['market_id'] = str(market.get('id', market.get('market_id')))
            m['bid'] = float(market['bid']) / 100 if isinstance(market['bid'], int) else float(market['bid'])
            result.append(m)
        return result

    def to_dict_polymarket(self) -> list[dict]:
        """Convert Polymarket events to dictionary."""
        result = []
        for event in self.polymarket_events:
            m = {k: v for k, v in event.items() if k != 'id'}
            m['slug'] = str(m.get('id', m.get('slug')))
            m['bid'] = float(event['bid']) / 100 if isinstance(event['bid'], int) else float(event['bid'])
            result.append(m)
        return result

    def to_list(self) -> list[dict]:
        """Convert all to a flat list."""
        items = []
        
        for market in self.kalshi_markets:
            m = {k: v for k, v in market.items() if k != 'id'}
            m['market_id'] = str(market.get('id', market.get('market_id')))
            m['bid'] = float(market['bid']) / 100 if isinstance(market['bid'], int) else float(market['bid'])
            items.append(m)
        
        for event in self.polymarket_events:
            e = {k: v for k, v in event.items() if k != 'id'}
            e['slug'] = str(e.get('id', e.get('slug')))
            e['bid'] = float(event['bid']) / 100 if isinstance(event['bid'], int) else float(event['bid'])
            items.append(e)
        
        for op in self.opportunities:
            items.append(self.to_dict(op))
        
        return items

    @classmethod
    def from_list(cls, data: list[dict]) -> "OpportunityDetector":
        """Load opportunity detector from a flat list."""
        kalshi = [m for m in data if 'market_id' in m]
        polymarket = [m for m in data if 'slug' in m]
        
        detector = cls()
        detector.kalshi_markets = kalshi
        detector.polymarket_events = polymarket
        
        # Build price maps
        detector.kalshi_price_map = {m['market_id']: m['bid'] for m in kalshi}
        detector.polymarket_price_map = {m['slug']: m['bid'] for m in polymarket}
        
        return detector

    @staticmethod
    def to_json(detector: "OpportunityDetector") -> str:
        """Convert opportunities to JSON."""
        from json import dumps
        return dumps(detector.to_dict(), indent=2)
