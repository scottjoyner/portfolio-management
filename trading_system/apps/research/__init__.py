"""Research Apps Module - Connect Agents to Financial Data Sources

This module provides:
1. ResearchAgents collection with API connectivity
2. Individual agents: NewsMonitorAgent, PriceWatcherAgent, FundamentalAnalystAgent, SentimentAnalyzerAgent
3. API endpoints for news, price, fundamentals, sentiment analysis
4. Hypothesis generation from multi-source data

Integration with /api/strategies endpoint:
- Provides live metrics and historical performance (from backtest integration)
- Supplies research hypotheses from agentic analysis
- Combines real-time market data with fundamental valuations

Database Tables Used:
┌─────────────────────────────┬─────────────────────────────────────────────┐
│ Table Name                  │ Purpose                                     │
├─────────────────────────────┼─────────────────────────────────────────────┤
│ research_notes               │ Agent observations and findings             │
│ market_regimes              │ Current market conditions (bull/bear)       │
│ hypothesis_log              │ Generated hypotheses with validation        │
└─────────────────────────────┴─────────────────────────────────────────────┘

Usage:
from apps.research.agents import ResearchAgents

research = ResearchAgents()
hypotheses = await research.get_hypothesis("AAPL")

Or use API endpoints:
curl http://localhost:8000/api/research/hypotheses | jq
"""

from .agents import (
    ResearchAgents,
    NewsMonitorAgent,
    PriceWatcherAgent,
    FundamentalAnalystAgent,
    SentimentAnalyzerAgent,
    HypothesisGeneratorAgent,
    get_research_hypotheses,
)

from .routes import (
    get_news,
    get_price,
    get_fundamentals,
    get_sentiment,
    get_hypotheses,
    run_comprehensive_analysis,
)

__all__ = [
    # Agent classes
    "ResearchAgents",
    "NewsMonitorAgent",
    "PriceWatcherAgent", 
    "FundamentalAnalystAgent",
    "SentimentAnalyzerAgent",
    "HypothesisGeneratorAgent",
    # Function endpoints
    "get_research_hypotheses",
    "get_news",
    "get_price",
    "get_fundamentals",
    "get_sentiment",
    "get_hypotheses",
    "run_comprehensive_analysis",
]
