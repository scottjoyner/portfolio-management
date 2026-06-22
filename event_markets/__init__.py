"""Event market connectors for Polymarket and Kalshi."""

from .unified_client import UnifiedPredictionMarketClient, PredictionMarket
from .signal_adapter import PredictionMarketAdapter
from .kalshi_client import KalshiClient, KalshiMarket
from .polymarket_client import PolymarketClient, PolymarketMarket, PolymarketBook
from .polymarket_relayer import (
    PolymarketRelayerClient,
    PolymarketRelayerCredentials,
    PolymarketBuilderCredentials,
)
from .knowledge_gap import (
    KnowledgeGapAnalyzer,
    KnowledgeGapAssessment,
    SentimentAnalyzer,
)
from .arbitrage import EventArbitrageScanner, ArbitrageOpportunity, ArbitrageLeg
from .crypto_divergence import CryptoPriceDivergenceDetector, CryptoDivergence

__all__ = [
    "UnifiedPredictionMarketClient", "PredictionMarket",
    "PredictionMarketAdapter",
    "KalshiClient", "KalshiMarket",
    "PolymarketClient", "PolymarketMarket", "PolymarketBook",
    "PolymarketRelayerClient", "PolymarketRelayerCredentials",
    "PolymarketBuilderCredentials",
    "KnowledgeGapAnalyzer", "KnowledgeGapAssessment",
    "SentimentAnalyzer",
    "EventArbitrageScanner", "ArbitrageOpportunity", "ArbitrageLeg",
    "CryptoPriceDivergenceDetector", "CryptoDivergence",
]
