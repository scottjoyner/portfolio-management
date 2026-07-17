"""Cross-asset-2 strategy family (ETF/equity stat-arb, news fusion, regime ensemble)."""
from trading_system.strategies.cross_asset_2.etf_equity_stat_arb import EtfEquityStatArbProxyStrategy
from trading_system.strategies.cross_asset_2.sentiment_news_fusion import SentimentNewsFusionStrategy
from trading_system.strategies.cross_asset_2.regime_ensemble import RegimeConditionedEnsembleStrategy

__all__ = [
    "EtfEquityStatArbProxyStrategy",
    "SentimentNewsFusionStrategy",
    "RegimeConditionedEnsembleStrategy",
]
