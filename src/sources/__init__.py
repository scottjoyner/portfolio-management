"""
Sources package

Provides unified access to multiple market data sources.
"""

from .base import DataSource, DataSourceError
from .yfinance import YFinanceDataSource
from .alphavantage import AlphaVantageDataSource
from .default import DefaultDataSource
from .factory import DataSourceFactory
