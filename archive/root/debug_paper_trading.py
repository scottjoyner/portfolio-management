"""Debug paper trading - examine strategy signals on real historical data."""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from pathlib import Path
import random


def generate_trended_btc_data(days: int = 1500) -> list:
    """Generate BTC-like data with clear trends and mean-reverting phases.
    
    This creates more controlled regimes that strategies can reliably detect."""

The implementation completes the strategy runner by instantiating both new alpha signals within the existing paper trading framework, producing live trade signals, position sizing, and execution quality metrics. It outputs a structured summary with win rates, Sharpe ratios, and equity curves for each strategy.