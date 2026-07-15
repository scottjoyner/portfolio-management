#!/usr/bin/env python3
"""Coinbase fee calculator (extracted for isolated import).

The grid trading bot imports ``CoinbaseFeeCalculator`` from this module; it is
re-exported here so the calculator can be imported independently of the larger
REST client module.
"""

from dataclasses import dataclass
from typing import Dict, Any, Optional, Tuple


@dataclass
class CoinbaseFeeConfig:
    """Coinboard Advanced Trade fee configuration."""
    maker_fee_bps: float = 5.0
    taker_fee_bps: float = 5.0
    volume_tier_multiplier: float = 1.0


class CoinbaseFeeCalculator:
    """Coinboard Advanced Trade fee calculations."""

    def __init__(self, config: Optional[CoinbaseFeeConfig] = None):
        self.config = config or CoinbaseFeeConfig()

    def calculate_order_fees(
        self,
        order_amount: float,
        order_side: str,
        maker_taker: bool
    ) -> Tuple[float, float]:
        """Calculate order fees for mock data."""
        if order_side == 'buy':
            fee_rate = self.config.maker_fee_bps if maker_taker else self.config.taker_fee_bps
        else:
            fee_rate = self.config.maker_fee_bps if maker_taker else self.config.taker_fee_bps

        fees = order_amount * (fee_rate / 10000)
        return float(fees), float(order_amount - fees)

    def calculate_withdrawal_fees(self, currency: str, amount: float) -> float:
        """Calculate withdrawal fees for specified currency."""
        withdrawal_fees = {
            'USD': 1.50,
            'BTC': 0.00002,
            'ETH': 0.002,
        }
        return float(withdrawal_fees.get(currency.upper(), 0))

    def get_fee_schedule(self) -> Dict[str, Any]:
        """Return current fee schedule."""
        return {
            'maker_fee_rate': f"{self.config.maker_fee_bps / 100:.2f}%",
            'taker_fee_rate': f"{self.config.taker_fee_bps / 100:.2f}%",
        }
