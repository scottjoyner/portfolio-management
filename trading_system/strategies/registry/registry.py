from strategies.accumulation.dca import LongHorizonDcaStrategy
from strategies.catalog.advanced import GenericSpecStrategy, advanced_specs
from strategies.ensemble.regime_allocator import RegimeSwitchingEnsembleAllocator
from strategies.ensemble.rotation import CrossSectionalRelativeStrengthStrategy
from strategies.exchange_bots.dca import DcaStrategy
from strategies.exchange_bots.smart_rebalance import SmartRebalanceStrategy
from strategies.exchange_bots.spot_grid import SpotGridStrategy
from strategies.exchange_bots.spot_martingale import SpotMartingaleStrategy
from strategies.exchange_bots.stair_step_tp import StairStepTakeProfitStrategy
from strategies.exchange_bots.twap import TwapStrategy
from strategies.execution_algos.vwap_twap import VwapTwapExecutionStrategy
from strategies.market_making.adaptive_spread_mm import AdaptiveSpreadMMStrategy
from strategies.market_making.stair_step_mm import StairStepMarketMakerStrategy
from strategies.mean_reversion.grid_capture import GridRebalanceCaptureStrategy
from strategies.mean_reversion.zscore import MeanReversionZScoreStrategy
from strategies.microstructure.orderbook_imbalance import OrderBookImbalanceStrategy
from strategies.special.basis_carry import BasisCarryDerivativesStrategy
from strategies.special.liquidity_snapback import LiquidityVacuumSnapbackStrategy
from strategies.stat_arb.pairs import PairsTradingStrategy
from strategies.trend.breakout import TrendFollowingBreakoutStrategy
from strategies.volatility.vol_breakout import VolatilityBreakoutStrategy


def load_strategies() -> list:
    base = [
        TrendFollowingBreakoutStrategy(),
        MeanReversionZScoreStrategy(),
        CrossSectionalRelativeStrengthStrategy(),
        StairStepMarketMakerStrategy(),
        AdaptiveSpreadMMStrategy(),
        GridRebalanceCaptureStrategy(),
        OrderBookImbalanceStrategy(),
        VwapTwapExecutionStrategy(),
        PairsTradingStrategy(),
        VolatilityBreakoutStrategy(),
        RegimeSwitchingEnsembleAllocator(),
        LongHorizonDcaStrategy(),
        LiquidityVacuumSnapbackStrategy(),
        BasisCarryDerivativesStrategy(),
        StairStepTakeProfitStrategy(strategy_id="stair_step_tp", strategy_type="exchange_bot"),
        SpotGridStrategy(),
        DcaStrategy(),
        SpotMartingaleStrategy(),
        SmartRebalanceStrategy(strategy_id="smart_rebalance", strategy_type="exchange_bot"),
        TwapStrategy(),
    ]
    advanced = [GenericSpecStrategy(spec) for spec in advanced_specs()]
    strategies = base + advanced

    ids = [s.strategy_id for s in strategies]
    if len(ids) != len(set(ids)):
        raise ValueError("duplicate strategy_id detected in registry")
    return strategies


def strategy_metadata_index() -> dict[str, dict]:
    return {strategy.strategy_id: strategy.metadata() for strategy in load_strategies()}
