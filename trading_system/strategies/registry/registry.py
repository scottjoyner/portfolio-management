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
from strategies.cross_asset.beta_reversion import BetaAdjustedCointegrationReversionStrategy
from strategies.cross_asset.momentum_divergence import CrossAssetMomentumDivergenceStrategy
from strategies.cross_asset.spread_zscore import SpreadZScoreReversionStrategy
from strategies.mean_reversion.bollinger_reversion_signal import BollingerBandReversionStrategy
from strategies.mean_reversion.donchian_mean_reversion import DonchianMeanReversionStrategy
from strategies.mean_reversion.grid_capture import GridRebalanceCaptureStrategy
from strategies.mean_reversion.rsi_bounce_reversion import RsiBounceReversionStrategy
from strategies.mean_reversion.zscore import MeanReversionZScoreStrategy
from strategies.microstructure.cvd_exhaustion import CvdExhaustionStrategy
from strategies.microstructure.exchange_netflow_proxy import ExchangeNetflowProxyStrategy
from strategies.microstructure.orderbook_imbalance import OrderBookImbalanceStrategy
from strategies.microstructure.roll_microprice_bias import RollMicropriceBiasStrategy
from strategies.microstructure.spread_compression import SpreadCompressionStrategy
from strategies.microstructure.stablecoin_flow_proxy import StablecoinFlowProxyStrategy
from strategies.microstructure.trade_flow_imbalance import TradeFlowImbalanceStrategy
from strategies.microstructure.vpin_proxy import VpinProxyStrategy
from strategies.microstructure.volume_flow_accdist import VolumeFlowAccDistStrategy
from strategies.microstructure.amihud_illiquidity import AmihudIlliquidityProxyStrategy
from strategies.momentum.adx_di_strength import AdxDiStrengthStrategy
from strategies.momentum.aroon_breakout import AroonBreakoutMomentumStrategy
from strategies.momentum.ema_macd_momentum import EmaMacdMomentumStrategy
from strategies.ml.kalman_mean_reversion import KalmanAdaptiveMeanReversionStrategy
from strategies.ml.online_linear_regression import OnlineLinearRegressionMomentumStrategy
from strategies.ml.volatility_regime_adaptive import VolatilityRegimeAdaptiveStrategy
from strategies.special.basis_carry import BasisCarryDerivativesStrategy
from strategies.special.liquidity_snapback import LiquidityVacuumSnapbackStrategy
from strategies.stat_arb.pairs import PairsTradingStrategy
from strategies.trend.breakout import TrendFollowingBreakoutStrategy
from strategies.volatility.bollinger_bandwidth_reversion import BollingerBandwidthReversionStrategy
from strategies.volatility.bollinger_squeeze_expansion import BollingerSqueezeVolExpansionStrategy
from strategies.volatility.donchian_choppiness_breakout import DonchianChoppinessVolBreakoutStrategy
from strategies.volatility.keltner_channel_breakout import KeltnerVolBreakoutStrategy
from strategies.volatility.vol_breakout import VolatilityBreakoutStrategy
from strategies.volatility.vol_filtered_breakout import VolFilteredBreakoutStrategy
from strategies.volatility.vol_term_structure_carry import VolTermStructureCarryStrategy
from strategies.ensemble.conviction_weighted import ConvictionWeightedCompositeStrategy
from strategies.ensemble.majority_vote import MajorityVoteEnsembleStrategy
from strategies.ensemble.regime_switching_blend import RegimeSwitchingBlendStrategy
from strategies.sentiment.price_acceleration import PriceAccelerationSentimentStrategy
from strategies.sentiment.return_dispersion import ReturnDispersionFearGreedStrategy
from strategies.sentiment.volume_attention import VolumeAttentionMomentumStrategy
from strategies.timeseries.dfa_alpha import DFAAlphaRegimeStrategy
from strategies.timeseries.hurst_regime import HurstRegimeStrategy
from strategies.timeseries.sample_entropy import SampleEntropyRegimeStrategy
from strategies.risk.vol_target_overlay import VolTargetOverlayStrategy
from strategies.risk.risk_parity_zscore import RiskParityZScoreStrategy
from strategies.risk.ewma_var_breakout import EwmaVarBreakoutStrategy
from strategies.trend.multitf_confluence import MultiTFTrendConfluenceStrategy
from strategies.trend.timeframe_momentum_divergence import TimeframeMomentumDivergenceStrategy
from strategies.trend.volatility_cycle_align import VolatilityCycleAlignStrategy
from strategies.volatility.garch_vol_forecast import GarchLiteVolForecastStrategy
from strategies.volatility.vol_clustering_breakout import VolClusteringBreakoutStrategy
from strategies.volatility.regime_persistence_vol import RegimePersistenceVolStrategy
from strategies.crypto.funding_contrarian_proxy import FundingRateContrarianProxyStrategy
from strategies.crypto.liquidation_cascade_proxy import LiquidationCascadeProxyStrategy
from strategies.crypto.ob_imbalance_extreme import OBImbalanceExtremeReversionStrategy


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
        BollingerBandReversionStrategy(),
        RsiBounceReversionStrategy(),
        DonchianMeanReversionStrategy(),
        EmaMacdMomentumStrategy(),
        AdxDiStrengthStrategy(),
        AroonBreakoutMomentumStrategy(),
        KeltnerVolBreakoutStrategy(),
        BollingerSqueezeVolExpansionStrategy(),
        DonchianChoppinessVolBreakoutStrategy(),
        TradeFlowImbalanceStrategy(),
        SpreadCompressionStrategy(),
        CvdExhaustionStrategy(),
        VolumeFlowAccDistStrategy(),
        ExchangeNetflowProxyStrategy(),
        StablecoinFlowProxyStrategy(),
        SpreadZScoreReversionStrategy(),
        CrossAssetMomentumDivergenceStrategy(),
        BetaAdjustedCointegrationReversionStrategy(),
        KalmanAdaptiveMeanReversionStrategy(),
        OnlineLinearRegressionMomentumStrategy(),
        VolatilityRegimeAdaptiveStrategy(),
        VolTermStructureCarryStrategy(),
        VolFilteredBreakoutStrategy(),
        BollingerBandwidthReversionStrategy(),
        AmihudIlliquidityProxyStrategy(),
        RollMicropriceBiasStrategy(),
        VpinProxyStrategy(),
        ReturnDispersionFearGreedStrategy(),
        VolumeAttentionMomentumStrategy(),
        PriceAccelerationSentimentStrategy(),
        HurstRegimeStrategy(),
        DFAAlphaRegimeStrategy(),
        SampleEntropyRegimeStrategy(),
        MajorityVoteEnsembleStrategy(),
        ConvictionWeightedCompositeStrategy(),
        RegimeSwitchingBlendStrategy(),
        VolTargetOverlayStrategy(),
        RiskParityZScoreStrategy(),
        EwmaVarBreakoutStrategy(),
        MultiTFTrendConfluenceStrategy(),
        TimeframeMomentumDivergenceStrategy(),
        VolatilityCycleAlignStrategy(),
        GarchLiteVolForecastStrategy(),
        VolClusteringBreakoutStrategy(),
        RegimePersistenceVolStrategy(),
        FundingRateContrarianProxyStrategy(),
        LiquidationCascadeProxyStrategy(),
        OBImbalanceExtremeReversionStrategy(),
    ]
    advanced = [GenericSpecStrategy(spec) for spec in advanced_specs()]
    strategies = base + advanced

    ids = [s.strategy_id for s in strategies]
    if len(ids) != len(set(ids)):
        raise ValueError("duplicate strategy_id detected in registry")
    return strategies


def strategy_metadata_index() -> dict[str, dict]:
    return {strategy.strategy_id: strategy.metadata() for strategy in load_strategies()}
