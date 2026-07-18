"""
Portfolio-level risk management with correlation-aware position sizing.
"""

import logging
import threading
import time
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Set
from collections import defaultdict

import numpy as np

log = logging.getLogger(__name__)


@dataclass
class RiskLimits:
    """Portfolio-level risk limits."""
    max_portfolio_drawdown_pct: float = 15.0      # Hard stop: flatten all
    max_daily_loss_pct: float = 5.0               # Daily loss limit
    max_sector_exposure_pct: float = 30.0         # Per correlation cluster
    max_single_asset_pct: float = 10.0            # Per product
    max_correlated_positions: int = 3             # Max positions with corr > 0.7
    max_leverage: float = 1.5                     # Portfolio gross leverage
    min_cash_buffer_pct: float = 5.0              # Minimum cash reserve


@dataclass
class Position:
    """Current position snapshot."""
    product_id: str
    side: str               # "LONG" or "SHORT"
    size: float             # Base currency size
    entry_price: float
    current_price: float
    unrealized_pnl: float
    notional: float         # size * current_price
    leverage: float = 1.0
    cluster: str = ""       # Correlation cluster name


@dataclass
class RiskMetrics:
    """Current portfolio risk metrics."""
    total_equity: float
    total_notional: float
    gross_leverage: float
    net_exposure: float
    portfolio_drawdown_pct: float
    daily_pnl_pct: float
    cluster_exposures: Dict[str, float]
    asset_exposures: Dict[str, float]
    correlated_groups: List[List[str]]
    risk_score: float       # 0-100, higher = riskier
    limit_breaches: List[str]


class PortfolioRiskManager:
    """
    Portfolio-level risk manager with correlation awareness.
    
    Features:
    - Real-time drawdown monitoring
    - Correlation cluster exposure limits
    - Correlation-aware position sizing
    - Daily loss limit enforcement
    - Automatic position scaling on risk limits
    """
    
    def __init__(self, limits: Optional[RiskLimits] = None):
        self.limits = limits or RiskLimits()
        self._positions: Dict[str, Position] = {}
        self._lock = threading.RLock()
        
        # Correlation matrix (updated periodically)
        self._corr_matrix: Dict[str, Dict[str, float]] = {}
        self._cluster_map: Dict[str, str] = {}  # product_id -> cluster_name
        
        # Daily tracking
        self._daily_start_equity: float = 0.0
        self._daily_peak_equity: float = 0.0
        self._last_update_ts: float = time.time()
        
        # Predefined correlation clusters
        self._init_default_clusters()
    
    def _init_default_clusters(self):
        """Initialize default correlation clusters."""
        self._cluster_map = {
            # BTC cluster
            "BTC-USD": "btc", "BTC-USDC": "btc", "BTC-USDT": "btc",
            # ETH cluster
            "ETH-USD": "eth", "ETH-USDC": "eth", "ETH-USDT": "eth",
            "ETH-BTC": "eth",
            # Major L1s (often correlated)
            "SOL-USD": "l1", "SOL-USDC": "l1", "AVAX-USD": "l1", "AVAX-USDC": "l1",
            "DOT-USD": "l1", "DOT-USDC": "l1", "ATOM-USD": "l1", "ATOM-USDC": "l1",
            "NEAR-USD": "l1", "NEAR-USDC": "l1", "APT-USD": "l1", "SUI-USD": "l1",
            # DeFi
            "UNI-USD": "defi", "UNI-USDC": "defi", "AAVE-USD": "defi", "AAVE-USDC": "defi",
            "COMP-USD": "defi", "CRV-USD": "defi", "LDO-USD": "defi",
            # Exchange tokens
            "BNB-USD": "cex", "FTT-USD": "cex", "CRO-USD": "cex", "OKB-USD": "cex",
            # Memes
            "DOGE-USD": "meme", "SHIB-USD": "meme", "PEPE-USD": "meme", 
            "FLOKI-USD": "meme", "BONK-USD": "meme", "WIF-USD": "meme",
            # Gaming/NFT
            "AXS-USD": "gaming", "SAND-USD": "gaming", "MANA-USD": "gaming",
            "ENJ-USD": "gaming", "GALA-USD": "gaming",
            # Payments
            "XRP-USD": "payments", "XLM-USD": "payments", "LTC-USD": "payments",
            "BCH-USD": "payments", "XMR-USD": "payments",
            # Storage/Infra
            "FIL-USD": "storage", "AR-USD": "storage", "STORJ-USD": "storage",
            "RNDR-USD": "compute", "AKT-USD": "compute",
            # Oracle
            "LINK-USD": "oracle", "BAND-USD": "oracle", "PYTH-USD": "oracle",
        }
        
        # Default all unmapped to "other"
        for cluster in set(self._cluster_map.values()):
            pass  # clusters defined above
    
    def update_correlation_matrix(self, returns_matrix: Dict[str, List[float]]) -> None:
        """Update correlation matrix from return series.
        
        Args:
            returns_matrix: {product_id: [daily_returns]} - at least 30 days
        """
        with self._lock:
            products = list(returns_matrix.keys())
            if len(products) < 2:
                return
            
            # Align lengths
            min_len = min(len(returns_matrix[p]) for p in products)
            if min_len < 10:
                return
            
            returns_array = np.array([returns_matrix[p][-min_len:] for p in products])
            
            # Compute correlation matrix
            corr = np.corrcoef(returns_array)
            
            self._corr_matrix = {}
            for i, p1 in enumerate(products):
                self._corr_matrix[p1] = {}
                for j, p2 in enumerate(products):
                    self._corr_matrix[p1][p2] = float(corr[i, j])
            
            # Update clusters based on correlation
            self._update_clusters_from_correlation()
            
            log.info(f"Updated correlation matrix for {len(products)} products")
    
    def _update_clusters_from_correlation(self, threshold: float = 0.7):
        """Re-cluster products based on correlation."""
        # Simple hierarchical clustering for highly correlated groups
        products = list(self._corr_matrix.keys())
        if not products:
            return
        
        visited = set()
        new_clusters = {}
        cluster_id = 0
        
        for p in products:
            if p in visited:
                continue
            
            # Find all products correlated with this one
            cluster_members = [p]
            for q in products:
                if q != p and q not in visited:
                    if self._corr_matrix.get(p, {}).get(q, 0) >= threshold:
                        cluster_members.append(q)
            
            if len(cluster_members) > 1:
                for m in cluster_members:
                    new_clusters[m] = f"corr_{cluster_id}"
                    visited.add(m)
                cluster_id += 1
            else:
                new_clusters[p] = self._cluster_map.get(p, "other")
                visited.add(p)
        
        self._cluster_map.update(new_clusters)
    
    def get_cluster(self, product_id: str) -> str:
        return self._cluster_map.get(product_id, "other")
    
    def update_positions(self, positions: Dict[str, Position]) -> None:
        """Update current positions."""
        with self._lock:
            self._positions = positions.copy()
            self._update_risk_metrics()
    
    def update_equity(self, equity: float) -> None:
        """Update equity for drawdown tracking."""
        with self._lock:
            if self._daily_start_equity == 0:
                self._daily_start_equity = equity
                self._daily_peak_equity = equity
            else:
                self._daily_peak_equity = max(self._daily_peak_equity, equity)
            self._last_update_ts = time.time()
    
    def _update_risk_metrics(self) -> None:
        """Recalculate all risk metrics."""
        if not self._positions:
            return
        
        total_notional = sum(p.notional for p in self._positions.values())
        long_notional = sum(p.notional for p in self._positions.values() if p.side == "LONG")
        short_notional = sum(p.notional for p in self._positions.values() if p.side == "SHORT")
        
        # Cluster exposures
        cluster_exposures = defaultdict(float)
        for p in self._positions.values():
            cluster_exposures[self.get_cluster(p.product_id)] += p.notional
        
        # Asset exposures
        asset_exposures = {p.product_id: p.notional for p in self._positions.values()}
        
        # Correlated groups (positions with corr > 0.7)
        correlated_groups = []
        products = list(self._positions.keys())
        for i, p1 in enumerate(products):
            for p2 in products[i+1:]:
                corr = self._corr_matrix.get(p1, {}).get(p2, 0)
                if corr > 0.7:
                    correlated_groups.append([p1, p2])
    
    def check_pre_trade(self, product_id: str, side: str, notional: float,
                        current_price: float, equity: float) -> tuple[bool, str, float]:
        """
        Check if a new trade passes risk limits.

        Returns:
            (allowed, reason, adjusted_notional)

        IMPORTANT: scaling checks (cluster, asset, gross-leverage) no longer
        early-return on a partial scale-down. They each compute the tightest
        allowed notional and we take the MINIMUM across all of them, evaluating
        every limit before returning. The previous implementation returned True
        from the cluster/asset scale-down before the gross-leverage check ever
        ran, which let aggregate notional blow past the max_leverage cap (e.g.
        10 clusters x 30% = 300% of equity). Gross leverage is now a true hard
        ceiling on total notional regardless of how positions are clustered.
        """
        with self._lock:
            # 1. Portfolio drawdown — HARD stop
            if self._daily_start_equity > 0:
                dd = (self._daily_peak_equity - equity) / self._daily_peak_equity * 100
                if dd >= self.limits.max_portfolio_drawdown_pct:
                    return False, f"Portfolio drawdown {dd:.1f}% exceeds limit {self.limits.max_portfolio_drawdown_pct}%", 0.0

            # 2. Daily loss limit — HARD stop
            if self._daily_start_equity > 0:
                daily_loss = (self._daily_start_equity - equity) / self._daily_start_equity * 100
                if daily_loss >= self.limits.max_daily_loss_pct:
                    return False, f"Daily loss {daily_loss:.1f}% exceeds limit {self.limits.max_daily_loss_pct}%", 0.0

            # Scaling checks: track the tightest allowed notional across all of them.
            allowed = notional
            scale_reason = "OK"

            # 3. Cluster (sector) exposure
            cluster = self.get_cluster(product_id)
            cluster_notional = sum(p.notional for p in self._positions.values() if self.get_cluster(p.product_id) == cluster)
            cluster_limit = equity * self.limits.max_sector_exposure_pct / 100
            cluster_allowed = cluster_limit - cluster_notional
            if cluster_notional + notional > cluster_limit:
                if cluster_allowed < 100:
                    return False, f"Cluster {cluster} exposure limit reached", 0.0
                allowed = min(allowed, cluster_allowed)
                scale_reason = f"Cluster limit, scaled to ${cluster_allowed:.0f}"

            # 4. Single asset exposure
            asset_notional = sum(p.notional for p in self._positions.values() if p.product_id == product_id)
            asset_limit = equity * self.limits.max_single_asset_pct / 100
            asset_allowed = asset_limit - asset_notional
            if asset_notional + notional > asset_limit:
                if asset_allowed < 100:
                    return False, f"Asset {product_id} exposure limit reached", 0.0
                allowed = min(allowed, asset_allowed)
                scale_reason = f"Asset limit, scaled to ${asset_allowed:.0f}"

            # 5. Correlation check — HARD stop (count highly correlated positions)
            corr_count = 0
            for p in self._positions.values():
                if p.product_id != product_id:
                    corr = self._corr_matrix.get(product_id, {}).get(p.product_id, 0)
                    if corr > 0.7:
                        corr_count += 1
            if corr_count >= self.limits.max_correlated_positions:
                return False, f"Too many correlated positions ({corr_count} >= {self.limits.max_correlated_positions})", 0.0

            # 6. Gross leverage — HARD ceiling on total notional. Always evaluated.
            current_total = sum(p.notional for p in self._positions.values())
            lev_allowed = equity * self.limits.max_leverage - current_total
            if current_total + notional > equity * self.limits.max_leverage:
                if lev_allowed < 100:
                    return False, f"Max leverage {self.limits.max_leverage}x reached", 0.0
                allowed = min(allowed, lev_allowed)
                scale_reason = f"Leverage limit, scaled to ${lev_allowed:.0f}"

            if allowed < 100:
                return False, "Scaled notional below $100 minimum", 0.0
            return True, scale_reason, allowed
    
    def get_risk_metrics(self, equity: float) -> RiskMetrics:
        """Get current risk metrics snapshot."""
        with self._lock:
            self.update_equity(equity)
            
            total_notional = sum(p.notional for p in self._positions.values())
            gross_leverage = total_notional / equity if equity > 0 else 0
            net_exposure = sum(p.notional if p.side == "LONG" else -p.notional 
                             for p in self._positions.values())
            
            dd = 0.0
            if self._daily_peak_equity > 0:
                dd = (self._daily_peak_equity - equity) / self._daily_peak_equity * 100
            
            daily_pnl = 0.0
            if self._daily_start_equity > 0:
                daily_pnl = (equity - self._daily_start_equity) / self._daily_start_equity * 100
            
            cluster_exposures = defaultdict(float)
            asset_exposures = {}
            for p in self._positions.values():
                c = self.get_cluster(p.product_id)
                cluster_exposures[c] += p.notional
                asset_exposures[p.product_id] = p.notional
            
            # Risk score (0-100)
            risk_score = min(100, 
                dd * 2 + 
                daily_pnl * -1 * 2 +  # negative daily pnl increases risk
                (gross_leverage / self.limits.max_leverage) * 30 +
                max(cluster_exposures.values(), default=0) / equity * 100 * 0.5 if equity > 0 else 0
            )
            
            # Check breaches
            breaches = []
            if dd >= self.limits.max_portfolio_drawdown_pct:
                breaches.append(f"Portfolio DD {dd:.1f}% >= {self.limits.max_portfolio_drawdown_pct}%")
            if daily_pnl <= -self.limits.max_daily_loss_pct:
                breaches.append(f"Daily loss {daily_pnl:.1f}% <= -{self.limits.max_daily_loss_pct}%")
            for cluster, exp in cluster_exposures.items():
                if exp > equity * self.limits.max_sector_exposure_pct / 100:
                    breaches.append(f"Cluster {cluster} ${exp:.0f} > limit")
            
            return RiskMetrics(
                total_equity=equity,
                total_notional=total_notional,
                gross_leverage=gross_leverage,
                net_exposure=net_exposure,
                portfolio_drawdown_pct=dd,
                daily_pnl_pct=daily_pnl,
                cluster_exposures=dict(cluster_exposures),
                asset_exposures=asset_exposures,
                correlated_groups=[],  # Computed in _update_risk_metrics
                risk_score=risk_score,
                limit_breaches=breaches,
            )
    
    def should_reduce_risk(self, equity: float) -> tuple[bool, float]:
        """Check if portfolio should reduce risk (returns scale factor 0-1)."""
        metrics = self.get_risk_metrics(equity)
        
        # Scale down as we approach limits
        scale = 1.0
        
        # Drawdown scaling
        dd_ratio = metrics.portfolio_drawdown_pct / max(1.0, self.limits.max_portfolio_drawdown_pct)
        if dd_ratio > 0.5:
            scale *= 1.0 - (dd_ratio - 0.5) * 1.5  # 50% DD -> 75% scale, 80% DD -> 30% scale
        
        # Daily loss scaling
        daily_ratio = abs(min(0, metrics.daily_pnl_pct)) / max(1.0, self.limits.max_daily_loss_pct)
        if daily_ratio > 0.5:
            scale *= 1.0 - (daily_ratio - 0.5) * 1.5
        
        # Leverage scaling
        lev_ratio = metrics.gross_leverage / max(0.1, self.limits.max_leverage)
        if lev_ratio > 0.8:
            scale *= 1.0 - (lev_ratio - 0.8) * 2.0
        
        scale = max(0.0, min(1.0, scale))
        return scale < 1.0, scale
    
    def get_allowed_size(self, product_id: str, side: str, price: float, equity: float) -> float:
        """Get maximum allowed position size in base currency."""
        allowed, _, adj_notional = self.check_pre_trade(product_id, side, 1e9, price, equity)
        if not allowed:
            return 0.0
        return adj_notional / price if price > 0 else 0.0


# Global instance
_RISK_MGR: Optional[PortfolioRiskManager] = None
_RISK_LOCK = threading.Lock()


def get_risk_manager() -> PortfolioRiskManager:
    global _RISK_MGR
    with _RISK_LOCK:
        if _RISK_MGR is None:
            _RISK_MGR = PortfolioRiskManager()
        return _RISK_MGR