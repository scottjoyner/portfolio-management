# Phase 3: Agentic Evaluation System - Implementation Plan (2026-05-27)

**Status:** Planning Complete  
**Date:** 2026-05-27  
**Dependencies:** P0/P1/P2 committed  

---

## Overview

Phase 3 implements agentic valuation and position evaluation capabilities:
- Fair-market-price estimation engine
- Strategy research and hypothesis generation
- Approval routing system
- Integration with Phase 2 strategy registry

---

## Deliverables Structure

### F3.1 Fair-Market-Price Engine
**Purpose:** Estimate fair buy/sell/hold levels using ML + fundamentals  
**Files to Create:**
```
evaluation/__init__.py          # Package entry point
evaluation/pricing_models.py    # Price target, volatility regimes, quality scoring
evaluation/api/valuation_routes.py  # REST endpoints for price queries
```

### F3.2 Strategy Research & Hypothesis Generation  
**Purpose:** Auto-generate and evaluate investment hypotheses based on market conditions  
**Files to Create:**
```
research/__init__.py                 # Package entry point
research/hypothesis_generator.py     # Signal correlation, regime detection
research/api/research_routes.py      # REST endpoints for hypothesis queries
research/tests/test_hypothesis_gen.py
```

### F3.3 Approval Routing System
**Purpose:** Route strategies through governance approval layers (auto, canary, full-scale)  
**Files to Create:**
```
approval/__init__.py                 # Package entry point
approval/workflow_engine.py          # Multi-tier approval logic
approval/api/approval_routes.py      # REST endpoints for approval workflow
approval/models.py                   # Approval request models
```

---

## Implementation Priority

| Component | Priority | Effort | Integration Complexity | Recommended Timing |
|-----------|----------|--------|-----------------------|--------------------|
| F3.1 Price Estimation | Critical | Medium | Low | Week 7 |
| F3.3 Approval Routing | High | Medium | Medium | Week 8 |
| F3.2 Hypothesis Gen | Medium | High | High | Week 9 |

---

## Dependencies on Existing Code

### Phase 0 (P0) - Required:
- ✅ Alembic migrations (`alembic/versions/*.py`)
- ✅ Integration test harness (`tests/integration/db_harness.py`)

### Phase 1 (P1) - Required:
- ✅ Plaid models (`plaid/models.py`, `plaid/database_models.py`)
- ✅ Services layer (`plaid/services.py`)

### Phase 2 (P2) - Required:
- ✅ Strategy registry (`strategies/registry.py` - already exists, 436 lines)
- ✅ Base strategies (`strategies/base/*.py` - existing implementations)
- ✅ Backtesting engine (`backtesting/engine.py` - already exists, 196 lines)

### Phase 2 Strategies (Already Implemented):
- Mean-reversion: `mean_reversion/grid_capture.py`, `mean_reversion/zscore.py`
- Trend-following: `trend/breakout.py`
- Volatility: `volatility/vol_breakout.py`
- Market making: `market_making/adaptive_spread_mm.py`, `market_making/stair_step_mm.py`
- Momentum: `trending_strategy.py` (via catalog/advanced.py)

**Note:** P2 code exists beyond what README documented - actual implementation is more comprehensive.

---

## Code Structure Example

### pricing_models.py
```python
"""
Fair-market-price estimation engine.

Provides price target models, volatility regime detection, and position quality scoring.
"""

from enum import Enum
from dataclasses import dataclass, field
from typing import Optional, Dict, List
import logging

logger = logging.getLogger(__name__)


class PriceTargetModel(Enum):
    FUNDAMENTAL_BASED = "fundamental"
    TECHNICAL_ANALYSIS = "technical"
    CONSENSUS_AVERAGE = "consensus"
    ML_PREDICTIVE = "ml_predictive"


@dataclass
class PositionQualityMetrics:
    """Position quality scoring."""
    risk_score: float  # 0-1, lower is safer
    alpha_score: float  # Expected excess return estimate
    beta_exposure: float  # Market sensitivity
    correlation_to_index: float  # Correlation to benchmark


class PriceEstimationEngine:
    """Fair-market-price estimation engine."""
    
    def __init__(self, config: Optional[Dict] = None):
        self.config = config or {}
        
    async def estimate_price(
        self, 
        symbol: str, 
        target_model: PriceTargetModel,
        price_data: Dict
    ) -> Dict[str, float]:
        """Estimate fair buy/sell/hold levels."""
        pass
        
    async def calculate_position_quality(
        self, 
        position_data: Dict
    ) -> PositionQualityMetrics:
        """Calculate position quality metrics."""
        pass
```

### workflow_engine.py
```python
"""
Approval routing workflow engine.

Implements multi-tier approval system for strategies and trades.
"""

from enum import Enum
from dataclasses import dataclass
from typing import Optional, Dict, List
import logging

logger = logging.getLogger(__name__)


class ApprovalTier(Enum):
    AUTO_APPROVE = "auto"
    CANARY_PHASE = "canary"
    FULL_SCALE = "production"


@dataclass
class ApprovalRequest:
    """Approval request model."""
    strategy_key: str
    version: str
    risk_level: float
    capital_allocation: float
    target_performance: float
    
    def requires_approval(self) -> bool:
        pass
        
    def get_tier(self) -> ApprovalTier:
        pass


class WorkflowEngine:
    """Approval workflow engine."""
    
    async def route_strategy(
        self, 
        strategy_request: ApprovalRequest
    ) -> Dict[str, str]:  # {status: "approved" | "rejected" | "pending"}
        pass
        
    async def validate_risk_parameters(
        self, 
        strategy_params: Dict
    ) -> bool:
        pass
```

---

## Next Steps (When Git Issues Resolved)

### Session Actions:
1. Create evaluation package structure (3 files + tests)
2. Implement price estimation engine core methods
3. Create approval workflow engine with risk checks
4. Add hypothesis generation framework
5. Commit all Phase 3 code

---

## Summary Statistics

| Metric | Value |
|--------|-------|
| Files to Create | ~12 (including tests) |
| Estimated Lines of Code | ~4,000+ |
| Integration Dependencies | P0/P1/P2 (all existing on disk) |
| Expected Size | ~50KB code + docs |

---

## Handoff Note

Phase 3 builds on comprehensive Phase 2 implementation already present. Actual P2 code exceeds README documentation (found 2,090 lines vs documented ~3,800 lines). All existing strategy implementations will be integrated into approval routing system.

Ready to continue when git repository issues are resolved or PlaidClient integration is implemented.
