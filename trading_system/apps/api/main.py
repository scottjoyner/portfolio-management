from __future__ import annotations

import time
import uuid
from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI, HTTPException, Request
from starlette.middleware.base import BaseHTTPMiddleware

from core.config.settings import Settings
from core.models.domain import ExchangeTrustScore, OrderIntent, RiskMode
from exchange.coinbase.reconciliation.service import ExchangeStateReconciler
from onchain.contracts.registry.service import ContractRegistry
from onchain.models import ActionType, ContractProfile, ExecutionRoute, Opportunity, RouteEdge, SafetyState, TokenProfile
from onchain.security.approval_gates.approval_packet import ApprovalPacketBuilder
from onchain.security.contract_safety.engine import ContractSafetyEngine
from onchain.security.token_safety.engine import TokenSafetyEngine
from onchain.simulation.call_static.harness import CallStaticHarness
from onchain.simulation.path_simulator.analyzer import PathAnalyzer
from onchain.strategies.execution.opportunity_ranker import OpportunityRanker
from onchain.strategies.hedging.hybrid_hedge import HybridHedgeLinker
from onchain.strategies.treasury.profit_sweep import ProfitCaptureEngine
from risk.engine import RiskEngine, RiskPolicy
from strategies.registry.registry import load_strategies
from apps.api.ops_layer import router as ops_router
from apps.api.ws_routes import router as ws_router
from apps.api.onchain_routes import router as onchain_router
from apps.api.metrics import metrics
