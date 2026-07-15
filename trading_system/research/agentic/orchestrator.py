"""Multi-agent market research orchestrator."""

import asyncio
import datetime
from typing import Dict, Any


class ResearchOrchestrator:
    """Multi-agent research workflow coordinator.
    
    Coordinates sentiment analysis, technical analysis, and fundamental analysis
    agents to generate comprehensive market intelligence for trading opportunities.
    """
    
    def __init__(self):
        self.agents: Dict[str, callable] = {}
        self.results_store: Dict[str, Any] = {}
        
    def register_agent(self, name: str, agent_func: callable) -> None:
        """Register research agent by function reference."""
        self.agents[name] = agent_func
    
    async def run_workflow(
        self,
        instrument: str,
        analysis_period_days: int = 30
    ) -> Dict[str, Any]:
        """Run complete multi-agent research workflow.
        
        Args:
            instrument: Instrument symbol (e.g., 'BTC/USD')
            analysis_period_days: Number of days to analyze
            
        Returns:
            Dictionary with consensus signal, market regime, and individual agent results
        """
        
        # Create tasks for parallel execution
        tasks = []
        
        if 'sentiment' in self.agents:
            tasks.append(asyncio.create_task(self.agents['sentiment'](instrument)))
        if 'technical' in self.agents:
            tasks.append(asyncio.create_task(self.agents['technical'](instrument)))
        if 'fundamental' in self.agents:
            tasks.append(asyncio.create_task(self.agents['fundamental'](instrument)))
        
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Aggregate results with confidence scoring
        consensus_result = {
            'instrument': instrument,
            'timestamp': str(datetime.datetime.utcnow().isoformat()),
            'agents': {},
            'consensus': self._calculate_consensus(results),
            'market_regime': self._detect_regime(results)
        }
        
        return consensus_result
    
    def _detect_regime(self, results: list) -> Dict[str, Any]:
        """Infer a coarse market regime from agent results."""
        bull = sum(
            r.get('confidence', 0) for r in results
            if isinstance(r, dict) and r.get('signal') == 'buy'
        )
        bear = sum(
            r.get('confidence', 0) for r in results
            if isinstance(r, dict) and r.get('signal') == 'sell'
        )
        if bull > bear:
            regime = 'bullish'
        elif bear > bull:
            regime = 'bearish'
        else:
            regime = 'neutral'
        return {
            'regime': regime,
            'bullish_confidence': bull,
            'bearish_confidence': bear,
        }
    
    def _calculate_consensus(self, results: list) -> Dict[str, Any]:
        """Calculate cross-agent agreement."""
        bull_signals = 0
        bear_signals = 0
        
        for result in results:
            if isinstance(result, Exception):
                continue
            
            signal = result.get('signal', 'neutral')
            confidence = result.get('confidence', 0)
            
            if signal == 'buy':
                bull_signals += confidence
            elif signal == 'sell':
                bear_signals += confidence
        
        total_confidence = bull_signals + bear_signals
        
        if total_confidence == 0:
            return {
                'signal': 'neutral',
                'confidence_score': 0.0,
                'bullish_confidence': 0.0,
                'bearish_confidence': 0.0
            }
        
        consensus_signal = 'buy' if bull_signals > bear_signals else \
                           'sell' if bear_signals > bull_signals else \
                           'neutral'
        
        avg_confidence = (bull_signals + bear_signals) / total_confidence
        
        return {
            'signal': consensus_signal,
            'confidence_score': min(avg_confidence * 1.5, 1.0),
            'bullish_confidence': bull_signals,
            'bearish_confidence': bear_signals
        }


__all__ = ["ResearchOrchestrator"]
