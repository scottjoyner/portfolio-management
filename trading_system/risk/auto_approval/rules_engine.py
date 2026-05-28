"""Auto-approval rules engine for trade approvals."""


class AutoApprovalRulesEngine:
    """Rules engine for automatic trade approval.
    
    Determines if trades meet criteria for auto-approval based on whitelist patterns
    and configurable thresholds.
    """
    
    def __init__(self):
        # White-listed opportunity rules (always approve)
        self.whitelist_patterns = [
            {
                'pattern': 'BTC/USD',
                'max_position_usd': 10000.0,
                'min_confidence_score': 0.85
            },
            {
                'pattern': 'ETH/USD',
                'max_position_usd': 25000.0,
                'min_confidence_score': 0.90
            }
        ]
        
        # Default approval rules for non-whitelisted opportunities  
        self.default_rules = {
            'max_position_usd': 15000.0,
            'min_confidence_score': 0.80
        }
    
    async def check_auto_approval(
        self,
        trade: dict,
        proposed_position_usd: float
    ) -> dict:
        """Check if trade meets auto-approval criteria.
        
        Args:
            trade: Trade dictionary with instrument and confidence score
            proposed_position_usd: Proposed position size
            
        Returns:
            Dictionary with auto_approved boolean, tier assignment, and reasoning
        """
        
        # Check whitelist patterns first
        matched_whitelist = None
        
        for pattern in self.whitelist_patterns:
            if trade.get('instrument', '').lower() == pattern['pattern'].lower():
                confidence = trade.get('confidence_score', 0)
                
                if (proposed_position_usd <= pattern['max_position_usd'] and 
                    confidence >= pattern['min_confidence_score']):
                    
                    matched_whitelist = {**pattern, 'reasoning': "Whitelisted opportunity meets size and confidence requirements"}
                    break
        
        if matched_whitelist:
            return {
                'auto_approved': True,
                'tier': 'FULL_SCALE',
                'reasoning': matched_whitelist['reasoning'],
                'analyst_reviews_required': 1
            }
        
        # Check default rules for non-whitelisted instruments
        if proposed_position_usd <= self.default_rules['max_position_usd']:
            confidence = trade.get('confidence_score', 0)
            
            if confidence >= self.default_rules['min_confidence_score']:
                return {
                    'auto_approved': True,
                    'tier': 'CANARY_PHASE',
                    'reasoning': "Meets default auto-approval thresholds",
                    'analyst_reviews_required': 1
                }
        
        return {
            'auto_approved': False,
            'tier': 'FULL_SCALE',
            'reasoning': "Does not meet auto-approval whitelist criteria",
            'analyst_reviews_required': 2
        }


__all__ = ["AutoApprovalRulesEngine"]
