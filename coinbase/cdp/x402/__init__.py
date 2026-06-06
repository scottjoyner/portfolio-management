"""
Coinbase x402 Payment Module

Provides HTTP-native payment functionality for monetizing APIs and resources.
"""

from typing import Optional, Dict, Any
import json
import time


class MockX402Client:
    """Mock x402 client for development"""
    
    def create_payment_link(self, amount: float, currency: str) -> Dict[str, Any]:
        return {"link": "https://x402.coinbase.com/payment/...", "amount": amount, "mock": True}


class X402:
    """
    Coinbase x402 Client
    
    HTTP-native payments for monetizing APIs and resources.
    
    Usage:
        x402 = X402(mock_mode=False)
        
        # Create payment link
        result = x402.create_payment_link(amount=0.01, currency="BTC")
    """
    
    def __init__(self, mock_mode: bool = False):
        self.mock_mode = mock_mode
    
    def create_payment_link(
        self, 
        amount: float,
        currency: str,
        webhook_url: Optional[str] = None,
        description: Optional[str] = None
    ) -> Dict[str, Any]:
        """Create a payment link for HTTP monetization"""
        if self.mock_mode:
            import time
            return {
                "link_id": f"x402_{int(time.time())}",
                "amount": amount,
                "currency": currency,
                "webhook_url": webhook_url,
                "description": description,
                "status": "pending",
                "mock": True
            }
        
        try:
            import subprocess
            args = ["cdp", "x402", "create"]
            args.extend(["--amount", f"{amount}"])
            args.extend(["--currency", currency])
            
            if webhook_url:
                args.extend(["--webhook-url", webhook_url])
            
            result = subprocess.run(
                args,
                capture_output=True,
                text=True,
                timeout=60
            )
            
            if result.returncode == 0:
                output = json.loads(result.stdout) if result.stdout else {}
                return {
                    "link_id": f"x402_{int(time.time())}",
                    **output,
                    "mock": False
                }
            else:
                raise Exception(result.stderr)
                
        except FileNotFoundError:
            print("CDP CLI not installed")
            return {"error": "CDP CLI not installed", "mock_mode": self.mock_mode}
    
    def get_payment_status(self, link_id: str) -> Dict[str, Any]:
        """Get payment link status"""
        if self.mock_mode:
            return {
                "id": link_id,
                "status": "completed",
                "amount_paid": 0.01,
                "mock": True
            }
        
        try:
            import subprocess
            result = subprocess.run(
                ["cdp", "x402", "status", link_id],
                capture_output=True,
                text=True,
                timeout=30
            )
            
            if result.returncode == 0:
                output = json.loads(result.stdout) if result.stdout else {}
                return {
                    "id": link_id,
                    **output,
                    "mock": False
                }
            else:
                raise Exception(result.stderr)
                
        except FileNotFoundError:
            print("CDP CLI not installed")
            return {"error": "CDP CLI not installed", "mock_mode": self.mock_mode}


# ==================== EXAMPLE USAGE ====================

if __name__ == "__main__":
    x402 = X402(mock_mode=True)
    
    result = x402.create_payment_link(
        amount=0.01,
        currency="BTC",
        description="API call payment"
    )
    
    print(f"Payment link: {result}")
