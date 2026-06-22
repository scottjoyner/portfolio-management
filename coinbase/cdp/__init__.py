"""
Coinbase Developer Platform (CDP) Integration Package

This package provides comprehensive integration with Coinbase Developer Platform APIs including:
- CDP CLI wrapper for all operations
- MCP server integration  
- Wallet management
- Onramp/fiat onboarding
- Paymaster gas sponsorship
- x402 HTTP payments
- Webhook subscriptions and verification
- AgentKit AI agent tooling
- Agentic Wallet for autonomous workflows

Quick Start:
    from cdp import CDPCLI
    
    cli = CDPCLI(mock_mode=False)  # Production use
    cli = CDPCLI(mock_mode=True)   # Development/testing
    
    # Check balance
    balance = cli.get_wallet_balance(wallet_id="wallet_123", account_type="wallet")
    
    # Send payment
    result = cli.send_payment(
        from_wallet="wallet_123",
        to_account="account_xyz",
        amount=0.01,
        currency="BTC"
    )

Submodules:
    - cdp_cli_wrapper: Main CLI wrapper with all operations
    - wallet: Wallet management operations
    - auth: Authentication (JWT, API keys)
    - onramp: Fiat-to-crypto onboarding
    - paymaster: Gas sponsorship
    - x402: HTTP-native payments
    - webhooks: Event subscriptions and verification
    - agent_kit: AI agent tooling
    - agentic_wallet: Autonomous wallet operations

Documentation: https://docs.cdp.coinbase.com
"""

from typing import Dict, Any, Optional

# Import main CLI wrapper, but keep the package importable if the wrapper
# module is absent in this checkout.
try:
    from .cdp_cli_wrapper import (
        CDPCLI,
        CDPCLIError,
        MockCDPClient,
        quick_wallet_operations,
        verify_webhook_signature,
        subscribe_to_events,
    )
except ImportError:
    class CDPCLIError(Exception):
        pass

    class MockCDPClient:
        pass

    def quick_wallet_operations(*args, **kwargs):
        return {"ok": False, "error": "cdp_cli_wrapper unavailable"}

    def verify_webhook_signature(*args, **kwargs):
        return False

    def subscribe_to_events(*args, **kwargs):
        return {"ok": False, "error": "cdp_cli_wrapper unavailable"}

    class CDPCLI:
        def __init__(self, mock_mode: bool = False):
            self.mock_mode = mock_mode

        def get_wallet_balance(self, *args, **kwargs):
            return {"ok": False, "error": "cdp_cli_wrapper unavailable"}

# Import wallet module
from .wallet import CDPWallet, CDPWalletError

# Import transfers module
try:
    from .transfers import (
        CDPTransferError,
        build_crypto_transfer,
        create_transfer,
        execute_transfer,
        get_transfer,
        list_accounts,
        list_balances,
        list_transfers,
        preview_crypto_transfer,
        submit_crypto_transfer,
        validate_crypto_transfer,
    )
except ImportError:
    CDPTransferError = None
    def build_crypto_transfer(*args, **kwargs):
        return {"ok": False, "error": "transfers module unavailable"}
    def create_transfer(*args, **kwargs):
        return {"ok": False, "error": "transfers module unavailable"}
    def execute_transfer(*args, **kwargs):
        return {"ok": False, "error": "transfers module unavailable"}
    def get_transfer(*args, **kwargs):
        return {"ok": False, "error": "transfers module unavailable"}
    def list_accounts(*args, **kwargs):
        return {"ok": False, "error": "transfers module unavailable"}
    def list_balances(*args, **kwargs):
        return {"ok": False, "error": "transfers module unavailable"}
    def list_transfers(*args, **kwargs):
        return {"ok": False, "error": "transfers module unavailable"}
    def preview_crypto_transfer(*args, **kwargs):
        return {"ok": False, "error": "transfers module unavailable"}
    def submit_crypto_transfer(*args, **kwargs):
        return {"ok": False, "error": "transfers module unavailable"}
    def validate_crypto_transfer(*args, **kwargs):
        return {"ok": False, "error": "transfers module unavailable"}

# Import auth module
try:
    from .auth import CDPAuthentication, CDPAuthenticationError
except ImportError:
    CDPAuthentication = None
    CDPAuthenticationError = None

# Import onramp module
try:
    from .onramp import Onramp
except ImportError:
    Onramp = None

# Import paymaster module (if exists)
try:
    from .paymaster import Paymaster
except ImportError:
    Paymaster = None

# Import x402 module
try:
    from .x402 import X402
except ImportError:
    X402 = None

# Import webhooks module
try:
    from .webhooks import Webhooks
except ImportError:
    Webhooks = None

# Import agent-kit module
try:
    from .agent_kit import AgentKit
except ImportError:
    AgentKit = None


class CDPCoreClient:
    """
    Unified Coinbase CDP Client
    
    Provides a single interface to all CDP operations.
    
    Usage:
        core = CDPCoreClient(mock_mode=False)
        
        # Get balance
        balance = core.get_balance(wallet_id="wallet_123")
        
        # Send payment
        result = core.send_payment(...)
        
        # Subscribe to events
        sub = core.subscribe_webhooks(["payment.received"], url="...")
    """
    
    def __init__(self, mock_mode: bool = False):
        self.mock_mode = mock_mode
        self.cli = CDPCLI(mock_mode=mock_mode)
        self.wallet = CDPWallet(mock_mode=mock_mode)
        
        # Initialize other modules if available
        self.auth = CDPAuthentication(mock_mode=mock_mode) if CDPAuthentication else None
        self.onramp = Onramp(mock_mode=mock_mode) if Onramp else None
        self.paymaster = Paymaster(mock_mode=mock_mode) if Paymaster else None
        self.x402 = X402(mock_mode=mock_mode) if X402 else None
        self.webhooks = Webhooks(mock_mode=mock_mode) if Webhooks else None
        self.agent_kit = AgentKit(mock_mode=mock_mode) if AgentKit else None
    
    def get_balance(self, wallet_id: str, account_type: Optional[str] = None) -> Dict[str, Any]:
        """Get wallet balance"""
        return self.wallet.get_balance(wallet_id, account_type)
    
    def send_payment(self, from_wallet: str, to_account: str, amount: float,
                    currency: str, account_type: Optional[str] = None) -> Dict[str, Any]:
        """Send payment"""
        return self.wallet.transfer(from_wallet, to_account, amount, currency, account_type)

    def create_transfer(self, payload_json: str, dry_run: bool = True) -> Dict[str, Any]:
        """Create or preview a transfer."""
        return create_transfer(payload_json, dry_run=dry_run)

    def execute_transfer(self, transfer_id: str) -> Dict[str, Any]:
        """Execute a previously created transfer."""
        return execute_transfer(transfer_id)

    def build_crypto_transfer(self, **kwargs) -> Dict[str, Any]:
        """Build a crypto transfer payload."""
        return build_crypto_transfer(**kwargs)

    def preview_crypto_transfer(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """Preview a crypto transfer."""
        return preview_crypto_transfer(payload)

    def validate_crypto_transfer(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """Validate a crypto transfer without initiating it."""
        return validate_crypto_transfer(payload)

    def submit_crypto_transfer(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """Submit a crypto transfer."""
        return submit_crypto_transfer(payload)
    
    def create_payment_link(self, amount: float, currency: str,
                           webhook_url: Optional[str] = None) -> Dict[str, Any]:
        """Create x402 payment link"""
        if self.x402:
            return self.x402.create_payment_link(amount, currency, webhook_url)
        raise NotImplementedError("x402 not initialized")
    
    def subscribe_webhooks(self, event_types: list, url: str,
                          wallet_id: Optional[str] = None) -> Dict[str, Any]:
        """Subscribe to webhook events"""
        if self.webhooks:
            return self.webhooks.subscribe_webhooks(event_types, url, wallet_id)
        raise NotImplementedError("webhooks not initialized")


def get_balance(wallet_id: str, account_type: Optional[str] = None, 
                mock_mode: bool = False) -> Dict[str, Any]:
    """Convenience function to get balance"""
    core = CDPCoreClient(mock_mode=mock_mode)
    return core.get_balance(wallet_id, account_type)


def send_payment(from_wallet: str, to_account: str, amount: float,
                currency: str, account_type: Optional[str] = None,
                mock_mode: bool = False) -> Dict[str, Any]:
    """Convenience function to send payment"""
    core = CDPCoreClient(mock_mode=mock_mode)
    return core.send_payment(from_wallet, to_account, amount, currency, account_type)


__all__ = [
    # Main CLI wrapper
    "CDPCLI",
    "CDPCLIError", 
    "MockCDPClient",
    
    # Core client (unified interface)
    "CDPCoreClient",
    "get_balance",
    "send_payment",
    "build_crypto_transfer",
    "create_transfer",
    "execute_transfer",
    "list_accounts",
    "list_balances",
    "list_transfers",
    "get_transfer",
    "preview_crypto_transfer",
    "submit_crypto_transfer",
    "validate_crypto_transfer",
    
    # Module classes (for explicit imports)
    "CDPWallet",
    "CDPWalletError",
    "CDPTransferError",
    "CDPAuthentication",
    "Onramp",
    "Paymaster",
    "X402",
    "Webhooks",
    "AgentKit",
    
    # Convenience functions
    "quick_wallet_operations",
    "verify_webhook_signature",
    "subscribe_to_events"
]
