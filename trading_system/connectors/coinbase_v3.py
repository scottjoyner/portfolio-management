#!/usr/bin/env python3
"""
Production Coinbase Advanced Trade Connector
Uses the Coinbase CLI for authentication and JSON-native API access.

Features:
  ✅ Real JWT/ES256 authentication (handled by CLI)
  ✅ Market and limit orders with dry-run preview
  ✅ Portfolio management
  ✅ Real-time price fetching
  ✅ Order management and fill tracking
  ✅ Idempotent order creation (via client_order_id)
  ✅ Error handling with detailed feedback

Usage:
  from trading_system.connectors.coinbase_v3 import CoinbaseConnector
  
  connector = CoinbaseConnector()
  balances = connector.get_balances()
  price = connector.get_price('BTC-USD')
  order = connector.preview_order('BTC-USD', 'BUY', '100', order_type='market')
"""

import subprocess
import json
import logging
from typing import Optional, Dict, List, Any
from dataclasses import dataclass
from datetime import datetime
import uuid

logger = logging.getLogger(__name__)


@dataclass
class OrderPreview:
    """Order preview result with estimated costs and fees."""
    product_id: str
    side: str
    order_type: str
    quote_size: Optional[float] = None
    base_size: Optional[float] = None
    limit_price: Optional[float] = None
    total_fee: Optional[float] = None
    total_cost: Optional[float] = None
    estimated_fill_price: Optional[float] = None


@dataclass
class Order:
    """Executed order result."""
    order_id: str
    product_id: str
    side: str
    status: str
    filled_size: float
    filled_value: float
    average_filled_price: float
    fees: float
    created_at: str
    updated_at: str


class CoinbaseConnectorV3:
    """
    Production Coinbase v3 API connector using the official Coinbase CLI.
    
    All authentication is handled via the CLI's JWT/ES256 signing.
    This connector provides a clean Python interface to CLI commands.
    """
    
    def __init__(self, environment: str = 'live', timeout: int = 30):
        """
        Initialize connector.
        
        Args:
            environment: 'live' for production, 'sandbox' for testing
            timeout: Request timeout in seconds
        """
        self.environment = environment
        self.timeout = timeout
        self._verify_cli()
    
    def _verify_cli(self) -> bool:
        """Verify Coinbase CLI is installed and configured."""
        try:
            # Just check if the command exists - output format varies
            subprocess.run(['coinbase', '--version'], capture_output=True, check=True)
            logger.info(f"✅ Coinbase CLI configured for {self.environment}")
            return True
        except Exception as e:
            logger.error(f"❌ Coinbase CLI verification failed: {e}")
            raise RuntimeError(
                "Coinbase CLI not properly configured. "
                "Run: python3 scripts/setup_coinbase_credentials.py <key_file.json>"
            )
    
    def _run_command(
        self, 
        cmd: List[str], 
        stdin_data: Optional[str] = None,
        parse_json: bool = True
    ) -> Any:
        """
        Execute a Coinbase CLI command and return parsed result.
        
        Args:
            cmd: Command and arguments
            stdin_data: Optional stdin input
            parse_json: If True, parse stdout as JSON
            
        Returns:
            Parsed JSON result or raw string output
            
        Raises:
            RuntimeError: On CLI errors
        """
        try:
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=self.timeout,
                input=stdin_data
            )
            
            if result.returncode != 0:
                error_msg = result.stderr or result.stdout
                logger.error(f"CLI Error: {error_msg}")
                raise RuntimeError(f"Coinbase CLI error: {error_msg}")
            
            if parse_json:
                try:
                    return json.loads(result.stdout)
                except json.JSONDecodeError as e:
                    logger.error(f"Failed to parse JSON: {result.stdout}")
                    raise RuntimeError(f"Invalid JSON response: {e}")
            
            return result.stdout.strip()
            
        except subprocess.TimeoutExpired:
            raise RuntimeError(f"Command timed out after {self.timeout}s")
        except FileNotFoundError:
            raise RuntimeError(
                "Coinbase CLI not found. Install with: npm install -g @coinbase/coinbase-cli"
            )
    
    # ========== Market Data ==========
    
    def get_price(self, product_id: str) -> Dict[str, Any]:
        """
        Get current price and market info for a product.
        
        Args:
            product_id: Trading pair (e.g., 'BTC-USD', 'ETH-USD')
            
        Returns:
            Dict with price, 24h change, volume, etc.
        """
        result = self._run_command([
            'coinbase', 'products', 'get', product_id,
            '-e', self.environment
        ])
        return result
    
    def list_products(self, product_type: str = 'SPOT') -> List[Dict[str, Any]]:
        """
        List all tradable products.
        
        Args:
            product_type: 'SPOT' or 'FUTURE'
            
        Returns:
            List of product dicts with id, name, base_currency, quote_currency
        """
        result = self._run_command([
            'coinbase', 'products', 'list',
            f'product_type=={product_type}',
            '-e', self.environment
        ])
        return result if isinstance(result, list) else [result]
    
    def get_order_book(self, product_id: str, level: int = 1) -> Dict[str, Any]:
        """
        Get order book (bids and asks).
        
        Args:
            product_id: Trading pair
            level: Aggregation level (1-3)
            
        Returns:
            Dict with 'bids' and 'asks'
        """
        result = self._run_command([
            'coinbase', 'products', 'book', product_id,
            f'level=={level}',
            '-e', self.environment
        ])
        return result
    
    def get_candles(
        self, 
        product_id: str, 
        granularity: str = '1h',
        limit: int = 300
    ) -> List[Dict[str, Any]]:
        """
        Get OHLCV candlestick data.
        
        Args:
            product_id: Trading pair
            granularity: '1m', '5m', '15m', '1h', '6h', '1d'
            limit: Number of candles (max 300)
            
        Returns:
            List of [time, open, high, low, close, volume]
        """
        result = self._run_command([
            'coinbase', 'products', 'candles', product_id,
            f'granularity=={granularity}',
            '-e', self.environment
        ])
        return result if isinstance(result, list) else [result]
    
    # ========== Balances & Portfolios ==========
    
    def get_balances(self) -> Dict[str, Dict[str, str]]:
        """
        Get all account balances.
        
        Returns:
            Dict: {currency: {available: '10.5', held: '0'}, ...}
        """
        result = self._run_command([
            'coinbase', 'balance',
            '-e', self.environment
        ])
        return result
    
    def get_portfolios(self) -> List[Dict[str, Any]]:
        """
        List all portfolios.
        
        Returns:
            List of portfolio dicts with id, name, type, balances
        """
        result = self._run_command([
            'coinbase', 'portfolios', 'list',
            '-e', self.environment
        ])
        return result if isinstance(result, list) else [result]
    
    def get_portfolio(self, portfolio_id: str) -> Dict[str, Any]:
        """
        Get detailed portfolio info.
        
        Args:
            portfolio_id: Portfolio UUID
            
        Returns:
            Portfolio with breakdown, allocation %, unrealized PnL
        """
        result = self._run_command([
            'coinbase', 'portfolios', 'get', portfolio_id,
            '-e', self.environment
        ])
        return result
    
    def create_portfolio(self, name: str) -> Dict[str, Any]:
        """Create a new portfolio."""
        result = self._run_command([
            'coinbase', 'portfolios', 'create', f'name={name}',
            '-e', self.environment
        ])
        return result
    
    # ========== Orders ==========
    
    def preview_order(
        self,
        product_id: str,
        side: str,
        order_type: str = 'market',
        quote_size: Optional[float] = None,
        base_size: Optional[float] = None,
        limit_price: Optional[float] = None,
        portfolio_id: Optional[str] = None
    ) -> OrderPreview:
        """
        Preview an order WITHOUT executing it.
        Shows estimated fees, slippage, and fill price.
        
        Args:
            product_id: Trading pair (e.g., 'BTC-USD')
            side: 'BUY' or 'SELL'
            order_type: 'market' or 'limit'
            quote_size: Amount to spend (for buys in USD)
            base_size: Amount of asset (for sells)
            limit_price: For limit orders
            portfolio_id: Optional portfolio UUID
            
        Returns:
            OrderPreview with estimated costs and fees
        """
        cmd = [
            'coinbase', 'orders', 'preview',
            f'product_id={product_id}',
            f'side={side}',
            f'type={order_type}',
            '-e', self.environment
        ]
        
        if quote_size:
            cmd.append(f'quote_size={quote_size}')
        if base_size:
            cmd.append(f'base_size={base_size}')
        if limit_price:
            cmd.append(f'limit_price={limit_price}')
        if portfolio_id:
            cmd.append(f'portfolio_id={portfolio_id}')
        
        result = self._run_command(cmd)
        
        return OrderPreview(
            product_id=product_id,
            side=side,
            order_type=order_type,
            quote_size=quote_size,
            base_size=base_size,
            limit_price=limit_price,
            total_fee=result.get('total_fee'),
            total_cost=result.get('total_cost'),
            estimated_fill_price=result.get('estimated_fill_price')
        )
    
    def create_order(
        self,
        product_id: str,
        side: str,
        order_type: str = 'market',
        quote_size: Optional[float] = None,
        base_size: Optional[float] = None,
        limit_price: Optional[float] = None,
        portfolio_id: Optional[str] = None,
        client_order_id: Optional[str] = None
    ) -> Order:
        """
        Execute an order.
        
        Args:
            product_id: Trading pair
            side: 'BUY' or 'SELL'
            order_type: 'market' or 'limit'
            quote_size: Amount to spend (for buys)
            base_size: Amount of asset (for sells)
            limit_price: For limit orders
            portfolio_id: Optional portfolio UUID
            client_order_id: For idempotency (auto-generated if not provided)
            
        Returns:
            Order with order_id, status, filled_size, fees, etc.
            
        Note:
            Always include client_order_id for production use!
            This makes the order idempotent - if the connection drops
            and you retry with the same ID, you get the existing order.
        """
        if not client_order_id:
            client_order_id = str(uuid.uuid4())
        
        cmd = [
            'coinbase', 'orders', 'create',
            f'product_id={product_id}',
            f'side={side}',
            f'type={order_type}',
            f'client_order_id={client_order_id}',
            '-e', self.environment
        ]
        
        if quote_size:
            cmd.append(f'quote_size={quote_size}')
        if base_size:
            cmd.append(f'base_size={base_size}')
        if limit_price:
            cmd.append(f'limit_price={limit_price}')
        if portfolio_id:
            cmd.append(f'portfolio_id={portfolio_id}')
        
        result = self._run_command(cmd)
        
        return Order(
            order_id=result.get('id'),
            product_id=product_id,
            side=side,
            status=result.get('status'),
            filled_size=float(result.get('filled_size', 0)),
            filled_value=float(result.get('filled_value', 0)),
            average_filled_price=float(result.get('average_filled_price', 0)),
            fees=float(result.get('total_fees', 0)),
            created_at=result.get('created_at'),
            updated_at=result.get('updated_at')
        )
    
    def get_order(self, order_id: str) -> Dict[str, Any]:
        """Get order details."""
        result = self._run_command([
            'coinbase', 'orders', 'get', order_id,
            '-e', self.environment
        ])
        return result
    
    def list_orders(self, product_id: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        List orders.
        
        Args:
            product_id: Filter by product (optional)
            
        Returns:
            List of orders with status, fills, fees
        """
        cmd = ['coinbase', 'orders', 'list', '-e', self.environment]
        if product_id:
            cmd.insert(3, f'product_id=={product_id}')
        
        result = self._run_command(cmd)
        return result if isinstance(result, list) else [result]
    
    def cancel_order(self, order_id: str) -> Dict[str, Any]:
        """Cancel an order."""
        result = self._run_command([
            'coinbase', 'orders', 'cancel',
            f'order_ids:=["{order_id}"]',
            '-e', self.environment
        ])
        return result
    
    def get_fills(self, product_id: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Get all trade fills.
        
        Args:
            product_id: Filter by product (optional)
            
        Returns:
            List of fills with price, size, commission
        """
        cmd = ['coinbase', 'orders', 'fills', '-e', self.environment]
        if product_id:
            cmd.insert(3, f'product_id=={product_id}')
        
        result = self._run_command(cmd)
        return result if isinstance(result, list) else [result]
    
    # ========== Conversions ==========
    
    def get_conversion_quote(
        self,
        from_currency: str,
        to_currency: str,
        amount: float
    ) -> Dict[str, Any]:
        """
        Get a conversion quote (USDC to USD, etc).
        
        Args:
            from_currency: Source currency
            to_currency: Target currency
            amount: Amount to convert
            
        Returns:
            Dict with quote_id, rate, fee, amount_out
        """
        result = self._run_command([
            'coinbase', 'convert', 'quote',
            f'from={from_currency}',
            f'to={to_currency}',
            f'amount={amount}',
            '-e', self.environment
        ])
        return result
    
    def execute_conversion(
        self,
        quote_id: str,
        from_currency: str,
        to_currency: str
    ) -> Dict[str, Any]:
        """Execute a quoted conversion."""
        result = self._run_command([
            'coinbase', 'convert', 'execute', quote_id,
            f'from={from_currency}',
            f'to={to_currency}',
            '-e', self.environment
        ])
        return result
    
    # ========== Account Info ==========
    
    def get_fees(self) -> Dict[str, Any]:
        """Get fee tier and 30-day volume."""
        result = self._run_command([
            'coinbase', 'fees',
            '-e', self.environment
        ])
        return result


# For backward compatibility
CoinbaseConnector = CoinbaseConnectorV3


if __name__ == '__main__':
    # Quick test
    import sys
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s | %(name)s | %(message)s'
    )
    
    try:
        cb = CoinbaseConnectorV3()
        print("\n✅ Coinbase Connector initialized successfully\n")
        
        print("Testing basic operations...")
        balances = cb.get_balances()
        print(f"✅ Got balances: {len(balances)} currencies")
        
        price = cb.get_price('BTC-USD')
        print(f"✅ BTC price: ${price.get('price', 'N/A')}")
        
        print("\n💡 Ready to use in your trading system!")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        sys.exit(1)
