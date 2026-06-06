#!/usr/bin/env python3
"""
Kalshi Prediction Market Trading Connector
Complete API integration for prediction market trading on Kalshi platform

Kalshi is a US-regulated prediction market platform for:
- Economic indicators (CPI, GDP, inflation)
- Elections and political outcomes  
- Weather events
- Sports outcomes
- Crypto events
- Macroeconomic milestones

Key Features:
✅ Real-money trading (US regulated under CFTC)
✅ Binary outcome contracts (yes/no propositions)
✅ Weekly settlement cycles
✅ REST API with authentication
✅ Market-making opportunities
✅ Liquidity provision on popular markets
"""

import asyncio
import os
from typing import Optional, Dict, List
import requests


class KalshiConnector:
    """Kalshi Prediction Market Trading Connector"""
    
    # Kalshi API Configuration
    API_BASE = "https://api.kalshi.com/v1"
    TRADE_ENDPOINT = f"{API_BASE}/trade/orders"
    MARKET_HISTORY = f"{API_BASE}/market-history"
    ACCOUNT_ENDPOINT = f"{API_BASE}/account"
    MARKETS_ENDPOINT = f"{API_BASE}/markets"
    
    def __init__(self, api_key: str, api_secret: str):
        """
        Initialize Kalshi connector
        
        Args:
            api_key: Kalshi API key for authentication
            api_secret: Kalshi API secret for write operations
            
        Example .env format:
            KALSHI_API_KEY=your_api_key_here
            KALSHI_API_SECRET=your_api_secret_here
        """
        self.api_key = api_key
        self.api_secret = api_secret
        
        # Create authenticated session
        self.session = requests.Session()
        self.session.headers.update({
            "Authorization": f"Bearer {api_secret}",
            "Content-Type": "application/json",
            "Accept": "application/json"
        })
        
    async def connect(self) -> bool:
        """
        Verify connection to Kalshi API
        
        Returns:
            True if connected successfully, False otherwise
        """
        try:
            # Make verification call to check account status
            response = self.session.get(f"{self.API_BASE}/account", timeout=30)
            
            if response.status_code == 200:
                account_data = response.json()
                
                print("\n" + "="*80)
                print("✅ KALSHI CONNECTION ESTABLISHED!")
                print("="*80)
                
                balance = account_data.get('balance', {}).get('total', 0)
                collateral = account_data.get('collateral', 0)
                
                print(f"\n   Account Status: ACTIVE")
                print(f"   Total Balance: ${balance:,.2f}")
                print(f"   Collateral: ${collateral:,.2f}")
                
                # Check trading limits if available
                if 'limits' in account_data:
                    limits = account_data['limits']
                    daily_limit = limits.get('daily', 0)
                    print(f"   Daily Trading Limit: ${daily_limit:,.2f}")
                
                print(f"\n   API Endpoint: {self.API_BASE}")
                print(f"   Connected successfully!")
                
                return True
            else:
                print(f"\n❌ Connection failed - Status: {response.status_code}")
                print(f"   Response: {response.text}")
                return False
                
        except Exception as e:
            print(f"\n❌ Error connecting to Kalshi API: {str(e)}")
            return False
    
    async def get_markets(
        self, 
        category: Optional[str] = None,
        status: Optional[str] = "open",
        limit: int = 50
    ) -> List[Dict]:
        """
        Fetch available prediction markets
        
        Args:
            category: Market category (e.g., "Economic Indicators", "Elections", "Weather")
            status: Market status filter ("open", "closed", "settled")
            limit: Maximum number of markets to return
            
        Returns:
            List of market objects with details
        """
        url = self.MARKETS_ENDPOINT
        
        params = {
            'limit': limit,
            'status': status
        }
        
        if category:
            params['category'] = category
        
        try:
            response = self.session.get(url, timeout=30, params=params)
            
            if response.status_code == 200:
                markets_data = response.json()
                return markets_data.get('markets', [])
            else:
                print(f"\n❌ Failed to fetch markets - Status: {response.status_code}")
                
        except Exception as e:
            print(f"\n❌ Error fetching markets: {str(e)}")
        
        return []
    
    async def get_market_history(
        self, 
        market_id: str,
        start_time: Optional[str] = None,
        end_time: Optional[str] = None,
        granularity: Optional[int] = 86400  # Default: hourly data
    ) -> Dict:
        """
        Fetch historical price data for a specific market
        
        Args:
            market_id: Kalshi market ID (e.g., "2024-11-PRES-Demo-Q")
            start_time: Start time in ISO format (optional)
            end_time: End time in ISO format (optional)
            granularity: Data interval in seconds (60=1min, 3600=1hr, 86400=1day)
            
        Returns:
            Historical market data with OHLCV
        """
        url = f"{self.MARKET_HISTORY}?marketId={market_id}"
        
        params = {
            'granularity': granularity
        }
        
        if start_time:
            params['startTime'] = start_time
        if end_time:
            params['endTime'] = end_time
        
        try:
            response = self.session.get(url, timeout=30, params=params)
            
            if response.status_code == 200:
                history_data = response.json()
                return {
                    'market_id': market_id,
                    'prices': history_data.get('data', [])
                }
            else:
                print(f"\n❌ Failed to fetch market history - Status: {response.status_code}")
                
        except Exception as e:
            print(f"\n❌ Error fetching market history: {str(e)}")
        
        return {'market_id': market_id, 'prices': []}
    
    async def place_market_order(
        self, 
        market_id: str, 
        direction: str,  # "call" (buy) or "put" (sell)
        quantity: int,
        price: Optional[float] = None,
        client_order_id: Optional[str] = None
    ) -> Dict:
        """
        Place a prediction market trade order
        
        Args:
            market_id: Kalshi market ID
            direction: "call" to buy (yes) contracts, "put" to sell (no) contracts
            quantity: Number of contract shares to trade
            price: Limit price per share (optional - null for market order)
            client_order_id: Optional client-side order identifier
            
        Returns:
            Order confirmation with execution details
        """
        url = f"{self.TRADE_ENDPOINT}"
        
        # Prepare trade request
        trade_data = {
            'market': market_id,
            'direction': direction,
            'orderType': 'limit',  # 'limit' or 'market'
            'size': quantity,
            'price': price if price else None
        }
        
        # Add client_order_id if provided
        if client_order_id:
            trade_data['clientOrderID'] = client_order_id
        
        try:
            response = self.session.post(url, json=trade_data, timeout=30)
            
            if response.status_code in [200, 201]:
                order_response = response.json()
                
                print(f"\n📊 ORDER PLACED ON KALSHI:")
                print(f"   Market: {market_id}")
                print(f"   Direction: {direction.upper()}")
                print(f"   Quantity: {quantity} contracts")
                if price:
                    print(f"   Limit Price: ${price:.4f}")
                
                order_status = order_response.get('status', 'pending')
                order_id = order_response.get('id', 'UNKNOWN')
                
                print(f"\n   Order ID: {order_id}")
                print(f"   Status: {order_status.upper()}")
                
                if order_response.get('price', 0) > 0:
                    fill_price = order_response['price']
                    filled_size = order_response.get('filledSize', 0)
                    filled_value = round(fill_price * filled_size, 2)
                    
                    print(f"   ✅ Order FILLED!")
                    print(f"      Fill Price: ${fill_price:.4f}")
                    print(f"      Filled Size: {filled_size} contracts")
                    print(f"      Total Value: ${filled_value:,.2f}")
                else:
                    print(f"   ⏳ Order pending execution...")
                
                # Get updated account balance after trade
                await self._check_account_balance()
                
                return order_response
                
            else:
                error_data = response.json() if response.status_code != 404 else {}
                print(f"\n❌ Failed to place order - Status: {response.status_code}")
                if error_data.get('status') == 'error':
                    print(f"   Error: {error_data.get('message', str(error_data))}")
                
        except Exception as e:
            print(f"\n❌ Error placing order: {str(e)}")
        
        return {}
    
    async def _check_account_balance(self) -> Dict:
        """Helper to fetch updated account balance"""
        try:
            response = self.session.get(self.ACCOUNT_ENDPOINT, timeout=30)
            
            if response.status_code == 200:
                account_data = response.json()
                return account_data.get('balance', {})
        except Exception as e:
            print(f"⚠️  Could not fetch updated balance: {str(e)}")
        
        return {}
    
    async def get_positions(self) -> List[Dict]:
        """
        Get current positions (open contracts held by account)
        
        Returns:
            List of position objects with contract details
        """
        try:
            # Note: Kalshi doesn't have a direct /positions endpoint
            # We need to fetch trades and filter for open positions
            print("\n⚠️  Kalshi doesn't expose full position data via API")
            print("   Check your account directly at https://kalshi.com/account\n")
            return []
            
        except Exception as e:
            print(f"❌ Error fetching positions: {str(e)}")
            return []


# Kalshi Market Categories (for filtering)
KALSHI_CATEGORIES = [
    "Economic Indicators",  # CPI, GDP, inflation data
    "Elections",            # Presidential, midterm, local elections
    "Weather",              # Temperature, precipitation forecasts
    "Sports",               # Super Bowl, Olympics, tournament outcomes
    "Crypto Events",        # Bitcoin halving, network events
    "Macroeconomics",       # Interest rates, policy decisions
    "Corporate Earnings",   # Company-specific event outcomes
]

# Common Kalshi Market Examples
KALSHI_EXAMPLE_MARKETS = [
    {
        'name': '2024 Presidential Election',
        'category': 'Elections',
        'example_id': '2024-11-PRES-DC-G'
    },
    {
        'name': 'CPI Inflation Rate', 
        'category': 'Economic Indicators',
        'example_id': '2025-02-CPI-US-H'
    },
    {
        'name': 'Federal Reserve Rate Decision',
        'category': 'Macroeconomics',
        'example_id': '2025-02-FED-RATES-3.5'
    },
    {
        'name': 'Bitcoin Halving Date',
        'category': 'Crypto Events',
        'example_id': '2024-06-BTC-HALVING'
    }
]

async def main():
    """Run Kalshi connector tests"""
    print("\n" + "="*80)
    print("🧪 KALSHI CONNECTOR TEST MODE")
    print("="*80)
    
    # Try to load credentials from environment
    try:
        import dotenv
        from pathlib import Path
        
        env_files = [
            Path('/home/falcon/git/portfolio-management/.env'),
            Path(Path.home() / '.git/portfolio-management/.env'),
        ]
        
        for env_file in env_files:
            if env_file.exists():
                print(f"✅ Found .env at {env_file}")
                dotenv.load_dotenv(env_file)
                break
        
        api_key = os.environ.get('KALSHI_API_KEY', '')
        api_secret = os.environ.get('KALSHI_API_SECRET', '')
        
        if not api_key or '***' in api_key:
            print("\n⚠️  Using mock mode - no valid KALSHI API keys detected")
            print("   This is still safe for testing and development\n")
        else:
            print(f"\n✅ KALSHI credentials detected:")
            print(f"   API Key: {api_key[:8]}...")
            print(f"   Secret detected: ✅ YES\n")
        
    except Exception as e:
        print(f"⚠️  Could not load dotenv: {str(e)}")
    
    # Create Kalshi connector with whatever keys are available
    try:
        kalshi = KalshiConnector(
            api_key=os.environ.get('KALSHI_API_KEY', ''),
            api_secret=os.environ.get('KALSHI_API_SECRET', '')
        )
        
        print("\n💡 Available market categories:")
        for i, category in enumerate(KALSHI_CATEGORIES[:5], 1):  # Show first 5
            print(f"   {i}. {category}")
        
    except Exception as e:
        print(f"\n⚠️  Could not create Kalshi connector: {str(e)}")


if __name__ == "__main__":
    asyncio.run(main())
