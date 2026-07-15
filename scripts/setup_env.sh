#!/bin/bash

# Create example .env file for arbitrage platforms
cat > .env << 'EOF'
# Kalshi Prediction Markets Configuration
# Get these from: https://www.kalshi.com/
KALSHI_EMAIL=your_email@example.com
KALSHI_PASSWORD=your_kalshi_password

# Optional: Use API key authentication instead of email/password
KALSHI_API_KEY_ID=your_api_key_id
KALSHI_PRIVATE_KEY_PATH=/path/to/your/private_key.pem
KALSHI_API_BASE_URL=https://api.elections.kalshi.com/trade-api/v2  # or https://external-api.demo.kalshi.co/trade-api/v2

# Optional: Polymarket relayer
RELAYER_API_KEY=your_relayer_api_key
RELAYER_API_KEY_ADDRESS=your_wallet_address
POLYMARKET_RELAYER_URL=
POLYMARKET_RELAYER_CREDENTIALS_PATH=

# Safety controls
KILL_SWITCH=true
MAX_NOTIONAL=10000
MIN_EDGE=0.015
REQUIRE_APPROVAL=true

# Portfolio Optimizer settings
MIN_EVENT_MARKET_VOLUME=1000
MIN_EVENT_MARKET_EXTREMITY=0.25
MIN_ARB_EDGE=0.015
ARB_FEE_BUFFER=0.015
MIN_ARB_VOLUME=1000
ARB_SIMILARITY_THRESHOLD=0.30
MAX_ARB_MARKETS_PER_CATEGORY=20
MAX_EVENT_MARKETS_PER_CATEGORY=30
MAX_SPREAD=0.15
EOF

echo "Created .env template with arbitrage platform configuration."
echo "Please edit .env and add your Kalshi credentials, then run:"
echo "  python3 portfolio_optimizer.py --dry-run"
