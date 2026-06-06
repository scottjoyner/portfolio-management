# Kalshi <-> Polymarket Arbitrage - VPS Setup Guide

## Quick Start (Recommended)

### Option 1: AWS EC2 t3.micro (Free Tier Eligible)

**Cost:** $0.04/hour (~$28.80/month) or free tier eligible for 12 months
**Location:** US-East (N. Virginia) or US-West (Oregon)

```bash
# Step 1: Create AWS account and EC2 instance
#   Go to console.aws.amazon.com/ec2/
#   Launch t3.micro Ubuntu 22.04 LTS in us-east-1

# Step 2: SSH into your instance
ssh -i your-key.pem ubuntu@your-instance-ip-address

# Step 3: Configure system
sudo apt update && sudo apt upgrade -y
sudo apt install python3-pip git curl -y

# Step 4: Clone repository or copy arbitrage code
git clone YOUR_REPOSITORY_URL  # OR copy trading_system directory

cd portfolio-management/trading_system/arbitrage

# Step 5: Set environment variables (replace with your API keys)
export KALSHI_API_KEY=your_kalshi_api_key_here
export POLYMARKET_API_KEY=your_polygon_io_api_key_here

# Step 6: Run detection and analysis
python3 detect_opportunities.py

# Step 7: Run full orchestration
python3 orchestrator.py
```

### Option 2: DigitalOcean Droplet

**Cost:** $5/month
**Location:** Select US region (NYC1, SFO1, etc.)

```bash
ssh root@your-droplet-ip

# Configure system
sudo apt update && sudo apt upgrade -y
sudo apt install python3-pip git -y

# Clone and run
git clone YOUR_REPOSITORY_URL  # OR copy code directly
cd portfolio-management/trading_system/arbitrage
export KALSHI_API_KEY=*** POLYMARKET_API_KEY=*** python3 orchestrator.py
```

### Option 3: Linode (Expo)

**Cost:** $5/month
**Location:** US regions

```bash
ssh root@your-linode-ip

sudo apt update && sudo apt upgrade -y
sudo apt install python3-pip git -y

# Run arbitrage system
cd portfolio-management/trading_system/arbitrage
export KALSHI_API_KEY=*** POLYMARKET_API_KEY=*** python3 orchestrator.py
```

## Alternative: VPN Setup (If you prefer not to rent VPS)

### Recommended VPN Services

**Surfshark:** $2.99/month - Unlimited devices, US servers
**NordVPN:** $3.39/month - Strong privacy, good for trading
**Mullvad:** $5/month - Privacy-focused, no-logs policy

```bash
# Install VPN client (example with NordVPN)
sudo apt install nordvpn

# Connect to US server
nordvpn connect us-new-york

# Now run arbitrage system
cd portfolio-management/trading_system/arbitrage
export KALSHI_API_KEY=*** POLYMARKET_API_KEY=*** python3 orchestrator.py
```

**Warning:** Some exchanges may block known VPN IPs. Test with small trades first.

## Performance Optimization Tips

### 1. Use AWS Free Tier (Best Value)

- **AWS EC2 t3.micro** with Amazon Linux 2 or Ubuntu
- Included in monthly AWS credits for new accounts
- Can run continuously without additional cost (within free tier limits)

### 2. Configure for Low Latency

```bash
# On VPS, configure network for optimal performance:
sudo sysctl -w net.core.rmem_max=16777216
sudo sysctl -w net.core.wmem_max=16777216
sudo sysctl -w net.ipv4.tcp_rmem="4096 87380 67108864"
sudo sysctl -w net.ipv4.tcp_wmem="4096 65536 16777216"

# Configure DNS for faster resolution
echo "nameserver 1.1.1.1" | sudo tee /etc/resolv.conf
echo "nameserver 8.8.8.8" | sudo tee -a /etc/resolv.conf
```

### 3. Set Up Auto-Running Service (Production)

```bash
# Create systemd service file
sudo nano /etc/systemd/arbitrage-trading.service

```ini
[Unit]
Description=Kalshi-Polymarket Arbitrage Trader
After=network.target

[Service]
Type=simple
User=ubuntu
WorkingDirectory=/home/ubuntu/portfolio-management/trading_system/arbitrage
Environment="KALSHI_API_KEY=***"
Environment="POLYMARKET_API_KEY=***"
ExecStart=/usr/bin/python3 orchestrator.py
Restart=always
RestartSec=60

[Install]
WantedBy=multi-user.target
```

```bash
# Enable and start service
sudo systemctl enable arbitrage-trading.service
sudo systemctl start arbitrage-trading.service
sudo systemctl status arbitrage-trading.service
```

### 4. Monitor Performance

```bash
# Check system resources
top -bn1 | head -20
free -m
df -h

# View logs
tail -f /home/ubuntu/portfolio-management/trading_system/arbitrage/arb_logs.log

# Check network latency to Kalshi/Polymarket
curl https://api.kalshi.com/v2/markets
curl https://api.polygon.io/v2/events
```

## Production Deployment Checklist

### Before Going Live with Real Money

- [ ] **Test with mock data first** (no API keys)
  ```bash
  export KALSHI_API_KEY="" POLYMARKET_API_KEY="" python3 orchestrator.py
  ```

- [ ] **Obtain real API keys**
  - Kalshi: Register at kalshi.com → Settings → API Keys
  - Polymarket/Polygon.io: Apply for developer access

- [ ] **Set position limits** (recommend max $500 per market)
  
- [ ] **Configure risk parameters:**
  ```python
  # In arbitrager.py, adjust these settings:
  position_size_usd = 500  # Starting with small amounts
  
  similarity_threshold = 0.80  # More strict matching
  min_divergence = 0.02  # Minimum 2% divergence for arbitrage
  ```

- [ ] **Implement alerts for errors:**
  ```python
  import subprocess
  subprocess.run(['telegram', 'sendmessage', 'bot_token=***,chat_id=***,text=Error!'])
  ```

- [ ] **Test with small amounts** before scaling up

### API Key Setup (Required for Real Trading)

#### Kalshi API Keys:
1. Go to https://kalshi.com/signup
2. Complete registration and KYC verification
3. Navigate to Settings → API Keys
4. Generate new API key
5. Copy and save securely

#### Polymarket API Keys (via Polygon.io):
1. Go to https://polygon.io/
2. Apply for developer account (requires email approval)
3. Once approved, generate API key from dashboard
4. Store securely

**Important:** Never share or commit API keys to Git repositories!

## Alternative: Local Development with VPS-like Environment

If you prefer not to rent a VPS, simulate one locally:

```bash
# WSL Ubuntu (already on your system)
cd /home/falcon/git/portfolio-management/trading_system/arbitrage

# Create environment file for consistent testing
cat > .env << EOF
KALSHI_API_KEY=your_key_here
POLYMARKET_API_KEY=your_key_here
HERMES_MODE=development
EOF

source .env
python3 orchestrator.py
```

## Cost Comparison

| Solution | Monthly Cost | Pros | Cons |
|----------|-------------|------|------|
| AWS EC2 t3.micro Free Tier | $0 (first 12 months) | Cheapest, scalable | Setup complexity, learning curve |
| DigitalOcean Droplet | $5/month | Easy, reliable | Small resources |
| Linode | $5/month | Good performance | Requires credit card |
| VPN Service | ~$4/month | Simple setup | Latency, privacy concerns |

## Monitoring Dashboard Setup

```bash
# Install monitoring tools on VPS
sudo apt install htop iotop nethogs -y

# Create simple monitoring script
cat > /home/ubuntu/portfolio-management/trading_system/arbitrage/monitor.sh << 'EOF'
#!/bin/bash
echo "=== System Resources ==="
free -m
echo ""
echo "=== Disk Usage ==="
df -h
echo ""
echo "=== Latest Log Entries ==="
tail -50 /home/ubuntu/portfolio-management/trading_system/arbitrage/arb_logs.log
EOF

chmod +x monitor.sh
```

## Security Best Practices

1. **Use SSH keys instead of passwords**
   ```bash
   # Generate key pair on your local machine
   ssh-keygen -t ed25519

   # Copy public key to VPS
   ssh-copy-id root@your-vps-ip
   ```

2. **Restrict API key access**
   - Use IP whitelisting where available
   - Never commit keys to Git
   - Use environment variables only

3. **Monitor for unusual activity**
   - Set up alerts for failed trades
   - Monitor position limits
   - Track API rate limiting

## Next Steps After Setup

1. **Run detection and analyze opportunities:**
   ```bash
   python3 detect_opportunities.py
   ```

2. **Review opportunity analysis:**
   ```bash
   cat trading_system/data/opportunity_analysis.json
   ```

3. **Test with small amounts once API keys are configured:**
   ```bash
   export KALSHI_API_KEY=*** POLYMARKET_API_KEY=*** python3 orchestrator.py
   ```

4. **Monitor and iterate on optimization**

## Support & Documentation

- Main documentation: `trading_system/arbitrage/README.md`
- API reference: Kalshi.com docs, Polygon.io docs
- Example code: See comprehensive test files

---

This guide provides multiple paths to deployment depending on your technical preference and budget constraints. The VPS approach is recommended for production use due to consistent internet access and 24/7 availability.
