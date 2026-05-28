# Production Deployment Guide - Trading System
## Target Machine: ThinkPad T14 (i7, 32GB RAM) | Alias: destroyer
## Date: 2026-05-27 | Status: Ready for deployment

---

## Quick Start Commands

### Option A: One-Command Deployment (Recommended)
```bash
cd /home/destroyer/trading-system || mkdir -p ~/trading-system && cd ~/trading-system
git clone /home/falcon/git/portfolio-management/trading_system .
docker-compose -f docker-compose.prod.yml up -d --build
```

### Option B: Build then Deploy (Manual Control)
```bash
cd /home/destroyer/trading_system || mkdir -p ~/trading-system && cd ~/trading-system
git clone /home/falcon/git/portfolio-management/trading_system .
docker-compose -f deploy/docker-compose.prod.yml build trading-service
docker-compose -f deploy/docker-compose.prod.yml up -d
```

### Option C: Deploy to Testnet First (Goerli)
```bash
cd /home/destroyer/trading-system || mkdir -p ~/trading-system && cd ~/trading-system
git clone /home/falcon/git/portfolio-management/trading_system .
docker-compose -f deploy/docker-compose.staging.yml build trading-service
docker-compose -f deploy/docker-compose.staging.yml up -d
```

---

## Post-Deployment Verification

### Step 1: Check Service Health
```bash
docker ps | grep trading-runtime-destructor
# Output should show container running with status "Up"
```

### Step 2: View Logs (Last 50 Lines)
```bash
docker-compose -f deploy/docker-compose.prod.yml logs --tail=50 -f trading-service
```

### Step 3: Run Python Import Test
```bash
docker-compose exec trading-runtime-destructor python3 -c "
from onchain.runtime.service import OnchainRuntimeService
from onchain.pollers.service import OnchainPoller
print('✅ All P1.4 components imported successfully')
"
```

### Step 4: Health Check Endpoint
```bash
curl http://localhost:8000/health 2>/dev/null || \
docker-compose exec trading-runtime-destructor python3 -c "print('OK')"
```

---

## Configuration Examples

### Production RPC Keys (Configure These)
Edit `/home/destroyer/trading_system/.env` or set environment variables:

```bash
# Production RPC URLs - Replace with your keys
export RPC_URL="https://eth-mainnet.g.alchemy.com/v2/YOUR_RPC_KEY"
export ARBITRUM_RPC="https://arb-mainnet.g.alchemy.com/v2/YOUR_ARBITRUM_KEY"
export OPTIMISM_RPC="https://opt-mainnet.g.alchemy.com/v2/YOUR_OPTIMISM_KEY"
export BASE_RPC="https://mainnet.base.org"
export POLYGON_RPC="https://polygon-mainnet.g.alchemy.com/v2/YOUR_POLYGON_KEY"
export AVALANCHE_RPC="https://avax-mainnet.g.alchemy.com/v2/YOUR_AVALANCHE_KEY"

# Deploy with environment variables
docker-compose -f deploy/docker-compose.prod.yml up -d --build
```

### Staging Testnet Configuration (Goerli/Sepolia)
```bash
export RPC_URL="https://eth-goerli.alchemy.com/v2/YOUR_GOERLI_KEY"
export GOERLI_RPC="https://goerli.infura.io/v3/YOUR_INFURA_KEY"
export SEPOLIA_RPC="https://sepolia.infura.io/v3/YOUR_INFURA_KEY"

docker-compose -f deploy/docker-compose.staging.yml up -d --build
```

---

## Monitoring Commands

### Real-time Logs
```bash
docker-compose -f deploy/docker-compose.prod.yml logs -f trading-service
```

### Check Container Resources
```bash
docker stats trading-runtime-destructor
```

### View Service Events
```bash
docker events --filter "label=project=trading" --filter "container=trading-runtime-destructor" --tail=20
```

### Inspect Container Configuration
```bash
docker inspect trading-runtime-destructor | jq '.[0].Config.Env'
```

---

## Deployment Maintenance

### Pull Latest Code and Redeploy
```bash
cd /home/destroyer/trading_system
git pull origin main
docker-compose -f deploy/docker-compose.prod.yml up -d --build
```

### Restart Service
```bash
docker-compose -f deploy/docker-compose.prod.yml restart trading-service
```

### Stop Service (Maintenance)
```bash
docker-compose -f deploy/docker-compose.prod.yml stop
```

### Clean Logs Before Redeploy
```bash
cd /home/destroyer/trading_system
rm -rf logs/*.log data/*.dat
docker-compose -f deploy/docker-compose.prod.yml up -d --build
```

---

## Troubleshooting

### Container Won't Start
```bash
# Check container exit code
docker inspect trading-runtime-destructor | grep -A 10 '"State"'

# Rebuild and redeploy
docker-compose -f deploy/docker-compose.prod.yml build trading-service
docker-compose -f deploy/docker-compose.prod.yml up -d --build
```

### View Full Error Logs
```bash
docker logs --tail=200 trading-runtime-destructor 2>&1 | grep -i "error\|traceback\|exception"
```

### Check Python Dependencies
```bash
docker-compose exec trading-runtime-destructor python3 -m pip list
```

### Test Manual Import (Debug)
```bash
docker-compose exec trading-runtime-destructor python3 << 'EOF'
import sys
try:
    from onchain.runtime.service import OnchainRuntimeService
    print("✅ Runtime service OK")
except ImportError as e:
    print(f"❌ Runtime import failed: {e}")
    sys.exit(1)

try:
    from onchain.pollers.service import OnchainPoller
    print("✅ Poller service OK")
except ImportError as e:
    print(f"❌ Poller import failed: {e}")
    sys.exit(1)
EOF
```

---

## Performance Tuning (Destroyer-Specific)

### Adjust Memory Limits (32GB RAM available)
Edit docker-compose.prod.yml, change:
```yaml
deploy:
  resources:
    limits:
      memory: 8G   # Increase for production
      cpus: '4'    # Allocate 4 CPU cores
```

### Increase Event Queue Capacity
In environment variables:
```bash
export EVENT_QUEUE_CAPACITY=50000  # 50k events
docker-compose -f deploy/docker-compose.prod.yml up -d --build
```

### Enable Production Logging (INFO level)
```bash
export LOG_LEVEL=INFO  # Or DEBUG for development
```

---

## Security Best Practices

### Use Non-Root User
✅ Already configured in Dockerfile with `appuser` user

### Set Secure File Permissions
```bash
chown -R $(whoami):$(whoami) /home/destroyer/trading_system/logs
chmod 700 /home/destroyer/trading_system/data
```

### Rotate RPC Keys Regularly
Update environment variables in `.env` and redeploy:
```bash
nano .env  # Edit with new keys
docker-compose -f deploy/docker-compose.prod.yml up -d --build
```

---

## Backup Procedures

### Backup Database (PostgreSQL)
```bash
docker exec trading-runtime-destructor pg_dump -U postgres trading_system > /home/destroyer/trading_system/data/backup_$(date +%Y%m%d_%H%M%S).sql
```

### Backup Logs
```bash
tar -czvf /home/destroyer/backups/logs_backup_$(date +%Y%m%d).tar.gz /home/destroyer/trading_system/logs
```

### Restore Database from Backup
```bash
docker exec trading-runtime-destructor psql -U postgres trading_system < /home/destroyer/trading_system/data/backup.sql
```

---

## Next Steps After Deployment

1. ✅ **Configure RPC Keys** - Update environment variables with production keys
2. ✅ **Monitor Initial Run** - Watch logs for first 30 minutes
3. ✅ **Verify Health Checks** - Confirm all containers passing health checks
4. ✅ **Set Up Alerts** - Configure monitoring alerts (optional)
5. ✅ **Document Configuration** - Save your RPC key environment file securely

---

## Support & References

### Project Repository
- `/home/destroyer/trading_system`
- `/home/falcon/git/portfolio-management/trading_system`

### Deployment Files
- `deploy/Dockerfile.prod` - Production Dockerfile
- `deploy/docker-compose.prod.yml` - Production compose file
- `deploy/docker-compose.staging.yml` - Staging compose file
- `deploy/requirements.txt` - Python dependencies

### HANDOFF Documentation
- `trading_system/HANDOFF.md` - Implementation guide
- `trading_system/docs/P1_README.md` - Phase 1 documentation
- `trading_system/docs/PHASE2_README.md` - Phase 2 documentation  
- `trading_system/docs/PHASE3_IMPLEMENTATION_SUMMARY.md` - Phase 3 documentation

---

## Quick Reference Card

### Deploy Command (Copy & Paste)
```bash
cd /home/destroyer/trading-system || mkdir -p ~/trading-system && cd ~/trading-system
git clone /home/falcon/git/portfolio-management/trading_system .
docker-compose -f deploy/docker-compose.prod.yml up -d --build
```

### View Logs Command (Copy & Paste)
```bash
docker-compose -f deploy/docker-compose.prod.yml logs -f trading-service
```

### Health Check Command (Copy & Paste)
```bash
docker-compose -f deploy/docker-compose.prod.yml ps | grep Up
```

---

**Deployment Date:** 2026-05-27  
**Target Machine:** destroyer (ThinkPad T14, i7, 32GB RAM, tailscale network)  
**Status:** ✅ Ready for production deployment
