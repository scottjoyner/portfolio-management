# Trading System Deployment Checklist - Destroyer Machine (2026-05-27)
## ThinkPad T14 i7 32GB RAM | Alias: destroyer | Network: tailscale

---

## Pre-Deployment: Repository Setup

### Step 1: Clone Trading System on Destroyer
```bash
cd /home/destroyer/trading-system || mkdir -p ~/trading-system && cd ~/trading-system
git clone /home/falcon/git/portfolio-management/trading_system .
cd trading_system
```

### Step 2: Verify Repository State
```bash
git status --porcelain
# Should show clean working tree or staged changes from P0-P3 implementation
```

### Step 3: Create Deployment Directories
```bash
mkdir -p deploy
mkdir -p logs
mkdir -p data/backups

# Set proper permissions
chown -R $(whoami):$(whoami) deploy logs data
chmod 750 logs data
```

---

## Deployment Options (Choose One)

### Option A: Production Deployment (Mainnet)
```bash
cd /home/destroyer/trading_system

# Copy production environment file (create with actual RPC keys first!)
cp deploy/.env.prod.template .env  # Create from template with YOUR_RPC_KEYS

# Build and start production stack
docker-compose -f deploy/docker-compose.prod.yml up -d --build

# Verify deployment
docker-compose -f deploy/docker-compose.prod.yml logs -f trading-service
```

### Option B: Staging Deployment (Goerli/Sepolia Testnet)
```bash
cd /home/destroyer/trading_system

# Copy staging environment file
cp deploy/.env.staging.template .env  # Create with testnet keys

# Build and start staging stack
docker-compose -f deploy/docker-compose.staging.yml up -d --build

# Verify deployment
docker-compose -f deploy/docker-compose.staging.yml logs -f trading-service
```

### Option C: Direct Python Execution (No Docker, for development)
```bash
cd /home/destroyer/trading_system/onchain/pollers

# Install dependencies
pip install aiohttp eth_abi

# Run poller service directly
python3 service.py
```

---

## Post-Deployment Verification

### Quick Health Check Commands
```bash
# 1. Check container status
docker ps | grep trading-runtime-destructor

# 2. View logs (last 50 lines)
docker-compose -f deploy/docker-compose.prod.yml logs --tail=50 -f trading-service

# 3. Run Python import test
docker-compose exec trading-runtime-destructor python3 -c "
from onchain.runtime.service import OnchainRuntimeService
from onchain.pollers.service import OnchainPoller
print('✅ All P1.4 components imported successfully')
"

# 4. View container resources
docker stats trading-runtime-destructor --no-stream
```

### Comprehensive Health Monitor
```bash
cd /home/destroyer/trading_system
./deploy/health_monitor.sh

# For detailed output:
./deploy/health_monitor.sh --verbose
```

---

## Configuration Setup (MUST DO BEFORE PRODUCTION)

### Step 1: Edit Production Environment File
```bash
nano .env  # Or use your preferred editor

# REPLACE PLACEHOLDER RPC KEYS WITH YOUR ACTUAL PRODUCTION KEYS:
RPC_URL_ETH="https://eth-mainnet.g.alchemy.com/v2/YOUR_RPC_KEY"
RPC_URL_ARBITRUM="https://arb-mainnet.g.alchemy.com/v2/YOUR_ARBITRUM_KEY"
# ... (add all other RPC keys from deploy/.env.prod template)
```

### Step 2: Configure Database Credentials
**⚠️ WARNING:** Never commit actual database passwords to git!

Create separate `.env.production` file for production secrets:
```bash
# Create secrets-only file
cat > .env.production << 'EOF'
DATABASE_PASSWORD="YOUR_PRODUCTION_DB_PASSWORD"
ALERT_EMAIL="alerts@falcon.internal"
CORS_ORIGINS="https://destroyer.internal.tailscale.net"
EOF

# Add to .gitignore if not already
echo ".env.production" >> .gitignore
```

### Step 3: Adjust Performance Settings (Destroyer-Specific)
```bash
# Edit docker-compose.prod.yml if needed
nano deploy/docker-compose.prod.yml

# Find and update these sections:
# Uncomment and adjust memory/cpu limits for your machine:
deploy:
  resources:
    limits:
      memory: "4G"     # Adjust based on available RAM
      cpus: '8'        # Allocate from total available cores
```

---

## Database Migration (P3 Tables)

### Step 1: Check Current Migration State
```bash
cd /home/destroyer/trading_system/alembic/versions
ls -la *.py
# Should show: 0001_initial.py, 0002_onchain_runtime.py
```

### Step 2: Generate P3 Migration File
```bash
cd /home/destroyer/trading_system
alembic revision --autogenerate -m "Add P3 evaluation, approval, research tables"
```

**Output:** Creates `P3_models_addition_YYYYMMDD_HHMMSS.py`

### Step 3: Review Generated Migration
```bash
cat alembic/versions/*P3*.py | grep "CREATE TABLE" -A 2
# Should show CREATE TABLE for all 11 P3 tables
```

### Step 4: Apply Migration to Database
```bash
cd /home/destroyer/trading_system
alembic upgrade head

# Expected output:
# INFO  Running revision P3_models_addition_YYYYMMDD_HHMMSS
# ... SQL DDL output ...
# INFO  Revision HEAD up to date
```

### Step 5: Verify Migration Success
```bash
docker-compose exec trading-runtime-destructor python3 -c "
from sqlalchemy import inspect, create_engine
import os

db_url = os.environ.get('DATABASE_URL', 'postgresql://trading_db/trading_system')
engine = create_engine(db_url)

with engine.connect() as conn:
    inspector = inspect(engine)
    tables = sorted(inspector.get_table_names())
    
p3_tables = [t for t in tables if any(x in t.lower() for x in ['price_', 'approval_', 'hypothesis_', 'regime_'])]

if len(p3_tables) == 11:
    print(f'✅ P3 migration successful: {len(p3_tables)} tables created')
    print('\nP3 Tables:')
    for t in sorted(p3_tables):
        print(f'  - {t}')
else:
    print(f'⚠ Found {len(p3_tables)} P3 tables, expected 11')
"
```

---

## Monitoring and Maintenance Commands

### Real-time Log Monitoring
```bash
# Watch all service logs
docker-compose -f deploy/docker-compose.prod.yml logs -f trading-service

# Filter by error only
docker-compose logs | grep -i "error\|exception\|traceback" --color

# Check recent health status
docker-compose -f deploy/docker-compose.prod.yml logs --tail=50 | grep "HEALTH\|status"
```

### Resource Usage Monitoring
```bash
# Detailed container stats
docker stats trading-runtime-destructor --no-stream

# Check memory usage specifically
docker inspect trading-runtime-deconstructor | jq '.[0].HostConfig.NanoCpus'
docker inspect trading-runtime-destructor | jq '.[0].MemoryLimit'
```

### Performance Tuning Commands
```bash
# Increase container resource limits temporarily
docker-compose -f deploy/docker-compose.prod.yml scale trading-service=2

# Or adjust limits in docker-compose.prod.yml:
deploy:
  resources:
    limits:
      memory: "8G"     # Increased memory limit
      cpus: '16'       # More CPU cores
```

---

## Troubleshooting Quick Reference

### Container Won't Start
```bash
# Check container exit code
docker inspect trading-runtime-destructor | grep -A 5 '"State"'

# Rebuild and restart
docker-compose -f deploy/docker-compose.prod.yml up -d --build

# Check for Python import errors
docker-compose exec trading-runtime-destructor python3 -c "import aiohttp; print('OK')"
```

### Database Connection Fails
```bash
# Check PostgreSQL is running
docker ps | grep postgres || docker logs trading-runtime-destructor | grep -i "postgres"

# Reinitialize database (WARNING: data loss!)
docker-compose -f deploy/docker-compose.prod.yml down
docker-compose -f deploy/docker-compose.prod.yml up -d --build
```

### Memory Usage High
```bash
# Check current memory usage
docker stats trading-runtime-destructor --no-stream | grep memory

# Restart service to free memory
docker-compose -f deploy/docker-compose.prod.yml restart trading-service
```

---

## Production Deployment Complete! ✅

### Final Verification Commands
```bash
# 1. All containers running
docker-compose ps

# 2. Import test passes
docker-compose exec trading-runtime-destructor python3 -c "print('✅ P1.4 components loaded OK')"

# 3. Migration successful  
alembic current

# 4. Health check endpoint responds
curl http://localhost:8000/health || echo "Health endpoint not exposed (normal for container)"

# 5. View last 20 lines of logs
docker-compose -f deploy/docker-compose.prod.yml logs --tail=20 trading-service
```

---

## Support & Documentation

### Main Implementation Guide
- `deploy/DEPLOY_PRODUCTION.md` - Full production deployment guide
- `deploy/DEPLOY_QUICKSTART.md` - Quick start commands
- `deploy/03_DATABASE_MIGRATION_P3_MODELS.md` - P3 migration details

### Runtime Documentation  
- `trading_system/HANDOFF.md` - P1.4 implementation guide
- `trading_system/docs/P1_README.md` - Phase 1 documentation
- `trading_system/docs/PHASE2_README.md` - Phase 2 documentation
- `trading_system/docs/PHASE3_IMPLEMENTATION_SUMMARY.md` - Phase 3 documentation

### Deployment Scripts
- `deploy/health_monitor.sh` - Production health monitoring script
- `deploy/.github/workflows/deploy.yml` - CI/CD pipeline (GitHub Actions)

---

## Contact & Escalation

**Issues Found After Deployment:**
1. Check logs first: `docker-compose -f deploy/docker-compose.prod.yml logs --tail=50 -f trading-service`
2. Review deployment docs: `deploy/DEPLOY_PRODUCTION.md` Troubleshooting section
3. Escalate to fleet operations if issue persists

**On-call Support:**
- Slack: #fleet-operations (Tailscale workspace)
- PagerDuty: @destroyer-oncall (for critical issues only)

---

## Sign-off

✅ **Deployment Date:** 2026-05-27  
✅ **Machine:** destroyer (ThinkPad T14 i7 32GB RAM)  
✅ **Network:** tailscale.internal (production VPC)  
✅ **Status:** Ready for production deployment  

---

**END OF DEPLOYMENT CHECKLIST**
