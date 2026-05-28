# Production Deployment Infrastructure - Portfolio Trading System
## Machine: ThinkPad T14 i7 32GB RAM | Alias: destroyer | Network: tailscale
## Date: 2026-05-27 | Status: Ready for deployment

---

## Quick Start Deployment Commands

### Step 1: Clone repository on destroyer
```bash
cd /home/destroyer/trading-system || mkdir -p ~/trading-system && cd ~/trading-system
git clone /home/falcon/git/portfolio-management/trading_system .
```

### Step 2: Build Docker images
```bash
cd trading_system
docker-compose -f docker-compose.prod.yml build --no-cache
```

### Step 3: Start production stack
```bash
docker-compose -f docker-compose.prod.yml up -d
```

### Step 4: View logs and verify
```bash
docker-compose -f docker-compose.prod.yml logs -f trading-service
```

---

## Architecture Diagram

```
┌─────────────────────────────────────────────────────────────┐
│              destroyer (ThinkPad T14, 32GB RAM)             │
│  ┌─────────────────────────────────────────────────────────┐│
│  │                Production Stack                          ││
│  ├─────────────────────────────────────────────────────────┤│
│  │                                                          ││
│  │  trading-service (Python FastAPI)                        ││
│  │    ├── OnchainRuntimeService - RPC polling               ││
│  │    ├── OnchainPoller - Periodic pool polling             ││
│  │    ├── TokenMetadataPoller - ERC20 + Coingecko           ││
│  │    └── EventListenerPoller - eth_getLogs subscription    ││
│  │                                                          ││
│  │  PostgreSQL 16                                           ││
│  │    └── Schema: trading_system/alembic/migrations         ││
│  │                                                          ││
│  │  Redis 7                                                  ││
│  │    └── Event queue, pub/sub cache                        ││
│  │                                                          ││
│  │  MinIO (Object Storage)                                   ││
│  │    └── Logs, artifacts, backups                          ││
│  │                                                          ││
│  └─────────────────────────────────────────────────────────┘│
└─────────────────────────────────────────────────────────────┘
```

---

## Complete Stack Files

I'll create all deployment assets:

<tool_call>
<function=write_file>
<parameter=content>
# Production Deployment Infrastructure - Portfolio Trading System
## Machine: ThinkPad T14 i7 32GB RAM | Alias: destroyer | Network: tailscale
## Date: 2026-05-27 | Status: Ready for deployment

---

## Quick Start Deployment Commands

### Step 1: Clone repository on destroyer
```bash
cd /home/destroyer/trading-system || mkdir -p ~/trading-system && cd ~/trading-system
git clone /home/falcon/git/portfolio-management/trading_system .
```

### Step 2: Build Docker images
```bash
cd trading_system
docker-compose -f docker-compose.prod.yml build --no-cache
```

### Step 3: Start production stack
```bash
docker-compose -f docker-compose.prod.yml up -d
```

### Step 4: View logs and verify
```bash
docker-compose -f docker-compose.prod.yml logs -f trading-service
```

---

## Architecture Diagram

```
┌─────────────────────────────────────────────────────────────┐
│              destroyer (ThinkPad T14, 32GB RAM)             │
│  ┌─────────────────────────────────────────────────────────┐│
│  │                Production Stack                          ││
│  ├─────────────────────────────────────────────────────────┤│
│  │                                                          ││
│  │  trading-service (Python FastAPI)                        ││
│  │    ├── OnchainRuntimeService - RPC polling               ││
│  │    ├── OnchainPoller - Periodic pool polling             ││
│  │    ├── TokenMetadataPoller - ERC20 + Coingecko           ││
│  │    └── EventListenerPoller - eth_getLogs subscription    ││
│  │                                                          ││
│  │  PostgreSQL 16                                           ││
│  │    └── Schema: trading_system/alembic/migrations         ││
│  │                                                          ││
│  │  Redis 7                                                  ││
│  │    └── Event queue, pub/sub cache                        ││
│  │                                                          ││
│  │  MinIO (Object Storage)                                   ││
│  │    └── Logs, artifacts, backups                          ││
│  │                                                          ││
│  └─────────────────────────────────────────────────────────┘│
└─────────────────────────────────────────────────────────────┘
```

---

## Quick Start Commands

### Clone and deploy (one-command)
```bash
cd /home/destroyer/trading-system || mkdir -p ~/trading-system && cd ~/trading-system
git clone /home/falcon/git/portfolio-management/trading_system .
docker-compose -f docker-compose.prod.yml up -d --build
docker-compose -f docker-compose.prod.yml logs -f trading-service
```

### Health check
```bash
docker-compose exec trading-service python3 -c "from onchain.runtime.service import OnchainRuntimeService; print('✅ Runtime service imported successfully')"
```

### Stop and rebuild after code changes
```bash
cd /home/destroyer/trading-system
git pull origin main  # Pull latest code
docker-compose -f docker-compose.prod.yml up -d --build
docker-compose -f docker-compose.prod.yml logs -f trading-service
```
