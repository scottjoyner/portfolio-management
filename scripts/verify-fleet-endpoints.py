#!/usr/bin/env python3
"""
Multi-Service Fleet Endpoint Verification for WSL
================================================================
This script tests all health endpoints across the 4-service portfolio management fleet.
Expected Services and Endpoints:
--------------------------------
1. Portfolio Manager (port 3001) - /health endpoint at expected
2. Data Collector (port 8080) - /health endpoint at expected  
3. Backtester (port 3002) - /health endpoint at expected
4. Alerts Service (port 3003) - /health endpoint at expected

WSL Context: All services run on Docker containers with port mapping to localhost.
"""

import sys
import subprocess
import time
from datetime import datetime
from dataclasses import dataclass, field
from typing import List, Optional

@dataclass
class ServiceEndpoint:
    name: str
    port: int
    endpoint: str
    expected_response: bool = False
    
    def format(self) -> str:
        return f"{self.name} [{self.port}] {self.endpoint}"

@dataclass
class EndpointCheckResult:
    name: str
    status: str  # pending, running, error, timeout
    exit_code: Optional[int] = None
    response_text: Optional[str] = None
    elapsed_ms: float = 0.0
    
    def format(self) -> str:
        prefix = "✓" if self.status == "running" else "✗" if self.exit_code else "."
        return f"{prefix} [{self.elapsed_ms:.1f}ms] {self.name}: {self.status}"

# ============================================================================
# WSL Fleet Architecture Discovery
# ============================================================================
WSL_FLEET_ARCHITECTURE = """
Portfolio Management Multi-Service Fleet (WSL Host)
---------------------------------------------------

Service              | Port  | Endpoint  | Purpose
---------------------|-------|-----------|----------------------------------
Portfolio Manager    | 3001  | /health   | Strategy orchestration & backtest
Data Collector       | 8080  | /health   | Market data ingestion & API calls
Backtester           | 3002  | /health   | Historical performance simulation  
Alerts Service       | 3003  | /health   | Webhook notifications (email/slack)

Network Configuration:
- Services run in Docker bridge network
- Ports mapped to WSL localhost interface
- Container internal health checks use /health endpoint
- External monitoring accessible via port mappings

Health Check Requirements:
- Each container implements /health endpoint
- Returns 200 OK when service is healthy
- May return 503 Service Unavailable during initialization
- Timeout threshold: 10 seconds per endpoint

Dependencies Graph:
- backtester depends on: portfolio-manager
- alerts depends on: portfolio-manager
"""

def get_wsldocker_status() -> str:
    """Check if Docker is available on WSL."""
    try:
        result = subprocess.run(
            ['docker', 'ps', '-a'],
            capture_output=True, text=True, timeout=15
        )
        return f"Docker available: {'running containers' if 'exited' in result.stdout else 'no containers'}"
    except FileNotFoundError:
        return "Docker not installed on WSL"
    except subprocess.TimeoutExpired:
        return "Docker command timed out"

def check_health_endpoint(service_name: str, port: int, endpoint: str, timeout: int = 10) -> EndpointCheckResult:
    """Check a health endpoint."""
    start_time = time.time()
    
    try:
        url = f"http://localhost:{port}{endpoint}"
        print(f"    Checking {url}...")
        
        # Use Python requests for better error handling
        response = subprocess.run(
            ['curl', '-s', '-m', str(timeout), url],
            capture_output=True, text=True, timeout=timeout + 5
        )
        
        elapsed_ms = (time.time() - start_time) * 1000
        
        if response.returncode == 0:
            return EndpointCheckResult(
                name=f"{service_name} {endpoint}",
                status="running",
                exit_code=response.returncode,
                response_text=response.stdout[:200],
                elapsed_ms=elapsed_ms
            )
        else:
            return EndpointCheckResult(
                name=f"{service_name} {endpoint}",
                status="error",
                exit_code=response.returncode,
                response_text=response.stderr[:200] if response.stderr else None,
                elapsed_ms=elapsed_ms
            )
            
    except subprocess.TimeoutExpired:
        return EndpointCheckResult(
            name=f"{service_name} {endpoint}",
            status="timeout",
            elapsed_ms=(time.time() - start_time) * 1000
        )

def get_docker_logs(container_name: str, recent_lines: int = 50) -> Optional[str]:
    """Get recent Docker container logs."""
    try:
        result = subprocess.run(
            ['docker', 'logs', '--tail', str(recent_lines), container_name],
            capture_output=True, text=True, timeout=15
        )
        return result.stdout if result.stdout.strip() else None
    except FileNotFoundError:
        return None

def get_container_status(container_name: str) -> Optional[str]:
    """Check if a Docker container is running."""
    try:
        result = subprocess.run(
            ['docker', 'ps', '-a', '--format', '{{.Status}}', container_name],
            capture_output=True, text=True, timeout=15
        )
        return result.stdout.strip()
    except FileNotFoundError:
        return None

# ============================================================================
# Main Verification Function
# ============================================================================
def verify_wsldocker_fleet() -> dict:
    """Verify all endpoints in the WSL Docker fleet."""
    
    print("=" * 80)
    print("WSL FLEET ENDPOINT VERIFICATION")
    print("=" * 80)
    print(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()
    
    # Print fleet architecture
    print(WSL_FLEET_ARCHITECTURE)
    print()
    
    # Check Docker availability
    print("--- Step 1: Environment Check ---")
    docker_status = get_wsldocker_status()
    print(f"{docker_status}")
    print()
    
    if "not installed" in docker_status or "not available" in docker_status.lower():
        print("\n⚠️  NOTE: Docker is not running on this WSL instance.")
        print("     Expected endpoints for fleet deployment:")
        endpoints = [
            ("Portfolio Manager", 3001, "/health"),
            ("Data Collector", 8080, "/health"),
            ("Backtester", 3002, "/health"),
            ("Alerts Service", 3003, "/health")
        ]
        
        print("\nEndpoint Summary:")
        for name, port, endpoint in endpoints:
            print(f"    {name} @ http://localhost:{port}{endpoint}")
        
        return {
            "docker_available": False,
            "status": "not_installed",
            "message": "Docker is not installed or available on WSL",
            "expected_endpoints": endpoints
        }
    
    # Fleet running with Docker
    print("--- Step 2: Checking Container Status ---")
    
    containers = ["portfolio_manager", "data_collector", "backtester", "alerts"]
    container_status = {}
    
    for container in containers:
        status = get_container_status(container)
        container_status[container] = status
        
        if status:
            print(f"    ✅ {container}: {status}")
        else:
            print(f"    📦 {container}: not running (will start on demand)")
    
    # Check endpoints
    print("\n--- Step 3: Endpoint Health Checks ---")
    
    services = [
        ("Portfolio Manager", 3001, "/health"),
        ("Data Collector", 8080, "/health"),
        ("Backtester", 3002, "/health"),
        ("Alerts Service", 3003, "/health")
    ]
    
    results = []
    for name, port, endpoint in services:
        check_result = check_health_endpoint(name, port, endpoint)
        results.append(check_result)
        print(f"\n{check_result.format()}")
        
        if check_result.response_text:
            print(f"    Response: {check_result.response_text[:150]}...")
    
    # Summary
    print("\n" + "=" * 80)
    print("VERIFICATION SUMMARY")
    print("=" * 80)
    
    running = sum(1 for r in results if r.status == "running")
    errors = sum(1 for r in results if r.exit_code)
    timeouts = sum(1 for r in results if r.status == "timeout")
    
    print(f"\nTotal Checks: {len(results)}")
    print(f"Running: {running}")
    print(f"Errors: {errors}")
    print(f"Timeouts: {timeouts}")
    
    return {
        "docker_available": True,
        "status": "complete",
        "results": results,
        "summary": {
            "running": running,
            "errors": errors,
            "timeouts": timeouts,
            "total": len(results)
        },
        "expected_endpoints": services
    }

def print_fleet_architecture() -> None:
    """Display fleet architecture for documentation."""
    print("=" * 80)
    print("WSL FLEET ARCHITECTURE DOCUMENTATION")
    print("=" * 80)
    
    print("\nFleets are deployed using Docker Compose with multi-service architecture.")
    print()
    
    services = [
        {
            "name": "Portfolio Manager",
            "port": 3001,
            "purpose": "Strategy orchestration and backtest execution",
            "health_endpoint": "/health"
        },
        {
            "name": "Data Collector", 
            "port": 8080,
            "purpose": "Market data ingestion from exchanges (Coinbase, Alpaca)",
            "health_endpoint": "/health"
        },
        {
            "name": "Backtester",
            "port": 3002,
            "purpose": "Historical performance simulation and strategy validation",
            "health_endpoint": "/health"
        },
        {
            "name": "Alerts Service",
            "port": 3003,
            "purpose": "Webhook notifications via email/Slack/Push",
            "health_endpoint": "/health"
        }
    ]
    
    print("Services and Endpoints:")
    print("-" * 80)
    for svc in services:
        print(f"  {svc['name']}")
        print(f"    Port: {svc['port']}")
        print(f"    Health Endpoint: http://localhost:{svc['port']}{svc['health_endpoint']}")
        print(f"    Purpose: {svc['purpose']}")
        print()
    
    print("Endpoint Verification:")
    print("-" * 80)
    for svc in services:
        expected = "✓ Expected" if svc["name"] != "Alerts Service" else "⚠️ Optional"
        print(f"  {svc['name']}: /health ({expected})")
    
    print("\nHealth Check Behavior:")
    print("  - Returns 200 OK when service is healthy and ready")
    print("  - May return 503 Service Unavailable during startup (normal)")
    print("  - Timeout after 10 seconds if endpoint unresponsive")
    print("  - Each container has healthcheck configuration in docker-compose.yml")

if __name__ == "__main__":
    # Check if script should just display architecture or run verification
    if len(sys.argv) > 1 and sys.argv[1] == "--help":
        print("Usage: fleet_verify.py [--help]")
        print("\nOptions:")
        print("  --help    Display fleet architecture without running checks")
        print()
        print_fleet_architecture()
    else:
        # Run verification
        result = verify_wsldocker_fleet()
        
        # Exit with appropriate code
        if "not_installed" in result.get("status", ""):
            sys.exit(0)  # Not an error, just Docker not installed
        
        errors = result["summary"]["errors"] + result["summary"]["timeouts"]
        if errors > 0 and result["docker_available"]:
            print(f"\n⚠️  Found {errors} endpoint issues. Services may need attention.")
        
        sys.exit(0)  # Always exit 0 for informational purposes
