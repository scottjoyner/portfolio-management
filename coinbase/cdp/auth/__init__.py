"""
Coinbase CDP Authentication Module

Provides comprehensive authentication functionality for CDP APIs including:
- JWT token generation
- API key management  
- Secure authentication patterns
- Environment-based configuration
"""

from typing import Optional, Dict, List
import os
import json
import time
import subprocess
from datetime import datetime
from pathlib import Path


class CDPAuthenticationError(Exception):
    """Exception for authentication errors"""
    pass


class MockAuthClient:
    """Mock authentication client for development and maintenance windows"""
    
    def __init__(self):
        self.mock_mode = True
    
    def authenticate(self, grant_type: str, **kwargs) -> Dict:
        """Mock authentication response"""
        return {
            "access_token": "mock_jwt_token_for_development",
            "token_type": "Bearer",
            "expires_in": 3600,
            "scope": "cdp.wallet cdp.onramp"
        }
    
    def validate_token(self, token: str) -> bool:
        """Validate authentication token"""
        return True


class CDPAuthentication:
    """
    Coinbase CDP Authentication Manager
    
    Handles all authentication aspects:
    - JWT token generation and validation
    - API key creation and rotation
    - Environment-based credential management
    - Security best practices implementation
    """
    
    def __init__(self, mock_mode: bool = False):
        self.mock_mode = mock_mode
        self.authenticated = False
        self.token: Optional[str] = None
        self.scopes: List[str] = []
        
        # Common CDP environments
        self.environments = {
            "testnet": "https://api.testnet.cdp.coinbase.com",
            "mainnet": "https://api.cdp.coinbase.com"
        }
    
    def _check_authentication_required(self, operation: str) -> bool:
        """Check if operation requires authentication"""
        auth_required_operations = [
            "wallet", "onramp", "paymaster", "webhook", 
            "transfer", "balance", "transaction"
        ]
        return any(op in operation.lower() for op in auth_required_operations)
    
    def authenticate(self, account_id: str, scopes: List[str], 
                    environment: str = "testnet") -> Dict[str, Any]:
        """
        Authenticate and obtain access tokens
        
        Args:
            account_id: CDP account identifier
            scopes: List of API scopes to request
            environment: Testnet or mainnet
            
        Returns:
            Authentication response with token
        """
        if self.mock_mode:
            return {
                "success": True,
                "environment": environment,
                "token_type": "Bearer",
                "access_token": f"mock_{account_id[:8]}_jwt_token",
                "expires_in": 3600,
                "scope": " ".join(scopes),
                "mock": True
            }
        
        # In production, use CDP CLI or SDK
        scopes_str = " ".join(scopes)
        
        try:
            result = subprocess.run(
                ["cdp", "auth", "jwt", "--account-id", account_id, f"--scopes='{scopes_str}'"],
                capture_output=True,
                text=True,
                timeout=30
            )
            
            if result.returncode == 0:
                output = json.loads(result.stdout) if result.stdout else {}
                self.token = output.get("access_token")
                self.scopes = scopes
                return {
                    "success": True,
                    "environment": environment,
                    "token_type": "Bearer",
                    **output,
                    "mock": False
                }
            else:
                raise CDPAuthenticationError(result.stderr)
                
        except FileNotFoundError:
            print("CDP CLI not installed. Install with: pip install cdp-cli")
            return {
                "success": False,
                "error": "CDP CLI not installed",
                "suggestion": "Install CDP CLI or use mock_mode=True for development"
            }
        except subprocess.TimeoutExpired:
            raise CDPAuthenticationError("Authentication timed out")
    
    def generate_jwt(self, account_id: str, scopes: List[str], 
                    environment: str = "testnet") -> Dict[str, Any]:
        """Generate JWT token for server-side API access
        
        Args:
            account_id: CDP account identifier  
            scopes: Required API scopes
            environment: Deployment environment
            
        Returns:
            JWT token response
        """
        result = self.authenticate(account_id, scopes, environment)
        
        if result.get("success"):
            self.token = result.get("access_token")
            self.authenticated = True
        
        return result
    
    def create_api_keys(self, account_id: str, environment: str = "testnet",
                       description: Optional[str] = None) -> Dict[str, Any]:
        """Create API keys for programmatic access
        
        Args:
            account_id: CDP account identifier
            environment: Testnet or mainnet
            description: Key description (optional)
            
        Returns:
            API keys response
        """
        if self.mock_mode:
            return {
                "success": True,
                "account_id": account_id,
                "environment": environment,
                "api_key_id": f"mock_{account_id[:6]}_key",
                "testnet_key": "cdp_testnet_m00k_k3y_xxx",
                "mainnet_key": None,  # Will be empty until requested
                "mock": True
            }
        
        try:
            result = subprocess.run(
                ["cdp", "auth", "api-keys", "--account-id", account_id, f"--environment={environment}"],
                capture_output=True,
                text=True,
                timeout=60
            )
            
            if result.returncode == 0:
                return {
                    "success": True,
                    "output": result.stdout.strip()
                }
            else:
                raise CDPAuthenticationError(result.stderr)
                
        except FileNotFoundError:
            print("CDP CLI not installed")
            return {
                "success": False,
                "error": "CDP CLI not installed"
            }
    
    def rotate_api_keys(self, account_id: str, old_key: str, 
                       environment: str = "testnet") -> Dict[str, Any]:
        """Rotate API keys for security
        
        Args:
            account_id: CDP account identifier
            old_key: Existing API key to rotate
            environment: Environment to rotate in
            
        Returns:
            Key rotation response
        """
        if self.mock_mode:
            return {
                "success": True,
                "account_id": account_id,
                "environment": environment,
                "new_key_id": f"rotated_{account_id[:6]}_key",
                "mock": True
            }
        
        try:
            result = subprocess.run(
                ["cdp", "auth", "api-keys", "rotate", "--account-id", account_id, 
                 f"--old-key={old_key}", f"--environment={environment}"],
                capture_output=True,
                text=True,
                timeout=60
            )
            
            if result.returncode == 0:
                return {
                    "success": True,
                    "output": result.stdout.strip()
                }
            else:
                raise CDPAuthenticationError(result.stderr)
                
        except FileNotFoundError:
            print("CDP CLI not installed")
            return {"success": False, "error": "CDP CLI not installed"}
    
    def validate_token(self, token: str) -> Dict[str, Any]:
        """Validate JWT or bearer token"""
        if self.mock_mode:
            return {
                "valid": True,
                "token_type": "Bearer",
                "mock": True
            }
        
        # Token validation logic would use CDP APIs
        try:
            result = subprocess.run(
                ["cdp", "auth", "validate", "--token", token],
                capture_output=True,
                text=True,
                timeout=30
            )
            
            if result.returncode == 0:
                return {
                    "valid": True,
                    "output": json.loads(result.stdout) if result.stdout else {}
                }
            else:
                raise CDPAuthenticationError("Token validation failed")
                
        except FileNotFoundError:
            return {
                "valid": False,
                "error": "CDP CLI not installed",
                "mock_mode": self.mock_mode
            }
    
    def get_token_expiry(self, token: str) -> Optional[int]:
        """Get remaining token expiry time in seconds"""
        if self.mock_mode:
            return 3500  # Mock expiry
        
        try:
            result = subprocess.run(
                ["cdp", "auth", "token", "--token", token, "--format=json"],
                capture_output=True,
                text=True,
                timeout=30
            )
            
            if result.returncode == 0:
                data = json.loads(result.stdout)
                return int(data.get("exp", 3600)) - int(time.time())
            else:
                return None
                
        except Exception:
            return None
    
    def sanitize_credentials(self, credentials_dict: Dict[str, str]) -> Dict[str, str]:
        """
        Sanitize credentials for safe logging
        
        Args:
            credentials_dict: Dictionary containing potentially sensitive data
            
        Returns:
            Dictionary with masked sensitive fields
        """
        sanitized = {}
        
        # Fields to mask
        sensitive_fields = ['key', 'secret', 'password', 'token', 'api_key']
        
        for key, value in credentials_dict.items():
            if any(field in key.lower() for field in sensitive_fields):
                # Mask the value
                sanitized[key] = f"{'*' * len(value)}"
            else:
                sanitized[key] = value
        
        return sanitized
    
    def load_credentials_from_env(self, env_name: str = "CDP") -> Dict[str, str]:
        """
        Load credentials from environment variables safely
        
        Args:
            env_name: Environment variable prefix (default: CDP)
            
        Returns:
            Dictionary of loaded credentials
        """
        credentials = {}
        
        # Common CDP credential patterns
        credential_patterns = [
            f"{env_name}_ACCOUNT_ID",
            f"{env_name}_API_KEY", 
            f"{env_name}_SECRET_KEY",
            f"{env_name}_TESTNET"
        ]
        
        for pattern in credential_patterns:
            value = os.getenv(pattern)
            if value:
                credentials[pattern] = value
        
        return credentials
    
    def save_credentials_securely(self, account_id: str, api_key: str, 
                                 key_file_path: Optional[str] = None) -> Dict[str, Any]:
        """
        Save credentials securely to a file
        
        Args:
            account_id: CDP account identifier
            api_key: API key to save
            key_file_path: Path to save credentials
            
        Returns:
            Result of save operation
        """
        if not key_file_path:
            # Default location in user home
            key_file_path = str(Path.home() / ".cdp" / f"{account_id}_credentials.json")
        
        # Create parent directory
        Path(key_file_path).parent.mkdir(parents=True, exist_ok=True)
        
        credentials = {
            "account_id": account_id,
            "api_key_id": api_key[:8],  # Store ID, not the key itself
            "created_at": datetime.now().isoformat(),
            "environment": os.getenv("CDP_ENVIRONMENT", "testnet")
        }
        
        # In production, use proper encryption for credential storage
        try:
            with open(key_file_path, 'w') as f:
                json.dump(credentials, f)
            
            # Set restrictive permissions
            os.chmod(key_file_path, 0o600)
            
            return {
                "success": True,
                "file_path": key_file_path,
                "permissions": "0600"
            }
        except Exception as e:
            raise CDPAuthenticationError(f"Failed to save credentials: {e}")
    
    def check_auth_status(self) -> Dict[str, Any]:
        """Check current authentication status"""
        return {
            "authenticated": self.authenticated,
            "token_type": "Bearer" if self.token else None,
            "scopes": self.scopes,
            "mock_mode": self.mock_mode
        }
    
    def get_scope_permissions(self, scope: str) -> List[str]:
        """Get detailed permissions for a scope"""
        scope_permissions = {
            "cdp.wallet": ["wallet:create", "wallet:read", "wallet:update", "wallet:delete"],
            "cdp.onramp": ["onramp:submit", "onramp:status"],
            "cdp.paymaster": ["paymaster:subscribe", "paymaster:cancel"],
            "cdp.x402": ["x402:create", "x402:read"],
            "cdp.webhook": ["webhook:subscribe", "webhook:unsubscribe"]
        }
        
        return scope_permissions.get(scope, [])
    
    def authorize_operation(self, operation: str) -> bool:
        """Check if authenticated user can perform operation"""
        if not self.authenticated:
            return False
        
        # Operations requiring authentication
        auth_required = [
            "wallet.balance", "wallet.transfer", "onramp.submit",
            "paymaster.sponsor", "webhook.subscribe"
        ]
        
        return any(op in operation for op in auth_required)


# ==================== EXAMPLE USAGE ====================

if __name__ == "__main__":
    # Example 1: Basic authentication
    auth = CDPAuthentication(mock_mode=True)
    
    print("Generating JWT token...")
    result = auth.generate_jwt(
        account_id="account_test_123",
        scopes=["cdp.wallet", "cdp.onramp"],
        environment="testnet"
    )
    
    print(f"Authentication result: {result}")
    
    # Example 2: Create API keys
    print("\nCreating API keys...")
    api_result = auth.create_api_keys(
        account_id="account_test_123",
        environment="testnet",
        description="Trading bot access"
    )
    
    print(f"API keys result: {api_result}")
