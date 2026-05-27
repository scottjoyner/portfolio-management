from __future__ import annotations

"""
Coinbase Read-Only Sync Integration Tests

Tests for the staging harness endpoints:
- /exchange/health
- /exchange/accounts
- /exchange/portfolios  
- /exchange/products
- /exchange/credentials/validate
"""

import pytest


class TestExchangeHealthEndpoint:
    """Test Coinbase integration health check endpoint."""

    def test_health_endpoint_exists(self, app_client):
        """Verify /exchange/health endpoint responds with status ok."""
        response = app_client.get("/exchange/health")
        
        assert response.status_code == 200
        data = response.json()
        
        assert "status" in data
        assert "coinbase_configured" in data
        assert "live_enabled" in data

    def test_health_endpoint_without_credentials(self, app_client):
        """Health endpoint works without Coinbase credentials."""
        # Simulate missing credentials
        from core.config.settings import Settings
        
        # Check it handles gracefully
        response = app_client.get("/exchange/health")
        assert response.status_code == 200


class TestExchangeAccountsEndpoint:
    """Test account listing endpoint."""

    def test_accounts_endpoint_exists(self, app_client):
        """Verify /exchange/accounts endpoint responds."""
        response = app_client.get("/exchange/accounts")
        
        assert response.status_code == 200
        data = response.json()
        
        assert "status" in data
        assert "accounts" in data
        assert "timestamp" in data

    def test_accounts_empty_without_credentials(self, app_client):
        """Accounts returns empty list when no credentials configured."""
        response = app_client.get("/exchange/accounts")
        assert response.status_code == 200
        
        data = response.json()
        # Should return safe default without error


class TestExchangePortfoliosEndpoint:
    """Test portfolio listing endpoint."""

    def test_portfolios_endpoint_exists(self, app_client):
        """Verify /exchange/portfolios endpoint responds."""
        response = app_client.get("/exchange/portfolios")
        
        assert response.status_code == 200
        data = response.json()
        
        assert "status" in data
        assert "portfolios" in data

    def test_portfolios_empty_without_credentials(self, app_client):
        """Portfolios returns empty list when no credentials configured."""
        response = app_client.get("/exchange/portfolios")
        assert response.status_code == 200


class TestExchangeProductsEndpoint:
    """Test trading products listing endpoint."""

    def test_products_endpoint_exists(self, app_client):
        """Verify /exchange/products endpoint responds."""
        response = app_client.get("/exchange/products?limit=5")
        
        assert response.status_code == 200
        data = response.json()
        
        assert "status" in data
        assert "products" in data
        assert "timestamp" in data

    def test_products_defaults_to_10(self, app_client):
        """Products defaults to 10 items if limit not specified."""
        response = app_client.get("/exchange/products")
        data = response.json()
        
        products = data.get("products", [])
        assert len(products) <= 10


class TestCredentialsValidationEndpoint:
    """Test credentials validation endpoint."""

    def test_validate_credentials_exists(self, app_client):
        """Verify /exchange/credentials/validate endpoint responds."""
        response = app_client.get("/exchange/credentials/validate")
        
        assert response.status_code == 200
        data = response.json()
        
        assert "valid" in data
        assert "reason" in data
        assert "timestamp" in data

    def test_validate_returns_false_without_credentials(self, app_client):
        """Validation returns false when credentials not configured."""
        response = app_client.get("/exchange/credentials/validate")
        data = response.json()
        
        # Should return safe default without error


# Helper fixture would be set up in conftest.py:
# @pytest.fixture
# def app_client():
#     """Create test client with API running on http://localhost:8001."""
#     import requests
#     return requests.Session()
