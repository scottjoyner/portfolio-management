from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]


def test_trading_dockerfile_declares_safe_defaults():
    dockerfile = REPO_ROOT / "Dockerfile.trading"
    assert dockerfile.exists()
    text = dockerfile.read_text()
    assert "LIVE_TRADING_ENABLED=false" in text
    assert "TRADING_MODE=paper" in text
    assert "@coinbase/coinbase-cli" in text
    assert "PYTHONPATH=/app" in text


def test_trading_compose_has_read_only_services_and_secret():
    compose = REPO_ROOT / "docker-compose.trading.yml"
    assert compose.exists()
    text = compose.read_text()
    for service in ["trading-api", "trading-ui", "trading-worker", "redis", "postgres"]:
        assert service in text
    assert "coinbase_cdp_api_key" in text
    assert "LIVE_TRADING_ENABLED: \"false\"" in text
    assert "TRADING_MODE: paper" in text
