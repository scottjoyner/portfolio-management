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


def test_production_compose_declares_supervised_paper_controls():
    compose = REPO_ROOT / "docker-compose.production.yml"
    assert compose.exists()
    text = compose.read_text()

    for service in [
        "postgres:",
        "migrate:",
        "api:",
        "economic-worker:",
        "postgres-backup:",
    ]:
        assert service in text

    assert 'OPERATOR_STORE: postgres' in text
    assert 'OPERATOR_AUTH_REQUIRED: "true"' in text
    assert 'CSRF_REQUIRED: "true"' in text
    assert 'MODE: paper' in text
    assert 'PAPER_TRADING: "true"' in text
    assert 'LIVE_TRADING: "false"' in text
    assert 'LIVE_TRADING_ENABLED: "false"' in text
    assert 'COINBASE_DRY_RUN: "true"' in text
    assert 'REMOTE_LLM_EXECUTION_ENABLED: "false"' in text
    assert 'LOCAL_LLM_EXECUTION_REQUIRED:' in text
    assert 'read_only: true' in text
    assert 'cap_drop:' in text
    assert 'no-new-privileges:true' in text
